package exchange.core2.tests.examples;

import exchange.core2.core.ExchangeApi;
import exchange.core2.core.ExchangeCore;
import exchange.core2.core.IEventsHandler;
import exchange.core2.core.SimpleEventsProcessor;
import exchange.core2.core.common.*;
import exchange.core2.core.common.api.*;
import exchange.core2.core.common.api.binary.BatchAddSymbolsCommand;
import exchange.core2.core.common.api.reports.*;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.config.*;
import exchange.core2.tests.util.ExchangeTestContainer;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.hamcrest.core.Is;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public class ITCoreExample {

    @Test
    public void sampleTest() throws Exception {

        // simple async events handler
        SimpleEventsProcessor eventsProcessor = new SimpleEventsProcessor(new IEventsHandler() {
            @Override
            public void tradeEvent(TradeEvent tradeEvent) {
                System.out.println("Trade event: " + tradeEvent);
            }

            @Override
            public void reduceEvent(ReduceEvent reduceEvent) {
                System.out.println("Reduce event: " + reduceEvent);
            }

            @Override
            public void rejectEvent(RejectEvent rejectEvent) {
                System.out.println("Reject event: " + rejectEvent);
            }

            @Override
            public void commandResult(ApiCommandResult commandResult) {
                System.out.println("Command result: " + commandResult);
            }

            @Override
            public void orderBook(OrderBook orderBook) {
                System.out.println("OrderBook event: " + orderBook);
            }
        });

        final String exchangeId = "ex01";
        final InitialStateConfiguration firstStartConfig = InitialStateConfiguration.cleanStart(exchangeId);

        ExchangeConfiguration conf = ExchangeConfiguration.defaultBuilder()
                .initStateCfg(firstStartConfig)
                .performanceCfg(PerformanceConfiguration.DEFAULT)
                .reportsQueriesCfg(ReportsQueriesConfiguration.DEFAULT)
                .ordersProcessingCfg(OrdersProcessingConfiguration.DEFAULT)
                .loggingCfg(LoggingConfiguration.DEFAULT)
                .serializationCfg(SerializationConfiguration.DISK_SNAPSHOT_ONLY)
                .build();

        // default exchange configuration
        //ExchangeConfiguration conf = ExchangeConfiguration.defaultBuilder().build();

        // build exchange core
        ExchangeCore exchangeCore = ExchangeCore.builder()
                .resultsConsumer(eventsProcessor)
                .exchangeConfiguration(conf)
                .build();

        // start up disruptor threads
        exchangeCore.startup();

        // get exchange API for publishing commands
        ExchangeApi api = exchangeCore.getApi();

        // currency code constants
        final int currencyCodeXbt = 11;
        final int currencyCodeLtc = 15;

        // symbol constants
        final int symbolXbtLtc = 241;

        Future<CommandResultCode> future;

        // create symbol specification and publish it
        CoreSymbolSpecification symbolSpecXbtLtc = CoreSymbolSpecification.builder()
                .symbolId(symbolXbtLtc)         // symbol id
                .type(SymbolType.CURRENCY_EXCHANGE_PAIR)
                .baseCurrency(currencyCodeXbt)    // base = satoshi (1E-8)
                .quoteCurrency(currencyCodeLtc)   // quote = litoshi (1E-8)
                .baseScaleK(1_000_000L) // 1 lot = 1M satoshi (0.01 BTC)
                .quoteScaleK(10_000L)   // 1 price step = 10K litoshi
                .takerFee(1900L)        // taker fee 1900 litoshi per 1 lot
                .makerFee(700L)         // maker fee 700 litoshi per 1 lot
                .build();

        future = api.submitBinaryDataAsync(new BatchAddSymbolsCommand(symbolSpecXbtLtc));
        System.out.println("BatchAddSymbolsCommand result: " + future.get());


        // create user uid=301
        future = api.submitCommandAsync(ApiAddUser.builder()
                .uid(301L)
                .build());

        System.out.println("ApiAddUser 1 result: " + future.get());


        // create user uid=302
        future = api.submitCommandAsync(ApiAddUser.builder()
                .uid(302L)
                .build());

        System.out.println("ApiAddUser 2 result: " + future.get());

        // first user deposits 20 LTC
        future = api.submitCommandAsync(ApiAdjustUserBalance.builder()
                .uid(301L)
                .currency(currencyCodeLtc)
                .amount(2_000_000_000L)
                .transactionId(1L)
                .build());

        System.out.println("ApiAdjustUserBalance 1 result: " + future.get());


        // second user deposits 0.10 BTC
        future = api.submitCommandAsync(ApiAdjustUserBalance.builder()
                .uid(302L)
                .currency(currencyCodeXbt)
                .amount(10_000_000L)
                .transactionId(2L)
                .build());

        System.out.println("ApiAdjustUserBalance 2 result: " + future.get());


        // first user places Good-till-Cancel Bid order
        // he assumes BTCLTC exchange rate 154 LTC for 1 BTC
        // bid price for 1 lot (0.01BTC) is 1.54 LTC => 1_5400_0000 litoshi => 10K * 15_400 (in price steps)
        future = api.submitCommandAsync(ApiPlaceOrder.builder()
                .uid(301L)
                .orderId(5001L)
                .price(15_400L)
                .reservePrice(15_600L) // can move bid order up to the 1.56 LTC, without replacing it
                .size(12L) // order size is 12 lots
                .action(OrderAction.BID)
                .orderType(OrderType.GTC) // Good-till-Cancel
                .symbol(symbolXbtLtc)
                .orderTakerFee(1900L)
                .orderMakerFee(700L)
                .build());

        System.out.println("ApiPlaceOrder 1 result: " + future.get());

        final long stateId = System.currentTimeMillis() * 1000;
        final ApiPersistState apiPersistState = ApiPersistState.builder().dumpId(stateId).build();
        final CommandResultCode resultCode = api.submitCommandAsync(apiPersistState).get();
        assertThat(resultCode, Is.is(CommandResultCode.SUCCESS));

        AtomicInteger uniqueIdCounterInt = new AtomicInteger();
        final long originalFinalStateHash = api.processReport(new StateHashReportQuery(), uniqueIdCounterInt.incrementAndGet()).get().getStateHash();

        final long snapshotBaseSeq = 0L;

        final InitialStateConfiguration fromSnapshotConfig = InitialStateConfiguration.fromSnapshotOnly(exchangeId, stateId, snapshotBaseSeq);

        log.debug("Creating new exchange from persisted state...");
        conf = ExchangeConfiguration.defaultBuilder()
                .initStateCfg(fromSnapshotConfig)
                .performanceCfg(PerformanceConfiguration.DEFAULT)
                .reportsQueriesCfg(ReportsQueriesConfiguration.DEFAULT)
                .ordersProcessingCfg(OrdersProcessingConfiguration.DEFAULT)
                .loggingCfg(LoggingConfiguration.DEFAULT)
                .serializationCfg(SerializationConfiguration.DISK_SNAPSHOT_ONLY)
                .build();

        ExchangeCore reCreatedExchangeCore = ExchangeCore.builder()
                .resultsConsumer(eventsProcessor)
                .exchangeConfiguration(conf)
                .build();

        // start up disruptor threads
        reCreatedExchangeCore.startup();
        api = reCreatedExchangeCore.getApi();
        final long recreatedFinalStateHash = api.processReport(new StateHashReportQuery(), uniqueIdCounterInt.incrementAndGet()).get().getStateHash();
        assertThat(recreatedFinalStateHash, is(originalFinalStateHash));

        // second user places Immediate-or-Cancel Ask (Sell) order
        // he assumes wost rate to sell 152.5 LTC for 1 BTC
        future = api.submitCommandAsync(ApiPlaceOrder.builder()
                .uid(302L)
                .orderId(5002L)
                .price(15_250L)
                .size(10L) // order size is 10 lots
                .action(OrderAction.ASK)
                .orderType(OrderType.IOC) // Immediate-or-Cancel
                .symbol(symbolXbtLtc)
                .orderTakerFee(1900L)
                .orderMakerFee(700L)
                .build());

        System.out.println("ApiPlaceOrder 2 result: " + future.get());


        // request order book
        CompletableFuture<L2MarketData> orderBookFuture = api.requestOrderBookAsync(symbolXbtLtc, 10);
        System.out.println("ApiOrderBookRequest result: " + orderBookFuture.get());


        // first user moves remaining order to price 1.53 LTC
        future = api.submitCommandAsync(ApiMoveOrder.builder()
                .uid(301L)
                .orderId(5001L)
                .newPrice(15_300L)
                .symbol(symbolXbtLtc)
                .build());

        System.out.println("ApiMoveOrder 2 result: " + future.get());

        // first user cancel remaining order
        future = api.submitCommandAsync(ApiCancelOrder.builder()
                .uid(301L)
                .orderId(5001L)
                .symbol(symbolXbtLtc)
                .build());

        System.out.println("ApiCancelOrder 2 result: " + future.get());

        // check balances
        Future<SingleUserReportResult> report1 = api.processReport(new SingleUserReportQuery(301), 0);
        System.out.println("SingleUserReportQuery 1 accounts: " + report1.get().getAccounts());

        Future<SingleUserReportResult> report2 = api.processReport(new SingleUserReportQuery(302), 0);
        System.out.println("SingleUserReportQuery 2 accounts: " + report2.get().getAccounts());

        // first user withdraws 0.10 BTC
        future = api.submitCommandAsync(ApiAdjustUserBalance.builder()
                .uid(301L)
                .currency(currencyCodeXbt)
                .amount(-10_000_000L)
                .transactionId(3L)
                .build());

        System.out.println("ApiAdjustUserBalance 1 result: " + future.get());

        // check fees collected
        Future<TotalCurrencyBalanceReportResult> totalsReport = api.processReport(new TotalCurrencyBalanceReportQuery(), 0);
        System.out.println("LTC fees collected: " + totalsReport.get().getFees().get(currencyCodeLtc));

    }

    @Test
    public void sampleTestFeeSpecificOrder() throws Exception {

        // simple async events handler
        SimpleEventsProcessor eventsProcessor = new SimpleEventsProcessor(new IEventsHandler() {
            @Override
            public void tradeEvent(TradeEvent tradeEvent) {
                System.out.println("Trade event: " + tradeEvent);
            }

            @Override
            public void reduceEvent(ReduceEvent reduceEvent) {
                System.out.println("Reduce event: " + reduceEvent);
            }

            @Override
            public void rejectEvent(RejectEvent rejectEvent) {
                System.out.println("Reject event: " + rejectEvent);
            }

            @Override
            public void commandResult(ApiCommandResult commandResult) {
                System.out.println("Command result: " + commandResult);
            }

            @Override
            public void orderBook(OrderBook orderBook) {
                System.out.println("OrderBook event: " + orderBook);
            }
        });

        final String exchangeId = "ex01";
        final InitialStateConfiguration firstStartConfig = InitialStateConfiguration.cleanStartJournaling(exchangeId);

        ExchangeConfiguration exchangeConfiguration = ExchangeConfiguration.defaultBuilder()
                .initStateCfg(firstStartConfig)
                .performanceCfg(PerformanceConfiguration.DEFAULT)
                .reportsQueriesCfg(ReportsQueriesConfiguration.createStandardConfig())
                .ordersProcessingCfg(OrdersProcessingConfiguration.DEFAULT)
                .loggingCfg(LoggingConfiguration.DEFAULT)
                .serializationCfg(SerializationConfiguration.DISK_SNAPSHOT_ONLY)
                .build();
        // default exchange configuration
        //ExchangeConfiguration conf = ExchangeConfiguration.defaultBuilder().build();

        // build exchange core
        ExchangeCore exchangeCore = ExchangeCore.builder()
                .resultsConsumer(eventsProcessor)
                .exchangeConfiguration(exchangeConfiguration)
                .build();

        // start up disruptor threads
        exchangeCore.startup();

        // get exchange API for publishing commands
        ExchangeApi api = exchangeCore.getApi();

        // currency code constants
        final int currencyCodeXbt = 11;
        final int currencyCodeLtc = 15;

        // symbol constants
        final int symbolXbtLtc = 241;

        Future<CommandResultCode> future;

        // create symbol specification and publish it
        CoreSymbolSpecification symbolSpecXbtLtc = CoreSymbolSpecification.builder()
                .symbolId(symbolXbtLtc)         // symbol id
                .type(SymbolType.CURRENCY_EXCHANGE_PAIR)
                .baseCurrency(currencyCodeXbt)    // base = satoshi (1E-8)
                .quoteCurrency(currencyCodeLtc)   // quote = litoshi (1E-8)
                .baseScaleK(1_000_000L) // 1 lot = 1M satoshi (0.01 BTC)
                .quoteScaleK(10_000L)   // 1 price step = 10K litoshi
                .takerFee(0L)        // taker fee 1900 litoshi per 1 lot
                .makerFee(0L)         // maker fee 700 litoshi per 1 lot
                .build();

        future = api.submitBinaryDataAsync(new BatchAddSymbolsCommand(symbolSpecXbtLtc));
        System.out.println("BatchAddSymbolsCommand result: " + future.get());


        // create user uid=301
        future = api.submitCommandAsync(ApiAddUser.builder()
                .uid(301L)
                .build());

        System.out.println("ApiAddUser 1 result: " + future.get());


        // create user uid=302
        future = api.submitCommandAsync(ApiAddUser.builder()
                .uid(302L)
                .build());

        System.out.println("ApiAddUser 2 result: " + future.get());

        // first user deposits 60 LTC
        future = api.submitCommandAsync(ApiAdjustUserBalance.builder()
                .uid(301L)
                .currency(currencyCodeLtc)
                .amount(6_000_000_000L)
                .transactionId(1L)
                .build());

        System.out.println("ApiAdjustUserBalance 1 result: " + future.get());


        // second user deposits 0.30 BTC
        future = api.submitCommandAsync(ApiAdjustUserBalance.builder()
                .uid(302L)
                .currency(currencyCodeXbt)
                .amount(30_000_000L)
                .transactionId(2L)
                .build());

        System.out.println("ApiAdjustUserBalance 2 result: " + future.get());

        // request order book
        CompletableFuture<L2MarketData> orderBookFuture = api.requestOrderBookAsync(symbolXbtLtc, 10);
        System.out.println("ApiOrderBookRequest result: " + orderBookFuture.get());

        // first user places Good-till-Cancel Bid order
        // he assumes BTCLTC exchange rate 154 LTC for 1 BTC
        // bid price for 1 lot (0.01BTC) is 1.54 LTC => 1_5400_0000 litoshi => 10K * 15_400 (in price steps)
        future = api.submitCommandAsync(ApiPlaceOrder.builder()
                .uid(301L)
                .orderId(5001L)
                .price(15_400L)
                .reservePrice(15_400L) // can move bid order up to the 1.56 LTC, without replacing it
                .size(12L) // order size is 12 lots
                .action(OrderAction.BID)
                .orderType(OrderType.GTC) // Good-till-Cancel
                .symbol(symbolXbtLtc)
                .orderTakerFee(10L)
                .orderMakerFee(5L)
                .build());

        System.out.println("ApiPlaceOrder 1 result: " + future.get());

        // check balances
        Future<SingleUserReportResult> report1 = api.processReport(new SingleUserReportQuery(301), 0);
        IntLongHashMap report1Accounts = report1.get().getAccounts();
        System.out.println("SingleUserReportQuery 1 accounts: " + report1Accounts);
        assertEquals(report1Accounts.get(15),4151999880L);

        Future<SingleUserReportResult> report2 = api.processReport(new SingleUserReportQuery(302), 0);
        IntLongHashMap report2Accounts = report2.get().getAccounts();
        System.out.println("SingleUserReportQuery 2 accounts: " + report2Accounts);
        assertEquals(report2Accounts.get(11),30000000L);
////////////////////
        final long stateId = System.currentTimeMillis() * 1000;
        final ApiPersistState apiPersistState = ApiPersistState.builder().dumpId(stateId).build();
        final CommandResultCode resultCode = api.submitCommandAsync(apiPersistState).get();
        assertThat(resultCode, Is.is(CommandResultCode.SUCCESS));

        AtomicInteger uniqueIdCounterInt = new AtomicInteger();
        final long originalFinalStateHash = api.processReport(new StateHashReportQuery(), uniqueIdCounterInt.incrementAndGet()).get().getStateHash();

        final long snapshotBaseSeq = 0L;

        final InitialStateConfiguration fromSnapshotConfig = InitialStateConfiguration.lastKnownStateFromJournal(exchangeId, stateId, snapshotBaseSeq);

        log.debug("Creating new exchange from persisted state...");
        exchangeConfiguration = ExchangeConfiguration.defaultBuilder()
                .initStateCfg(fromSnapshotConfig)
                .performanceCfg(PerformanceConfiguration.DEFAULT)
                .reportsQueriesCfg(ReportsQueriesConfiguration.createStandardConfig())
                .ordersProcessingCfg(OrdersProcessingConfiguration.DEFAULT)
                .loggingCfg(LoggingConfiguration.DEFAULT)
                .serializationCfg(SerializationConfiguration.DISK_SNAPSHOT_ONLY)
                .build();

        ExchangeCore reCreatedExchangeCore = ExchangeCore.builder()
                .resultsConsumer(eventsProcessor)
                .exchangeConfiguration(exchangeConfiguration)
                .build();

        // start up disruptor threads
        reCreatedExchangeCore.startup();

        // get exchange API for publishing commands
        api = reCreatedExchangeCore.getApi();
        final TotalCurrencyBalanceReportResult res = api.processReport(new TotalCurrencyBalanceReportQuery(), uniqueIdCounterInt.incrementAndGet()).join();
        final IntLongHashMap openInterestLong = res.getOpenInterestLong();
        final IntLongHashMap openInterestShort = res.getOpenInterestShort();
        final IntLongHashMap openInterestDiff = new IntLongHashMap(openInterestLong);
        openInterestShort.forEachKeyValue((k, v) -> openInterestDiff.addToValue(k, -v));
        if (openInterestDiff.anySatisfy(vol -> vol != 0)) {
            throw new IllegalStateException("Open Interest balance check failed");
        }
        System.out.println("WARM UP: " + res);

//        System.out.println("-----------------------------------Check Balance------------------------------------------------");
//        // check balances
//        report1 = api.processReport(new SingleUserReportQuery(301), 0);
//        report1Accounts = report1.get().getAccounts();
//        System.out.println("SingleUserReportQuery 1 accounts: " + report1Accounts);
//        assertEquals(report1Accounts.get(15),4151999880L);
//
//        report2 = api.processReport(new SingleUserReportQuery(302), 0);
//        report2Accounts = report2.get().getAccounts();
//        System.out.println("SingleUserReportQuery 2 accounts: " + report2Accounts);
//        assertEquals(report2Accounts.get(11),30000000L);
//        System.out.println("------------------------------------Check Balance-----------------------------------------------");

        final long recreatedFinalStateHash = api.processReport(new StateHashReportQuery(), uniqueIdCounterInt.incrementAndGet()).get().getStateHash();
        assertThat(recreatedFinalStateHash, is(originalFinalStateHash));
        // second user places Good-till-Cancel Ask (Sell) order
        // he assumes wost rate to sell 152.5 LTC for 1 BTC
        future = api.submitCommandAsync(ApiPlaceOrder.builder()
                .uid(302L)
                .orderId(5002L)
                .price(15_250L)
                .size(20L)
                .action(OrderAction.ASK)
                .orderType(OrderType.GTC)
                .symbol(symbolXbtLtc)
                .orderTakerFee(8L)
                .orderMakerFee(3L)
                .build());

        System.out.println("ApiPlaceOrder 2 result: " + future.get());

        // check balances
        report1 = api.processReport(new SingleUserReportQuery(301), 0);
        report1Accounts = report1.get().getAccounts();
        System.out.println("SingleUserReportQuery 1 accounts: " + report1Accounts);
        assertEquals(report1Accounts.get(15), 4151999940L);
        assertEquals(report1Accounts.get(11), 12000000L);

        report2 = api.processReport(new SingleUserReportQuery(302), 0);
        report2Accounts = report2.get().getAccounts();
        System.out.println("SingleUserReportQuery 2 accounts: " + report2Accounts);
        assertEquals(report2Accounts.get(15), 1847999904L);
        assertEquals(report2Accounts.get(11), 10000000L);

        // first user places another Good-till-Cancel Bid order
        future = api.submitCommandAsync(ApiPlaceOrder.builder()
                .uid(301L)
                .orderId(5003L)
                .price(15_400L)
                .reservePrice(15_400L) // can move bid order up to the 1.56 LTC, without replacing it
                .size(10L) // order size is 12 lots
                .action(OrderAction.BID)
                .orderType(OrderType.GTC) // Good-till-Cancel
                .symbol(symbolXbtLtc)
                .orderTakerFee(9L)
                .orderMakerFee(7L)
                .build());

        System.out.println("ApiPlaceOrder 3 result: " + future.get());

        // check balances
        report1 = api.processReport(new SingleUserReportQuery(301), 0);
        report1Accounts = report1.get().getAccounts();
        System.out.println("SingleUserReportQuery 1 accounts: " + report1Accounts);
        assertEquals(report1Accounts.get(15), 2623999850L);
        assertEquals(report1Accounts.get(11), 20000000L);

        report2 = api.processReport(new SingleUserReportQuery(302), 0);
        report2Accounts = report2.get().getAccounts();
        System.out.println("SingleUserReportQuery 2 accounts: " + report2Accounts);
        assertEquals(report2Accounts.get(15), 3067999880L);
        assertEquals(report2Accounts.get(11), 10000000L);

        // second user places another Good-till-Cancel Ask (Sell) order
        future = api.submitCommandAsync(ApiPlaceOrder.builder()
                .uid(302L)
                .orderId(5004L)
                .price(15_250L)
                .size(5L)
                .action(OrderAction.ASK)
                .orderType(OrderType.GTC)
                .symbol(symbolXbtLtc)
                .orderTakerFee(7L)
                .orderMakerFee(4L)
                .build());

        System.out.println("ApiPlaceOrder 3 result: " + future.get());

        // check balances
        report1 = api.processReport(new SingleUserReportQuery(301), 0);
        report1Accounts = report1.get().getAccounts();
        System.out.println("SingleUserReportQuery 1 accounts: " + report1Accounts);
        assertEquals(report1Accounts.get(15), 2623999854L);
        assertEquals(report1Accounts.get(11), 22000000L);

        report2 = api.processReport(new SingleUserReportQuery(302), 0);
        report2Accounts = report2.get().getAccounts();
        System.out.println("SingleUserReportQuery 2 accounts: " + report2Accounts);
        assertEquals(report2Accounts.get(15), 3375999866L);
        assertEquals(report2Accounts.get(11), 5000000L);

        // second user cancel remaining order
        future = api.submitCommandAsync(ApiCancelOrder.builder()
                .uid(302L)
                .orderId(5004L)
                .symbol(symbolXbtLtc)
                .build());

        System.out.println("ApiCancelOrder 3 result: " + future.get());

        // check balances
        report1 = api.processReport(new SingleUserReportQuery(301), 0);
        report1Accounts = report1.get().getAccounts();
        System.out.println("SingleUserReportQuery 1 accounts: " + report1Accounts);
        assertEquals(report1Accounts.get(15), 2623999854L);
        assertEquals(report1Accounts.get(11), 22000000L);

        report2 = api.processReport(new SingleUserReportQuery(302), 0);
        report2Accounts = report2.get().getAccounts();
        System.out.println("SingleUserReportQuery 2 accounts: " + report2Accounts);
        assertEquals(report2Accounts.get(15), 3375999866L);
        assertEquals(report2Accounts.get(11), 8000000L);


        // check fees collected
        Future<TotalCurrencyBalanceReportResult> totalsReport = api.processReport(new TotalCurrencyBalanceReportQuery(), 0);
        long collectedFeeInLTC = totalsReport.get().getFees().get(currencyCodeLtc);
        System.out.println("LTC fees collected: " + collectedFeeInLTC);
        assertEquals(collectedFeeInLTC, 280);

    }

    @Test
    public void sampleTest_fromMaster() throws Exception {

        // simple async events handler
        SimpleEventsProcessor eventsProcessor = new SimpleEventsProcessor(new IEventsHandler() {
            @Override
            public void tradeEvent(TradeEvent tradeEvent) {
                System.out.println("Trade event: " + tradeEvent);
            }

            @Override
            public void reduceEvent(ReduceEvent reduceEvent) {
                System.out.println("Reduce event: " + reduceEvent);
            }

            @Override
            public void rejectEvent(RejectEvent rejectEvent) {
                System.out.println("Reject event: " + rejectEvent);
            }

            @Override
            public void commandResult(ApiCommandResult commandResult) {
                System.out.println("Command result: " + commandResult);
            }

            @Override
            public void orderBook(OrderBook orderBook) {
                System.out.println("OrderBook event: " + orderBook);
            }
        });

        // default exchange configuration
        ExchangeConfiguration conf = ExchangeConfiguration.defaultBuilder().build();

        // build exchange core
        ExchangeCore exchangeCore = ExchangeCore.builder()
                .resultsConsumer(eventsProcessor)
                .exchangeConfiguration(conf)
                .build();

        // start up disruptor threads
        exchangeCore.startup();

        // get exchange API for publishing commands
        ExchangeApi api = exchangeCore.getApi();

        // currency code constants
        final int currencyCodeXbt = 11;
        final int currencyCodeLtc = 15;

        // symbol constants
        final int symbolXbtLtc = 241;

        Future<CommandResultCode> future;

        // create symbol specification and publish it
        CoreSymbolSpecification symbolSpecXbtLtc = CoreSymbolSpecification.builder()
                .symbolId(symbolXbtLtc)         // symbol id
                .type(SymbolType.CURRENCY_EXCHANGE_PAIR)
                .baseCurrency(currencyCodeXbt)    // base = satoshi (1E-8)
                .quoteCurrency(currencyCodeLtc)   // quote = litoshi (1E-8)
                .baseScaleK(1_000_000L) // 1 lot = 1M satoshi (0.01 BTC)
                .quoteScaleK(10_000L)   // 1 price step = 10K litoshi
                .takerFee(1900L)        // taker fee 1900 litoshi per 1 lot
                .makerFee(700L)         // maker fee 700 litoshi per 1 lot
                .build();

        future = api.submitBinaryDataAsync(new BatchAddSymbolsCommand(symbolSpecXbtLtc));
        System.out.println("BatchAddSymbolsCommand result: " + future.get());


        // create user uid=301
        future = api.submitCommandAsync(ApiAddUser.builder()
                .uid(301L)
                .build());

        System.out.println("ApiAddUser 1 result: " + future.get());


        // create user uid=302
        future = api.submitCommandAsync(ApiAddUser.builder()
                .uid(302L)
                .build());

        System.out.println("ApiAddUser 2 result: " + future.get());

        // first user deposits 20 LTC
        future = api.submitCommandAsync(ApiAdjustUserBalance.builder()
                .uid(301L)
                .currency(currencyCodeLtc)
                .amount(2_000_000_000L)
                .transactionId(1L)
                .build());

        System.out.println("ApiAdjustUserBalance 1 result: " + future.get());


        // second user deposits 0.10 BTC
        future = api.submitCommandAsync(ApiAdjustUserBalance.builder()
                .uid(302L)
                .currency(currencyCodeXbt)
                .amount(10_000_000L)
                .transactionId(2L)
                .build());

        System.out.println("ApiAdjustUserBalance 2 result: " + future.get());


        // first user places Good-till-Cancel Bid order
        // he assumes BTCLTC exchange rate 154 LTC for 1 BTC
        // bid price for 1 lot (0.01BTC) is 1.54 LTC => 1_5400_0000 litoshi => 10K * 15_400 (in price steps)
        future = api.submitCommandAsync(ApiPlaceOrder.builder()
                .uid(301L)
                .orderId(5001L)
                .price(15_400L)
                .reservePrice(15_600L) // can move bid order up to the 1.56 LTC, without replacing it
                .size(12L) // order size is 12 lots
                .action(OrderAction.BID)
                .orderType(OrderType.GTC) // Good-till-Cancel
                .symbol(symbolXbtLtc)
                .orderTakerFee(1900L)
                .orderMakerFee(700L)
                .build());

        System.out.println("ApiPlaceOrder 1 result: " + future.get());


        // second user places Immediate-or-Cancel Ask (Sell) order
        // he assumes wost rate to sell 152.5 LTC for 1 BTC
        future = api.submitCommandAsync(ApiPlaceOrder.builder()
                .uid(302L)
                .orderId(5002L)
                .price(15_250L)
                .size(10L) // order size is 10 lots
                .action(OrderAction.ASK)
                .orderType(OrderType.IOC) // Immediate-or-Cancel
                .symbol(symbolXbtLtc)
                .orderTakerFee(1900L)
                .orderMakerFee(700L)
                .build());

        System.out.println("ApiPlaceOrder 2 result: " + future.get());


        // request order book
        CompletableFuture<L2MarketData> orderBookFuture = api.requestOrderBookAsync(symbolXbtLtc, 10);
        System.out.println("ApiOrderBookRequest result: " + orderBookFuture.get());


        // first user moves remaining order to price 1.53 LTC
        future = api.submitCommandAsync(ApiMoveOrder.builder()
                .uid(301L)
                .orderId(5001L)
                .newPrice(15_300L)
                .symbol(symbolXbtLtc)
                .build());

        System.out.println("ApiMoveOrder 2 result: " + future.get());

        // first user cancel remaining order
        future = api.submitCommandAsync(ApiCancelOrder.builder()
                .uid(301L)
                .orderId(5001L)
                .symbol(symbolXbtLtc)
                .build());

        System.out.println("ApiCancelOrder 2 result: " + future.get());

        // check balances
        Future<SingleUserReportResult> report1 = api.processReport(new SingleUserReportQuery(301), 0);
        System.out.println("SingleUserReportQuery 1 accounts: " + report1.get().getAccounts());

        Future<SingleUserReportResult> report2 = api.processReport(new SingleUserReportQuery(302), 0);
        System.out.println("SingleUserReportQuery 2 accounts: " + report2.get().getAccounts());

        // first user withdraws 0.10 BTC
        future = api.submitCommandAsync(ApiAdjustUserBalance.builder()
                .uid(301L)
                .currency(currencyCodeXbt)
                .amount(-10_000_000L)
                .transactionId(3L)
                .build());

        System.out.println("ApiAdjustUserBalance 1 result: " + future.get());

        // check fees collected
        Future<TotalCurrencyBalanceReportResult> totalsReport = api.processReport(new TotalCurrencyBalanceReportQuery(), 0);
        System.out.println("LTC fees collected: " + totalsReport.get().getFees().get(currencyCodeLtc));

    }

    @Test
    public void sampleTest_withJournal() throws Exception {

        // simple async events handler
        SimpleEventsProcessor eventsProcessor = new SimpleEventsProcessor(new IEventsHandler() {
            @Override
            public void tradeEvent(TradeEvent tradeEvent) {
                System.out.println("Trade event: " + tradeEvent);
            }

            @Override
            public void reduceEvent(ReduceEvent reduceEvent) {
                System.out.println("Reduce event: " + reduceEvent);
            }

            @Override
            public void rejectEvent(RejectEvent rejectEvent) {
                System.out.println("Reject event: " + rejectEvent);
            }

            @Override
            public void commandResult(ApiCommandResult commandResult) {
                System.out.println("Command result: " + commandResult);
            }

            @Override
            public void orderBook(OrderBook orderBook) {
                System.out.println("OrderBook event: " + orderBook);
            }
        });

        final String exchangeId = "ex01";
        final InitialStateConfiguration firstStartConfig = InitialStateConfiguration.cleanStartJournaling(exchangeId);

        ExchangeConfiguration conf = ExchangeConfiguration.defaultBuilder()
                .initStateCfg(firstStartConfig)
                .performanceCfg(PerformanceConfiguration.DEFAULT)
                .reportsQueriesCfg(ReportsQueriesConfiguration.DEFAULT)
                .ordersProcessingCfg(OrdersProcessingConfiguration.DEFAULT)
                .loggingCfg(LoggingConfiguration.DEFAULT)
                .serializationCfg(SerializationConfiguration.DISK_JOURNALING)
                .build();

        // default exchange configuration
        //ExchangeConfiguration conf = ExchangeConfiguration.defaultBuilder().build();

        // build exchange core
        ExchangeCore exchangeCore = ExchangeCore.builder()
                .resultsConsumer(eventsProcessor)
                .exchangeConfiguration(conf)
                .build();

        // start up disruptor threads
        exchangeCore.startup();

        // get exchange API for publishing commands
        ExchangeApi api = exchangeCore.getApi();

        // currency code constants
        final int currencyCodeXbt = 11;
        final int currencyCodeLtc = 15;

        // symbol constants
        final int symbolXbtLtc = 241;

        Future<CommandResultCode> future;

        // create symbol specification and publish it
        CoreSymbolSpecification symbolSpecXbtLtc = CoreSymbolSpecification.builder()
                .symbolId(symbolXbtLtc)         // symbol id
                .type(SymbolType.CURRENCY_EXCHANGE_PAIR)
                .baseCurrency(currencyCodeXbt)    // base = satoshi (1E-8)
                .quoteCurrency(currencyCodeLtc)   // quote = litoshi (1E-8)
                .baseScaleK(1_000_000L) // 1 lot = 1M satoshi (0.01 BTC)
                .quoteScaleK(10_000L)   // 1 price step = 10K litoshi
                .takerFee(1900L)        // taker fee 1900 litoshi per 1 lot
                .makerFee(700L)         // maker fee 700 litoshi per 1 lot
                .build();

        future = api.submitBinaryDataAsync(new BatchAddSymbolsCommand(symbolSpecXbtLtc));
        System.out.println("BatchAddSymbolsCommand result: " + future.get());


        // create user uid=301
        future = api.submitCommandAsync(ApiAddUser.builder()
                .uid(301L)
                .build());

        System.out.println("ApiAddUser 1 result: " + future.get());


        // create user uid=302
        future = api.submitCommandAsync(ApiAddUser.builder()
                .uid(302L)
                .build());

        System.out.println("ApiAddUser 2 result: " + future.get());

        // first user deposits 20 LTC
        future = api.submitCommandAsync(ApiAdjustUserBalance.builder()
                .uid(301L)
                .currency(currencyCodeLtc)
                .amount(2_000_000_000L)
                .transactionId(1L)
                .build());

        System.out.println("ApiAdjustUserBalance 1 result: " + future.get());


        // second user deposits 0.10 BTC
        future = api.submitCommandAsync(ApiAdjustUserBalance.builder()
                .uid(302L)
                .currency(currencyCodeXbt)
                .amount(10_000_000L)
                .transactionId(2L)
                .build());

        System.out.println("ApiAdjustUserBalance 2 result: " + future.get());


        // first user places Good-till-Cancel Bid order
        // he assumes BTCLTC exchange rate 154 LTC for 1 BTC
        // bid price for 1 lot (0.01BTC) is 1.54 LTC => 1_5400_0000 litoshi => 10K * 15_400 (in price steps)
        future = api.submitCommandAsync(ApiPlaceOrder.builder()
                .uid(301L)
                .orderId(5001L)
                .price(15_400L)
                .reservePrice(15_600L) // can move bid order up to the 1.56 LTC, without replacing it
                .size(12L) // order size is 12 lots
                .action(OrderAction.BID)
                .orderType(OrderType.GTC) // Good-till-Cancel
                .symbol(symbolXbtLtc)
                .orderTakerFee(1900L)
                .orderMakerFee(700L)
                .build());

        System.out.println("ApiPlaceOrder 1 result: " + future.get());

        //take a snapshot
        final long stateId = System.currentTimeMillis() * 1000;
        final ApiPersistState apiPersistState = ApiPersistState.builder().dumpId(stateId).build();
        final CommandResultCode resultCode = api.submitCommandAsync(apiPersistState).get();
        assertThat(resultCode, Is.is(CommandResultCode.SUCCESS));

        AtomicInteger uniqueIdCounterInt = new AtomicInteger();
        final long originalFinalStateHash = api.processReport(new StateHashReportQuery(), uniqueIdCounterInt.incrementAndGet()).get().getStateHash();

        // second user places Immediate-or-Cancel Ask (Sell) order
        // he assumes wost rate to sell 152.5 LTC for 1 BTC
        future = api.submitCommandAsync(ApiPlaceOrder.builder()
                .uid(302L)
                .orderId(5002L)
                .price(15_250L)
                .size(10L) // order size is 10 lots
                .action(OrderAction.ASK)
                .orderType(OrderType.IOC) // Immediate-or-Cancel
                .symbol(symbolXbtLtc)
                .orderTakerFee(1900L)
                .orderMakerFee(700L)
                .build());

        System.out.println("ApiPlaceOrder 2 result: " + future.get());


        // request order book
        CompletableFuture<L2MarketData> orderBookFuture = api.requestOrderBookAsync(symbolXbtLtc, 10);
        System.out.println("ApiOrderBookRequest result: " + orderBookFuture.get());


        // first user moves remaining order to price 1.53 LTC
        future = api.submitCommandAsync(ApiMoveOrder.builder()
                .uid(301L)
                .orderId(5001L)
                .newPrice(15_300L)
                .symbol(symbolXbtLtc)
                .build());

        System.out.println("ApiMoveOrder 2 result: " + future.get());

        //load from snapshot and replay from journal
        final long snapshotBaseSeq = 0L;

        final InitialStateConfiguration fromSnapshotConfig = InitialStateConfiguration.lastKnownStateFromJournal(exchangeId, stateId, snapshotBaseSeq);

        log.debug("Creating new exchange from persisted state...");
        conf = ExchangeConfiguration.defaultBuilder()
                .initStateCfg(fromSnapshotConfig)
                .performanceCfg(PerformanceConfiguration.DEFAULT)
                .reportsQueriesCfg(ReportsQueriesConfiguration.DEFAULT)
                .ordersProcessingCfg(OrdersProcessingConfiguration.DEFAULT)
                .loggingCfg(LoggingConfiguration.DEFAULT)
                .serializationCfg(SerializationConfiguration.DISK_JOURNALING)
                .build();

        ExchangeCore reCreatedExchangeCore = ExchangeCore.builder()
                .resultsConsumer(eventsProcessor)
                .exchangeConfiguration(conf)
                .build();

        // start up disruptor threads
        reCreatedExchangeCore.startup();
        api = reCreatedExchangeCore.getApi();
        final long recreatedFinalStateHash = api.processReport(new StateHashReportQuery(), uniqueIdCounterInt.incrementAndGet()).get().getStateHash();
       // assertThat(recreatedFinalStateHash, is(originalFinalStateHash));

        // first user cancel remaining order
        future = api.submitCommandAsync(ApiCancelOrder.builder()
                .uid(301L)
                .orderId(5001L)
                .symbol(symbolXbtLtc)
                .build());

        System.out.println("ApiCancelOrder 2 result: " + future.get());

        // check balances
        Future<SingleUserReportResult> report1 = api.processReport(new SingleUserReportQuery(301), 0);
        System.out.println("SingleUserReportQuery 1 accounts: " + report1.get().getAccounts());

        Future<SingleUserReportResult> report2 = api.processReport(new SingleUserReportQuery(302), 0);
        System.out.println("SingleUserReportQuery 2 accounts: " + report2.get().getAccounts());

        // first user withdraws 0.10 BTC
        future = api.submitCommandAsync(ApiAdjustUserBalance.builder()
                .uid(301L)
                .currency(currencyCodeXbt)
                .amount(-10_000_000L)
                .transactionId(3L)
                .build());

        System.out.println("ApiAdjustUserBalance 1 result: " + future.get());

        // check fees collected
        Future<TotalCurrencyBalanceReportResult> totalsReport = api.processReport(new TotalCurrencyBalanceReportQuery(), 0);
        System.out.println("LTC fees collected: " + totalsReport.get().getFees().get(currencyCodeLtc));

    }
}
