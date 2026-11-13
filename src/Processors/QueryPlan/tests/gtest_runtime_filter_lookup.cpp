#include <gtest/gtest.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <base/unit.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <array>
#include <barrier>
#include <initializer_list>
#include <memory>
#include <optional>
#include <thread>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{

ColumnPtr makeUInt64Column(std::initializer_list<UInt64> values)
{
    auto column = ColumnUInt64::create();
    for (const auto value : values)
        column->insertValue(value);
    return column;
}

ColumnWithTypeAndName makeUInt64ColumnWithType(std::initializer_list<UInt64> values, const DataTypePtr & type)
{
    return ColumnWithTypeAndName(makeUInt64Column(values), type, "k");
}

void expectMask(ColumnPtr mask, std::initializer_list<UInt8> expected_values)
{
    ASSERT_EQ(mask->size(), expected_values.size());

    size_t row = 0;
    for (const auto expected : expected_values)
    {
        EXPECT_EQ(mask->getBool(row), expected != 0) << "Unexpected value at row " << row;
        ++row;
    }
}

DataTypePtr makeUInt64Type()
{
    tryRegisterFunctions();
    return std::make_shared<DataTypeUInt64>();
}

RuntimeFilterConfig makeRuntimeFilterConfig()
{
    return RuntimeFilterConfig{/*pass_ratio_threshold_for_disabling=*/1.0,
                               /*blocks_to_skip_before_reenabling=*/30};
}

}

TEST(RuntimeFilterLookup, ExactContainsQueriesSet)
{
    const auto type = makeUInt64Type();
    RuntimeFilter filter(
        /*filters_to_merge_=*/0,
        makeRuntimeFilterConfig(),
        RuntimeFilter::ExactContains(
            type,
            /*bytes_limit_=*/1_MiB,
            /*exact_values_limit_=*/100));

    filter.insert(makeUInt64Column({1, 3, 5}));
    filter.finishInsert();

    EXPECT_EQ(filter.getFilterColumnTargetType(), type);
    expectMask(filter.find(makeUInt64ColumnWithType({1, 2, 5, 7}, type)), {1, 0, 1, 0});
    EXPECT_EQ(filter.getStats().rows_checked.load(), 4);
    EXPECT_EQ(filter.getStats().rows_passed.load(), 2);
}

TEST(RuntimeFilterLookup, ExactNotContainsQueriesSet)
{
    const auto type = makeUInt64Type();
    RuntimeFilter filter(
        /*filters_to_merge_=*/0,
        makeRuntimeFilterConfig(),
        RuntimeFilter::ExactNotContains(
            type,
            /*bytes_limit_=*/1_MiB,
            /*exact_values_limit_=*/100));

    filter.insert(makeUInt64Column({1, 3, 5}));
    filter.finishInsert();

    EXPECT_EQ(filter.getFilterColumnTargetType(), type);
    expectMask(filter.find(makeUInt64ColumnWithType({1, 2, 5, 7}, type)), {0, 1, 0, 1});
    EXPECT_EQ(filter.getStats().rows_checked.load(), 4);
    EXPECT_EQ(filter.getStats().rows_passed.load(), 2);
}

TEST(RuntimeFilterLookup, ApproximateRuntimeFilterQueriesBloomFilter)
{
    const auto type = makeUInt64Type();
    RuntimeFilter filter(
        /*filters_to_merge_=*/0,
        makeRuntimeFilterConfig(),
        RuntimeFilter::Adaptive(
            type,
            /*bytes_limit_=*/1_MiB,
            /*exact_values_limit_=*/1,
            /*bloom_filter_hash_functions_=*/3,
            /*max_ratio_of_set_bits_in_bloom_filter_=*/1.0,
            /*distinct_keys_hint=*/std::nullopt,
            /*distinct_keys_hint_matches_filter_key_=*/false));

    EXPECT_EQ(filter.getFilterColumnTargetType(), type);
    filter.enableIndexAnalysis();
    filter.insert(makeUInt64Column({1, 3}));
    filter.insert(makeUInt64Column({5}));
    filter.finishInsert();

    EXPECT_EQ(filter.getFilterColumnTargetType(), type);
    EXPECT_FALSE(filter.getRecordedKeyValues());
    auto range = filter.getRecordedKeyRanges();
    ASSERT_TRUE(range);
    EXPECT_EQ(range->left.safeGet<UInt64>(), 1);
    EXPECT_EQ(range->right.safeGet<UInt64>(), 5);
    expectMask(filter.find(makeUInt64ColumnWithType({1, 3, 5}, type)), {1, 1, 1});
    EXPECT_EQ(filter.getStats().rows_checked.load(), 3);
    EXPECT_EQ(filter.getStats().rows_passed.load(), 3);
}

TEST(RuntimeFilterLookup, PredictedBloomSaturationDropsKeySetAndPreservesMergedRange)
{
    const auto type = makeUInt64Type();
    RuntimeFilter destination(
        /*filters_to_merge_=*/1,
        makeRuntimeFilterConfig(),
        RuntimeFilter::Adaptive(
            type,
            /*bytes_limit_=*/1_KiB,
            /*exact_values_limit_=*/1,
            /*bloom_filter_hash_functions_=*/3,
            /*max_ratio_of_set_bits_in_bloom_filter_=*/0.05,
            /*distinct_keys_hint_=*/2'000'000,
            /*distinct_keys_hint_matches_filter_key_=*/true));
    destination.enableIndexAnalysis();
    destination.insert(makeUInt64Column({1}));

    RuntimeFilter source(
        /*filters_to_merge_=*/0,
        makeRuntimeFilterConfig(),
        RuntimeFilter::Adaptive(
            type,
            /*bytes_limit_=*/1_KiB,
            /*exact_values_limit_=*/1,
            /*bloom_filter_hash_functions_=*/3,
            /*max_ratio_of_set_bits_in_bloom_filter_=*/0.05,
            /*distinct_keys_hint_=*/2'000'000,
            /*distinct_keys_hint_matches_filter_key_=*/true));
    source.enableIndexAnalysis();
    source.insert(makeUInt64Column({3, 5}));
    source.finishInsert();

    destination.merge(source);
    destination.finishInsert();

    EXPECT_FALSE(destination.getRecordedKeyValues());
    auto range = destination.getRecordedKeyRanges();
    ASSERT_TRUE(range);
    EXPECT_EQ(range->left.safeGet<UInt64>(), 1);
    EXPECT_EQ(range->right.safeGet<UInt64>(), 5);
    expectMask(destination.find(makeUInt64ColumnWithType({2, 4}, type)), {1, 1});
    EXPECT_EQ(destination.getStats().rows_checked.load(), 0);
    EXPECT_EQ(destination.getStats().rows_skipped.load(), 2);
}

TEST(RuntimeFilterLookup, LookupMergesExactContainsFilters)
{
    const auto type = makeUInt64Type();
    auto lookup = createRuntimeFilterLookup();
    auto query_context = Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setRuntimeFilterLookup(lookup);

    /// Some unit-test configurations initialize MainThreadStatus before this test, while others
    /// leave current_thread unset. Reuse the existing status when present instead of replacing it.
    std::optional<ThreadStatus> thread_status;
    if (!CurrentThread::isInitialized())
        thread_status.emplace();
    auto thread_group = std::make_shared<ThreadGroup>(query_context, 0);
    ThreadGroupSwitcher thread_group_switcher(thread_group, ThreadName::UNKNOWN, /*allow_existing_group=*/true);

    const auto id_type = std::make_shared<DataTypeString>();
    ColumnsWithTypeAndName arguments{
        {id_type->createColumnConst(3, "runtime_filter"), id_type, "filter_id"}, makeUInt64ColumnWithType({1, 3, 5}, type)};
    auto apply_filter = FunctionFactory::instance().get("__applyFilter", query_context)->build(arguments);

    auto first_filter = std::make_unique<RuntimeFilter>(
        /*filters_to_merge_=*/1,
        makeRuntimeFilterConfig(),
        RuntimeFilter::ExactContains(
            type,
            /*bytes_limit_=*/1_MiB,
            /*exact_values_limit_=*/100));
    first_filter->insert(makeUInt64Column({1}));
    lookup->add("runtime_filter", "runtime_filter", std::move(first_filter));

    /// Publication must be able to find the first stream's filter while a merge is pending.
    auto pending_filter = lookup->find("runtime_filter");
    ASSERT_TRUE(pending_filter);
    EXPECT_FALSE(pending_filter->isReady());
    expectMask(apply_filter->execute(arguments, apply_filter->getResultType(), 3, false), {1, 1, 1});
    EXPECT_EQ(pending_filter->getStats().blocks_processed.load(), 0);

    auto second_filter = std::make_unique<RuntimeFilter>(
        /*filters_to_merge_=*/0,
        makeRuntimeFilterConfig(),
        RuntimeFilter::ExactContains(
            type,
            /*bytes_limit_=*/1_MiB,
            /*exact_values_limit_=*/100));
    second_filter->insert(makeUInt64Column({5}));
    lookup->add("runtime_filter", "runtime_filter", std::move(second_filter));

    auto filter = lookup->find("runtime_filter");
    ASSERT_TRUE(filter);
    EXPECT_TRUE(filter->isReady());
    EXPECT_TRUE(pending_filter->isReady());
    expectMask(apply_filter->execute(arguments, apply_filter->getResultType(), 3, false), {1, 0, 1});
    EXPECT_EQ(filter->getStats().blocks_processed.load(), 1);
    EXPECT_EQ(filter->getStats().rows_checked.load(), 3);
}

TEST(RuntimeFilterLookup, SkipBudgetPreservesSerialExhaustionSemantics)
{
    RuntimeFilterEvaluationState state(
        RuntimeFilterConfig{/*pass_ratio_threshold_for_disabling=*/0.5,
                            /*blocks_to_skip_before_reenabling=*/1});

    state.updateStats(/*rows_checked=*/100, /*rows_passed=*/100);
    EXPECT_TRUE(state.shouldSkip(60));
    EXPECT_FALSE(state.shouldSkip(60));
    EXPECT_EQ(state.getStats().blocks_skipped.load(), 1);
    EXPECT_EQ(state.getStats().rows_skipped.load(), 60);

    state.updateStats(/*rows_checked=*/100, /*rows_passed=*/100);
    EXPECT_FALSE(state.shouldSkip(100));
}

TEST(RuntimeFilterLookup, SkipBudgetConsumptionIsLinearizable)
{
    constexpr size_t iterations = 2000;
    RuntimeFilterEvaluationState state(
        RuntimeFilterConfig{/*pass_ratio_threshold_for_disabling=*/0.5,
                            /*blocks_to_skip_before_reenabling=*/1});
    std::barrier<> sync(3);
    std::array<bool, 2> should_skip{};

    auto consume_budget = [&](size_t thread_index)
    {
        for (size_t iteration = 0; iteration < iterations; ++iteration)
        {
            sync.arrive_and_wait();
            should_skip[thread_index] = state.shouldSkip(60);
            sync.arrive_and_wait();
        }
    };

    std::thread first_consumer(consume_budget, 0);
    std::thread second_consumer(consume_budget, 1);
    size_t non_linearizable_iterations = 0;
    for (size_t iteration = 0; iteration < iterations; ++iteration)
    {
        state.updateStats(/*rows_checked=*/100, /*rows_passed=*/100);
        sync.arrive_and_wait();
        sync.arrive_and_wait();
        if (should_skip[0] == should_skip[1])
            ++non_linearizable_iterations;
    }
    first_consumer.join();
    second_consumer.join();

    EXPECT_EQ(non_linearizable_iterations, 0);
}

TEST(RuntimeFilterLookup, SkipBudgetDoesNotLoseConcurrentReplenishment)
{
    constexpr size_t iterations = 10000;
    RuntimeFilterEvaluationState state(
        RuntimeFilterConfig{/*pass_ratio_threshold_for_disabling=*/0.5,
                            /*blocks_to_skip_before_reenabling=*/1});
    std::barrier<> sync(3);

    std::thread consumer(
        [&]
        {
            for (size_t iteration = 0; iteration < iterations; ++iteration)
            {
                sync.arrive_and_wait();
                state.shouldSkip(100);
                sync.arrive_and_wait();
            }
        });
    std::thread replenisher(
        [&]
        {
            for (size_t iteration = 0; iteration < iterations; ++iteration)
            {
                sync.arrive_and_wait();
                state.updateStats(/*rows_checked=*/100, /*rows_passed=*/100);
                sync.arrive_and_wait();
            }
        });

    size_t lost_replenishments = 0;
    for (size_t iteration = 0; iteration < iterations; ++iteration)
    {
        state.updateStats(/*rows_checked=*/100, /*rows_passed=*/100);
        sync.arrive_and_wait();
        sync.arrive_and_wait();
        if (!state.shouldSkip(99))
            ++lost_replenishments;
        EXPECT_FALSE(state.shouldSkip(1));
    }
    consumer.join();
    replenisher.join();

    EXPECT_EQ(lost_replenishments, 0);
}

#ifdef DEBUG_OR_SANITIZER_BUILD
TEST(RuntimeFilterLookupDeathTest, MergeRejectsSelf)
#else
TEST(RuntimeFilterLookup, MergeRejectsSelf)
#endif
{
    const auto type = makeUInt64Type();
    RuntimeFilter filter(
        /*filters_to_merge_=*/1,
        makeRuntimeFilterConfig(),
        RuntimeFilter::ExactContains(
            type,
            /*bytes_limit_=*/1_MiB,
            /*exact_values_limit_=*/100));

#ifdef DEBUG_OR_SANITIZER_BUILD
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    EXPECT_DEATH(filter.merge(filter), "Trying to merge a runtime filter with itself");
#else
    try
    {
        filter.merge(filter);
        FAIL() << "Expected LOGICAL_ERROR when merging a runtime filter with itself";
    }
    catch (Exception & e)
    {
        e.markAsLogged();
        EXPECT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR);
        EXPECT_EQ(e.message(), "Trying to merge a runtime filter with itself");
    }
#endif
}

TEST(RuntimeFilterLookup, ReciprocalMergesAreSerialized)
{
    const auto type = makeUInt64Type();
    RuntimeFilter first(
        /*filters_to_merge_=*/1,
        makeRuntimeFilterConfig(),
        RuntimeFilter::ExactContains(type, /*bytes_limit_=*/1_MiB, /*exact_values_limit_=*/100));
    RuntimeFilter second(
        /*filters_to_merge_=*/1,
        makeRuntimeFilterConfig(),
        RuntimeFilter::ExactContains(type, /*bytes_limit_=*/1_MiB, /*exact_values_limit_=*/100));
    first.insert(makeUInt64Column({1}));
    second.insert(makeUInt64Column({2}));

    std::barrier<> sync(3);
    std::thread first_merge(
        [&]
        {
            sync.arrive_and_wait();
            first.merge(second);
        });
    std::thread second_merge(
        [&]
        {
            sync.arrive_and_wait();
            second.merge(first);
        });
    sync.arrive_and_wait();
    first_merge.join();
    second_merge.join();

    first.finishInsert();
    second.finishInsert();
    expectMask(first.find(makeUInt64ColumnWithType({1, 2, 3}, type)), {1, 1, 0});
    expectMask(second.find(makeUInt64ColumnWithType({1, 2, 3}, type)), {1, 1, 0});
}

TEST(RuntimeFilterLookup, ExactContainsMergesFinalizedSingleFilter)
{
    const auto type = makeUInt64Type();
    RuntimeFilter destination(
        /*filters_to_merge_=*/1,
        makeRuntimeFilterConfig(),
        RuntimeFilter::ExactContains(
            type,
            /*bytes_limit_=*/1_MiB,
            /*exact_values_limit_=*/100));

    RuntimeFilter source(
        /*filters_to_merge_=*/0,
        makeRuntimeFilterConfig(),
        RuntimeFilter::ExactContains(
            type,
            /*bytes_limit_=*/1_MiB,
            /*exact_values_limit_=*/100));
    source.insert(makeUInt64Column({5}));
    source.finishInsert();

    destination.merge(source);
    destination.finishInsert();

    expectMask(destination.find(makeUInt64ColumnWithType({5, 7}, type)), {1, 0});
}

TEST(RuntimeFilterLookup, ExactContainsMergesFinalizedEmptyFilter)
{
    const auto type = makeUInt64Type();
    RuntimeFilter destination(
        /*filters_to_merge_=*/1,
        makeRuntimeFilterConfig(),
        RuntimeFilter::ExactContains(
            type,
            /*bytes_limit_=*/1_MiB,
            /*exact_values_limit_=*/100));
    destination.insert(makeUInt64Column({1}));

    RuntimeFilter source(
        /*filters_to_merge_=*/0,
        makeRuntimeFilterConfig(),
        RuntimeFilter::ExactContains(
            type,
            /*bytes_limit_=*/1_MiB,
            /*exact_values_limit_=*/100));
    source.finishInsert();

    destination.merge(source);
    destination.finishInsert();

    expectMask(destination.find(makeUInt64ColumnWithType({1, 2}, type)), {1, 0});
}

TEST(RuntimeFilterLookup, IndexAnalysisRecordsExactValuesAndMergedRange)
{
    const auto type = makeUInt64Type();
    RuntimeFilter destination(
        /*filters_to_merge_=*/1,
        makeRuntimeFilterConfig(),
        RuntimeFilter::Adaptive(type, 1_MiB, 100, 3, 1.0, std::nullopt, false));
    destination.enableIndexAnalysis();
    destination.insert(makeUInt64Column({3, 7}));

    RuntimeFilter source(
        /*filters_to_merge_=*/0,
        makeRuntimeFilterConfig(),
        RuntimeFilter::Adaptive(type, 1_MiB, 100, 3, 1.0, std::nullopt, false));
    source.enableIndexAnalysis();
    source.insert(makeUInt64Column({1, 9}));
    source.finishInsert();

    destination.merge(source);
    destination.finishInsert();

    auto values = destination.getRecordedKeyValues();
    ASSERT_TRUE(values);
    EXPECT_EQ(values->size(), 4);
    auto range = destination.getRecordedKeyRanges();
    ASSERT_TRUE(range);
    EXPECT_EQ(range->left.safeGet<UInt64>(), 1);
    EXPECT_EQ(range->right.safeGet<UInt64>(), 9);
}

TEST(RuntimeFilterLookup, ExactNotContainsHasNoPositiveIndexMetadata)
{
    const auto type = makeUInt64Type();
    RuntimeFilter filter(
        /*filters_to_merge_=*/0, makeRuntimeFilterConfig(), RuntimeFilter::ExactNotContains(type, 1_MiB, 100));
    filter.enableIndexAnalysis();
    filter.insert(makeUInt64Column({1, 3}));
    filter.finishInsert();

    EXPECT_FALSE(filter.getRecordedKeyValues());
    EXPECT_FALSE(filter.getRecordedKeyRanges());
}

TEST(RuntimeFilterLookup, LateAddAfterSharedFilterPublicationFailsOpenForIndexMetadata)
{
    const auto type = makeUInt64Type();
    auto lookup = createRuntimeFilterLookup();

    /// Register the first of two stream-local filters. It remains unfinished while one merge is
    /// pending, so index analysis must not expose its partial exact values or range.
    auto initial_filter = std::make_unique<RuntimeFilter>(
        /*filters_to_merge_=*/1,
        makeRuntimeFilterConfig(),
        RuntimeFilter::ExactContains(
            type,
            /*bytes_limit_=*/1_MiB,
            /*exact_values_limit_=*/100));
    initial_filter->enableIndexAnalysis();
    initial_filter->insert(makeUInt64Column({3, 7}));
    lookup->add("runtime_filter", "runtime_filter", std::move(initial_filter));

    auto existing = lookup->find("runtime_filter");
    ASSERT_TRUE(existing);
    EXPECT_FALSE(existing->isReady());
    auto recorded_key_range = existing->getRecordedKeyRanges();
    auto recorded_key_values = existing->getRecordedKeyValues();
    EXPECT_FALSE(recorded_key_range);
    EXPECT_FALSE(recorded_key_values);

    /// Simulate post-build publication. The probe sees the complete hash table, including key 1
    /// whose stream-local filter has not registered yet. The unfinished filter contributes no
    /// metadata, so read-side index analysis fails open instead of pruning by {3, 7}.
    auto shared_filter = std::make_unique<RuntimeFilter>(
        /*filters_to_merge_=*/0,
        existing->getConfig(),
        RuntimeFilter::SharedFixedHashTable(
            existing->getFilterColumnTargetType(),
            [](const ColumnWithTypeAndName & values)
            {
                auto result = ColumnUInt8::create();
                auto & result_data = result->getData();
                result_data.resize(values.column->size());
                for (size_t row = 0; row < values.column->size(); ++row)
                {
                    const auto value = values.column->getUInt(row);
                    result_data[row] = value == 1 || value == 3 || value == 7;
                }
                return result;
            },
            std::move(recorded_key_range),
            std::move(recorded_key_values)));
    lookup->replace("runtime_filter", std::move(shared_filter));

    auto late_filter = std::make_unique<RuntimeFilter>(
        /*filters_to_merge_=*/0,
        makeRuntimeFilterConfig(),
        RuntimeFilter::ExactContains(
            type,
            /*bytes_limit_=*/1_MiB,
            /*exact_values_limit_=*/100));
    late_filter->enableIndexAnalysis();
    late_filter->insert(makeUInt64Column({1}));
    EXPECT_NO_THROW(lookup->add("runtime_filter", "runtime_filter", std::move(late_filter)));

    auto filter = lookup->find("runtime_filter");
    ASSERT_TRUE(filter);
    EXPECT_TRUE(filter->isReady());
    expectMask(filter->find(makeUInt64ColumnWithType({1, 3, 7}, type)), {1, 1, 1});
    EXPECT_FALSE(filter->getRecordedKeyValues());
    EXPECT_FALSE(filter->getRecordedKeyRanges());
}

TEST(RuntimeFilterLookup, SharedFixedHashTableSuppressesUnsupportedRange)
{
    const auto type = std::make_shared<DataTypeFloat64>();
    auto mutable_exact_values = ColumnFloat64::create();
    mutable_exact_values->insertValue(3.0);
    ColumnPtr exact_values = std::move(mutable_exact_values);
    RuntimeFilter filter(
        /*filters_to_merge_=*/0,
        makeRuntimeFilterConfig(),
        RuntimeFilter::SharedFixedHashTable(
            type,
            [](const ColumnWithTypeAndName & values) { return DataTypeUInt8().createColumnConst(values.column->size(), true); },
            Range(Float64{3.0}, true, Float64{3.0}, true),
            exact_values));

    EXPECT_EQ(filter.getFilterColumnTargetType(), type);
    EXPECT_TRUE(filter.isReady());
    EXPECT_FALSE(filter.getRecordedKeyRanges());
    EXPECT_EQ(filter.getRecordedKeyValues(), exact_values);
}

TEST(RuntimeFilterLookup, SharedFixedHashTableRuntimeFilterDelegatesProbe)
{
    const auto type = makeUInt64Type();
    RuntimeFilter filter(
        /*filters_to_merge_=*/0,
        makeRuntimeFilterConfig(),
        RuntimeFilter::SharedFixedHashTable(
            type,
            [](const ColumnWithTypeAndName & values)
            {
                auto result = ColumnUInt8::create();
                auto & result_data = result->getData();
                result_data.resize(values.column->size());
                for (size_t row = 0; row < values.column->size(); ++row)
                    result_data[row] = values.column->getUInt(row) == 3 || values.column->getUInt(row) == 7;
                return result;
            },
            Range(UInt64{3}, true, UInt64{7}, true)));

    EXPECT_EQ(filter.getFilterColumnTargetType(), type);
    EXPECT_NO_THROW(filter.insert(makeUInt64Column({42})));
    auto range = filter.getRecordedKeyRanges();
    ASSERT_TRUE(range);
    EXPECT_EQ(range->left.safeGet<UInt64>(), 3);
    EXPECT_EQ(range->right.safeGet<UInt64>(), 7);
    expectMask(filter.find(makeUInt64ColumnWithType({1, 3, 5, 7, 42}, type)), {0, 1, 0, 1, 0});
    EXPECT_EQ(filter.getStats().rows_checked.load(), 5);
    EXPECT_EQ(filter.getStats().rows_passed.load(), 2);
}

}
