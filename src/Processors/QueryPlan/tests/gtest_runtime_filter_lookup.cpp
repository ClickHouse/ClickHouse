#include <gtest/gtest.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <base/scope_guard.h>
#include <base/unit.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <array>
#include <barrier>
#include <initializer_list>
#include <limits>
#include <memory>
#include <optional>
#include <thread>

namespace ProfileEvents
{
extern const Event RuntimeFilterDenseBitsetsBuilt;
}

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int MEMORY_LIMIT_EXCEEDED;
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

/// `bytes_limit` sets both the exact-phase byte budget and the bloom filter size, as for a locally
/// built filter. `gtest_runtime_filter_serialization` covers geometries where the two differ.
RuntimeFilterGeometry makeGeometry(
    UInt64 bytes_limit, UInt64 exact_values_limit, UInt64 bloom_filter_hash_functions, Float64 max_ratio_of_set_bits_in_bloom_filter)
{
    return RuntimeFilterGeometry{
        .exact_values_limit = exact_values_limit,
        .exact_bytes_limit = bytes_limit,
        .bloom_filter_bytes = bytes_limit,
        .bloom_filter_hash_functions = bloom_filter_hash_functions,
        .pass_ratio_threshold_for_disabling = 1.0,
        .blocks_to_skip_before_reenabling = 30,
        .max_ratio_of_set_bits_in_bloom_filter = max_ratio_of_set_bits_in_bloom_filter,
    };
}

/// Runs `action` with the memory limit of the current thread just above its usage, as when a query reaches its limit,
/// and returns whether it threw MEMORY_LIMIT_EXCEEDED. Allocations below `max_untracked_memory` (4 MiB) are not checked.
template <typename Action>
bool throwsAtMemoryLimit(Action && action)
{
    MainThreadStatus::getInstance();
    auto & thread_tracker = CurrentThread::get().memory_tracker;
    const Int64 previous_hard_limit = thread_tracker.getHardLimit();
    CurrentThread::flushUntrackedMemory();
    thread_tracker.setHardLimit(thread_tracker.get() + 64 * 1024);
    SCOPE_EXIT({
        thread_tracker.setHardLimit(previous_hard_limit);
        CurrentThread::flushUntrackedMemory();
    });
    try
    {
        action();
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
            throw;
        return true;
    }
    return false;
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
            makeGeometry(1_MiB, 1, 3, 1.0),
            /*distinct_keys_hint=*/std::nullopt,
            /*distinct_keys_hint_matches_filter_key_=*/false,
            /*distinct_keys_in_last_run_=*/std::nullopt));

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
            makeGeometry(1_KiB, 1, 3, 0.05),
            /*distinct_keys_hint_=*/2'000'000,
            /*distinct_keys_hint_matches_filter_key_=*/true,
            /*distinct_keys_in_last_run_=*/std::nullopt));
    destination.enableIndexAnalysis();
    destination.insert(makeUInt64Column({1}));

    RuntimeFilter source(
        /*filters_to_merge_=*/0,
        makeRuntimeFilterConfig(),
        RuntimeFilter::Adaptive(
            type,
            makeGeometry(1_KiB, 1, 3, 0.05),
            /*distinct_keys_hint_=*/2'000'000,
            /*distinct_keys_hint_matches_filter_key_=*/true,
            /*distinct_keys_in_last_run_=*/std::nullopt));
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

template <typename T>
void checkDenseBitsetFindsExactlyTheKeys()
{
    const auto type = std::make_shared<DataTypeNumber<T>>();
    auto make_filter = [&](size_t filters_to_merge)
    {
        return std::make_unique<RuntimeFilter>(
            filters_to_merge,
            makeRuntimeFilterConfig(),
            RuntimeFilter::Adaptive(type, makeGeometry(1_MiB, /*exact_values_limit=*/2, 3, 1.0), std::nullopt, false, std::nullopt));
    };
    auto make_keys = [](std::initializer_list<T> keys)
    {
        auto column = ColumnVector<T>::create();
        for (const T key : keys)
            column->insertValue(key);
        return column;
    };
    const Int64 min = std::numeric_limits<T>::min();
    const Int64 max = std::numeric_limits<T>::max();
    const ProfileEvents::Count bitsets_before = ProfileEvents::global_counters[ProfileEvents::RuntimeFilterDenseBitsetsBuilt];

    /// The sources overflow their exact sets, and the first one receives one more block as a bitset. The destination
    /// overflows its exact set when the first source is merged into it, and merges the second source into its bitset.
    auto first_source = make_filter(0);
    first_source->insert(make_keys({static_cast<T>(min), static_cast<T>(max), 1}));
    first_source->insert(make_keys({3}));
    first_source->finishInsert();
    auto second_source = make_filter(0);
    second_source->insert(make_keys({static_cast<T>(min + 1), 5, 9}));
    second_source->finishInsert();
    auto destination = make_filter(2);
    destination->insert(make_keys({7}));
    destination->merge(*first_source);
    destination->merge(*second_source);
    destination->finishInsert();
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::RuntimeFilterDenseBitsetsBuilt] - bitsets_before, 3);

    const auto description = destination->describe();
    EXPECT_EQ(description.kind, RuntimeFilterKind::DenseBitset);
    ASSERT_TRUE(description.keys.has_value());
    /// For an unsigned type, `min + 1` is the key 1.
    EXPECT_EQ(*description.keys, std::is_signed_v<T> ? 8 : 7);
    ASSERT_TRUE(description.bytes.has_value());
    EXPECT_EQ(*description.bytes, (1ULL << (8 * sizeof(T))) / 8);

    auto probe = ColumnVector<T>::create();
    for (Int64 key = min; key <= max; ++key)
        probe->insertValue(static_cast<T>(key));
    const auto mask = destination->find(ColumnWithTypeAndName(std::move(probe), type, "k"));
    ASSERT_EQ(mask->size(), static_cast<size_t>(max - min + 1));
    for (Int64 key = min; key <= max; ++key)
        EXPECT_EQ(mask->getBool(static_cast<size_t>(key - min)), key == min || key == min + 1 || key == max || key == 1 || key == 3 || key == 5 || key == 7 || key == 9) << key;
}

TEST(RuntimeFilterLookup, DenseBitsetFindsExactlyTheKeys)
{
    tryRegisterFunctions();
    checkDenseBitsetFindsExactlyTheKeys<UInt8>();
    checkDenseBitsetFindsExactlyTheKeys<Int8>();
    checkDenseBitsetFindsExactlyTheKeys<UInt16>();
    checkDenseBitsetFindsExactlyTheKeys<Int16>();
}

TEST(RuntimeFilterLookup, AdaptiveFilterSwitchesAtTheFirstNewKeyPastTheLimit)
{
    /// Duplicates at the limit stay in the exact set. The first new key switches it to a dense bitset, which takes the
    /// rows after that key.
    tryRegisterFunctions();
    const auto type = std::make_shared<DataTypeUInt16>();
    auto make_keys = [](std::initializer_list<UInt16> keys)
    {
        auto column = ColumnUInt16::create();
        for (const UInt16 key : keys)
            column->insertValue(key);
        return column;
    };
    RuntimeFilter filter(
        /*filters_to_merge_=*/0,
        makeRuntimeFilterConfig(),
        RuntimeFilter::Adaptive(type, makeGeometry(1_MiB, /*exact_values_limit=*/2, 3, 1.0), std::nullopt, false, std::nullopt));
    const ProfileEvents::Count bitsets_before = ProfileEvents::global_counters[ProfileEvents::RuntimeFilterDenseBitsetsBuilt];

    filter.insert(make_keys({0}));
    filter.insert(make_keys({0, 5, 0, 5}));
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::RuntimeFilterDenseBitsetsBuilt] - bitsets_before, 0);
    filter.insert(make_keys({5, 0, 7, 8, 5, 0}));
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::RuntimeFilterDenseBitsetsBuilt] - bitsets_before, 1);
    filter.finishInsert();

    const auto description = filter.describe();
    EXPECT_EQ(description.kind, RuntimeFilterKind::DenseBitset);
    ASSERT_TRUE(description.keys.has_value());
    EXPECT_EQ(*description.keys, 4);
    expectMask(filter.find(ColumnWithTypeAndName(make_keys({0, 1, 5, 7, 8, 9}), type, "k")), {1, 0, 1, 1, 1, 0});
}

TEST(RuntimeFilterLookup, AdaptiveFilterRetriesTheSwitchFromTheExactSetAfterItThrew)
{
    /// The full exact set switches to an 8 MiB Bloom filter, which the memory limit refuses. The set keeps its keys, and
    /// the next block retries the switch, as after `ISimpleTransform` passed the exception downstream.
    const auto type = makeUInt64Type();
    RuntimeFilter filter(
        /*filters_to_merge_=*/0,
        makeRuntimeFilterConfig(),
        RuntimeFilter::Adaptive(type, makeGeometry(8_MiB, /*exact_values_limit=*/2, 3, 1.0), std::nullopt, false, std::nullopt));
    filter.insert(makeUInt64Column({0, 5}));

    ColumnPtr block = makeUInt64Column({7});
    ASSERT_TRUE(throwsAtMemoryLimit([&] { filter.insert(block); }));

    filter.insert(makeUInt64Column({6}));
    filter.finishInsert();
    /// No key is lost, and the probe is checked rather than skipped as for a dropped key set. Other values may pass
    /// the Bloom filter.
    expectMask(filter.find(makeUInt64ColumnWithType({0, 5, 6, 7}, type)), {1, 1, 1, 1});
    EXPECT_EQ(filter.getStats().rows_checked.load(), 4);
    EXPECT_EQ(filter.getStats().rows_skipped.load(), 0);
}

TEST(RuntimeFilterLookup, AdaptiveFilterKeepsTheRowsUpToTheFirstNewKeyWhenItsSwitchThrows)
{
    /// The exact set reaches its limit at key 5 and takes the duplicate 0 after it. The new key 7 fills it, and its switch
    /// to an 8 MiB Bloom filter is refused by the memory limit, so the set keeps the rows up to 7 and 8 is in no filter.
    const auto type = makeUInt64Type();
    RuntimeFilter filter(
        /*filters_to_merge_=*/0,
        makeRuntimeFilterConfig(),
        RuntimeFilter::Adaptive(type, makeGeometry(8_MiB, /*exact_values_limit=*/2, 3, 1.0), std::nullopt, false, std::nullopt));
    filter.enableIndexAnalysis();
    filter.insert(makeUInt64Column({0}));

    ColumnPtr block = makeUInt64Column({0, 5, 0, 7, 5, 8});
    ASSERT_TRUE(throwsAtMemoryLimit([&] { filter.insert(block); }));
    filter.finishInsert();

    /// A full set gives index analysis nothing.
    EXPECT_FALSE(filter.getRecordedKeyValues());
    expectMask(filter.find(makeUInt64ColumnWithType({0, 5, 7, 8}, type)), {1, 1, 1, 0});
    EXPECT_EQ(filter.getStats().rows_checked.load(), 4);
    EXPECT_EQ(filter.getStats().rows_skipped.load(), 0);
}

TEST(RuntimeFilterLookup, AdaptiveFilterSwitchesToABloomFilterAtTheFirstNewKeyPastTheLimit)
{
    /// The new key 7 fills the exact set, and the Bloom filter that replaces it takes the rows after 7.
    const auto type = makeUInt64Type();
    RuntimeFilter filter(
        /*filters_to_merge_=*/0,
        makeRuntimeFilterConfig(),
        RuntimeFilter::Adaptive(type, makeGeometry(1_MiB, /*exact_values_limit=*/2, 3, 1.0), std::nullopt, false, std::nullopt));
    filter.insert(makeUInt64Column({0}));
    filter.insert(makeUInt64Column({0, 5, 0, 7, 8, 5}));
    filter.finishInsert();

    EXPECT_EQ(filter.describe().kind, RuntimeFilterKind::BloomFilter);
    expectMask(filter.find(makeUInt64ColumnWithType({0, 5, 7, 8}, type)), {1, 1, 1, 1});
}

TEST(RuntimeFilterLookup, ReplacementFiltersTakeTheRowsFromFirstRow)
{
    /// The rows before `first_row` are in the exact set that seeds the replacement, so they are not inserted again.
    tryRegisterFunctions();
    const auto type = std::make_shared<DataTypeUInt16>();
    auto column = ColumnUInt16::create();
    for (UInt16 key = 1; key <= 4; ++key)
        column->insertValue(key);
    const ColumnPtr keys = std::move(column);
    const ColumnWithTypeAndName probe(keys, type, "k");
    std::optional<size_t> rows_passed;

    DenseBitsetRuntimeFilter bitset_filter(type);
    bitset_filter.insert(keys, /*first_row=*/2);
    expectMask(bitset_filter.find(probe, rows_passed), {0, 0, 1, 1});

    ApproximateSetRuntimeFilter bloom_filter(1_MiB, 3);
    bloom_filter.insert(keys, /*first_row=*/2);
    expectMask(bloom_filter.find(probe, rows_passed), {0, 0, 1, 1});
}

TEST(RuntimeFilterLookup, ExactSetReportsTheRowsItTookUntilItIsFull)
{
    /// With a limit of 2 keys, the second block is taken up to the new key 7, which fills the set.
    const auto type = makeUInt64Type();
    ExactSetRuntimeFilter<false> filter(type, /*bytes_limit_=*/1_MiB, /*exact_values_limit_=*/2);
    EXPECT_EQ(filter.insert(makeUInt64Column({0, 5, 0})), 3);
    EXPECT_FALSE(filter.isFull());
    EXPECT_EQ(filter.insert(makeUInt64Column({5, 7, 8, 0})), 2);
    EXPECT_TRUE(filter.isFull());
    EXPECT_EQ(filter.getValuesColumn()->size(), 3);
}

TEST(RuntimeFilterLookup, ExactNotContainsTakesTheWholeBlockPastTheLimit)
{
    /// `NOT IN` filters by every key of a full set, so it does not stop at the limit.
    const auto type = makeUInt64Type();
    ExactSetRuntimeFilter<true> filter(type, /*bytes_limit_=*/1_MiB, /*exact_values_limit_=*/2);
    EXPECT_EQ(filter.insert(makeUInt64Column({0, 5, 7, 8, 0})), 5);
    EXPECT_TRUE(filter.isFull());
    EXPECT_EQ(filter.getValuesColumn()->size(), 4);
}

TEST(RuntimeFilterLookup, AdaptiveFilterKeepsItsExactSetWhenMergingABloomFilterThrows)
{
    /// Merging a Bloom filter into the exact set allocates an 8 MiB Bloom filter, which the memory limit refuses. The
    /// destination keeps its keys and still takes the partials of the other build streams.
    const auto type = makeUInt64Type();
    const auto make_filter = [&](size_t filters_to_merge)
    {
        return std::make_unique<RuntimeFilter>(
            filters_to_merge,
            makeRuntimeFilterConfig(),
            RuntimeFilter::Adaptive(type, makeGeometry(8_MiB, /*exact_values_limit=*/2, 3, 1.0), std::nullopt, false, std::nullopt));
    };
    auto destination = make_filter(1);
    destination->insert(makeUInt64Column({0, 5}));
    auto approximate_source = make_filter(0);
    approximate_source->insert(makeUInt64Column({1, 2, 3}));
    approximate_source->finishInsert();

    ASSERT_TRUE(throwsAtMemoryLimit([&] { destination->merge(*approximate_source); }));

    auto exact_source = make_filter(0);
    exact_source->insert(makeUInt64Column({6}));
    exact_source->finishInsert();
    destination->merge(*exact_source);
    destination->finishInsert();
    ASSERT_TRUE(destination->isReady());
    expectMask(destination->find(makeUInt64ColumnWithType({0, 5, 6}, type)), {1, 1, 1});
    EXPECT_EQ(destination->getStats().rows_checked.load(), 3);
    EXPECT_EQ(destination->getStats().rows_skipped.load(), 0);
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
    EXPECT_EQ(pending_filter->getStats().rows_before_ready.load(), 3);
    EXPECT_EQ(pending_filter->describe().kind, RuntimeFilterKind::Incomplete);

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
        RuntimeFilter::Adaptive(type, makeGeometry(1_MiB, 100, 3, 1.0), std::nullopt, false, std::nullopt));
    destination.enableIndexAnalysis();
    destination.insert(makeUInt64Column({3, 7}));

    RuntimeFilter source(
        /*filters_to_merge_=*/0,
        makeRuntimeFilterConfig(),
        RuntimeFilter::Adaptive(type, makeGeometry(1_MiB, 100, 3, 1.0), std::nullopt, false, std::nullopt));
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

TEST(RuntimeFilterLookup, DescribeKeepsThePredictedBloomSaturationAcrossMerges)
{
    const auto type = makeUInt64Type();
    auto make_filter = [&](size_t filters_to_merge)
    {
        return RuntimeFilter(
            filters_to_merge,
            makeRuntimeFilterConfig(),
            RuntimeFilter::Adaptive(
                type,
                makeGeometry(1_KiB, 1, 3, 0.05),
                /*distinct_keys_hint_=*/2'000'000,
                /*distinct_keys_hint_matches_filter_key_=*/true,
                /*distinct_keys_in_last_run_=*/std::nullopt));
    };

    /// Only the source overflows its exact set; the destination learns of the dropped key set by the merge.
    auto destination = make_filter(/*filters_to_merge=*/1);
    destination.insert(makeUInt64Column({1}));
    auto source = make_filter(/*filters_to_merge=*/0);
    source.insert(makeUInt64Column({3, 5}));
    source.finishInsert();
    EXPECT_EQ(source.describe().disabled_reason, RuntimeFilterDisabledReason::BloomFilterPredictedSaturated);

    destination.merge(source);
    destination.finishInsert();

    const auto description = destination.describe();
    EXPECT_EQ(description.kind, RuntimeFilterKind::Disabled);
    EXPECT_EQ(description.disabled_reason, RuntimeFilterDisabledReason::BloomFilterPredictedSaturated);
    EXPECT_FALSE(description.bytes);
    EXPECT_FALSE(description.set_bits_ratio);
}

TEST(RuntimeFilterLookup, ReplacementKeepsStatsAndNamesTheReplacedFilter)
{
    const auto type = makeUInt64Type();
    auto lookup = createRuntimeFilterLookup();

    auto built_filter = std::make_unique<RuntimeFilter>(
        /*filters_to_merge_=*/0,
        makeRuntimeFilterConfig(),
        RuntimeFilter::ExactContains(type, /*bytes_limit_=*/1_MiB, /*exact_values_limit_=*/100));
    built_filter->insert(makeUInt64Column({3, 7}));
    lookup->add("runtime_filter", "runtime_filter", std::move(built_filter));

    auto existing = lookup->find("runtime_filter");
    ASSERT_TRUE(existing);
    expectMask(existing->find(makeUInt64ColumnWithType({1, 3}, type)), {0, 1});

    auto replacement = std::make_unique<RuntimeFilter>(
        /*filters_to_merge_=*/0,
        existing->getConfig(),
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
            }));
    lookup->replace("runtime_filter", std::move(replacement));

    auto filter = lookup->find("runtime_filter");
    ASSERT_TRUE(filter);
    expectMask(filter->find(makeUInt64ColumnWithType({5, 7, 9}, type)), {0, 1, 0});

    const auto description = filter->describe();
    EXPECT_EQ(description.kind, RuntimeFilterKind::SharedFixedHashMap);
    ASSERT_TRUE(description.replaced_kind);
    EXPECT_EQ(*description.replaced_kind, RuntimeFilterKind::ExactSet);

    EXPECT_EQ(&filter->getStats(), &existing->getStats());
    EXPECT_EQ(filter->getStats().blocks_processed.load(), 2);
    EXPECT_EQ(filter->getStats().rows_checked.load(), 5);
    EXPECT_EQ(filter->getStats().rows_passed.load(), 2);
}

}
