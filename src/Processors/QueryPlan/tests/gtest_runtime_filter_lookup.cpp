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
#include <vector>

namespace ProfileEvents
{
extern const Event RuntimeFilterBloomFilterBuildsSkipped;
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

/// Values [0, count), followed by `extra`.
MutableColumnPtr makeUInt64SequenceColumn(UInt64 count, std::initializer_list<UInt64> extra = {})
{
    auto column = ColumnUInt64::create();
    for (UInt64 value = 0; value < count; ++value)
        column->insertValue(value);
    for (const auto value : extra)
        column->insertValue(value);
    return column;
}

size_t countRejectedRows(const ColumnPtr & mask)
{
    size_t rejected = 0;
    for (size_t row = 0; row < mask->size(); ++row)
        rejected += !mask->getBool(row);
    return rejected;
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
            /*distinct_keys_hint_matches_filter_key_=*/false));

    EXPECT_EQ(filter.getFilterColumnTargetType(), type);
    filter.enableIndexAnalysis();
    const ProfileEvents::Count bitsets_before = ProfileEvents::global_counters[ProfileEvents::RuntimeFilterDenseBitsetsBuilt];
    /// The key range is too wide for a dense bitset, so the keys go to a Bloom filter.
    filter.insert(makeUInt64Column({1, 5'000'000'000}));
    filter.insert(makeUInt64Column({3}));
    filter.finishInsert();
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::RuntimeFilterDenseBitsetsBuilt] - bitsets_before, 0);

    EXPECT_EQ(filter.getFilterColumnTargetType(), type);
    EXPECT_FALSE(filter.getRecordedKeyValues());
    auto range = filter.getRecordedKeyRanges();
    ASSERT_TRUE(range);
    EXPECT_EQ(range->left.safeGet<UInt64>(), 1);
    EXPECT_EQ(range->right.safeGet<UInt64>(), 5'000'000'000);
    expectMask(filter.find(makeUInt64ColumnWithType({1, 3, 5'000'000'000}, type)), {1, 1, 1});
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
            /*distinct_keys_hint_matches_filter_key_=*/true));
    destination.enableIndexAnalysis();
    destination.insert(makeUInt64Column({1}));

    RuntimeFilter source(
        /*filters_to_merge_=*/0,
        makeRuntimeFilterConfig(),
        RuntimeFilter::Adaptive(
            type,
            makeGeometry(1_KiB, 1, 3, 0.05),
            /*distinct_keys_hint_=*/2'000'000,
            /*distinct_keys_hint_matches_filter_key_=*/true));
    source.enableIndexAnalysis();
    /// The key range is too wide for a dense bitset, so the keys go to a Bloom filter, whose saturation is predicted.
    source.insert(makeUInt64Column({3, 5'000'000'000}));
    source.finishInsert();

    destination.merge(source);
    destination.finishInsert();

    EXPECT_FALSE(destination.getRecordedKeyValues());
    auto range = destination.getRecordedKeyRanges();
    ASSERT_TRUE(range);
    EXPECT_EQ(range->left.safeGet<UInt64>(), 1);
    EXPECT_EQ(range->right.safeGet<UInt64>(), 5'000'000'000);
    expectMask(destination.find(makeUInt64ColumnWithType({2, 4}, type)), {1, 1});
    EXPECT_EQ(destination.getStats().rows_checked.load(), 0);
    EXPECT_EQ(destination.getStats().rows_skipped.load(), 2);
}

TEST(RuntimeFilterLookup, DenseBitsetReplacesPredictedSaturatedBloomFilter)
{
    /// The same filters as above, but the keys fit a dense bitset: the filter stays exact instead of being dropped.
    const auto type = makeUInt64Type();
    const auto make_filter = [&](size_t filters_to_merge)
    {
        return RuntimeFilter(
            filters_to_merge,
            makeRuntimeFilterConfig(),
            RuntimeFilter::Adaptive(
                type,
                makeGeometry(1_KiB, 1, 3, 0.05),
                /*distinct_keys_hint_=*/2'000'000,
                /*distinct_keys_hint_matches_filter_key_=*/true));
    };
    RuntimeFilter destination = make_filter(1);
    destination.enableIndexAnalysis();
    destination.insert(makeUInt64Column({1}));

    RuntimeFilter source = make_filter(0);
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
    expectMask(destination.find(makeUInt64ColumnWithType({0, 1, 2, 3, 4, 5, 6, 64, 1000}, type)), {0, 1, 0, 1, 0, 1, 0, 0, 0});
    EXPECT_EQ(destination.getStats().rows_checked.load(), 9);
    EXPECT_EQ(destination.getStats().rows_passed.load(), 3);
}

TEST(RuntimeFilterLookup, DenseBitsetAllowanceGrowsOnlyWithStatisticsOfTheFilterKey)
{
    /// 16 x 1 KiB of bits cover 131072 values, too few for keys up to 1000000. Statistics of 100000 distinct values of
    /// the filter key raise the allowance to 800000 bytes; statistics of another key do not; nothing exceeds 32 MiB.
    const auto type = makeUInt64Type();
    const auto make_filter = [&](std::optional<UInt64> distinct_keys_hint, bool distinct_keys_hint_matches_filter_key)
    {
        return std::make_unique<RuntimeFilter>(
            /*filters_to_merge_=*/0,
            makeRuntimeFilterConfig(),
            RuntimeFilter::Adaptive(
                type, makeGeometry(1_KiB, 1, 3, 0.05), distinct_keys_hint, distinct_keys_hint_matches_filter_key));
    };

    {
        SCOPED_TRACE("statistics of the filter key");
        const ProfileEvents::Count bitsets_before = ProfileEvents::global_counters[ProfileEvents::RuntimeFilterDenseBitsetsBuilt];
        auto filter = make_filter(100'000, true);
        filter->insert(makeUInt64Column({0, 1, 2, 1'000'000}));
        filter->finishInsert();
        EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::RuntimeFilterDenseBitsetsBuilt] - bitsets_before, 1);
        expectMask(filter->find(makeUInt64ColumnWithType({0, 1, 2, 3, 999'999, 1'000'000, 1'000'001}, type)), {1, 1, 1, 0, 0, 1, 0});
        EXPECT_EQ(filter->getStats().rows_checked.load(), 7);
        EXPECT_EQ(filter->getStats().rows_passed.load(), 4);
    }
    {
        SCOPED_TRACE("statistics of another key");
        const ProfileEvents::Count bitsets_before = ProfileEvents::global_counters[ProfileEvents::RuntimeFilterDenseBitsetsBuilt];
        auto filter = make_filter(100'000, false);
        filter->insert(makeUInt64Column({0, 1, 2, 1'000'000}));
        filter->finishInsert();
        EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::RuntimeFilterDenseBitsetsBuilt] - bitsets_before, 0);
        EXPECT_EQ(countRejectedRows(filter->find(makeUInt64ColumnWithType({0, 1, 2, 1'000'000}, type))), 0);
    }
    {
        SCOPED_TRACE("statistics above the 32 MiB cap");
        const ProfileEvents::Count bitsets_before = ProfileEvents::global_counters[ProfileEvents::RuntimeFilterDenseBitsetsBuilt];
        auto filter = make_filter(100'000'000, true);
        filter->insert(makeUInt64Column({0, 1, 2, 300'000'000}));
        filter->finishInsert();
        EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::RuntimeFilterDenseBitsetsBuilt] - bitsets_before, 0);
        EXPECT_EQ(countRejectedRows(filter->find(makeUInt64ColumnWithType({0, 1, 2, 300'000'000}, type))), 0);
    }
}

TEST(RuntimeFilterLookup, DenseBitsetMergesPartialFiltersInAnyRepresentation)
{
    /// Partial filters of several build streams: exact sets (one empty), bitsets that keep sparse keys as values, and
    /// bitsets over dense keys at different words. In every merge order, with unfinished sources as in
    /// `RuntimeFilterLookup::add` and with finished ones, the merged filter is one bitset with the exact union.
    /// A Bloom filter would be dropped (at most 1e-6 of its bits may be set) and pass every row.
    const auto type = makeUInt64Type();
    const auto make_filter = [&](size_t filters_to_merge)
    {
        /// 16 x 64 KiB of bits cover 8M values; at most 2 exact keys.
        return std::make_unique<RuntimeFilter>(
            filters_to_merge,
            makeRuntimeFilterConfig(),
            RuntimeFilter::Adaptive(type, makeGeometry(64_KiB, 2, 3, 0.000001), std::nullopt, false));
    };
    const auto make_column = [](const std::vector<UInt64> & values)
    {
        auto column = ColumnUInt64::create();
        for (const auto value : values)
            column->insertValue(value);
        return column;
    };
    const std::vector<std::vector<UInt64>> parts = {
        {7},
        {1000, 100000, 8000000},
        {},
        {64, 65, 66, 67, 68, 69, 70, 71, 72},
        {120005, 120006, 120007, 120008, 120009, 120010, 120011, 120012, 120013, 120014},
        {5000000, 6000000, 7000000}};
    for (const bool finish_sources : {false, true})
    {
        for (size_t first = 0; first < parts.size(); ++first)
        {
            SCOPED_TRACE(fmt::format("finish_sources {}, first {}", finish_sources, first));
            const ProfileEvents::Count bitsets_before = ProfileEvents::global_counters[ProfileEvents::RuntimeFilterDenseBitsetsBuilt];
            auto destination = make_filter(parts.size() - 1);
            destination->insert(make_column(parts[first]));
            for (size_t i = 0; i < parts.size(); ++i)
            {
                if (i == first)
                    continue;
                auto source = make_filter(0);
                source->insert(make_column(parts[i]));
                if (finish_sources)
                    source->finishInsert();
                destination->merge(*source);
            }
            destination->finishInsert();
            /// The four parts with more than 2 keys switched to a bitset; the exact ones did not.
            EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::RuntimeFilterDenseBitsetsBuilt] - bitsets_before, 4);
            expectMask(
                destination->find(makeUInt64ColumnWithType(
                    {7, 8, 63, 64, 72, 73, 1000, 1001, 100000, 120004, 120005, 120014, 120015,
                     5000000, 6000000, 7000000, 7000001, 7999999, 8000000, 8000001, 9000000, 5},
                    type)),
                {1, 0, 0, 1, 1, 0, 1, 0, 1, 0, 1, 1, 0, 1, 1, 1, 0, 0, 1, 0, 0, 0});
            EXPECT_EQ(destination->getStats().rows_checked.load(), 22);
            EXPECT_EQ(destination->getStats().rows_passed.load(), 11);
        }
    }
}

TEST(RuntimeFilterLookup, DenseBitsetMovesKeysToBloomFilterWhenTheRangeOutgrowsTheAllowance)
{
    /// 16 x 1 KiB of bits cover 131072 values.
    const auto type = makeUInt64Type();
    RuntimeFilter filter(
        /*filters_to_merge_=*/0,
        makeRuntimeFilterConfig(),
        RuntimeFilter::Adaptive(type, makeGeometry(1_KiB, 2, 3, 1.0), std::nullopt, false));
    filter.insert(makeUInt64Column({10, 20, 30}));
    filter.insert(makeUInt64Column({131000}));
    filter.insert(makeUInt64Column({200000}));
    filter.finishInsert();
    /// No key is lost; other values may pass the Bloom filter.
    expectMask(filter.find(makeUInt64ColumnWithType({10, 20, 30, 131000, 200000}, type)), {1, 1, 1, 1, 1});
}

TEST(RuntimeFilterLookup, DenseBitsetMovesManyKeysToBloomFilterInBatches)
{
    /// 16 x 64 KiB of bits cover 8M values. The bitset holds more keys than one batch of the move to the Bloom filter.
    const auto type = makeUInt64Type();
    RuntimeFilter filter(
        /*filters_to_merge_=*/0,
        makeRuntimeFilterConfig(),
        RuntimeFilter::Adaptive(type, makeGeometry(64_KiB, 2, 3, 1.0), std::nullopt, false));
    filter.insert(makeUInt64SequenceColumn(200000));
    filter.insert(makeUInt64Column({10'000'000'000}));
    filter.finishInsert();

    EXPECT_EQ(countRejectedRows(filter.find(ColumnWithTypeAndName(makeUInt64SequenceColumn(200000, {10'000'000'000}), type, "k"))), 0);
    EXPECT_EQ(filter.getStats().rows_passed.load(), 200001);
}

TEST(RuntimeFilterLookup, DenseBitsetMergedIntoBloomFilterInBatches)
{
    const auto type = makeUInt64Type();
    const auto make_filter = [&](size_t filters_to_merge)
    {
        return RuntimeFilter(
            filters_to_merge, makeRuntimeFilterConfig(), RuntimeFilter::Adaptive(type, makeGeometry(64_KiB, 2, 3, 1.0), std::nullopt, false));
    };
    /// The key range is too wide for a dense bitset, so the destination has a Bloom filter.
    RuntimeFilter destination = make_filter(1);
    destination.insert(makeUInt64Column({0, 5, 10'000'000'000}));

    RuntimeFilter source = make_filter(0);
    source.insert(makeUInt64SequenceColumn(200000));
    source.finishInsert();

    destination.merge(source);
    destination.finishInsert();

    EXPECT_EQ(
        countRejectedRows(destination.find(ColumnWithTypeAndName(makeUInt64SequenceColumn(200000, {10'000'000'000}), type, "k"))), 0);
    EXPECT_EQ(destination.getStats().rows_passed.load(), 200001);
}

TEST(RuntimeFilterLookup, DenseBitsetDropsKeysThatWouldSaturateTheBloomFilter)
{
    /// 8192 Bloom bits, and 16 x 1 KiB of bits for the dense bitset cover 131072 values.
    const auto type = makeUInt64Type();
    RuntimeFilter filter(
        /*filters_to_merge_=*/0,
        makeRuntimeFilterConfig(),
        RuntimeFilter::Adaptive(type, makeGeometry(1_KiB, 2, 3, 0.7), std::nullopt, false));
    filter.insert(makeUInt64SequenceColumn(100000));

    const ProfileEvents::Count skipped_before = ProfileEvents::global_counters[ProfileEvents::RuntimeFilterBloomFilterBuildsSkipped];
    filter.insert(makeUInt64Column({10'000'000'000}));
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::RuntimeFilterBloomFilterBuildsSkipped] - skipped_before, 1);
    filter.finishInsert();

    expectMask(filter.find(makeUInt64ColumnWithType({1, 2}, type)), {1, 1});
    EXPECT_EQ(filter.getStats().rows_checked.load(), 0);
    EXPECT_EQ(filter.getStats().rows_skipped.load(), 2);
}

TEST(RuntimeFilterLookup, DenseBitsetHandlesSignedKeys)
{
    tryRegisterFunctions();
    const auto type = std::make_shared<DataTypeInt64>();
    RuntimeFilter filter(
        /*filters_to_merge_=*/0,
        makeRuntimeFilterConfig(),
        RuntimeFilter::Adaptive(type, makeGeometry(1_KiB, 2, 3, 1.0), std::nullopt, false));
    auto build = ColumnInt64::create();
    for (const Int64 value : std::initializer_list<Int64>{-1000, -65, -64, -1, 0, 63, 5000})
        build->insertValue(value);
    filter.insert(std::move(build));
    filter.finishInsert();

    auto probe = ColumnInt64::create();
    for (const Int64 value : std::initializer_list<Int64>{
             -1001, -1000, -66, -65, -64, -63, -2, -1, 0, 1, 63, 64, 5000, 5001,
             std::numeric_limits<Int64>::min(), std::numeric_limits<Int64>::max()})
        probe->insertValue(value);
    expectMask(filter.find(ColumnWithTypeAndName(std::move(probe), type, "k")), {0, 1, 0, 1, 1, 0, 0, 1, 1, 0, 1, 0, 1, 0, 0, 0});
}

TEST(RuntimeFilterLookup, DenseBitsetAllocationIsRefusedAtTheMemoryLimit)
{
    /// 16 x 1 MiB of bits cover 128M values. Three keys over 100M values stay as values until `finishInsert` allocates
    /// 12.5 MB of bits, which the memory limit refuses as it would a Bloom filter's. The filter stays usable.
    const auto type = makeUInt64Type();
    RuntimeFilter filter(
        /*filters_to_merge_=*/0,
        makeRuntimeFilterConfig(),
        RuntimeFilter::Adaptive(type, makeGeometry(1_MiB, 2, 3, 1.0), std::nullopt, false));
    filter.insert(makeUInt64Column({0, 5, 100'000'000}));

    ASSERT_TRUE(throwsAtMemoryLimit([&] { filter.finishInsert(); }));
    EXPECT_FALSE(filter.isReady());

    filter.finishInsert();
    expectMask(filter.find(makeUInt64ColumnWithType({0, 1, 5, 99'999'999, 100'000'000, 100'000'001}, type)), {1, 0, 1, 0, 1, 0});
}

TEST(RuntimeFilterLookup, DenseBitsetKeepsItsKeysWhenAnAppendThrows)
{
    /// The bitset keeps three keys over 100M values as values, and appending 1M more (8 MB) reaches the memory limit.
    /// The stream goes on as after `ISimpleTransform` passed the exception downstream; the failed block is not recorded.
    const auto type = makeUInt64Type();
    RuntimeFilter filter(
        /*filters_to_merge_=*/0,
        makeRuntimeFilterConfig(),
        RuntimeFilter::Adaptive(type, makeGeometry(1_MiB, 2, 3, 1.0), std::nullopt, false));
    filter.insert(makeUInt64Column({0, 5, 100'000'000}));

    ColumnPtr block = makeUInt64SequenceColumn(1'000'000);
    ASSERT_TRUE(throwsAtMemoryLimit([&] { filter.insert(block); }));

    filter.insert(makeUInt64Column({6}));
    filter.finishInsert();
    expectMask(filter.find(makeUInt64ColumnWithType({0, 5, 6, 7, 100'000'000}, type)), {1, 1, 1, 0, 1});
}

TEST(RuntimeFilterLookup, DenseBitsetKeepsItsKeysWhenAMergeThrows)
{
    /// Both partials keep their keys as values, and taking the source's 1M keys (8 MB) reaches the memory limit.
    /// The destination still takes the next partial, as when the build streams that remain add theirs.
    const auto type = makeUInt64Type();
    const auto make_filter = [&](size_t filters_to_merge)
    {
        /// 16 x 1 MiB of bits cover 128M values; at most 2 exact keys.
        return std::make_unique<RuntimeFilter>(
            filters_to_merge,
            makeRuntimeFilterConfig(),
            RuntimeFilter::Adaptive(type, makeGeometry(1_MiB, 2, 3, 1.0), std::nullopt, false));
    };
    auto destination = make_filter(1);
    destination->insert(makeUInt64Column({0, 5, 100'000'000}));
    auto source = make_filter(0);
    source->insert(makeUInt64SequenceColumn(1'000'000, {100'000'000}));
    ASSERT_TRUE(source->hasPendingKeys());

    ASSERT_TRUE(throwsAtMemoryLimit([&] { destination->merge(*source); }));
    EXPECT_EQ(destination->getFiltersToMerge(), 1);

    auto other = make_filter(0);
    other->insert(makeUInt64Column({6, 7, 100'000'001}));
    destination->merge(*other);
    destination->finishInsert();
    ASSERT_TRUE(destination->isReady());
    expectMask(
        destination->find(makeUInt64ColumnWithType({0, 5, 6, 7, 8, 100'000'000, 100'000'001, 100'000'002}, type)),
        {1, 1, 1, 1, 0, 1, 1, 0});
}

TEST(RuntimeFilterLookup, AdaptiveFilterRetriesTheSwitchFromTheExactSetAfterItThrew)
{
    /// Keys over 1e10 values do not fit a bitset, so the full exact set switches to an 8 MiB Bloom filter, which the
    /// memory limit refuses. The exact set stays full, and the next block retries the switch.
    const auto type = makeUInt64Type();
    RuntimeFilter filter(
        /*filters_to_merge_=*/0,
        makeRuntimeFilterConfig(),
        RuntimeFilter::Adaptive(type, makeGeometry(8_MiB, 2, 3, 1.0), std::nullopt, false));
    filter.insert(makeUInt64Column({0, 5}));

    ColumnPtr block = makeUInt64Column({10'000'000'000});
    ASSERT_TRUE(throwsAtMemoryLimit([&] { filter.insert(block); }));

    filter.insert(makeUInt64Column({6}));
    filter.finishInsert();
    /// No key is lost; other values may pass the Bloom filter.
    expectMask(filter.find(makeUInt64ColumnWithType({0, 5, 6, 10'000'000'000}, type)), {1, 1, 1, 1});
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

TEST(RuntimeFilterLookup, LookupMergesPartialsWithPendingKeysInPairs)
{
    /// Dense bitset partials that keep their keys as values (3 keys over 2M values; 16 x 64 KiB of bits cover 8M values)
    /// are merged with each other; the registered filter takes their union in one merge when the last expected partial
    /// is added, so its count of expected merges stays 3 until then.
    const auto type = makeUInt64Type();
    auto lookup = createRuntimeFilterLookup();
    const auto make_filter = [&](UInt64 key)
    {
        auto filter = std::make_unique<RuntimeFilter>(
            /*filters_to_merge_=*/3,
            makeRuntimeFilterConfig(),
            RuntimeFilter::Adaptive(type, makeGeometry(64_KiB, 2, 3, 1.0), std::nullopt, false));
        filter->insert(makeUInt64Column({key, 1'000'000 + key, 2'000'000 + key}));
        EXPECT_TRUE(filter->hasPendingKeys());
        return filter;
    };
    lookup->add("runtime_filter", "runtime_filter", make_filter(1));
    auto filter = lookup->find("runtime_filter");
    ASSERT_TRUE(filter);
    for (const UInt64 key : {2, 3})
    {
        lookup->add("runtime_filter", "runtime_filter", make_filter(key));
        EXPECT_FALSE(filter->isReady());
        EXPECT_EQ(filter->getFiltersToMerge(), 3);
    }
    lookup->add("runtime_filter", "runtime_filter", make_filter(4));
    ASSERT_TRUE(filter->isReady());
    expectMask(
        filter->find(makeUInt64ColumnWithType({1, 2, 3, 4, 5, 1'000'001, 1'000'004, 1'000'005, 2'000'003, 2'000'000}, type)),
        {1, 1, 1, 1, 0, 1, 1, 0, 1, 0});
}

TEST(RuntimeFilterLookup, LookupMergesPartialsOfConcurrentStreams)
{
    /// All streams add their partials at once: half keep sparse keys as values and are merged in pairs, half have
    /// their dense keys in bits and are merged directly. The registered filter becomes ready with every key.
    const auto type = makeUInt64Type();
    constexpr UInt64 streams = 32;
    auto lookup = createRuntimeFilterLookup();
    std::barrier<> sync(streams);
    std::vector<std::thread> threads;
    std::vector<UInt64> keys;
    for (UInt64 stream = 0; stream < streams; ++stream)
    {
        if (stream % 2 == 0)
            keys.insert(keys.end(), {stream, 1'000'000 + stream, 2'000'000 + stream});
        else
            for (UInt64 i = 0; i < 200; ++i)
                keys.push_back(3'000'000 + stream * 256 + i);
    }
    for (UInt64 stream = 0; stream < streams; ++stream)
    {
        threads.emplace_back(
            [&, stream]
            {
                auto filter = std::make_unique<RuntimeFilter>(
                    streams - 1,
                    makeRuntimeFilterConfig(),
                    RuntimeFilter::Adaptive(type, makeGeometry(64_KiB, 2, 3, 1.0), std::nullopt, false));
                auto column = ColumnUInt64::create();
                for (const UInt64 key : keys)
                    if ((key < 3'000'000 ? key % 1'000'000 : (key - 3'000'000) / 256) == stream)
                        column->insertValue(key);
                filter->insert(std::move(column));
                EXPECT_EQ(filter->hasPendingKeys(), stream % 2 == 0);
                sync.arrive_and_wait();
                lookup->add("runtime_filter", "runtime_filter", std::move(filter));
            });
    }
    for (auto & thread : threads)
        thread.join();

    auto filter = lookup->find("runtime_filter");
    ASSERT_TRUE(filter);
    ASSERT_TRUE(filter->isReady());
    auto probe = ColumnUInt64::create();
    for (const UInt64 key : keys)
        probe->insertValue(key);
    for (UInt64 stream = 0; stream < streams; ++stream)
    {
        probe->insertValue(500'000 + stream);
        probe->insertValue(3'000'000 + stream * 256 + (stream % 2 == 0 ? 0 : 200));
        probe->insertValue(3'000'000 + stream * 256 + 255);
    }
    const auto mask = filter->find(ColumnWithTypeAndName(std::move(probe), type, "k"));
    ASSERT_EQ(mask->size(), keys.size() + 3 * streams);
    for (size_t row = 0; row < mask->size(); ++row)
        EXPECT_EQ(mask->getUInt(row), row < keys.size() ? 1 : 0) << row;
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
        RuntimeFilter::Adaptive(type, makeGeometry(1_MiB, 100, 3, 1.0), std::nullopt, false));
    destination.enableIndexAnalysis();
    destination.insert(makeUInt64Column({3, 7}));

    RuntimeFilter source(
        /*filters_to_merge_=*/0,
        makeRuntimeFilterConfig(),
        RuntimeFilter::Adaptive(type, makeGeometry(1_MiB, 100, 3, 1.0), std::nullopt, false));
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
