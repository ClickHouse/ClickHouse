#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <base/unit.h>
#include <Common/tests/gtest_global_register.h>

#include <initializer_list>
#include <memory>

namespace DB
{
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
            /*distinct_keys_hint=*/std::nullopt));

    filter.insert(makeUInt64Column({1, 3}));
    filter.insert(makeUInt64Column({5}));
    filter.finishInsert();

    expectMask(filter.find(makeUInt64ColumnWithType({1, 3, 5}, type)), {1, 1, 1});
    EXPECT_EQ(filter.getStats().rows_checked.load(), 3);
    EXPECT_EQ(filter.getStats().rows_passed.load(), 3);
}

TEST(RuntimeFilterLookup, LookupMergesExactContainsFilters)
{
    const auto type = makeUInt64Type();
    auto lookup = createRuntimeFilterLookup();

    auto first_filter = std::make_unique<RuntimeFilter>(
        /*filters_to_merge_=*/1,
        makeRuntimeFilterConfig(),
        RuntimeFilter::ExactContains(
            type,
            /*bytes_limit_=*/1_MiB,
            /*exact_values_limit_=*/100));
    first_filter->insert(makeUInt64Column({1}));
    lookup->add("runtime_filter", "runtime_filter", std::move(first_filter));

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
    expectMask(filter->find(makeUInt64ColumnWithType({1, 3, 5}, type)), {1, 0, 1});
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

    destination.merge(&source);
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

    destination.merge(&source);
    destination.finishInsert();

    expectMask(destination.find(makeUInt64ColumnWithType({1, 2}, type)), {1, 0});
}

TEST(RuntimeFilterLookup, IndexAnalysisRecordsExactValuesAndMergedRange)
{
    const auto type = makeUInt64Type();
    RuntimeFilter destination(
        /*filters_to_merge_=*/1, makeRuntimeFilterConfig(), RuntimeFilter::Adaptive(type, 1_MiB, 100, 3, 1.0, std::nullopt));
    destination.enableIndexAnalysis();
    destination.insert(makeUInt64Column({3, 7}));

    RuntimeFilter source(
        /*filters_to_merge_=*/0, makeRuntimeFilterConfig(), RuntimeFilter::Adaptive(type, 1_MiB, 100, 3, 1.0, std::nullopt));
    source.enableIndexAnalysis();
    source.insert(makeUInt64Column({1, 9}));
    source.finishInsert();

    destination.merge(&source);
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

    auto incomplete_filter = lookup->find("runtime_filter");
    ASSERT_TRUE(incomplete_filter);
    auto recorded_key_range = incomplete_filter->getRecordedKeyRanges();
    auto recorded_key_values = incomplete_filter->getRecordedKeyValues();
    EXPECT_FALSE(recorded_key_range);
    EXPECT_FALSE(recorded_key_values);

    /// Simulate post-build publication. The probe sees the complete hash table, including key 1
    /// whose stream-local filter has not registered yet. The unfinished filter contributes no
    /// metadata, so read-side index analysis fails open instead of pruning by {3, 7}.
    auto shared_filter = std::make_unique<RuntimeFilter>(
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
    expectMask(filter->find(makeUInt64ColumnWithType({1, 3, 7}, type)), {1, 1, 1});
    EXPECT_FALSE(filter->getRecordedKeyValues());
    EXPECT_FALSE(filter->getRecordedKeyRanges());
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
            }));

    EXPECT_NO_THROW(filter.insert(makeUInt64Column({42})));
    EXPECT_FALSE(filter.getRecordedKeyRanges());
    expectMask(filter.find(makeUInt64ColumnWithType({1, 3, 5, 7, 42}, type)), {0, 1, 0, 1, 0});
    EXPECT_EQ(filter.getStats().rows_checked.load(), 5);
    EXPECT_EQ(filter.getStats().rows_passed.load(), 2);
}

}
