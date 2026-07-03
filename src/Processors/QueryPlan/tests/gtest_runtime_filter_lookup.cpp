#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Common/tests/gtest_global_register.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <base/unit.h>

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
    return RuntimeFilterConfig{
        /*pass_ratio_threshold_for_disabling=*/1.0,
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
            /*max_ratio_of_set_bits_in_bloom_filter_=*/1.0));

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

TEST(RuntimeFilterLookup, SharedFixedHashTableRuntimeFilterDelegatesProbe)
{
    const auto type = makeUInt64Type();
    RuntimeFilter filter(
        /*filters_to_merge_=*/0,
        makeRuntimeFilterConfig(),
        RuntimeFilter::SharedFixedHashTable(
            [](const ColumnWithTypeAndName & values)
            {
                auto result = ColumnUInt8::create();
                auto & result_data = result->getData();
                result_data.resize(values.column->size());
                for (size_t row = 0; row < values.column->size(); ++row)
                    result_data[row] = values.column->getUInt(row) == 3 || values.column->getUInt(row) == 7;
                return result;
            }));

    expectMask(filter.find(makeUInt64ColumnWithType({1, 3, 5, 7}, type)), {0, 1, 0, 1});
    EXPECT_EQ(filter.getStats().rows_checked.load(), 4);
    EXPECT_EQ(filter.getStats().rows_passed.load(), 2);
}

}
