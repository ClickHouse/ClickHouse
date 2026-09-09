#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnsNumber.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/tests/gtest_disk.h>
#include <Interpreters/Aggregator.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

class AggregatorMemoryUsage : public ::testing::TestWithParam<String>
{
protected:
    ThreadStatus thread_status;
    DiskPtr disk;
    Block header{{std::make_shared<DataTypeUInt64>(), "k"}};

    void SetUp() override
    {
        getContext();
        tryRegisterAggregateFunctions();
        disk = createDisk("aggregator_memory_usage");
    }

    void TearDown() override
    {
        destroyDisk(disk);
    }

    Aggregator::Params makeParams() const
    {
        AggregateDescriptions aggregates;
        const auto & function_name = GetParam();
        if (!function_name.empty())
        {
            AggregateDescription description;
            AggregateFunctionProperties properties;
            description.argument_names = {"k"};
            description.column_name = function_name;
            description.function = AggregateFunctionFactory::instance().get(
                function_name, NullsAction::EMPTY, {header.getByName("k").type}, {}, properties);
            aggregates.push_back(std::move(description));
        }

        auto volume = std::make_shared<SingleDiskVolume>("temporary", disk);
        auto tmp_data_scope = std::make_shared<TemporaryDataOnDiskScope>(TemporaryDataOnDiskSettings{}, volume);
        return Aggregator::Params(
            Names{"k"},
            aggregates,
            /*overflow_row=*/false,
            /*max_rows_to_group_by=*/0,
            OverflowMode::THROW,
            /*max_bytes_to_group_by=*/2 * 1024 * 1024,
            /*limit_errors=*/{},
            /*group_by_two_level_threshold=*/1,
            /*group_by_two_level_threshold_bytes=*/0,
            /*max_bytes_before_external_group_by=*/0,
            /*empty_result_for_aggregation_by_empty_set=*/false,
            std::move(tmp_data_scope),
            /*max_threads=*/1,
            /*min_free_disk_space=*/0,
            /*compile_aggregate_expressions=*/false,
            /*min_count_to_compile_aggregate_expression=*/0,
            /*max_block_size=*/65536,
            /*enable_prefetch=*/false,
            /*only_merge=*/false,
            /*optimize_group_by_constant_keys=*/false,
            /*min_hit_rate_to_use_consecutive_keys_optimization=*/0.5f,
            StatsCollectingParams{},
            /*enable_producing_buckets_out_of_order_in_aggregation=*/true,
            /*serialize_string_with_zero_byte=*/false,
            /*enable_parallel_single_level_merge=*/false,
            /*enable_packed_string_keys=*/true,
            /*enable_adaptive_aggregator=*/false,
            /*adaptive_aggregator_freeze_threshold=*/0,
            /*adaptive_aggregator_freeze_threshold_bytes=*/0);
    }

    void aggregate(Aggregator & aggregator, AggregatedDataVariants & variants, size_t rows) const
    {
        auto column = ColumnUInt64::create();
        for (UInt64 key = 0; key < rows; ++key)
            column->insertValue(key);

        ColumnRawPtrs key_columns(1);
        Aggregator::AggregateColumns aggregate_columns(aggregator.getParams().aggregates_size);
        bool no_more_keys = false;
        ASSERT_TRUE(aggregator.executeOnBlock(
            Columns{std::move(column)}, 0, rows, variants, key_columns, aggregate_columns, no_more_keys, nullptr));
        ASSERT_TRUE(variants.isTwoLevel());
        ASSERT_EQ(variants.sizeWithoutOverflowRow(), rows);
    }
};

TEST_P(AggregatorMemoryUsage, SpillReinitializesBufferAccounting)
{
    Aggregator aggregator(header, makeParams());
    AggregatedDataVariants variants;
    for (size_t rows : {1, 10000, 10000})
    {
        ASSERT_NO_FATAL_FAILURE(aggregate(aggregator, variants, rows));
        aggregator.writeToTemporaryFile(variants);
        EXPECT_TRUE(variants.isTwoLevel());
        EXPECT_EQ(variants.sizeWithoutOverflowRow(), 0);
        EXPECT_EQ(variants.accounted_bytes, variants.allocatedBytes());
    }
}

TEST_P(AggregatorMemoryUsage, ConsumingSpillReleasesBufferAccounting)
{
    Aggregator aggregator(header, makeParams());
    for (size_t rows : {1, 10000, 10000})
    {
        AggregatedDataVariants variants;
        ASSERT_NO_FATAL_FAILURE(aggregate(aggregator, variants, rows));
        aggregator.consumeToTemporaryFile(variants);
        EXPECT_TRUE(variants.empty());
        EXPECT_EQ(variants.accounted_bytes, 0);
    }
}

INSTANTIATE_TEST_SUITE_P(
    AggregateMethods,
    AggregatorMemoryUsage,
    ::testing::Values(String{}, String{"count"}, String{"sum"}),
    [](const ::testing::TestParamInfo<String> & test_info)
    {
        return test_info.param.empty() ? "KeysOnly" : test_info.param;
    });

}
