#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

using namespace DB;

/// Regression test for the read_stream_count_was_reduced flag leaking past its consumer.
///
/// ReadFromMergeTree sets QueryPipelineBuilder::read_stream_count_was_reduced when it
/// deliberately produces fewer streams than requested (small data). AggregatingStep is the
/// sole consumer: it uses the flag to cap max_threads, then must clear it. If it does not,
/// the flag persists on the builder and leaks into downstream pipeline compositions (JOIN
/// returns the left builder, UNION/merge can carry it through), where a later AggregatingStep
/// would wrongly cap its stream count, producing a LOGICAL_ERROR "Port is not connected".
///
/// This asserts the invariant directly: after transformPipeline, the flag is cleared.
/// See https://github.com/ClickHouse/clickhouse-core-incidents/issues/1640
TEST(AggregatingStep, ClearsReadStreamCountWasReducedFlag)
{
    MainThreadStatus::getInstance();
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    const auto & context_holder = getContext();
    auto context = Context::createCopy(context_holder.context);

    /// Minimal header: a single UInt64 key column "k".
    auto type = std::make_shared<DataTypeUInt64>();
    Block header({ColumnWithTypeAndName(type->createColumn(), type, "k")});
    auto shared_header = std::make_shared<const Block>(header);

    /// Aggregator params: group by "k", no aggregate functions.
    /// The merge-only constructor is the simplest path through transformPipeline.
    Aggregator::Params agg_params(
        Names{"k"},              // keys
        AggregateDescriptions{}, // aggregates
        false,                   // overflow_row
        1,                       // max_threads
        65536,                   // max_block_size
        0.5f,                    // min_hit_rate_to_use_consecutive_keys_optimization
        false);                  // serialize_string_with_zero_byte

    AggregatingStep step(
        shared_header,
        agg_params,
        GroupingSetsParamsList{}, // empty → no grouping sets
        /*final=*/true,
        /*max_block_size=*/65536,
        /*aggregation_in_order_max_block_bytes=*/0,
        /*merge_threads=*/1,
        /*temporary_data_merge_threads=*/1,
        /*storage_has_evenly_distributed_read=*/false,
        /*group_by_use_nulls=*/false,
        /*sort_description_for_merging=*/SortDescription{},
        /*group_by_sort_description=*/SortDescription{},
        /*should_produce_results_in_order_of_bucket_number=*/false,
        /*memory_bound_merging_of_aggregation_results_enabled=*/false,
        /*explicit_sorting_required_for_aggregation_in_order=*/false,
        /*enable_sharding_aggregator=*/false);

    /// Build a single-stream pipeline from a header-only source.
    QueryPipelineBuilder builder;
    builder.init(Pipe(std::make_shared<SourceFromSingleChunk>(shared_header)));

    /// Simulate what ReadFromMergeTree does when actual streams < requested streams.
    builder.setReadStreamCountWasReduced(true);
    ASSERT_TRUE(builder.getReadStreamCountWasReduced());

    BuildQueryPipelineSettings settings(context);
    step.transformPipeline(builder, settings);

    /// The flag must be cleared after transformPipeline so it cannot leak downstream.
    EXPECT_FALSE(builder.getReadStreamCountWasReduced());
}
