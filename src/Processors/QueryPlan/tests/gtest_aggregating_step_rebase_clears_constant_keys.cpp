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

namespace
{

SharedHeader makeHeader()
{
    auto type = std::make_shared<DataTypeUInt64>();
    Block header({
        ColumnWithTypeAndName(type->createColumn(), type, "k"),
        ColumnWithTypeAndName(type->createColumn(), type, "j")});
    return std::make_shared<const Block>(std::move(header));
}

std::unique_ptr<AggregatingStep> makeStep(const SharedHeader & header, const Names & keys)
{
    /// The short constructor is the merge-only one; the pre-aggregation resize this test is about is
    /// chosen only for an ordinary (not merge-only) aggregation, hence `cloneWithKeys`.
    Aggregator::Params merge_params(
        keys,                    // keys
        AggregateDescriptions{}, // aggregates
        false,                   // overflow_row
        1,                       // max_threads
        65536,                   // max_block_size
        0.5f,                    // min_hit_rate_to_use_consecutive_keys_optimization
        false,                   // serialize_string_with_zero_byte
        true);                   // enable_packed_string_keys
    auto agg_params = merge_params.cloneWithKeys(keys, /*only_merge=*/false);

    return std::make_unique<AggregatingStep>(
        header,
        agg_params,
        GroupingSetsParamsList{}, // empty → no grouping sets
        /*final=*/false,
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
        /*explicit_sorting_required_for_aggregation_in_order=*/false);
}

/// Two-stream pipeline over header-only sources, so the pre-aggregation resize is inserted.
bool buildsGradualResize(AggregatingStep & step, const SharedHeader & header, ContextMutablePtr context)
{
    QueryPipelineBuilder builder;
    Pipes pipes;
    pipes.emplace_back(std::make_shared<SourceFromSingleChunk>(header));
    pipes.emplace_back(std::make_shared<SourceFromSingleChunk>(header));
    builder.init(Pipe::unitePipes(std::move(pipes)));

    BuildQueryPipelineSettings settings(context);
    step.transformPipeline(builder, settings);

    for (const auto & processor : builder.getProcessors())
        if (processor->getName() == "GradualResize")
            return true;
    return false;
}

}

/// `AggregationPushdown` clones the planned `GROUP BY` step and rebases it onto the join keys it
/// pushes the aggregation below. `group_by_keys_semantically_constant` was decided for the original
/// key set (`GROUP BY materialize(1)`), so carrying it over would keep claiming a single group for
/// a step that now groups by the join keys, and the gradual pre-aggregation resize the user opted
/// into with `min_rows_per_stream_for_gradual_resize` would silently stop applying on that carrier.
TEST(AggregatingStep, RebaseOntoInputClearsSemanticallyConstantKeys)
{
    MainThreadStatus::getInstance();
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    const auto & context_holder = getContext();
    auto context = Context::createCopy(context_holder.context);
    context->setSetting("min_rows_per_stream_for_gradual_resize", 1000);

    auto header = makeHeader();

    /// Control: the mark alone keeps the step on the strict resize.
    {
        auto step = makeStep(header, Names{"k"});
        step->enableGradualResize();
        step->markGroupByKeysSemanticallyConstant();
        EXPECT_FALSE(buildsGradualResize(*step, header, context));
    }

    /// Rebasing onto another key set drops the stale mark.
    {
        auto step = makeStep(header, Names{"k"});
        step->enableGradualResize();
        step->markGroupByKeysSemanticallyConstant();
        step->rebaseOntoInput(header, Names{"j"});
        EXPECT_TRUE(buildsGradualResize(*step, header, context));
    }
}
