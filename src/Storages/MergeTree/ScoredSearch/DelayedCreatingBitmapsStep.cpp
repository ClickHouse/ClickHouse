#include <roaring/roaring.hh>
#include <Storages/MergeTree/ScoredSearch/DelayedCreatingBitmapsStep.h>

#include <algorithm>
#include <Columns/ColumnsNumber.h>
#include <Common/ProfileEvents.h>
#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace ProfileEvents
{
    extern const Event ScoredSearchPrefilterBitmapRows;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
}

BuildBitmapsTransform::BuildBitmapsTransform(
    SharedHeader input_header,
    SharedHeader output_header,
    PerPartBitmaps output_bitmaps_,
    UInt64 rows_budget_)
    : IAccumulatingTransform(std::move(input_header), std::move(output_header))
    , output_bitmaps(std::move(output_bitmaps_))
    , rows_budget(rows_budget_)
{
    if (std::ranges::any_of(output_bitmaps, [](const auto & slot) { return slot == nullptr; }))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "BuildBitmapsTransform requires every PerPartBitmaps slot to be pre-allocated");
}

void BuildBitmapsTransform::consume(Chunk chunk)
{
    const size_t num_rows = chunk.getNumRows();
    if (num_rows == 0)
        return;

    ProfileEvents::increment(ProfileEvents::ScoredSearchPrefilterBitmapRows, num_rows);
    rows_used += num_rows;

    /// TODO: implement a fallback to postfiltering if the budget is exceeded.
    if (rows_used > rows_budget)
    {
        throw Exception(ErrorCodes::LIMIT_EXCEEDED,
            "Bitmap prefilter would exceed the configured row budget (used {} rows, budget {} rows). See setting search_topk_prefilter_max_rows.",
            rows_used, rows_budget);
    }

    const auto & columns = chunk.getColumns();
    const auto & header_block = getInputPort().getHeader();
    const size_t part_index_pos = header_block.getPositionByName("_part_index");
    const size_t part_offset_pos = header_block.getPositionByName("_part_offset");

    /// `_part_index` cant arrive as a `ColumnConst`.
    /// Materialise both columns so positional access is uniform.
    auto part_index_column = columns[part_index_pos]->convertToFullColumnIfConst();
    auto part_offset_column = columns[part_offset_pos]->convertToFullColumnIfConst();

    const auto * part_index_typed = typeid_cast<const ColumnUInt64 *>(part_index_column.get());
    const auto * part_offset_typed = typeid_cast<const ColumnUInt64 *>(part_offset_column.get());

    if (!part_index_typed || !part_offset_typed)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "BuildBitmapsTransform expects UInt64 _part_index and _part_offset columns — got {} and {}",
            part_index_column->getName(), part_offset_column->getName());
    }

    const auto & part_index_data = part_index_typed->getData();
    const auto & part_offset_data = part_offset_typed->getData();

    /// Group contiguous runs of the same `_part_index` and add them in a batch.
    size_t run_start = 0;
    size_t num_parts = output_bitmaps.size();

    while (run_start < num_rows)
    {
        const UInt64 part_index = part_index_data[run_start];
        size_t run_end = run_start + 1;

        while (run_end < num_rows && part_index_data[run_end] == part_index)
        {
            ++run_end;
        }

        if (part_index >= num_parts)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "BuildBitmapsTransform: _part_index {} out of range (PerPartBitmaps size {})",
                part_index, num_parts);
        }

        auto & slot = *output_bitmaps[part_index];
        roaring::BulkContext bulk_context;

        for (size_t i = run_start; i < run_end; ++i)
        {
            UInt64 offset = part_offset_data[i];
            slot.addBulk(bulk_context, static_cast<UInt32>(offset));
        }

        run_start = run_end;
    }
}

Chunk BuildBitmapsTransform::generate()
{
    finishConsume();
    return {};
}


DelayedCreatingBitmapsStep::DelayedCreatingBitmapsStep(
    SharedHeader input_header,
    LazyBitmapSubqueryStatePtr state_,
    ContextPtr context_)
    : state(std::move(state_))
    , context(std::move(context_))
{
    if (!state)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DelayedCreatingBitmapsStep requires a non-null LazyBitmapSubqueryState");

    input_headers = {input_header};
    output_header = std::move(input_header);
}

QueryPipelineBuilderPtr DelayedCreatingBitmapsStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings)
{
    if (pipelines.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DelayedCreatingBitmapsStep cannot be created with no inputs");

    auto main_pipeline = std::move(pipelines.front());

    /// No WHERE clause arrived — pass the main pipeline through unchanged.
    /// The scorer runs unfiltered.
    if (!state->subquery_plan)
        return main_pipeline;

    if (!state->bitmaps)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DelayedCreatingBitmapsStep has a subquery plan but no pre-allocated bitmaps");

    auto bitmap_pipeline = state->subquery_plan->buildQueryPipeline(QueryPlanOptimizationSettings(context), settings);
    /// Resize `BuildBitmapsTransform` to single stream to allow filling the bitmaps without locking.
    bitmap_pipeline->resize(1);

    auto bitmap_transform = std::make_shared<BuildBitmapsTransform>(
        bitmap_pipeline->getSharedHeader(),
        std::make_shared<const Block>(),
        *state->bitmaps,
        state->rows_budget);

    bitmap_pipeline->addTransform(std::move(bitmap_transform));

    /// `addPipelineBefore` gates the main pipe behind the bitmap pipe
    /// via `DelayedPortsProcessor`, so the subquery is fully drained first.
    /// The collector makes the added processors visible to `EXPLAIN PIPELINE`.
    {
        QueryPipelineProcessorsCollector collector(*main_pipeline, this);
        main_pipeline->addPipelineBefore(std::move(*bitmap_pipeline));
        processors = collector.detachProcessors();
    }

    return main_pipeline;
}

void DelayedCreatingBitmapsStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

}
