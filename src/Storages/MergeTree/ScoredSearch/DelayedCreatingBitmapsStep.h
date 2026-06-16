#pragma once
#include <Core/Block_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/IAccumulatingTransform.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Storages/MergeTree/ScoredSearch/ScoredSearchUtils.h>

namespace DB
{

/// Accumulating transform that consumes `(_part_index, _part_offset)`
/// chunks from the bitmap subquery and fills the pre-allocated per-part bitmaps.
/// Throws `LIMIT_EXCEEDED` when the row budget is exceeded.
class BuildBitmapsTransform final : public IAccumulatingTransform
{
public:
    BuildBitmapsTransform(
        SharedHeader input_header,
        SharedHeader output_header,
        PerPartBitmaps output_bitmaps_,
        UInt64 rows_budget_);

    String getName() const override { return "BuildBitmapsTransform"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    PerPartBitmaps output_bitmaps;
    UInt64 rows_budget;
    UInt64 rows_used = 0;
};

/// QueryPlan step that owns the bitmap subquery and drains it before the
/// scorer pipeline produces rows (modelled on `DelayedCreatingSetsStep`).
class DelayedCreatingBitmapsStep final : public IQueryPlanStep
{
public:
    DelayedCreatingBitmapsStep(
        SharedHeader input_header,
        LazyBitmapSubqueryStatePtr state_,
        ContextPtr context_);

    String getName() const override { return "DelayedCreatingBitmaps"; }
    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;
    void describePipeline(FormatSettings & settings) const override;

private:
    void updateOutputHeader() override { output_header = getInputHeaders().front(); }

    LazyBitmapSubqueryStatePtr state;
    ContextPtr context;
};

}
