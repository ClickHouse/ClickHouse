#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

struct LazyMaterializingRows;
using LazyMaterializingRowsPtr = std::shared_ptr<LazyMaterializingRows>;

/// This is a step for lazy materialization optimization.
/// Works like a JOIN by `_part_starting_offset + _part_offset` but more optimal.
class JoinLazyColumnsStep final : public IQueryPlanStep
{
public:
    JoinLazyColumnsStep(
        const SharedHeader & left_header_,
        const SharedHeader & right_header_,
        LazyMaterializingRowsPtr lazy_materializing_rows_);
    ~JoinLazyColumnsStep() override;

    String getName() const override { return "JoinLazyColumnsStep"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;
    void describePipeline(FormatSettings & settings) const override;

    bool supportsDataflowStatisticsCollection() const override { return true; }

protected:
    void updateOutputHeader() override;

    LazyMaterializingRowsPtr lazy_materializing_rows;
};

}
