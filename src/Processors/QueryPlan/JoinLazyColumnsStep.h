#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

struct ILazyMaterializingRows;
using ILazyMaterializingRowsPtr = std::shared_ptr<ILazyMaterializingRows>;

/// This is a step for lazy materialization optimization.
/// Works like a JOIN by the global row index (e.g. `_part_starting_offset + _part_offset`
/// for MergeTree) but more optimal.
class JoinLazyColumnsStep final : public IQueryPlanStep
{
public:
    JoinLazyColumnsStep(
        const SharedHeader & left_header_,
        const SharedHeader & right_header_,
        ILazyMaterializingRowsPtr lazy_materializing_rows_);
    ~JoinLazyColumnsStep() override;

    String getName() const override { return "JoinLazyColumnsStep"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;
    void describePipeline(FormatSettings & settings) const override;

    bool supportsDataflowStatisticsCollection() const override { return true; }

    void setPassThrough(bool value);

protected:
    void updateOutputHeader() override;

    ILazyMaterializingRowsPtr lazy_materializing_rows;
    bool pass_through = false;
};

}
