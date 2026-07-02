#pragma once

#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

using ColumnIdentifier = std::string;
using ColumnIdentifiers = std::vector<ColumnIdentifier>;

class CommonSubplanReferenceStep : public ISourceStep
{
public:
    explicit CommonSubplanReferenceStep(
        const SharedHeader & header_,
        QueryPlan::Node * subplan_root_,
        ColumnIdentifiers columns_to_use_,
        bool must_materialize_ = false)
        : ISourceStep(header_)
        , subplan_root(subplan_root_)
        , columns_to_use(std::move(columns_to_use_))
        , must_materialize(must_materialize_)
    {}

    String getName() const override { return "CommonSubplanReference"; }

    void initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &) override;

    QueryPlanStepPtr clone() const override
    {
        return std::make_unique<CommonSubplanReferenceStep>(getOutputHeader(), subplan_root, columns_to_use, must_materialize);
    }

    QueryPlan::Node * getSubplanReferenceRoot() const { return subplan_root; }

    const ColumnIdentifiers & getColumnsToUse() const { return columns_to_use; }

    ColumnIdentifiers extractColumnsToUse() { return std::move(columns_to_use); }

    /// The in-memory buffer can only guarantee that the subquery input is fully evaluated before
    /// the subquery itself when the input is on the build side of the result join (join_kind = right).
    /// For join_kind = left the input ends up on the probe side, so the reference must be materialized
    /// instead of buffered. This is decided per decorrelation, because the join kind can differ between
    /// branches of a set operation whose per-branch SETTINGS are not visible to the global optimizer flag.
    bool mustMaterialize() const { return must_materialize; }

private:
    QueryPlan::Node * subplan_root = nullptr;

    ColumnIdentifiers columns_to_use;

    bool must_materialize = false;
};

}
