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
        ColumnIdentifiers columns_to_use_)
        : ISourceStep(header_)
        , subplan_root(subplan_root_)
        , columns_to_use(std::move(columns_to_use_))
    {}

    String getName() const override { return "CommonSubplanReference"; }

    void initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &) override;

    QueryPlanStepPtr clone() const override
    {
        return std::make_unique<CommonSubplanReferenceStep>(getOutputHeader(), subplan_root, columns_to_use);
    }

    QueryPlan::Node * getSubplanReferenceRoot() const { return subplan_root; }

    const ColumnIdentifiers & getColumnsToUse() const { return columns_to_use; }

    ColumnIdentifiers extractColumnsToUse() { return std::move(columns_to_use); }

private:
    QueryPlan::Node * subplan_root = nullptr;

    ColumnIdentifiers columns_to_use;
};

}
