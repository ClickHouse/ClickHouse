#pragma once

#include <Columns/IColumn.h>
#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <QueryCoordination/PlanNode.h>
#include <QueryPipeline/QueryPlanResourceHolder.h>

#include <list>
#include <memory>
#include <set>
#include <vector>

namespace DB
{

class SubQueryPlan
{
public:
    using Node = PlanNode;

    SubQueryPlan() = default;
    ~SubQueryPlan() = default;
    SubQueryPlan(SubQueryPlan &&) noexcept = default;
    SubQueryPlan & operator=(SubQueryPlan &&) noexcept = default;

    void addStep(QueryPlanStepPtr step);

    const Node & getRoot() const
    {
        return *root;
    }

private:
    using Nodes = std::list<Node>;

    Nodes nodes;
    Node * root = nullptr;
};

}
