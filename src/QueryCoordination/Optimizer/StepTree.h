#pragma once

#include <Columns/IColumn.h>
#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <QueryCoordination/PlanNode.h>
#include <QueryPipeline/QueryPlanResourceHolder.h>
#include <Processors/QueryPlan/QueryPlan.h>

#include <list>
#include <memory>
#include <set>
#include <vector>

namespace DB
{

class StepTree
{
public:
    using Node = PlanNode;
    using ExplainPlanOptions = QueryPlan::ExplainPlanOptions;

    StepTree() = default;
    ~StepTree() = default;
    StepTree(StepTree &&) noexcept = default;
    StepTree & operator=(StepTree &&) noexcept = default;

    bool isInitialized() const { return root != nullptr; }

    void addStep(QueryPlanStepPtr step);

    const DataStream & getCurrentDataStream() const;

    void unitePlans(QueryPlanStepPtr step, std::vector<std::shared_ptr<StepTree>> & plans);

    Node * getRoot() const
    {
        return root;
    }

    void explainPlan(WriteBuffer & buffer, const ExplainPlanOptions & options) const;
    JSONBuilder::ItemPtr explainPlan(const ExplainPlanOptions & options);

private:
    using Nodes = std::list<Node>;

    void checkInitialized() const;
    bool isCompleted() const;
    void checkNotCompleted() const;

    Nodes nodes;
    Node * root = nullptr;
};

using StepTreePtr = std::shared_ptr<StepTree>;

}
