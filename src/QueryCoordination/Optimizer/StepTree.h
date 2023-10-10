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

class StepTree
{
public:
    struct ExplainPlanOptions
    {
        /// Add output header to step.
        bool header = false;
        /// Add description of step.
        bool description = true;
        /// Add detailed information about step actions.
        bool actions = false;
        /// Add information about indexes actions.
        bool indexes = false;
        /// Add information about sorting
        bool sorting = false;
    };

    using Node = PlanNode;

    StepTree() = default;
    ~StepTree() = default;
    StepTree(StepTree &&) noexcept = default;
    StepTree & operator=(StepTree &&) noexcept = default;

    void addStep(QueryPlanStepPtr step);

    const DataStream & getCurrentDataStream() const; /// Checks that (isInitialized() && !isCompleted())

    void unitePlans(QueryPlanStepPtr step, std::vector<std::shared_ptr<StepTree>> & plans);

    Node * getRoot() const
    {
        return root;
    }

    void explainPlan(WriteBuffer & buffer, const ExplainPlanOptions & options) const;

private:
    using Nodes = std::list<Node>;

    Nodes nodes;
    Node * root = nullptr;
};

using StepTreePtr = std::shared_ptr<StepTree>;

}
