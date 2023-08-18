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

    SubQueryPlan() = default;
    ~SubQueryPlan() = default;
    SubQueryPlan(SubQueryPlan &&) noexcept = default;
    SubQueryPlan & operator=(SubQueryPlan &&) noexcept = default;

    void addStep(QueryPlanStepPtr step);

    const DataStream & getCurrentDataStream() const; /// Checks that (isInitialized() && !isCompleted())

    void unitePlans(QueryPlanStepPtr step, std::vector<std::unique_ptr<SubQueryPlan>> plans);

    const Node & getRoot() const
    {
        return *root;
    }

    void explainPlan(WriteBuffer & buffer, const ExplainPlanOptions & options) const;

private:
    using Nodes = std::list<Node>;

    Nodes nodes;
    Node * root = nullptr;
};

using SubQueryPlanPtr = std::unique_ptr<SubQueryPlan>;

}
