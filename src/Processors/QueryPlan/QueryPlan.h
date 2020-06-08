#pragma once
#include <memory>
#include <list>
#include <vector>

namespace DB
{

class DataStream;

class IQueryPlanStep;
using QueryPlanStepPtr = std::unique_ptr<IQueryPlanStep>;

class QueryPipeline;
using QueryPipelinePtr = std::unique_ptr<QueryPipeline>;

/// A tree of query steps.
class QueryPlan
{
public:
    void addStep(QueryPlanStepPtr step);

    bool isInitialized() const { return root != nullptr; } /// Tree is not empty
    bool isCompleted() const; /// Tree is not empty and root hasOutputStream()
    const DataStream & getCurrentDataStream() const; /// Checks that (isInitialized() && !isCompleted())

    QueryPipelinePtr buildQueryPipeline();

private:
    struct Node
    {
        QueryPlanStepPtr step;
        std::vector<Node *> children;
    };

    using Nodes = std::list<Node>;
    Nodes nodes;

    Node * root = nullptr;

    void checkInitialized() const;
    void checkNotCompleted() const;
};

}
