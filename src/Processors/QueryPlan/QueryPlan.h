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

class Context;

/// A tree of query steps.
/// The goal of QueryPlan is to build QueryPipeline.
/// QueryPlan let delay pipeline creation which is helpful for pipeline-level optimisations.
class QueryPlan
{
public:
    ~QueryPlan();

    void unitePlans(QueryPlanStepPtr step, std::vector<QueryPlan> plans);
    void addStep(QueryPlanStepPtr step);

    bool isInitialized() const { return root != nullptr; } /// Tree is not empty
    bool isCompleted() const; /// Tree is not empty and root hasOutputStream()
    const DataStream & getCurrentDataStream() const; /// Checks that (isInitialized() && !isCompleted())

    QueryPipelinePtr buildQueryPipeline();

    /// Set upper limit for the recommend number of threads. Will be applied to the newly-created pipelines.
    /// TODO: make it in a better way.
    void setMaxThreads(size_t max_threads_) { max_threads = max_threads_; }

    void addInterpreterContext(std::shared_ptr<Context> context);

private:
    /// Tree node. Step and it's children.
    struct Node
    {
        QueryPlanStepPtr step;
        std::vector<Node *> children = {};
    };

    using Nodes = std::list<Node>;
    Nodes nodes;

    Node * root = nullptr;

    void checkInitialized() const;
    void checkNotCompleted() const;

    /// Those fields are passed to QueryPipeline.
    size_t max_threads = 0;
    std::vector<std::shared_ptr<Context>> interpreter_context;
};

}
