#pragma once

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Columns/IColumn.h>
#include <QueryPipeline/PipelineResourcesHolder.h>

#include <list>
#include <memory>
#include <set>
#include <vector>

namespace DB
{

class DataStream;

class IQueryPlanStep;
using QueryPlanStepPtr = std::unique_ptr<IQueryPlanStep>;

class QueryPipelineBuilder;
using QueryPipelineBuilderPtr = std::unique_ptr<QueryPipelineBuilder>;

class WriteBuffer;

class QueryPlan;
using QueryPlanPtr = std::unique_ptr<QueryPlan>;

class Pipe;

struct QueryPlanOptimizationSettings;
struct BuildQueryPipelineSettings;

namespace JSONBuilder
{
    class IItem;
    using ItemPtr = std::unique_ptr<IItem>;
}

/// A tree of query steps.
/// The goal of QueryPlan is to build QueryPipeline.
/// QueryPlan let delay pipeline creation which is helpful for pipeline-level optimizations.
class QueryPlan
{
public:
    QueryPlan();
    ~QueryPlan();
    QueryPlan(QueryPlan &&) noexcept;
    QueryPlan & operator=(QueryPlan &&) noexcept;

    void unitePlans(QueryPlanStepPtr step, std::vector<QueryPlanPtr> plans);
    void addStep(QueryPlanStepPtr step);

    bool isInitialized() const { return root != nullptr; } /// Tree is not empty
    bool isCompleted() const; /// Tree is not empty and root hasOutputStream()
    const DataStream & getCurrentDataStream() const; /// Checks that (isInitialized() && !isCompleted())

    void optimize(const QueryPlanOptimizationSettings & optimization_settings);

    QueryPipelineBuilderPtr buildQueryPipeline(
        const QueryPlanOptimizationSettings & optimization_settings,
        const BuildQueryPipelineSettings & build_pipeline_settings);

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
        /// Add information about sort mode
        bool sort_mode = false;
    };

    struct ExplainPipelineOptions
    {
        /// Show header of output ports.
        bool header = false;
    };

    JSONBuilder::ItemPtr explainPlan(const ExplainPlanOptions & options);
    void explainPlan(WriteBuffer & buffer, const ExplainPlanOptions & options);
    void explainPipeline(WriteBuffer & buffer, const ExplainPipelineOptions & options);
    void explainEstimate(MutableColumns & columns);

    /// Do not allow to change the table while the pipeline alive.
    void addTableLock(TableLockHolder lock) { resources.table_locks.emplace_back(std::move(lock)); }
    void addInterpreterContext(std::shared_ptr<const Context> context) { resources.interpreter_context.emplace_back(std::move(context)); }
    void addStorageHolder(StoragePtr storage) { resources.storage_holders.emplace_back(std::move(storage)); }

    void addResources(QueryPlanResourceHolder resources_) { resources = std::move(resources_); }

    /// Set upper limit for the recommend number of threads. Will be applied to the newly-created pipelines.
    /// TODO: make it in a better way.
    void setMaxThreads(size_t max_threads_) { max_threads = max_threads_; }
    size_t getMaxThreads() const { return max_threads; }

    /// Tree node. Step and it's children.
    struct Node
    {
        QueryPlanStepPtr step;
        std::vector<Node *> children = {};
    };

    const Node * getRootNode() const { return root; }

    using Nodes = std::list<Node>;

private:
    QueryPlanResourceHolder resources;
    Nodes nodes;
    Node * root = nullptr;

    void checkInitialized() const;
    void checkNotCompleted() const;

    /// Those fields are passed to QueryPipeline.
    size_t max_threads = 0;
};

std::string debugExplainStep(const IQueryPlanStep & step);

}
