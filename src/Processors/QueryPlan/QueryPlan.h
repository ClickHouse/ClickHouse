#pragma once

#include <Core/Names.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Interpreters/Context_fwd.h>
#include <Columns/IColumn.h>
#include <QueryPipeline/QueryPlanResourceHolder.h>
#include <Parsers/IAST_fwd.h>

#include <list>
#include <memory>
#include <vector>

namespace DB
{

class DataStream;

class IQueryPlanStep;
using QueryPlanStepPtr = std::unique_ptr<IQueryPlanStep>;

class QueryPipelineBuilder;
using QueryPipelineBuilderPtr = std::unique_ptr<QueryPipelineBuilder>;

class ReadBuffer;
class WriteBuffer;

class QueryPlan;
using QueryPlanPtr = std::unique_ptr<QueryPlan>;

class Pipe;

struct QueryPlanOptimizationSettings;
struct BuildQueryPipelineSettings;

class ColumnSet;
namespace JSONBuilder
{
    class IItem;
    using ItemPtr = std::unique_ptr<IItem>;
}

struct QueryPlanAndSets;

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

    void serialize(WriteBuffer & out) const;
    static QueryPlanAndSets deserialize(ReadBuffer & in, const ContextPtr & context);

    static void resolveReadFromTable(QueryPlan & plan, const ContextPtr & context);
    static QueryPlan resolveStorages(QueryPlanAndSets plan_and_sets, const ContextPtr & context);

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
        /// Add information about sorting
        bool sorting = false;
    };

    struct ExplainPipelineOptions
    {
        /// Show header of output ports.
        bool header = false;
    };

    JSONBuilder::ItemPtr explainPlan(const ExplainPlanOptions & options);
    void explainPlan(WriteBuffer & buffer, const ExplainPlanOptions & options, size_t indent = 0);
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

    void setConcurrencyControl(bool concurrency_control_) { concurrency_control = concurrency_control_; }
    bool getConcurrencyControl() const { return concurrency_control; }

    /// Tree node. Step and it's children.
    struct Node
    {
        QueryPlanStepPtr step;
        std::vector<Node *> children = {};
    };

    using Nodes = std::list<Node>;

    Node * getRootNode() const { return root; }
    static std::pair<Nodes, QueryPlanResourceHolder> detachNodesAndResources(QueryPlan && plan);

private:
    QueryPlanResourceHolder resources;
    Nodes nodes;
    Node * root = nullptr;

    void checkInitialized() const;
    void checkNotCompleted() const;

    /// Those fields are passed to QueryPipeline.
    size_t max_threads = 0;
    bool concurrency_control = false;
};

class FutureSetFromSubquery;
using FutureSetFromSubqueryPtr = std::shared_ptr<FutureSetFromSubquery>;

/// This is a structure which contains a query plan and a list of StorageSet.
/// The reason is that StorageSet is specified by name,
/// and we do not want to resolve the storage name while deserializing.
/// Now, it allows to deserialize the plan without the context.
/// Potentially, it may help to get the atomic snapshot for all the storages.
///
/// Use resolveStorages to get an ordinary plan.
struct QueryPlanAndSets
{
    struct Set
    {
        CityHash_v1_0_2::uint128 hash;
        std::list<ColumnSet *> columns;
    };
    struct SetFromStorage : public Set
    {
        std::string storage_name;
    };

    struct SetFromTuple : public Set
    {
        ColumnsWithTypeAndName set_columns;
    };

    struct SetFromSubquery : public Set
    {
        std::unique_ptr<QueryPlan> plan;
        std::list<SetFromSubquery> sets;
    };

    QueryPlan plan;
    std::list<SetFromStorage> sets_from_storage;
    std::list<SetFromTuple> sets_from_tuple;
    std::list<SetFromSubquery> sets_from_subquery;
};

std::string debugExplainStep(const IQueryPlanStep & step);

}
