#pragma once

#include <Core/Block_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Columns/IColumn_fwd.h>
#include <QueryPipeline/QueryPlanResourceHolder.h>
#include <Parsers/IAST_fwd.h>

#include <list>
#include <memory>
#include <vector>
#include <IO/WriteBufferFromString.h>

namespace DB
{

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
struct SerializedSetsRegistry;
struct DeserializedSetsRegistry;

class SettingsChanges;

/// Options from EXPLAIN PLAN query.
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
    /// Add information about projections.
    bool projections = false;
    /// Add information about sorting
    bool sorting = false;
    /// Show remote plans for distributed query.
    bool distributed = false;
    /// Add input headers to step.
    bool input_headers = false;
    /// Print structure of columns instead of just their names and types.
    bool column_structure = false;

    SettingsChanges toSettingsChanges() const;
};

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
    const SharedHeader & getCurrentHeader() const; /// Checks that (isInitialized() && !isCompleted())

    void serialize(WriteBuffer & out, size_t max_supported_version) const;
    static QueryPlanAndSets deserialize(ReadBuffer & in, const ContextPtr & context);
    static QueryPlan makeSets(QueryPlanAndSets plan_and_sets, const ContextPtr & context);

    /// Serializes the query plan and store the result
    void ensureSerialized(size_t max_supported_version) const;

    /// Get cached serialized data
    std::string_view getSerializedData() const;

    /// Check if already serialized
    bool isSerialized() const;

    void resolveStorages(const ContextPtr & context);

    void optimize(const QueryPlanOptimizationSettings & optimization_settings);

    QueryPipelineBuilderPtr buildQueryPipeline(
        const QueryPlanOptimizationSettings & optimization_settings,
        const BuildQueryPipelineSettings & build_pipeline_settings,
        bool do_optimize=true);

    struct ExplainPipelineOptions
    {
        /// Show header of output ports.
        bool header = false;
    };

    JSONBuilder::ItemPtr explainPlan(const ExplainPlanOptions & options) const;
    void explainPlan(WriteBuffer & buffer, const ExplainPlanOptions & options, size_t indent = 0, size_t max_description_length = 0) const;
    void explainPipeline(WriteBuffer & buffer, const ExplainPipelineOptions & options) const;
    void explainEstimate(MutableColumns & columns) const;

    /// Do not allow to change the table while the pipeline alive.
    void addTableLock(TableLockHolder lock) { resources.table_locks.emplace_back(std::move(lock)); }
    void addInterpreterContext(std::shared_ptr<const Context> context) { resources.interpreter_context.emplace_back(std::move(context)); }
    auto getInterpretersContexts() const { return resources.interpreter_context; }
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

    /// Extract subplan from plan from the root node.
    /// The root node and all the children will be removed from the nodes.
    static QueryPlan extractSubplan(Node * root, Nodes & nodes);

    Node * getRootNode() const { return root; }
    static std::pair<Nodes, QueryPlanResourceHolder> detachNodesAndResources(QueryPlan && plan);
    void replaceNodeWithPlan(Node * node, QueryPlan plan);

    QueryPlan extractSubplan(Node * subplan_root);
    void cloneInplace(Node * node_to_replace, Node * subplan_root);
    QueryPlan clone() const;

    static void cloneSubplanAndReplace(Node * node_to_replace, Node * subplan_root, Nodes & nodes);

private:
    struct SerializationFlags;

    void serialize(WriteBuffer & out, const SerializationFlags & flags) const;
    static QueryPlanAndSets deserialize(ReadBuffer & in, const ContextPtr & context, const SerializationFlags & flags);

    static void serializeSets(SerializedSetsRegistry & registry, WriteBuffer & out, const QueryPlan::SerializationFlags & flags);
    static QueryPlanAndSets deserializeSets(QueryPlan plan, DeserializedSetsRegistry & registry, ReadBuffer & in, const SerializationFlags & flags, const ContextPtr & context);

    QueryPlanResourceHolder resources;
    Nodes nodes;
    Node * root = nullptr;

    void checkInitialized() const;
    void checkNotCompleted() const;

    /// Those fields are passed to QueryPipeline.
    size_t max_threads = 0;
    bool concurrency_control = false;

    /// Cached serialized representation
    /// FIXME: temporary measure to avoid changing many methods to bypass serialized plan
    mutable std::unique_ptr<WriteBufferFromOwnString> serialized_plan;
};

/// This is a structure which contains a query plan and a list of sets.
/// The reason is that StorageSet is specified by name,
/// and we do not want to resolve the storage name while deserializing.
/// Now, it allows to deserialize the plan without the context.
/// Potentially, it may help to get the atomic snapshot for all the storages.
///
/// Use QueryPlan::makeSets to get an ordinary plan.
struct QueryPlanAndSets
{
    QueryPlanAndSets();
    ~QueryPlanAndSets();
    QueryPlanAndSets(QueryPlanAndSets &&) noexcept;

    struct Set;
    struct SetFromStorage;
    struct SetFromTuple;
    struct SetFromSubquery;

    QueryPlan plan;
    std::list<SetFromStorage> sets_from_storage;
    std::list<SetFromTuple> sets_from_tuple;
    std::list<SetFromSubquery> sets_from_subquery;
};

std::string debugExplainStep(IQueryPlanStep & step);
std::string debugExplainPlan(const QueryPlan & plan);

}
