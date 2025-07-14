#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/TableExpressionModifiers.h>
#include <Core/SortDescription.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <QueryPipeline/StreamLocalLimits.h>

#include <memory>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

struct PrewhereInfo;
using PrewhereInfoPtr = std::shared_ptr<PrewhereInfo>;

struct FilterDAGInfo;
using FilterDAGInfoPtr = std::shared_ptr<FilterDAGInfo>;

struct InputOrderInfo;
using InputOrderInfoPtr = std::shared_ptr<const InputOrderInfo>;

struct TreeRewriterResult;
using TreeRewriterResultPtr = std::shared_ptr<const TreeRewriterResult>;

class ReadInOrderOptimizer;
using ReadInOrderOptimizerPtr = std::shared_ptr<const ReadInOrderOptimizer>;

class Cluster;
using ClusterPtr = std::shared_ptr<Cluster>;

class PlannerContext;
using PlannerContextPtr = std::shared_ptr<PlannerContext>;

class PreparedSets;
using PreparedSetsPtr = std::shared_ptr<PreparedSets>;

struct PrewhereInfo
{
    /// Actions for row level security filter. Applied separately before prewhere_actions.
    /// This actions are separate because prewhere condition should not be executed over filtered rows.
    std::optional<ActionsDAG> row_level_filter;
    /// Actions which are executed on block in order to get filter column for prewhere step.
    ActionsDAG prewhere_actions;
    String row_level_column_name;
    String prewhere_column_name;
    bool remove_prewhere_column = false;
    bool need_filter = false;
    bool generated_by_optimizer = false;

    PrewhereInfo() = default;
    explicit PrewhereInfo(ActionsDAG prewhere_actions_, String prewhere_column_name_)
            : prewhere_actions(std::move(prewhere_actions_)), prewhere_column_name(std::move(prewhere_column_name_)) {}

    std::string dump() const;

    PrewhereInfoPtr clone() const
    {
        PrewhereInfoPtr prewhere_info = std::make_shared<PrewhereInfo>();

        if (row_level_filter)
            prewhere_info->row_level_filter = row_level_filter->clone();

        prewhere_info->prewhere_actions = prewhere_actions.clone();

        prewhere_info->row_level_column_name = row_level_column_name;
        prewhere_info->prewhere_column_name = prewhere_column_name;
        prewhere_info->remove_prewhere_column = remove_prewhere_column;
        prewhere_info->need_filter = need_filter;
        prewhere_info->generated_by_optimizer = generated_by_optimizer;

        return prewhere_info;
    }
};

/// Same as FilterInfo, but with ActionsDAG.
struct FilterDAGInfo
{
    ActionsDAG actions;
    String column_name;
    bool do_remove_column = false;

    std::string dump() const;
};

struct InputOrderInfo
{
    /// Sort description for merging of already sorted streams.
    /// Always a prefix of ORDER BY or GROUP BY description specified in query.
    SortDescription sort_description_for_merging;

    /** Size of prefix of sorting key that is already
     * sorted before execution of sorting or aggreagation.
     *
     * Contains both columns that scpecified in
     * ORDER BY or GROUP BY clause of query
     * and columns that turned out to be already sorted.
     *
     * E.g. if we have sorting key ORDER BY (a, b, c, d)
     * and query with `WHERE a = 'x' AND b = 'y' ORDER BY c, d` clauses.
     * sort_description_for_merging will be equal to (c, d) and
     * used_prefix_of_sorting_key_size will be equal to 4.
     */
    const size_t used_prefix_of_sorting_key_size;

    const int direction;
    const UInt64 limit;

    InputOrderInfo(
        const SortDescription & sort_description_for_merging_,
        size_t used_prefix_of_sorting_key_size_,
        int direction_, UInt64 limit_)
        : sort_description_for_merging(sort_description_for_merging_)
        , used_prefix_of_sorting_key_size(used_prefix_of_sorting_key_size_)
        , direction(direction_), limit(limit_)
    {
    }

    bool operator==(const InputOrderInfo &) const = default;
};

class IMergeTreeDataPart;

using ManyExpressionActions = std::vector<ExpressionActionsPtr>;

struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

/** Query along with some additional data,
  *  that can be used during query processing
  *  inside storage engines.
  */
struct SelectQueryInfo
{
    SelectQueryInfo();

    ASTPtr query;
    ASTPtr view_query; /// Optimized VIEW query

    /// Query tree
    QueryTreeNodePtr query_tree;

    /// Planner context
    PlannerContextPtr planner_context;

    /// Storage table expression
    /// It's guaranteed to be present in JOIN TREE of `query_tree`
    QueryTreeNodePtr table_expression;

    /// Table expression modifiers for storage
    std::optional<TableExpressionModifiers> table_expression_modifiers;

    std::shared_ptr<const StorageLimitsList> storage_limits;

    /// Local storage limits
    StorageLimits local_storage_limits;

    /// This is a leak of abstraction.
    /// StorageMerge/StorageBuffer/StorageMaterializedView replace storage into query_tree. However, column types may be changed for inner table.
    /// So, resolved query tree might have incompatible types.
    /// StorageDistributed uses this query tree to calculate a header, throws if we use storage snapshot.
    /// To avoid this, we use initial_storage_snapshot.
    StorageSnapshotPtr initial_storage_snapshot;

    /// Cluster for the query.
    ClusterPtr cluster;
    /// Optimized cluster for the query.
    /// In case of optimize_skip_unused_shards it may differs from original cluster.
    ///
    /// Configured in StorageDistributed::getQueryProcessingStage()
    ClusterPtr optimized_cluster;

    TreeRewriterResultPtr syntax_analyzer_result;

    /// This is an additional filer applied to current table.
    ASTPtr additional_filter_ast;

    /// It is needed for PK analysis based on row_level_policy and additional_filters.
    ASTs filter_asts;

    /// Filter actions dag for current storage.
    /// NOTE: Currently we store two copies of the filter DAGs:
    /// (1) SourceStepWithFilter::filter_actions_dag, (2) SelectQueryInfo::filter_actions_dag.
    /// Prefer to use the one in SourceStepWithFilter, not this one.
    /// (See comment in ReadFromMergeTree::applyFilters.)
    std::shared_ptr<const ActionsDAG> filter_actions_dag;

    ReadInOrderOptimizerPtr order_optimizer;
    /// Can be modified while reading from storage
    InputOrderInfoPtr input_order_info;

    /// Prepared sets are used for indices by storage engine.
    /// New analyzer stores prepared sets in planner_context and hashes computed of QueryTree instead of AST.
    /// Example: x IN (1, 2, 3)
    PreparedSetsPtr prepared_sets;

    /// Cached value of ExpressionAnalysisResult
    bool has_window = false;
    bool has_order_by = false;
    bool need_aggregate = false;
    PrewhereInfoPtr prewhere_info;

    /// If query has aggregate functions
    bool has_aggregates = false;

    ClusterPtr getCluster() const { return !optimized_cluster ? cluster : optimized_cluster; }

    bool settings_limit_offset_done = false;
    bool is_internal = false;
    bool parallel_replicas_disabled = false;
    bool is_parameterized_view = false;
    bool optimize_trivial_count = false;

    // If not 0, that means it's a trivial limit query.
    UInt64 trivial_limit = 0;

    /// For IStorageSystemOneBlock
    std::vector<UInt8> columns_mask;

    /// During read from MergeTree parts will be removed from snapshot after they are not needed
    bool merge_tree_enable_remove_parts_from_snapshot_optimization = true;

    bool isFinal() const;

    /// Analyzer generates unique ColumnIdentifiers like __table1.__partition_id in filter nodes,
    /// while key analysis still requires unqualified column names.
    /// This function generates a map that maps the unique names to table column names,
    /// for the current table (`table_expression`).
    std::unordered_map<std::string, ColumnWithTypeAndName> buildNodeNameToInputNodeColumn() const;
};
}
