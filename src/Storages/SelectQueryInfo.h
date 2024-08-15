#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/TableExpressionModifiers.h>
#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/PreparedSets.h>
#include <Planner/PlannerContext.h>
#include <QueryPipeline/StreamLocalLimits.h>
#include <Storages/ProjectionsDescription.h>

#include <memory>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class ActionsDAG;
using ActionsDAGPtr = std::shared_ptr<ActionsDAG>;

struct PrewhereInfo;
using PrewhereInfoPtr = std::shared_ptr<PrewhereInfo>;

struct FilterInfo;
using FilterInfoPtr = std::shared_ptr<FilterInfo>;

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

struct MergeTreeDataSelectAnalysisResult;
using MergeTreeDataSelectAnalysisResultPtr = std::shared_ptr<MergeTreeDataSelectAnalysisResult>;

struct PrewhereInfo
{
    /// Actions for row level security filter. Applied separately before prewhere_actions.
    /// This actions are separate because prewhere condition should not be executed over filtered rows.
    ActionsDAGPtr row_level_filter;
    /// Actions which are executed on block in order to get filter column for prewhere step.
    ActionsDAGPtr prewhere_actions;
    String row_level_column_name;
    String prewhere_column_name;
    bool remove_prewhere_column = false;
    bool need_filter = false;

    PrewhereInfo() = default;
    explicit PrewhereInfo(ActionsDAGPtr prewhere_actions_, String prewhere_column_name_)
            : prewhere_actions(std::move(prewhere_actions_)), prewhere_column_name(std::move(prewhere_column_name_)) {}

    std::string dump() const;

    PrewhereInfoPtr clone() const
    {
        PrewhereInfoPtr prewhere_info = std::make_shared<PrewhereInfo>();

        if (row_level_filter)
            prewhere_info->row_level_filter = row_level_filter->clone();

        if (prewhere_actions)
            prewhere_info->prewhere_actions = prewhere_actions->clone();

        prewhere_info->row_level_column_name = row_level_column_name;
        prewhere_info->prewhere_column_name = prewhere_column_name;
        prewhere_info->remove_prewhere_column = remove_prewhere_column;
        prewhere_info->need_filter = need_filter;

        return prewhere_info;
    }
};

/// Helper struct to store all the information about the filter expression.
struct FilterInfo
{
    ExpressionActionsPtr alias_actions;
    ExpressionActionsPtr actions;
    String column_name;
    bool do_remove_column = false;
};

/// Same as FilterInfo, but with ActionsDAG.
struct FilterDAGInfo
{
    ActionsDAGPtr actions;
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

// The projection selected to execute current query
struct ProjectionCandidate
{
    ProjectionDescriptionRawPtr desc{};
    PrewhereInfoPtr prewhere_info;
    ActionsDAGPtr before_where;
    String where_column_name;
    bool remove_where_filter = false;
    ActionsDAGPtr before_aggregation;
    Names required_columns;
    NamesAndTypesList aggregation_keys;
    AggregateDescriptions aggregate_descriptions;
    bool aggregate_overflow_row = false;
    bool aggregate_final = false;
    bool complete = false;
    ReadInOrderOptimizerPtr order_optimizer;
    InputOrderInfoPtr input_order_info;
    ManyExpressionActions group_by_elements_actions;
    SortDescription group_by_elements_order_descr;
    MergeTreeDataSelectAnalysisResultPtr merge_tree_projection_select_result_ptr;
    MergeTreeDataSelectAnalysisResultPtr merge_tree_normal_select_result_ptr;

    /// Because projection analysis uses a separate interpreter.
    ContextPtr context;
};

/** Query along with some additional data,
  *  that can be used during query processing
  *  inside storage engines.
  */
struct SelectQueryInfo
{
    SelectQueryInfo()
        : prepared_sets(std::make_shared<PreparedSets>())
    {}

    ASTPtr query;
    ASTPtr view_query; /// Optimized VIEW query
    ASTPtr original_query; /// Unmodified query for projection analysis

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

    /// Cluster for the query.
    ClusterPtr cluster;
    /// Optimized cluster for the query.
    /// In case of optimize_skip_unused_shards it may differs from original cluster.
    ///
    /// Configured in StorageDistributed::getQueryProcessingStage()
    ClusterPtr optimized_cluster;
    /// should we use custom key with the cluster
    bool use_custom_key = false;

    TreeRewriterResultPtr syntax_analyzer_result;

    /// This is an additional filer applied to current table.
    ASTPtr additional_filter_ast;

    /// It is needed for PK analysis based on row_level_policy and additional_filters.
    ASTs filter_asts;

    ASTPtr parallel_replica_custom_key_ast;

    /// Filter actions dag for current storage
    ActionsDAGPtr filter_actions_dag;

    ReadInOrderOptimizerPtr order_optimizer;
    /// Can be modified while reading from storage
    InputOrderInfoPtr input_order_info;

    /// Prepared sets are used for indices by storage engine.
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

    /// If not null, it means we choose a projection to execute current query.
    std::optional<ProjectionCandidate> projection;
    bool ignore_projections = false;
    bool is_projection_query = false;
    bool merge_tree_empty_result = false;
    bool settings_limit_offset_done = false;
    bool is_internal = false;
    Block minmax_count_projection_block;
    MergeTreeDataSelectAnalysisResultPtr merge_tree_select_result_ptr;

    bool parallel_replicas_disabled = false;

    bool is_parameterized_view = false;

    bool optimize_trivial_count = false;

    // If limit is not 0, that means it's a trivial limit query.
    UInt64 limit = 0;

    /// For IStorageSystemOneBlock
    std::vector<UInt8> columns_mask;

    InputOrderInfoPtr getInputOrderInfo() const
    {
        return input_order_info ? input_order_info : (projection ? projection->input_order_info : nullptr);
    }

    bool isFinal() const;
};
}
