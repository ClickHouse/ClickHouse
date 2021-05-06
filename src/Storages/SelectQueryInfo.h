#pragma once

#include <Interpreters/PreparedSets.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Core/SortDescription.h>
#include <Core/Names.h>
#include <memory>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class ActionsDAG;
using ActionsDAGPtr = std::shared_ptr<ActionsDAG>;

struct PrewhereInfo;
using PrewhereInfoPtr = std::shared_ptr<PrewhereInfo>;

struct PrewhereDAGInfo;
using PrewhereDAGInfoPtr = std::shared_ptr<PrewhereDAGInfo>;

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

struct PrewhereInfo
{
    /// Actions which are executed in order to alias columns are used for prewhere actions.
    ExpressionActionsPtr alias_actions;
    /// Actions for row level security filter. Applied separately before prewhere_actions.
    /// This actions are separate because prewhere condition should not be executed over filtered rows.
    ExpressionActionsPtr row_level_filter;
    /// Actions which are executed on block in order to get filter column for prewhere step.
    ExpressionActionsPtr prewhere_actions;
    /// Actions which are executed after reading from storage in order to remove unused columns.
    ExpressionActionsPtr remove_columns_actions;
    String row_level_column_name;
    String prewhere_column_name;
    bool remove_prewhere_column = false;
    bool need_filter = false;
};

/// Same as PrewhereInfo, but with ActionsDAG.
struct PrewhereDAGInfo
{
    ActionsDAGPtr alias_actions;
    ActionsDAGPtr row_level_filter_actions;
    ActionsDAGPtr prewhere_actions;
    ActionsDAGPtr remove_columns_actions;
    String row_level_column_name;
    String prewhere_column_name;
    bool remove_prewhere_column = false;
    bool need_filter = false;

    PrewhereDAGInfo() = default;
    explicit PrewhereDAGInfo(ActionsDAGPtr prewhere_actions_, String prewhere_column_name_)
            : prewhere_actions(std::move(prewhere_actions_)), prewhere_column_name(std::move(prewhere_column_name_)) {}

    std::string dump() const;
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
    SortDescription order_key_prefix_descr;
    int direction;

    InputOrderInfo(const SortDescription & order_key_prefix_descr_, int direction_)
        : order_key_prefix_descr(order_key_prefix_descr_), direction(direction_) {}

    bool operator ==(const InputOrderInfo & other) const
    {
        return order_key_prefix_descr == other.order_key_prefix_descr && direction == other.direction;
    }

    bool operator !=(const InputOrderInfo & other) const { return !(*this == other); }
};

/** Query along with some additional data,
  *  that can be used during query processing
  *  inside storage engines.
  */
struct SelectQueryInfo
{
    ASTPtr query;
    ASTPtr view_query; /// Optimized VIEW query

    /// Cluster for the query.
    ClusterPtr cluster;
    /// Optimized cluster for the query.
    /// In case of optimize_skip_unused_shards it may differs from original cluster.
    ///
    /// Configured in StorageDistributed::getQueryProcessingStage()
    ClusterPtr optimized_cluster;

    TreeRewriterResultPtr syntax_analyzer_result;

    PrewhereInfoPtr prewhere_info;

    ReadInOrderOptimizerPtr order_optimizer;
    /// Can be modified while reading from storage
    InputOrderInfoPtr input_order_info;

    /// Prepared sets are used for indices by storage engine.
    /// Example: x IN (1, 2, 3)
    PreparedSets sets;

    ClusterPtr getCluster() const { return !optimized_cluster ? cluster : optimized_cluster; }
};

}
