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

struct PrewhereInfo
{
    /// Actions which are executed in order to alias columns are used for prewhere actions.
    ExpressionActionsPtr alias_actions;
    /// Actions which are executed on block in order to get filter column for prewhere step.
    ExpressionActionsPtr prewhere_actions;
    /// Actions which are executed after reading from storage in order to remove unused columns.
    ExpressionActionsPtr remove_columns_actions;
    String prewhere_column_name;
    bool remove_prewhere_column = false;
    bool need_filter = false;

    PrewhereInfo() = default;
    explicit PrewhereInfo(ExpressionActionsPtr prewhere_actions_, String prewhere_column_name_)
        : prewhere_actions(std::move(prewhere_actions_)), prewhere_column_name(std::move(prewhere_column_name_)) {}
};

/// Helper struct to store all the information about the filter expression.
struct FilterInfo
{
    ExpressionActionsPtr actions;
    String column_name;
    bool do_remove_column = false;
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

using PrewhereInfoPtr = std::shared_ptr<PrewhereInfo>;
using FilterInfoPtr = std::shared_ptr<FilterInfo>;
using InputOrderInfoPtr = std::shared_ptr<const InputOrderInfo>;

struct TreeRewriterResult;
using TreeRewriterResultPtr = std::shared_ptr<const TreeRewriterResult>;

class ReadInOrderOptimizer;
using ReadInOrderOptimizerPtr = std::shared_ptr<const ReadInOrderOptimizer>;


/** Query along with some additional data,
  *  that can be used during query processing
  *  inside storage engines.
  */
struct SelectQueryInfo
{
    ASTPtr query;
    ASTPtr view_query; /// Optimized VIEW query

    TreeRewriterResultPtr syntax_analyzer_result;

    PrewhereInfoPtr prewhere_info;

    ReadInOrderOptimizerPtr order_optimizer;
    /// We can modify it while reading from storage
    mutable InputOrderInfoPtr input_order_info;

    /// Prepared sets are used for indices by storage engine.
    /// Example: x IN (1, 2, 3)
    PreparedSets sets;
};

}
