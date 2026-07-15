#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/Cluster.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class ASTFunction;

/// True when the sharding key expression contains an IN whose right-hand set is a subquery that is not
/// built yet (`future_set->get()` is null). The sharding key expression is built standalone from the
/// sharding-key AST, so such a set is never populated during planning. Executing the expression on
/// constant values to prune shards would then hit `FunctionIn` with an unready set and throw
/// "Not-ready Set is passed as the second argument for function 'in'" (LOGICAL_ERROR, aborts in
/// debug/sanitizer builds). Callers must skip the shard-pruning optimization and query all shards
/// in that case. Materialized tuple/storage sets are already filled, so they are safe and not reported.
bool shardingKeyExpressionContainsNotReadySet(const ExpressionActionsPtr & sharding_key_expr);

/// Rewrite `sharding_key IN (...)` for specific shard,
/// so that it will contain only values that belong to this specific shard.
///
/// See also:
/// - evaluateExpressionOverConstantCondition()
/// - StorageDistributed::createSelector()
/// - createBlockSelector()
struct OptimizeShardingKeyRewriteInMatcher
{
    /// Cluster::SlotToShard
    using SlotToShard = std::vector<UInt64>;

    struct Data
    {
        /// Expression of sharding_key for the Distributed() table
        const ExpressionActionsPtr & sharding_key_expr;
        /// Name of the column for sharding_expr
        const std::string & sharding_key_column_name;
        /// Info for the current shard (to compare shard_num with calculated)
        const Cluster::ShardInfo & shard_info;
        /// weight -> shard mapping
        const Cluster::SlotToShard & slots;
    };

    static bool needChildVisit(ASTPtr & /*node*/, const ASTPtr & /*child*/);
    static void visit(ASTPtr & node, Data & data);
    static void visit(ASTFunction & function, Data & data);
};

using OptimizeShardingKeyRewriteInVisitor = InDepthNodeVisitor<OptimizeShardingKeyRewriteInMatcher, true>;

void optimizeShardingKeyRewriteIn(QueryTreeNodePtr & node, OptimizeShardingKeyRewriteInVisitor::Data data, ContextPtr context);

}
