#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/Cluster.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class ASTFunction;

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
        const ExpressionActionsPtr & sharding_key_expr;
        const std::string & sharding_key_column_name;
        const Cluster::ShardInfo & shard_info;
        const Cluster::SlotToShard & slots;
    };

    static bool needChildVisit(ASTPtr & /*node*/, const ASTPtr & /*child*/);
    static void visit(ASTPtr & node, Data & data);
    static void visit(ASTFunction & function, Data & data);
};

using OptimizeShardingKeyRewriteInVisitor = InDepthNodeVisitor<OptimizeShardingKeyRewriteInMatcher, true>;

}
