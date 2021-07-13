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
        /// Expression of sharding_key for the Distributed() table
        const ExpressionActionsPtr & sharding_key_expr;
        /// Type of sharding_key column.
        const DataTypePtr & sharding_key_type;
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

}
