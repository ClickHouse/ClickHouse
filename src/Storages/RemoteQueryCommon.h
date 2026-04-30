#pragma once

#include <Analyzer/ColumnNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Core/Names.h>
#include <Core/QueryProcessingStage.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

#include <Processors/QueryPlan/QueryPlan.h>
#include <Core/Block.h>

#include <list>

namespace DB
{

struct StorageID;
struct StorageLimits;
using StorageLimitsList = std::list<StorageLimits>;
class IThrottler;
using ThrottlerPtr = std::shared_ptr<IThrottler>;
struct UnavailableShardTracker;
using UnavailableShardTrackerPtr = std::shared_ptr<UnavailableShardTracker>;

/// Convert `block` to match `header` structure, applying implicit type conversions.
/// Returns the block unchanged if structures already match.
Block adoptBlock(const Block & header, const Block & block, LoggerPtr log);

/// Build a minimal `INSERT INTO database.table (col1, col2, ...)` query AST
/// suitable for forwarding blocks to a remote server via `RemoteSink`.
ASTPtr createInsertToRemoteTableQuery(const std::string & database, const std::string & table, const Names & column_names);

ASTPtr createOptimizeForRemoteTableQuery(
    const StorageID & storage_id,
    const String & partition_id,
    bool final,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    bool cleanup);

/// Build `_partition_id IN ('pid1', 'pid2', ...)` filter AST for partition routing.
/// Used by selective replication and distributed query routing.
ASTPtr buildPartitionFilterAST(const Strings & partition_ids);

/// Unite multiple query plans with UnionStep. Handles empty / single / multi cases.
/// Used by selective replication and distributed query routing.
void unitePlanList(QueryPlan & result, std::vector<QueryPlanPtr> plans);

/// Build a `QueryPlan` containing a single `ReadFromRemote` step.
/// Used by selective replication and distributed query routing.
QueryPlanPtr createReadFromRemotePlan(
    ClusterProxy::SelectStreamFactory::Shards remote_shards,
    SharedHeader header,
    QueryProcessingStage::Enum processed_stage,
    const StorageID & main_table,
    const ASTPtr & table_func_ptr,
    ContextMutablePtr remote_context,
    ContextPtr source_context,
    LoggerPtr log,
    UInt32 shard_count,
    std::shared_ptr<const StorageLimitsList> storage_limits,
    const String & cluster_name,
    const String & step_description,
    ThrottlerPtr throttler = nullptr,
    UnavailableShardTrackerPtr unavailable_shard_tracker = nullptr);

/// Helper visitor to replace alias columns with their underlying expressions.
/// Used in distributed and selective replication routing to ensure remote nodes
/// receive normalized column names.
class ReplaceAliasColumnsVisitor : public InDepthQueryTreeVisitor<ReplaceAliasColumnsVisitor>
{
    static QueryTreeNodePtr getColumnNodeAliasExpression(const QueryTreeNodePtr & node)
    {
        const auto * column_node = node->as<ColumnNode>();
        if (!column_node || !column_node->hasExpression())
            return nullptr;

        const auto & column_source = column_node->getColumnSourceOrNull();
        if (!column_source || column_source->getNodeType() == QueryTreeNodeType::JOIN
                           || column_source->getNodeType() == QueryTreeNodeType::CROSS_JOIN
                           || column_source->getNodeType() == QueryTreeNodeType::ARRAY_JOIN)
            return nullptr;

        auto column_expression = column_node->getExpression();
        column_expression->setAlias(column_node->getColumnName());
        return column_expression;
    }

public:
    void visitImpl(QueryTreeNodePtr & node)
    {
        if (auto column_expression = getColumnNodeAliasExpression(node))
            node = column_expression;
    }
};

}
