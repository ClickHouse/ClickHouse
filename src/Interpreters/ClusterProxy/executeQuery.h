#pragma once

#include <Core/QueryProcessingStage.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

struct Settings;
struct DistributedSettings;
class Cluster;
using ClusterPtr = std::shared_ptr<Cluster>;
struct SelectQueryInfo;

class ColumnsDescription;
struct StorageSnapshot;

using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

class Pipe;
class QueryPlan;

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

struct StorageID;

struct StorageLimits;
using StorageLimitsList = std::list<StorageLimits>;

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;

class PlannerContext;
using PlannerContextPtr = std::shared_ptr<PlannerContext>;

class IQueryPlanStep;
using QueryPlanStepPtr = std::unique_ptr<IQueryPlanStep>;

namespace ClusterProxy
{

class SelectStreamFactory;

/// Update settings for Distributed query.
///
/// - Removes different restrictions (like max_concurrent_queries_for_user, max_memory_usage_for_user, etc.)
///   (but only if cluster does not have secret, since if it has, the user is the same)
/// - Update some settings depends on force_optimize_skip_unused_shards and:
///   - force_optimize_skip_unused_shards_nesting
///   - optimize_skip_unused_shards_nesting
///
/// @return new Context with adjusted settings
ContextMutablePtr updateSettingsForCluster(const Cluster & cluster, ContextPtr context, const Settings & settings, const StorageID & main_table);

using AdditionalShardFilterGenerator = std::function<ASTPtr(uint64_t)>;
AdditionalShardFilterGenerator
getShardFilterGeneratorForCustomKey(const Cluster & cluster, ContextPtr context, const ColumnsDescription & columns);

bool canUseParallelReplicasOnInitiator(const ContextPtr & context);

/// Execute a distributed query, creating a query plan, from which the query pipeline can be built.
/// `stream_factory` object encapsulates the logic of creating plans for a different type of query
/// (currently SELECT, DESCRIBE).
void executeQuery(
    QueryPlan & query_plan,
    const Block & header,
    QueryProcessingStage::Enum processed_stage,
    const StorageID & main_table,
    const ASTPtr & table_func_ptr,
    SelectStreamFactory & stream_factory,
    LoggerPtr log,
    ContextPtr context,
    const SelectQueryInfo & query_info,
    const ExpressionActionsPtr & sharding_key_expr,
    const std::string & sharding_key_column_name,
    const DistributedSettings & distributed_settings,
    AdditionalShardFilterGenerator shard_filter_generator,
    bool is_remote_function);

void executeQueryWithParallelReplicas(
    QueryPlan & query_plan,
    const StorageID & storage_id,
    const Block & header,
    QueryProcessingStage::Enum processed_stage,
    const ASTPtr & query_ast,
    ContextPtr context,
    std::shared_ptr<const StorageLimitsList> storage_limits,
    QueryPlanStepPtr read_from_merge_tree = nullptr);

void executeQueryWithParallelReplicas(
    QueryPlan & query_plan,
    const StorageID & storage_id,
    QueryProcessingStage::Enum processed_stage,
    const ASTPtr & query_ast,
    ContextPtr context,
    std::shared_ptr<const StorageLimitsList> storage_limits);

void executeQueryWithParallelReplicas(
    QueryPlan & query_plan,
    const StorageID & storage_id,
    QueryProcessingStage::Enum processed_stage,
    const QueryTreeNodePtr & query_tree,
    const PlannerContextPtr & planner_context,
    ContextPtr context,
    std::shared_ptr<const StorageLimitsList> storage_limits,
    QueryPlanStepPtr read_from_merge_tree);

void executeQueryWithParallelReplicasCustomKey(
    QueryPlan & query_plan,
    const StorageID & storage_id,
    const SelectQueryInfo & query_info,
    const ColumnsDescription & columns,
    const StorageSnapshotPtr & snapshot,
    QueryProcessingStage::Enum processed_stage,
    const Block & header,
    ContextPtr context);

void executeQueryWithParallelReplicasCustomKey(
    QueryPlan & query_plan,
    const StorageID & storage_id,
    const SelectQueryInfo & query_info,
    const ColumnsDescription & columns,
    const StorageSnapshotPtr & snapshot,
    QueryProcessingStage::Enum processed_stage,
    const QueryTreeNodePtr & query_tree,
    ContextPtr context);

void executeQueryWithParallelReplicasCustomKey(
    QueryPlan & query_plan,
    const StorageID & storage_id,
    SelectQueryInfo query_info,
    const ColumnsDescription & columns,
    const StorageSnapshotPtr & snapshot,
    QueryProcessingStage::Enum processed_stage,
    const ASTPtr & query_ast,
    ContextPtr context);
}

}
