#pragma once

#include <Core/QueryProcessingStage.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPipeline/QueryPipeline.h>

#include <optional>
#include <string_view>

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

class ASTInsertQuery;

class QueryPipeline;

class ParallelReplicasReadingCoordinator;
using ParallelReplicasReadingCoordinatorPtr = std::shared_ptr<ParallelReplicasReadingCoordinator>;

namespace ClusterProxy
{

class SelectStreamFactory;

/// `database` is an initiator-only setting: it selects the default database for the user's query
/// (the equivalent of `USE`), and a remote server applies it the same way. But a query sent to a
/// remote server as part of a distributed query (a `Distributed` fan-out, a cluster table function,
/// parallel replicas) must resolve an unqualified table against the server's own default database —
/// set from the cluster config (`default_database` per replica) or from the connection. Forwarding
/// `database` would make the remote server `USE` the initiator's database first, reading the wrong
/// same-named table or failing with `UNKNOWN_TABLE` (and would also attribute the secondary queries
/// to the initiator's database in `system.query_log`). Strip it from every set of settings that is
/// sent along with such a query.
void stripDatabaseSetting(Settings & settings);

/// Reset every "initiator-only" setting — the query-shaping settings (`select`, `order`, `sort`,
/// `filter`, `limit`, `offset`, `page`, `additional_result_filter`), the result-serialisation
/// settings (`format`, `output_format`, `default_format`, `compression`), and the HTTP/path-only
/// settings (`http_allow_database_as_path`, `http_allow_table_as_file`, `http_allow_filters_as_path`,
/// `http_allow_filters_as_unrecognized_url_parameters`, `implicit_table_at_top_level`), plus
/// `database` (via `stripDatabaseSetting`). These are materialized on the initiator and must not be
/// forwarded to remote servers, where they would re-shape the per-shard subquery a second time, break
/// it (see the `format = 'Null'` case in the implementation), or — for the settings new to this
/// feature — be rejected as `UNKNOWN_SETTING` by an older shard during a rolling upgrade. Shared by
/// the `Distributed` fan-out, the `*Cluster` table functions (`IStorageCluster`), and the optimized
/// `parallel_distributed_insert_select` paths in `StorageDistributed`.
void stripInitiatorOnlySettings(Settings & settings);

/// True for exactly the settings reset by `stripInitiatorOnlySettings`. Used to also strip those
/// settings from a query's own `SETTINGS` clause before the query *text* is forwarded to a shard (the
/// optimized `parallel_distributed_insert_select` paths in `StorageDistributed` send a formatted query
/// string, not just a settings packet).
bool isInitiatorOnlySettingName(std::string_view name);

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

bool isSuitableForInsertSelectWithParallelReplicas(const ASTPtr & select, const ContextPtr & context);
bool canUseParallelReplicasOnInitiator(const ContextPtr & context);
ParallelReplicasReadingCoordinatorPtr dropReadFromRemoteInPlan(QueryPlan & query_plan);

/// Execute a distributed query, creating a query plan, from which the query pipeline can be built.
/// `stream_factory` object encapsulates the logic of creating plans for a different type of query
/// (currently SELECT, DESCRIBE).
void executeQuery(
    QueryPlan & query_plan,
    SharedHeader header,
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

std::optional<QueryPipeline> executeInsertSelectWithParallelReplicas(
    const ASTInsertQuery & query_ast,
    const ContextPtr & context,
    std::optional<QueryPipeline> pipeline = std::nullopt,
    std::optional<ParallelReplicasReadingCoordinatorPtr> coordinator = std::nullopt);

void executeQueryWithParallelReplicas(
    QueryPlan & query_plan,
    const StorageID & storage_id,
    SharedHeader header,
    QueryProcessingStage::Enum processed_stage,
    const ASTPtr & query_ast,
    QueryTreeNodePtr query_tree,
    PlannerContextPtr planner_context,
    ContextPtr context,
    std::shared_ptr<const StorageLimitsList> storage_limits,
    QueryPlanStepPtr read_from_merge_tree);

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
    SharedHeader header,
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
