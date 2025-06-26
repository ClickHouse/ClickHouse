#include <Storages/StorageDistributed.h>

#include <Databases/IDatabase.h>

#include <Disks/IDisk.h>

#include <QueryPipeline/RemoteQueryExecutor.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/ObjectUtils.h>
#include <DataTypes/NestedUtils.h>

#include <Disks/IVolume.h>

#include <Storages/Distributed/DistributedSettings.h>
#include <Storages/Distributed/DistributedSink.h>
#include <Storages/StorageFactory.h>
#include <Storages/AlterCommands.h>
#include <Storages/getStructureOfRemoteTable.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/StorageDummy.h>
#include <Storages/removeGroupingFunctionSpecializations.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <Columns/ColumnConst.h>

#include <Common/CurrentMetrics.h>
#include <Common/Macros.h>
#include <Common/ProfileEvents.h>
#include <Common/escapeForFileName.h>
#include <Common/formatReadable.h>
#include <Common/quoteString.h>
#include <Common/randomSeed.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/typeid_cast.h>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/IAST.h>
#include <Parsers/IdentifierQuotingStyle.h>
#include <Parsers/parseQuery.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/Passes/QueryAnalysisPass.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/WindowFunctionsUtils.h>
#include <Analyzer/Utils.h>

#include <Planner/Planner.h>
#include <Planner/Utils.h>

#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/JoinedTables.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/Context.h>
#include <Interpreters/createBlockSelector.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/getClusterName.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Interpreters/getHeaderForProcessingStage.h>

#include <TableFunctions/TableFunctionView.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <Storages/buildQueryTreeForShard.h>
#include <Storages/IStorageCluster.h>

#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sources/RemoteSource.h>
#include <Processors/Sinks/EmptySink.h>

#include <Core/Settings.h>
#include <Core/SettingsEnums.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <IO/ConnectionTimeouts.h>

#include <base/range.h>

#include <memory>
#include <filesystem>
#include <cassert>

#include <boost/algorithm/string/find_iterator.hpp>
#include <boost/algorithm/string/finder.hpp>


namespace fs = std::filesystem;

namespace
{
const UInt64 FORCE_OPTIMIZE_SKIP_UNUSED_SHARDS_HAS_SHARDING_KEY = 1;
const UInt64 FORCE_OPTIMIZE_SKIP_UNUSED_SHARDS_ALWAYS           = 2;

const UInt64 DISTRIBUTED_GROUP_BY_NO_MERGE_AFTER_AGGREGATION = 2;

const UInt64 PARALLEL_DISTRIBUTED_INSERT_SELECT_ALL = 2;
}

namespace ProfileEvents
{
    extern const Event DistributedRejectedInserts;
    extern const Event DistributedDelayedInserts;
    extern const Event DistributedDelayedInsertsMilliseconds;
}

namespace CurrentMetrics
{
    extern const Metric StorageDistributedThreads;
    extern const Metric StorageDistributedThreadsActive;
    extern const Metric StorageDistributedThreadsScheduled;
}

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool allow_nondeterministic_optimize_skip_unused_shards;
    extern const SettingsBool async_socket_for_remote;
    extern const SettingsBool async_query_sending_for_remote;
    extern const SettingsBool distributed_background_insert_batch;
    extern const SettingsUInt64 distributed_background_insert_timeout;
    extern const SettingsMilliseconds distributed_background_insert_sleep_time_ms;
    extern const SettingsMilliseconds distributed_background_insert_max_sleep_time_ms;
    extern const SettingsBool distributed_background_insert_split_batch_on_failure;
    extern const SettingsUInt64 distributed_group_by_no_merge;
    extern const SettingsBool distributed_foreground_insert;
    extern const SettingsUInt64 distributed_push_down_limit;
    extern const SettingsBool extremes;
    extern const SettingsUInt64 force_optimize_skip_unused_shards;
    extern const SettingsBool insert_allow_materialized_columns;
    extern const SettingsBool insert_distributed_one_random_shard;
    extern const SettingsUInt64 insert_shard_id;
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsUInt64 max_distributed_depth;
    extern const SettingsNonZeroUInt64 max_parallel_replicas;
    extern const SettingsBool optimize_distributed_group_by_sharding_key;
    extern const SettingsBool optimize_skip_unused_shards;
    extern const SettingsUInt64 optimize_skip_unused_shards_limit;
    extern const SettingsUInt64 parallel_distributed_insert_select;
    extern const SettingsBool prefer_localhost_replica;
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
    extern const SettingsBool prefer_global_in_and_join;
}

namespace DistributedSetting
{
    extern const DistributedSettingsUInt64 background_insert_batch;
    extern const DistributedSettingsMilliseconds background_insert_max_sleep_time_ms;
    extern const DistributedSettingsMilliseconds background_insert_sleep_time_ms;
    extern const DistributedSettingsUInt64 background_insert_split_batch_on_failure;
    extern const DistributedSettingsUInt64 bytes_to_delay_insert;
    extern const DistributedSettingsUInt64 bytes_to_throw_insert;
    extern const DistributedSettingsBool flush_on_detach;
    extern const DistributedSettingsUInt64 max_delay_to_insert;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int STORAGE_REQUIRES_PARAMETER;
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int INFINITE_LOOP;
    extern const int TYPE_MISMATCH;
    extern const int TOO_MANY_ROWS;
    extern const int UNABLE_TO_SKIP_UNUSED_SHARDS;
    extern const int INVALID_SHARD_ID;
    extern const int ALTER_OF_COLUMN_IS_FORBIDDEN;
    extern const int DISTRIBUTED_TOO_MANY_PENDING_BYTES;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int TOO_LARGE_DISTRIBUTED_DEPTH;
}

namespace ActionLocks
{
    extern const StorageActionBlockType DistributedSend;
}

namespace
{

/// Calculate maximum number in file names in directory and all subdirectories.
/// To ensure global order of data blocks yet to be sent across server restarts.
UInt64 getMaximumFileNumber(const std::string & dir_path)
{
    UInt64 res = 0;

    std::filesystem::recursive_directory_iterator begin(dir_path);
    std::filesystem::recursive_directory_iterator end;
    for (auto it = begin; it != end; ++it)
    {
        const auto & file_path = it->path();

        if (!std::filesystem::is_regular_file(*it) || !endsWith(file_path.filename().string(), ".bin"))
            continue;

        UInt64 num = 0;
        try
        {
            num = parse<UInt64>(file_path.filename().stem().string());
        }
        catch (Exception & e)
        {
            e.addMessage("Unexpected file name " + file_path.filename().string() + " found at " + file_path.parent_path().string() + ", should have numeric base name.");
            throw;
        }

        res = std::max(num, res);
    }

    return res;
}

std::string makeFormattedListOfShards(const ClusterPtr & cluster)
{
    WriteBufferFromOwnString buf;

    bool head = true;
    buf << "[";
    for (const auto & shard_info : cluster->getShardsInfo())
    {
        (head ? buf : buf << ", ") << shard_info.shard_num;
        head = false;
    }
    buf << "]";

    return buf.str();
}

ExpressionActionsPtr buildShardingKeyExpression(const ASTPtr & sharding_key, ContextPtr context, const NamesAndTypesList & columns, bool project)
{
    ASTPtr query = sharding_key;
    auto syntax_result = TreeRewriter(context).analyze(query, columns);
    return ExpressionAnalyzer(query, syntax_result, context).getActions(project);
}

bool isExpressionActionsDeterministic(const ExpressionActionsPtr & actions)
{
    for (const auto & action : actions->getActions())
    {
        if (action.node->type != ActionsDAG::ActionType::FUNCTION)
            continue;
        if (!action.node->function_base->isDeterministic())
            return false;
    }
    return true;
}

class ReplacingConstantExpressionsMatcher
{
public:
    using Data = Block;

    static bool needChildVisit(ASTPtr &, const ASTPtr &)
    {
        return true;
    }

    static void visit(ASTPtr & node, Block & block_with_constants)
    {
        if (!node->as<ASTFunction>())
            return;

        std::string name = node->getColumnName();
        if (block_with_constants.has(name))
        {
            const auto & result = block_with_constants.getByName(name);
            if (!isColumnConst(*result.column))
                return;

            node = std::make_shared<ASTLiteral>(assert_cast<const ColumnConst &>(*result.column).getField());
        }
    }
};

void replaceConstantExpressions(
    ASTPtr & node,
    ContextPtr context,
    const NamesAndTypesList & columns,
    ConstStoragePtr storage,
    const StorageSnapshotPtr & storage_snapshot)
{
    auto syntax_result = TreeRewriter(context).analyze(node, columns, storage, storage_snapshot);
    Block block_with_constants = KeyCondition::getBlockWithConstants(node, syntax_result, context);

    InDepthNodeVisitor<ReplacingConstantExpressionsMatcher, true> visitor(block_with_constants);
    visitor.visit(node);
}

size_t getClusterQueriedNodes(const Settings & settings, const ClusterPtr & cluster)
{
    size_t num_local_shards = cluster->getLocalShardCount();
    size_t num_remote_shards = cluster->getRemoteShardCount();
    UInt64 max_parallel_replicas = settings[Setting::allow_experimental_parallel_reading_from_replicas]
        ? settings[Setting::max_parallel_replicas] : 1;

    return (num_remote_shards + num_local_shards) * max_parallel_replicas;
}

}

/// For destruction of std::unique_ptr of type that is incomplete in class definition.
StorageDistributed::~StorageDistributed() = default;


VirtualColumnsDescription StorageDistributed::createVirtuals()
{
    /// NOTE: This is weird.
    /// Most of these virtual columns are part of MergeTree
    /// tables info. But Distributed is general-purpose engine.
    StorageInMemoryMetadata metadata;
    auto desc = MergeTreeData::createVirtuals(metadata);

    desc.addEphemeral("_shard_num", std::make_shared<DataTypeUInt32>(), "Deprecated. Use function shardNum instead");

    /// Add virtual columns from table with Merge engine.
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "The name of database which the row comes from");
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "The name of table which the row comes from");

    return desc;
}

StorageDistributed::StorageDistributed(
    const StorageID & id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    const String & remote_database_,
    const String & remote_table_,
    const String & cluster_name_,
    ContextPtr context_,
    const ASTPtr & sharding_key_,
    const String & storage_policy_name_,
    const String & relative_data_path_,
    const DistributedSettings & distributed_settings_,
    LoadingStrictnessLevel mode,
    ClusterPtr owned_cluster_,
    ASTPtr remote_table_function_ptr_,
    bool is_remote_function_)
    : IStorage(id_)
    , WithContext(context_->getGlobalContext())
    , remote_database(remote_database_)
    , remote_table(remote_table_)
    , remote_table_function_ptr(remote_table_function_ptr_)
    , remote_storage(remote_table_function_ptr ? StorageID::createEmpty() : StorageID{remote_database, remote_table})
    , log(getLogger("StorageDistributed (" + id_.table_name + ")"))
    , owned_cluster(std::move(owned_cluster_))
    , cluster_name(getContext()->getMacros()->expand(cluster_name_))
    , has_sharding_key(sharding_key_)
    , relative_data_path(relative_data_path_)
    , distributed_settings(std::make_unique<DistributedSettings>(distributed_settings_))
    , rng(randomSeed())
    , is_remote_function(is_remote_function_)
{
    if (!(*distributed_settings)[DistributedSetting::flush_on_detach] && (*distributed_settings)[DistributedSetting::background_insert_batch])
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Settings flush_on_detach=0 and background_insert_batch=1 are incompatible");

    StorageInMemoryMetadata storage_metadata;
    if (columns_.empty())
    {
        StorageID id = StorageID::createEmpty();
        id.table_name = remote_table;
        id.database_name = remote_database;
        storage_metadata.setColumns(getStructureOfRemoteTable(*getCluster(), id, getContext(), remote_table_function_ptr));
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
    setVirtuals(createVirtuals());

    if (sharding_key_)
    {
        sharding_key_expr = buildShardingKeyExpression(sharding_key_, getContext(), storage_metadata.getColumns().getAllPhysical(), false);
        sharding_key_column_name = sharding_key_->getColumnName();
        sharding_key_is_deterministic = isExpressionActionsDeterministic(sharding_key_expr);
    }

    if (!relative_data_path.empty())
    {
        storage_policy = getContext()->getStoragePolicy(storage_policy_name_);
        data_volume = storage_policy->getVolume(0);
        if (storage_policy->getVolumes().size() > 1)
            LOG_WARNING(log, "Storage policy for Distributed table has multiple volumes. "
                             "Only {} volume will be used to store data. Other will be ignored.", data_volume->getName());
    }

    /// Sanity check. Skip check if the table is already created to allow the server to start.
    if (mode <= LoadingStrictnessLevel::CREATE)
    {
        if (remote_database.empty() && !remote_table_function_ptr && !getCluster()->maybeCrossReplication())
            LOG_WARNING(log, "Name of remote database is empty. Default database will be used implicitly.");

        size_t num_local_shards = getCluster()->getLocalShardCount();
        if (num_local_shards && (remote_database.empty() || remote_database == id_.database_name) && remote_table == id_.table_name)
            throw Exception(ErrorCodes::INFINITE_LOOP, "Distributed table {} looks at itself", id_.table_name);
    }

    initializeFromDisk();
}


QueryProcessingStage::Enum StorageDistributed::getQueryProcessingStage(
    ContextPtr local_context,
    QueryProcessingStage::Enum to_stage,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info) const
{
    const auto & settings = local_context->getSettingsRef();
    ClusterPtr cluster = getCluster();

    size_t nodes = getClusterQueriedNodes(settings, cluster);

    query_info.cluster = cluster;

    if (!local_context->canUseParallelReplicasCustomKeyForCluster(*cluster))
    {
        if (nodes > 1 && settings[Setting::optimize_skip_unused_shards])
        {
            /// Always calculate optimized cluster here, to avoid conditions during read()
            /// (Anyway it will be calculated in the read())
            auto syntax_analyzer_result = query_info.syntax_analyzer_result;
            ClusterPtr optimized_cluster = getOptimizedCluster(local_context, storage_snapshot, query_info, syntax_analyzer_result);
            if (optimized_cluster)
            {
                LOG_DEBUG(log, "Skipping irrelevant shards - the query will be sent to the following shards of the cluster (shard numbers): {}",
                        makeFormattedListOfShards(optimized_cluster));

                cluster = optimized_cluster;
                query_info.optimized_cluster = cluster;

                nodes = getClusterQueriedNodes(settings, cluster);
            }
            else
            {
                LOG_DEBUG(log, "Unable to figure out irrelevant shards from WHERE/PREWHERE clauses - the query will be sent to all shards of the cluster{}",
                        has_sharding_key ? "" : " (no sharding key)");
            }
        }
    }

    if (settings[Setting::distributed_group_by_no_merge])
    {
        if (settings[Setting::distributed_group_by_no_merge] == DISTRIBUTED_GROUP_BY_NO_MERGE_AFTER_AGGREGATION)
        {
            if (settings[Setting::distributed_push_down_limit])
                return QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit;
            return QueryProcessingStage::WithMergeableStateAfterAggregation;
        }

        /// NOTE: distributed_group_by_no_merge=1 does not respect distributed_push_down_limit
        /// (since in this case queries processed separately and the initiator is just a proxy in this case).
        if (to_stage != QueryProcessingStage::Complete)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Queries with distributed_group_by_no_merge=1 should be processed to Complete stage");
        return QueryProcessingStage::Complete;
    }

    /// Nested distributed query cannot return Complete stage,
    /// since the parent query need to aggregate the results after.
    if (to_stage == QueryProcessingStage::WithMergeableState)
        return QueryProcessingStage::WithMergeableState;

    /// If there is only one node, the query can be fully processed by the
    /// shard, initiator will work as a proxy only.
    if (nodes == 1)
    {
        /// In case the query was processed to
        /// WithMergeableStateAfterAggregation/WithMergeableStateAfterAggregationAndLimit
        /// (which are greater the Complete stage)
        /// we cannot return Complete (will break aliases and similar),
        /// relevant for Distributed over Distributed
        return std::max(to_stage, QueryProcessingStage::Complete);
    }
    if (nodes == 0)
    {
        /// In case of 0 shards, the query should be processed fully on the initiator,
        /// since we need to apply aggregations.
        /// That's why we need to return FetchColumns.
        return QueryProcessingStage::FetchColumns;
    }

    std::optional<QueryProcessingStage::Enum> optimized_stage;
    if (settings[Setting::allow_experimental_analyzer])
        optimized_stage = getOptimizedQueryProcessingStageAnalyzer(query_info, settings);
    else
        optimized_stage = getOptimizedQueryProcessingStage(query_info, settings);
    if (optimized_stage)
    {
        if (*optimized_stage == QueryProcessingStage::Complete)
            return std::min(to_stage, *optimized_stage);
        return *optimized_stage;
    }

    return QueryProcessingStage::WithMergeableState;
}

/// Reuses the logic of isPartitionKeySuitsGroupByKey in useDataParallelAggregation.cpp
/// Will skip merging step in the initial server when the following conditions are met:
/// 1. Sharding key columns should be a subset of expression columns.
/// 2. Sharding key expression is a deterministic function of col1, ..., coln and expression key is injective functions of these col1, ..., coln.
/// 3. If the expression contains non-injective function, return false.
bool StorageDistributed::isShardingKeySuitsQueryTreeNodeExpression(
    const QueryTreeNodePtr & expr, const SelectQueryInfo & query_info) const
{
    ColumnsWithTypeAndName empty_input_columns;
    ColumnNodePtrWithHashSet empty_correlated_columns_set;
    // When comparing sharding key expressions, we need to ignore table qualifiers in column names
    // because the sharding key is defined without table qualifiers, but the query expression
    // may have internal table aliases (e.g. __table1.id). Setting use_column_identifier_as_action_node_name=false
    // makes the DAG builder use plain column names without table qualifiers.
    auto [expression_dag, correlated_subtrees] = buildActionsDAGFromExpressionNode(
        expr,
        empty_input_columns,
        query_info.planner_context,
        empty_correlated_columns_set,
        false /* use_column_identifier_as_action_node_name */);

    correlated_subtrees.assertEmpty("in sharding key expression");

    if (expression_dag.hasArrayJoin() || expression_dag.hasStatefulFunctions() || expression_dag.hasNonDeterministic())
        return false;

    const auto & expr_key_required_columns = expression_dag.getRequiredColumnsNames();

    const auto & sharding_key_dag = sharding_key_expr->getActionsDAG();

    for (const auto & col : sharding_key_dag.getRequiredColumnsNames())
    {
        if (std::ranges::find(expr_key_required_columns, col) == expr_key_required_columns.end())
            return false;
    }

    auto irreducibe_nodes = removeInjectiveFunctionsFromResultsRecursively(expression_dag);
    for (const auto & node : irreducibe_nodes)
    {
        if (node->type == ActionsDAG::ActionType::FUNCTION && !isInjectiveFunction(node))
        {
            return false;
        }
    }
    const auto matches = matchTrees(expression_dag.getOutputs(), sharding_key_dag);
    return allOutputsDependsOnlyOnAllowedNodes(sharding_key_dag, irreducibe_nodes, matches);
}

std::optional<QueryProcessingStage::Enum> StorageDistributed::getOptimizedQueryProcessingStageAnalyzer(const SelectQueryInfo & query_info, const Settings & settings) const
{
    bool optimize_sharding_key_aggregation = settings[Setting::optimize_skip_unused_shards] && settings[Setting::optimize_distributed_group_by_sharding_key]
        && has_sharding_key && (settings[Setting::allow_nondeterministic_optimize_skip_unused_shards] || sharding_key_is_deterministic);

    QueryProcessingStage::Enum default_stage = QueryProcessingStage::WithMergeableStateAfterAggregation;
    if (settings[Setting::distributed_push_down_limit])
        default_stage = QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit;

    const auto & query_node = query_info.query_tree->as<const QueryNode &>();

    // GROUP BY qualifiers
    // - TODO: WITH TOTALS can be implemented
    // - TODO: WITH ROLLUP can be implemented (I guess)
    if (query_node.isGroupByWithTotals() || query_node.isGroupByWithRollup() || query_node.isGroupByWithCube())
        return {};

    // Window functions are not supported.
    if (hasWindowFunctionNodes(query_info.query_tree))
        return {};
    // TODO: extremes support can be implemented
    if (settings[Setting::extremes])
        return {};

    // DISTINCT
    if (query_node.isDistinct())
    {
        if (!optimize_sharding_key_aggregation || !isShardingKeySuitsQueryTreeNodeExpression(query_node.getProjectionNode(), query_info))
            return {};
    }

    // GROUP BY
    if (query_info.has_aggregates || query_node.hasGroupBy())
    {
        if (!optimize_sharding_key_aggregation || !query_node.hasGroupBy() || query_node.isGroupByWithGroupingSets() || !isShardingKeySuitsQueryTreeNodeExpression(query_node.getGroupByNode(), query_info))
            return {};
    }

    // LIMIT BY
    if (query_node.hasLimitBy())
    {
        if (!optimize_sharding_key_aggregation || !isShardingKeySuitsQueryTreeNodeExpression(query_node.getLimitByNode(), query_info))
            return {};
    }

    // ORDER BY
    if (query_node.hasOrderBy())
        return default_stage;

    // LIMIT
    // OFFSET
    if (query_node.hasLimit() || query_node.hasOffset())
        return default_stage;

    // Only simple SELECT FROM GROUP BY sharding_key can use Complete state.
    return QueryProcessingStage::Complete;
}

std::optional<QueryProcessingStage::Enum> StorageDistributed::getOptimizedQueryProcessingStage(const SelectQueryInfo & query_info, const Settings & settings) const
{
    bool optimize_sharding_key_aggregation = settings[Setting::optimize_skip_unused_shards] && settings[Setting::optimize_distributed_group_by_sharding_key]
        && has_sharding_key && (settings[Setting::allow_nondeterministic_optimize_skip_unused_shards] || sharding_key_is_deterministic);

    QueryProcessingStage::Enum default_stage = QueryProcessingStage::WithMergeableStateAfterAggregation;
    if (settings[Setting::distributed_push_down_limit])
        default_stage = QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit;

    const auto & select = query_info.query->as<ASTSelectQuery &>();

    auto expr_contains_sharding_key = [&](const auto & exprs) -> bool
    {
        std::unordered_set<std::string> expr_columns;
        for (auto & expr : exprs)
        {
            auto id = expr->template as<ASTIdentifier>();
            if (!id)
                continue;
            expr_columns.emplace(id->name());
        }

        for (const auto & column : sharding_key_expr->getRequiredColumns())
        {
            if (!expr_columns.contains(column))
                return false;
        }

        return true;
    };

    // GROUP BY qualifiers
    // - TODO: WITH TOTALS can be implemented
    // - TODO: WITH ROLLUP can be implemented (I guess)
    if (select.group_by_with_totals || select.group_by_with_rollup || select.group_by_with_cube)
        return {};
    // Window functions are not supported.
    if (query_info.has_window)
        return {};
    // TODO: extremes support can be implemented
    if (settings[Setting::extremes])
        return {};

    // DISTINCT
    if (select.distinct)
    {
        if (!optimize_sharding_key_aggregation || !expr_contains_sharding_key(select.select()->children))
            return {};
    }

    // GROUP BY
    const ASTPtr group_by = select.groupBy();

    bool has_aggregates = query_info.has_aggregates;
    if (query_info.syntax_analyzer_result)
        has_aggregates = !query_info.syntax_analyzer_result->aggregates.empty();

    if (has_aggregates || group_by)
    {
        if (!optimize_sharding_key_aggregation || !group_by || !expr_contains_sharding_key(group_by->children))
            return {};
    }

    // LIMIT BY
    if (const ASTPtr limit_by = select.limitBy())
    {
        if (!optimize_sharding_key_aggregation || !expr_contains_sharding_key(limit_by->children))
            return {};
    }

    // ORDER BY
    if (const ASTPtr order_by = select.orderBy())
        return default_stage;

    // LIMIT
    // OFFSET
    if (select.limitLength() || select.limitOffset())
        return default_stage;

    // Only simple SELECT FROM GROUP BY sharding_key can use Complete state.
    return QueryProcessingStage::Complete;
}

static bool requiresObjectColumns(const ColumnsDescription & all_columns, ASTPtr query)
{
    if (!hasDynamicSubcolumnsDeprecated(all_columns))
        return false;

    if (!query)
        return true;

    RequiredSourceColumnsVisitor::Data columns_context;
    RequiredSourceColumnsVisitor(columns_context).visit(query);

    auto required_columns = columns_context.requiredColumns();
    for (const auto & required_column : required_columns)
    {
        auto name_in_storage = Nested::splitName(required_column).first;
        auto column_in_storage = all_columns.tryGetPhysical(name_in_storage);

        if (column_in_storage && column_in_storage->type->hasDynamicSubcolumnsDeprecated())
            return true;
    }

    return false;
}

StorageSnapshotPtr StorageDistributed::getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const
{
    return getStorageSnapshotForQuery(metadata_snapshot, nullptr, query_context);
}

StorageSnapshotPtr StorageDistributed::getStorageSnapshotForQuery(
    const StorageMetadataPtr & metadata_snapshot, const ASTPtr & query, ContextPtr /*query_context*/) const
{
    /// If query doesn't use columns of type Object, don't deduce
    /// concrete types for them, because it required extra round trip.
    auto snapshot_data = std::make_unique<SnapshotData>();
    if (!requiresObjectColumns(metadata_snapshot->getColumns(), query))
        return std::make_shared<StorageSnapshot>(*this, metadata_snapshot, ColumnsDescription{}, std::move(snapshot_data));

    snapshot_data->objects_by_shard = getExtendedObjectsOfRemoteTables(
        *getCluster(),
        StorageID{remote_database, remote_table},
        metadata_snapshot->getColumns(),
        getContext());

    auto object_columns = DB::getConcreteObjectColumns(
        snapshot_data->objects_by_shard.begin(),
        snapshot_data->objects_by_shard.end(),
        metadata_snapshot->getColumns(),
        [](const auto & shard_num_and_columns) -> const auto & { return shard_num_and_columns.second; });

    return std::make_shared<StorageSnapshot>(*this, metadata_snapshot, std::move(object_columns), std::move(snapshot_data));
}

namespace
{

class ReplaseAliasColumnsVisitor : public InDepthQueryTreeVisitor<ReplaseAliasColumnsVisitor>
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

class RewriteInToGlobalInVisitor : public InDepthQueryTreeVisitorWithContext<RewriteInToGlobalInVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<RewriteInToGlobalInVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (auto * function_node = node->as<FunctionNode>(); function_node && isNameOfLocalInFunction(function_node->getFunctionName()))
        {
            auto * query = function_node->getArguments().getNodes()[1]->as<QueryNode>();
            if (!query)
                return;
            bool no_replace = true;
            for (const auto & table_node : extractTableExpressions(query->getJoinTree(), false, true))
            {
                const StorageDistributed * storage_distributed = nullptr;
                if (const TableNode * table_node_typed = table_node->as<TableNode>())
                    storage_distributed = typeid_cast<const StorageDistributed *>(table_node_typed->getStorage().get());
                else if (const TableFunctionNode * table_function_node_typed = table_node->as<TableFunctionNode>())
                    storage_distributed = typeid_cast<const StorageDistributed *>(table_function_node_typed->getStorage().get());

                if (!storage_distributed)
                {
                    no_replace = false;
                    break;
                }
            }
            if (no_replace)
                return;

            auto result_function = std::make_shared<FunctionNode>(getGlobalInFunctionNameForLocalInFunctionName(function_node->getFunctionName()));
            result_function->getArguments().getNodes() = std::move(function_node->getArguments().getNodes());
            resolveOrdinaryFunctionNodeByName(*result_function, result_function->getFunctionName(), getContext());
            node = result_function;
        }
    }

    static bool needChildVisit(QueryTreeNodePtr & parent, QueryTreeNodePtr &)
    {
        if (auto * function_node = parent->as<FunctionNode>(); function_node && function_node->getFunctionName().starts_with("global"))
            return false;

        return true;
    }
};

bool rewriteJoinToGlobalJoinIfNeeded(QueryTreeNodePtr join_tree)
{
    bool rewrite = false;

    auto * join = join_tree->as<JoinNode>();
    if (!join)
        return rewrite;

    auto table_expression = join->getRightTableExpression();

    if (QueryNode * query = table_expression->as<QueryNode>())
        rewrite = rewriteJoinToGlobalJoinIfNeeded(query->getJoinTree());
    else if (const TableNode * table_node_typed = table_expression->as<TableNode>())
    {
        if (!typeid_cast<const StorageDistributed *>(table_node_typed->getStorage().get()))
            rewrite = true;
    }
    else if (const TableFunctionNode * table_function_node_typed = table_expression->as<TableFunctionNode>())
    {
        if (!typeid_cast<const StorageDistributed *>(table_function_node_typed->getStorage().get()))
            rewrite = true;
    }

    if (rewrite)
        join->setLocality(JoinLocality::Global);

    rewriteJoinToGlobalJoinIfNeeded(join->getLeftTableExpression());

    return rewrite;
}

QueryTreeNodePtr buildQueryTreeDistributed(SelectQueryInfo & query_info,
    const StorageSnapshotPtr & distributed_storage_snapshot,
    const StorageID & remote_storage_id,
    const ASTPtr & remote_table_function)
{
    auto & planner_context = query_info.planner_context;
    const auto & query_context = planner_context->getQueryContext();

    std::optional<TableExpressionModifiers> table_expression_modifiers;

    if (auto * query_info_table_node = query_info.table_expression->as<TableNode>())
        table_expression_modifiers = query_info_table_node->getTableExpressionModifiers();
    else if (auto * query_info_table_function_node = query_info.table_expression->as<TableFunctionNode>())
        table_expression_modifiers = query_info_table_function_node->getTableExpressionModifiers();

    QueryTreeNodePtr replacement_table_expression;

    if (remote_table_function)
    {
        auto remote_table_function_query_tree = buildQueryTree(remote_table_function, query_context);
        auto & remote_table_function_node = remote_table_function_query_tree->as<FunctionNode &>();

        auto table_function_node = std::make_shared<TableFunctionNode>(remote_table_function_node.getFunctionName());
        table_function_node->getArgumentsNode() = remote_table_function_node.getArgumentsNode();

        if (table_expression_modifiers)
            table_function_node->setTableExpressionModifiers(*table_expression_modifiers);

        QueryAnalysisPass query_analysis_pass;
        QueryTreeNodePtr node = table_function_node;
        query_analysis_pass.run(node, query_context);

        replacement_table_expression = std::move(table_function_node);
    }
    else
    {
        auto get_column_options = GetColumnsOptions(GetColumnsOptions::All).withExtendedObjects().withVirtuals();

        auto column_names_and_types = distributed_storage_snapshot->getColumns(get_column_options);

        auto storage = std::make_shared<StorageDummy>(remote_storage_id, ColumnsDescription{column_names_and_types});
        auto table_node = std::make_shared<TableNode>(std::move(storage), query_context);

        if (table_expression_modifiers)
            table_node->setTableExpressionModifiers(*table_expression_modifiers);

        replacement_table_expression = std::move(table_node);
    }

    replacement_table_expression->setAlias(query_info.table_expression->getAlias());

    auto query_tree_to_modify = query_info.query_tree->cloneAndReplace(query_info.table_expression, std::move(replacement_table_expression));
    ReplaseAliasColumnsVisitor replase_alias_columns_visitor;
    replase_alias_columns_visitor.visit(query_tree_to_modify);

    const auto & settings = query_context->getSettingsRef();

    if (settings[Setting::prefer_global_in_and_join])
    {
        auto & query_node = query_tree_to_modify->as<QueryNode&>();
        if (query_node.hasWhere())
        {
            RewriteInToGlobalInVisitor visitor(query_context);
            visitor.visit(query_node.getWhere());
        }

        rewriteJoinToGlobalJoinIfNeeded(query_node.getJoinTree());
    }

    return buildQueryTreeForShard(query_info.planner_context, query_tree_to_modify, /*allow_global_join_for_right_table*/ false);
}

}

void StorageDistributed::read(
    QueryPlan & query_plan,
    const Names &,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    const size_t /*max_block_size*/,
    const size_t /*num_streams*/)
{
    Block header;

    SelectQueryInfo modified_query_info = query_info;

    const auto & settings = local_context->getSettingsRef();

    if (settings[Setting::allow_experimental_analyzer])
    {
        StorageID remote_storage_id = StorageID::createEmpty();
        if (!remote_table_function_ptr)
            remote_storage_id = StorageID{remote_database, remote_table};

        auto query_tree_distributed = buildQueryTreeDistributed(modified_query_info,
            query_info.initial_storage_snapshot ? query_info.initial_storage_snapshot : storage_snapshot,
            remote_storage_id,
            remote_table_function_ptr);
        header = InterpreterSelectQueryAnalyzer::getSampleBlock(query_tree_distributed, local_context, SelectQueryOptions(processed_stage).analyze());
        /** For distributed tables we do not need constants in header, since we don't send them to remote servers.
          * Moreover, constants can break some functions like `hostName` that are constants only for local queries.
          */
        for (auto & column : header)
            column.column = column.column->convertToFullColumnIfConst();
        modified_query_info.query = queryNodeToDistributedSelectQuery(query_tree_distributed);

        modified_query_info.query_tree = std::move(query_tree_distributed);

        /// Return directly (with correct header) if no shard to query.
        if (modified_query_info.getCluster()->getShardsInfo().empty())
            return;
    }
    else
    {
        header = InterpreterSelectQuery(modified_query_info.query, local_context, SelectQueryOptions(processed_stage).analyze()).getSampleBlock();

        modified_query_info.query = ClusterProxy::rewriteSelectQuery(
            local_context, modified_query_info.query,
            remote_database, remote_table, remote_table_function_ptr);

        if (modified_query_info.getCluster()->getShardsInfo().empty())
        {
            Pipe pipe(std::make_shared<NullSource>(header));
            auto read_from_pipe = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
            read_from_pipe->setStepDescription("Read from NullSource (Distributed)");
            query_plan.addStep(std::move(read_from_pipe));

            return;
        }
    }

    const auto & snapshot_data = assert_cast<const SnapshotData &>(*storage_snapshot->data);
    ClusterProxy::SelectStreamFactory select_stream_factory =
        ClusterProxy::SelectStreamFactory(
            header,
            snapshot_data.objects_by_shard,
            storage_snapshot,
            processed_stage);

    auto shard_filter_generator = ClusterProxy::getShardFilterGeneratorForCustomKey(
        *modified_query_info.getCluster(), local_context, getInMemoryMetadataPtr()->columns);

    ClusterProxy::executeQuery(
        query_plan,
        header,
        processed_stage,
        remote_storage,
        remote_table_function_ptr,
        select_stream_factory,
        log,
        local_context,
        modified_query_info,
        sharding_key_expr,
        sharding_key_column_name,
        *distributed_settings,
        shard_filter_generator,
        is_remote_function);

    /// This is a bug, it is possible only when there is no shards to query, and this is handled earlier.
    if (!query_plan.isInitialized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline is not initialized");
}


SinkToStoragePtr StorageDistributed::write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, bool /*async_insert*/)
{
    auto cluster = getCluster();
    const auto & settings = local_context->getSettingsRef();

    auto shard_num = cluster->getLocalShardCount() + cluster->getRemoteShardCount();

    /// If sharding key is not specified, then you can only write to a shard containing only one shard
    if (!settings[Setting::insert_shard_id] && !settings[Setting::insert_distributed_one_random_shard] && !has_sharding_key && shard_num >= 2)
    {
        throw Exception(ErrorCodes::STORAGE_REQUIRES_PARAMETER,
                        "Method write is not supported by storage {} with more than one shard and no sharding key provided", getName());
    }

    if (settings[Setting::insert_shard_id] && settings[Setting::insert_shard_id] > shard_num)
    {
        throw Exception(ErrorCodes::INVALID_SHARD_ID, "Shard id should be range from 1 to shard number");
    }

    /// Force sync insertion if it is remote() table function
    bool insert_sync = settings[Setting::distributed_foreground_insert] || settings[Setting::insert_shard_id] || owned_cluster || relative_data_path.empty();
    auto timeout = settings[Setting::distributed_background_insert_timeout];

    Names columns_to_send;
    if (settings[Setting::insert_allow_materialized_columns])
        columns_to_send = metadata_snapshot->getSampleBlock().getNames();
    else
        columns_to_send = metadata_snapshot->getSampleBlockNonMaterialized().getNames();

    /// DistributedSink will not own cluster, but will own ConnectionPools of the cluster
    return std::make_shared<DistributedSink>(local_context, *this, metadata_snapshot, cluster, insert_sync, timeout, columns_to_send);
}


std::optional<QueryPipeline> StorageDistributed::distributedWriteBetweenDistributedTables(const StorageDistributed & src_distributed, const ASTInsertQuery & query, ContextPtr local_context) const
{
    const auto & settings = local_context->getSettingsRef();
    auto new_query = std::dynamic_pointer_cast<ASTInsertQuery>(query.clone());

    if (src_distributed.remote_table_function_ptr)
    {
        const TableFunctionPtr src_table_function =
            TableFunctionFactory::instance().get(src_distributed.remote_table_function_ptr, local_context);
        if (const TableFunctionView * view_function = typeid_cast<const TableFunctionView *>(src_table_function.get()))
        {
            new_query->select = view_function->getSelectQuery().clone();
        }
        else
        {
            const auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
            select_with_union_query->list_of_selects = std::make_shared<ASTExpressionList>();

            const auto select = std::make_shared<ASTSelectQuery>();

            auto expression_list = std::make_shared<ASTExpressionList>();
            expression_list->children.push_back(std::make_shared<ASTAsterisk>());
            select->setExpression(ASTSelectQuery::Expression::SELECT, expression_list->clone());
            select->addTableFunction(src_distributed.remote_table_function_ptr);

            select_with_union_query->list_of_selects->children.push_back(select->clone());

            new_query->select = select_with_union_query;
        }
    }
    else
    {
        const auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
        select_with_union_query->list_of_selects = std::make_shared<ASTExpressionList>();

        auto * select = query.select->as<ASTSelectWithUnionQuery &>().list_of_selects->children.at(0)->as<ASTSelectQuery>();
        auto new_select_query = std::dynamic_pointer_cast<ASTSelectQuery>(select->clone());
        select_with_union_query->list_of_selects->children.push_back(new_select_query);

        new_select_query->replaceDatabaseAndTable(src_distributed.getRemoteDatabaseName(), src_distributed.getRemoteTableName());

        new_query->select = select_with_union_query;
    }

    const auto src_cluster = src_distributed.getCluster();
    const auto dst_cluster = getCluster();
    const Cluster::AddressesWithFailover & src_addresses = src_cluster->getShardsAddresses();
    const Cluster::AddressesWithFailover & dst_addresses = dst_cluster->getShardsAddresses();
    /// Compare addresses instead of cluster name, to handle remote()/cluster().
    /// (since for remote()/cluster() the getClusterName() is empty string)
    if (src_addresses != dst_addresses)
    {
        /// The warning should be produced only for root queries,
        /// since in case of parallel_distributed_insert_select=1,
        /// it will produce warning for the rewritten insert,
        /// since destination table is still Distributed there.
        if (local_context->getClientInfo().distributed_depth == 0)
        {
            LOG_WARNING(log,
                "Parallel distributed INSERT SELECT is not possible "
                "(source cluster={} ({} addresses), destination cluster={} ({} addresses))",
                src_distributed.getClusterName(),
                src_addresses.size(),
                getClusterName(),
                dst_addresses.size());
        }
        return {};
    }

    if (settings[Setting::parallel_distributed_insert_select] == PARALLEL_DISTRIBUTED_INSERT_SELECT_ALL)
    {
        new_query->table_id = StorageID(getRemoteDatabaseName(), getRemoteTableName());
        /// Reset table function for INSERT INTO remote()/cluster()
        new_query->table_function.reset();
    }

    const auto & shards_info = dst_cluster->getShardsInfo();

    String new_query_str;
    {
        WriteBufferFromOwnString buf;
        IAST::FormatSettings ast_format_settings(
            /*one_line*/ true, /*hilite*/ false, /*identifier_quoting_rule_=*/IdentifierQuotingRule::Always);
        new_query->IAST::format(buf, ast_format_settings);
        new_query_str = buf.str();
    }

    QueryPipeline pipeline;
    ContextMutablePtr query_context = Context::createCopy(local_context);
    query_context->increaseDistributedDepth();
    query_context->setSetting("enable_parallel_replicas", Field{0}); // TODO: allow parallel inserts with PR for distributed tables

    for (size_t shard_index : collections::range(0, shards_info.size()))
    {
        const auto & shard_info = shards_info[shard_index];
        if (shard_info.isLocal() && settings[Setting::prefer_localhost_replica])
        {
            InterpreterInsertQuery interpreter(
                new_query,
                query_context,
                /* allow_materialized */ false,
                /* no_squash */ false,
                /* no_destination */ false,
                /* async_isnert */ false);
            pipeline.addCompletedPipeline(interpreter.execute().pipeline);
        }
        else
        {
            auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(settings);
            auto connections = shard_info.pool->getMany(timeouts, settings, PoolMode::GET_ONE);
            if (connections.empty() || connections.front().isNull())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected exactly one connection for shard {}",
                    shard_info.shard_num);

            ///  INSERT SELECT query returns empty block
            auto remote_query_executor
                = std::make_shared<RemoteQueryExecutor>(std::move(connections), new_query_str, Block{}, query_context);
            QueryPipeline remote_pipeline(std::make_shared<RemoteSource>(
                remote_query_executor, false, settings[Setting::async_socket_for_remote], settings[Setting::async_query_sending_for_remote]));
            remote_pipeline.complete(std::make_shared<EmptySink>(remote_query_executor->getHeader()));

            pipeline.addCompletedPipeline(std::move(remote_pipeline));
        }
    }

    return pipeline;
}

static std::shared_ptr<const ActionsDAG> getFilterFromQuery(const ASTPtr & ast, ContextPtr context)
{
    QueryPlan plan;
    SelectQueryOptions options;
    options.only_analyze = true;
    if (context->getSettingsRef()[Setting::allow_experimental_analyzer])
    {
        InterpreterSelectQueryAnalyzer interpreter(ast, context, options);
        plan = std::move(interpreter).extractQueryPlan();
    }
    else
    {
        InterpreterSelectWithUnionQuery interpreter(ast, context, options);
        interpreter.buildQueryPlan(plan);
    }

    plan.optimize(QueryPlanOptimizationSettings(context));

    std::stack<QueryPlan::Node *> nodes;
    nodes.push(plan.getRootNode());

    SourceStepWithFilter * source = nullptr;

    while (!nodes.empty())
    {
        const auto * node = nodes.top();
        nodes.pop();

        if (auto * with_filter = dynamic_cast<SourceStepWithFilter *>(node->step.get()))
        {
            if (source)
            {
                WriteBufferFromOwnString buf;
                plan.explainPlan(buf, {});
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Found multiple source steps for query\n{}\nPlan\n{}",
                    ast->formatForErrorMessage(), buf.str());
            }

            source = with_filter;
        }
    }

    if (!source)
        return nullptr;

    return source->detachFilterActionsDAG();
}


std::optional<QueryPipeline> StorageDistributed::distributedWriteFromClusterStorage(const IStorageCluster & src_storage_cluster, const ASTInsertQuery & query, ContextPtr local_context) const
{
    const auto & settings = local_context->getSettingsRef();

    auto filter = getFilterFromQuery(query.select, local_context);
    const ActionsDAG::Node * predicate = nullptr;
    if (filter)
        predicate = filter->getOutputs().at(0);

    auto dst_cluster = getCluster();

    auto new_query = std::dynamic_pointer_cast<ASTInsertQuery>(query.clone());
    if (settings[Setting::parallel_distributed_insert_select] == PARALLEL_DISTRIBUTED_INSERT_SELECT_ALL)
    {
        new_query->table_id = StorageID(getRemoteDatabaseName(), getRemoteTableName());
        /// Reset table function for INSERT INTO remote()/cluster()
        new_query->table_function.reset();
    }

    String new_query_str;
    {
        WriteBufferFromOwnString buf;
        IAST::FormatSettings ast_format_settings(
            /*one_line=*/true, /*hilite=*/false, /*identifier_quoting_rule=*/IdentifierQuotingRule::Always);
        new_query->IAST::format(buf, ast_format_settings);
        new_query_str = buf.str();
    }

    QueryPipeline pipeline;
    ContextMutablePtr query_context = Context::createCopy(local_context);
    query_context->increaseDistributedDepth();

    const auto & current_settings = query_context->getSettingsRef();
    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(current_settings);

    const auto cluster = getCluster();

    /// Select query is needed for pruining on virtual columns
    auto number_of_replicas = static_cast<UInt64>(cluster->getShardsInfo().size());
    auto extension = src_storage_cluster.getTaskIteratorExtension(
        predicate, filter.get(), local_context, number_of_replicas);

    /// Here we take addresses from destination cluster and assume source table exists on these nodes
    size_t replica_index = 0;
    for (const auto & replicas : cluster->getShardsInfo())
    {
        /// Skip unavailable hosts if necessary
        auto try_results = replicas.pool->getMany(
            timeouts, current_settings, PoolMode::GET_MANY, /*async_callback*/ {}, /*skip_unavailable_endpoints*/ true);

        /// There will be only one replica, because we consider each replica as a shard
        for (const auto & try_result : try_results)
        {
            IConnections::ReplicaInfo replica_info{ .number_of_current_replica = replica_index++ };

            auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
                std::vector<IConnectionPool::Entry>{try_result},
                new_query_str,
                Block{},
                query_context,
                /*throttler=*/nullptr,
                Scalars{},
                Tables{},
                QueryProcessingStage::Complete,
                nullptr,
                RemoteQueryExecutor::Extension{.task_iterator = extension.task_iterator, .replica_info = std::move(replica_info)});

            Pipe pipe{std::make_shared<RemoteSource>(
                remote_query_executor,
                false,
                settings[Setting::async_socket_for_remote],
                settings[Setting::async_query_sending_for_remote])};
            pipe.addSimpleTransform([&](const Block & header) { return std::make_shared<UnmarshallBlocksTransform>(header); });
            QueryPipeline remote_pipeline{std::move(pipe)};
            remote_pipeline.complete(std::make_shared<EmptySink>(remote_query_executor->getHeader()));

            pipeline.addCompletedPipeline(std::move(remote_pipeline));
        }
    }

    return pipeline;
}


std::optional<QueryPipeline> StorageDistributed::distributedWrite(const ASTInsertQuery & query, ContextPtr local_context)
{
    const Settings & settings = local_context->getSettingsRef();
    if (settings[Setting::max_distributed_depth] && local_context->getClientInfo().distributed_depth >= settings[Setting::max_distributed_depth])
        throw Exception(ErrorCodes::TOO_LARGE_DISTRIBUTED_DEPTH, "Maximum distributed depth exceeded");

    auto & select = query.select->as<ASTSelectWithUnionQuery &>();

    StoragePtr src_storage;

    /// Distributed write only works in the most trivial case INSERT ... SELECT
    /// without any unions or joins on the right side
    if (select.list_of_selects->children.size() == 1)
    {
        if (auto * select_query = select.list_of_selects->children.at(0)->as<ASTSelectQuery>())
        {
            JoinedTables joined_tables(Context::createCopy(local_context), *select_query);

            if (joined_tables.tablesCount() == 1)
            {
                src_storage = joined_tables.getLeftTableStorage();
            }
        }
    }

    if (!src_storage)
        return {};

    if (auto src_distributed = std::dynamic_pointer_cast<StorageDistributed>(src_storage))
    {
        return distributedWriteBetweenDistributedTables(*src_distributed, query, local_context);
    }
    if (auto src_storage_cluster = std::dynamic_pointer_cast<IStorageCluster>(src_storage))
    {
        return distributedWriteFromClusterStorage(*src_storage_cluster, query, local_context);
    }
    if (local_context->getClientInfo().distributed_depth == 0)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parallel distributed INSERT SELECT is not possible. "\
                        "Reason: distributed reading is supported only from Distributed engine "
                        "or *Cluster table functions, but got {} storage", src_storage->getName());
    }

    return {};
}


void StorageDistributed::checkAlterIsPossible(const AlterCommands & commands, ContextPtr local_context) const
{
    std::optional<NameDependencies> name_deps{};
    for (const auto & command : commands)
    {
        if (command.type != AlterCommand::Type::ADD_COLUMN && command.type != AlterCommand::Type::MODIFY_COLUMN
            && command.type != AlterCommand::Type::DROP_COLUMN && command.type != AlterCommand::Type::COMMENT_COLUMN
            && command.type != AlterCommand::Type::RENAME_COLUMN && command.type != AlterCommand::Type::COMMENT_TABLE)

            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter of type '{}' is not supported by storage {}",
                command.type, getName());

        if (command.type == AlterCommand::DROP_COLUMN && !command.clear)
        {
            if (!name_deps)
                name_deps = getDependentViewsByColumn(local_context);
            const auto & deps_mv = name_deps.value()[command.column_name];
            if (!deps_mv.empty())
            {
                throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                    "Trying to ALTER DROP column {} which is referenced by materialized view {}",
                    backQuoteIfNeed(command.column_name), toString(deps_mv));
            }
        }
    }
}

void StorageDistributed::alter(const AlterCommands & params, ContextPtr local_context, AlterLockHolder &)
{
    auto table_id = getStorageID();

    checkAlterIsPossible(params, local_context);
    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
    params.apply(new_metadata, local_context);
    DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(local_context, table_id, new_metadata);
    setInMemoryMetadata(new_metadata);
}

void StorageDistributed::initializeFromDisk()
{
    if (!storage_policy)
        return;

    const auto & disks = data_volume->getDisks();

    /// Make initialization for large number of disks parallel.
    ThreadPool pool(CurrentMetrics::StorageDistributedThreads, CurrentMetrics::StorageDistributedThreadsActive, CurrentMetrics::StorageDistributedThreadsScheduled, disks.size());
    ThreadPoolCallbackRunnerLocal<void> runner(pool, "DistInit");

    for (const DiskPtr & disk : disks)
    {
        runner([this, disk_to_init = disk]
        {
            initializeDirectoryQueuesForDisk(disk_to_init);
        });
    }
    runner.waitForAllToFinishAndRethrowFirstError();

    const auto & paths = getDataPaths();
    std::vector<UInt64> last_increment(paths.size());
    for (size_t i = 0; i < paths.size(); ++i)
    {
        runner([&paths, &last_increment, i]
        {
            last_increment[i] = getMaximumFileNumber(paths[i]);
        });
    }
    runner.waitForAllToFinishAndRethrowFirstError();

    for (const auto inc : last_increment)
    {
        if (inc > file_names_increment.value)
            file_names_increment.value.store(inc);
    }
    LOG_DEBUG(log, "Auto-increment is {}", file_names_increment.value.load());
}


void StorageDistributed::shutdown(bool)
{
    auto holder = async_insert_blocker.cancel();

    std::lock_guard lock(cluster_nodes_mutex);

    LOG_DEBUG(log, "Joining background threads for async INSERT");
    cluster_nodes_data.clear();
    LOG_DEBUG(log, "Background threads for async INSERT joined");

    async_insert_blocker.cancelForever();
}

void StorageDistributed::drop()
{
    // Some INSERT in-between shutdown() and drop() can call
    // getDirectoryQueue() again, so call shutdown() to clear them, but
    // when the drop() (this function) executed none of INSERT is allowed in
    // parallel.
    //
    // And second time shutdown() should be fast, since none of
    // DirectoryMonitor should not do anything, because ActionBlocker is
    // canceled (in shutdown()).
    shutdown(true);

    // Distributed table without sharding_key does not allows INSERTs
    if (relative_data_path.empty())
        return;

    LOG_DEBUG(log, "Removing pending blocks for async INSERT from filesystem on DROP TABLE");

    auto disks = data_volume->getDisks();
    for (const auto & disk : disks)
    {
        if (!disk->existsDirectory(relative_data_path))
        {
            LOG_INFO(log, "Path {} is already removed from disk {}", relative_data_path, disk->getName());
            continue;
        }

        disk->removeRecursive(relative_data_path);
    }
}

Strings StorageDistributed::getDataPaths() const
{
    Strings paths;

    if (relative_data_path.empty())
        return paths;

    for (const DiskPtr & disk : data_volume->getDisks())
        paths.push_back(disk->getPath() + relative_data_path);

    return paths;
}

void StorageDistributed::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &)
{
    std::lock_guard lock(cluster_nodes_mutex);

    LOG_DEBUG(log, "Removing pending blocks for async INSERT from filesystem on TRUNCATE TABLE");

    for (auto it = cluster_nodes_data.begin(); it != cluster_nodes_data.end();)
    {
        it->second.directory_queue->shutdownAndDropAllData();
        it = cluster_nodes_data.erase(it);
    }
}

StoragePolicyPtr StorageDistributed::getStoragePolicy() const
{
    return storage_policy;
}

void StorageDistributed::initializeDirectoryQueuesForDisk(const DiskPtr & disk)
{
    const std::string path(disk->getPath() + relative_data_path);
    fs::create_directories(path);

    std::filesystem::directory_iterator begin(path);
    std::filesystem::directory_iterator end;
    for (auto it = begin; it != end; ++it)
    {
        const auto & dir_path = it->path();
        if (std::filesystem::is_directory(dir_path))
        {
            /// Created by DistributedSink
            const auto & tmp_path = dir_path / "tmp";
            if (std::filesystem::is_directory(tmp_path) && std::filesystem::is_empty(tmp_path))
                std::filesystem::remove(tmp_path);

            const auto & broken_path = dir_path / "broken";
            if (std::filesystem::is_directory(broken_path) && std::filesystem::is_empty(broken_path))
                std::filesystem::remove(broken_path);

            if (std::filesystem::is_empty(dir_path))
            {
                LOG_DEBUG(log, "Removing {} (used for async INSERT into Distributed)", dir_path.string());
                /// Will be created by DistributedSink on demand.
                std::filesystem::remove(dir_path);
            }
            else
            {
                getDirectoryQueue(disk, dir_path.filename().string());
            }
        }
    }
}


DistributedAsyncInsertDirectoryQueue & StorageDistributed::getDirectoryQueue(const DiskPtr & disk, const std::string & name)
{
    const std::string & disk_path = disk->getPath();
    const std::string key(disk_path + name);

    std::lock_guard lock(cluster_nodes_mutex);
    auto & node_data = cluster_nodes_data[key];
    /// If the node changes, you need to recreate the DistributedAsyncInsertDirectoryQueue
    if (!node_data.directory_queue
        || (node_data.clusters_version < getContext()->getClustersVersion() && node_data.addresses != parseAddresses(name)))
    {
        node_data.addresses = parseAddresses(name);
        node_data.clusters_version = getContext()->getClustersVersion();
        node_data.connection_pool = DistributedAsyncInsertDirectoryQueue::createPool(node_data.addresses, *this);
        node_data.directory_queue = std::make_unique<DistributedAsyncInsertDirectoryQueue>(
            *this, disk, relative_data_path + name,
            node_data.connection_pool,
            async_insert_blocker,
            getContext()->getDistributedSchedulePool());
    }
    return *node_data.directory_queue;
}

std::vector<DistributedAsyncInsertDirectoryQueue::Status> StorageDistributed::getDirectoryQueueStatuses() const
{
    std::vector<DistributedAsyncInsertDirectoryQueue::Status> statuses;
    std::lock_guard lock(cluster_nodes_mutex);
    statuses.reserve(cluster_nodes_data.size());
    for (const auto & node : cluster_nodes_data)
        statuses.push_back(node.second.directory_queue->getStatus());
    return statuses;
}

Cluster::Addresses StorageDistributed::parseAddresses(const std::string & name) const
{
    Cluster::Addresses addresses;

    const auto & cluster = getCluster();
    const auto & shards_info = cluster->getShardsInfo();
    const auto & shards_addresses = cluster->getShardsAddresses();

    for (auto it = boost::make_split_iterator(name, boost::first_finder(",")); it != decltype(it){}; ++it)
    {
        const std::string & dirname = boost::copy_range<std::string>(*it);
        Cluster::Address address = Cluster::Address::fromFullString(dirname);

        /// Check new format shard{shard_index}_replica{replica_index}
        /// (shard_index and replica_index starts from 1).
        if (address.shard_index)
        {
            if (address.shard_index > shards_info.size())
            {
                LOG_ERROR(log, "No shard with shard_index={} ({})", address.shard_index, name);
                continue;
            }

            const auto & replicas_addresses = shards_addresses[address.shard_index - 1];
            size_t replicas = replicas_addresses.size();

            if (dirname.ends_with("_all_replicas"))
            {
                for (const auto & replica_address : replicas_addresses)
                    addresses.push_back(replica_address);
                continue;
            }

            if (address.replica_index > replicas)
            {
                LOG_ERROR(log, "No shard with replica_index={} ({})", address.replica_index, name);
                continue;
            }

            addresses.push_back(replicas_addresses[address.replica_index - 1]);
        }
        else
            addresses.push_back(address);
    }
    return addresses;
}

std::optional<UInt64> StorageDistributed::totalBytes(ContextPtr) const
{
    UInt64 total_bytes = 0;
    for (const auto & status : getDirectoryQueueStatuses())
        total_bytes += status.bytes_count;
    return total_bytes;
}

size_t StorageDistributed::getShardCount() const
{
    return getCluster()->getShardCount();
}

ClusterPtr StorageDistributed::getCluster() const
{
    return owned_cluster ? owned_cluster : getContext()->getCluster(cluster_name);
}

ClusterPtr StorageDistributed::getOptimizedCluster(
    ContextPtr local_context,
    const StorageSnapshotPtr & storage_snapshot,
    const SelectQueryInfo & query_info,
    const TreeRewriterResultPtr & syntax_analyzer_result) const
{
    ClusterPtr cluster = getCluster();
    const Settings & settings = local_context->getSettingsRef();

    bool sharding_key_is_usable = settings[Setting::allow_nondeterministic_optimize_skip_unused_shards] || sharding_key_is_deterministic;

    if (has_sharding_key && sharding_key_is_usable)
    {
        ClusterPtr optimized = skipUnusedShards(cluster, query_info, syntax_analyzer_result, storage_snapshot, local_context);
        if (optimized)
            return optimized;
    }

    UInt64 force = settings[Setting::force_optimize_skip_unused_shards];
    if (force == FORCE_OPTIMIZE_SKIP_UNUSED_SHARDS_ALWAYS || (force == FORCE_OPTIMIZE_SKIP_UNUSED_SHARDS_HAS_SHARDING_KEY && has_sharding_key))
    {
        if (!has_sharding_key)
            throw Exception(ErrorCodes::UNABLE_TO_SKIP_UNUSED_SHARDS, "No sharding key");
        if (!sharding_key_is_usable)
            throw Exception(ErrorCodes::UNABLE_TO_SKIP_UNUSED_SHARDS, "Sharding key is not deterministic");
        throw Exception(ErrorCodes::UNABLE_TO_SKIP_UNUSED_SHARDS, "Sharding key {} is not used", sharding_key_column_name);
    }

    return {};
}

IColumn::Selector StorageDistributed::createSelector(const ClusterPtr cluster, const ColumnWithTypeAndName & result)
{
    const auto & slot_to_shard = cluster->getSlotToShard();
    const IColumn * column = result.column.get();

/// NOLINTBEGIN(readability-else-after-return)
// If result.type is DataTypeLowCardinality, do shard according to its dictionaryType
#define CREATE_FOR_TYPE(TYPE)                                                                                       \
    if (typeid_cast<const DataType##TYPE *>(result.type.get()))                                                     \
        return createBlockSelector<TYPE>(*column, slot_to_shard);                                            \
    else if (auto * type_low_cardinality = typeid_cast<const DataTypeLowCardinality *>(result.type.get()))          \
        if (typeid_cast<const DataType ## TYPE *>(type_low_cardinality->getDictionaryType().get()))                 \
            return createBlockSelector<TYPE>(*column->convertToFullColumnIfLowCardinality(), slot_to_shard);

    CREATE_FOR_TYPE(UInt8)
    CREATE_FOR_TYPE(UInt16)
    CREATE_FOR_TYPE(UInt32)
    CREATE_FOR_TYPE(UInt64)
    CREATE_FOR_TYPE(Int8)
    CREATE_FOR_TYPE(Int16)
    CREATE_FOR_TYPE(Int32)
    CREATE_FOR_TYPE(Int64)

#undef CREATE_FOR_TYPE

    throw Exception(ErrorCodes::TYPE_MISMATCH, "Sharding key expression does not evaluate to an integer type");
}
/// NOLINTEND(readability-else-after-return)

ClusterPtr StorageDistributed::skipUnusedShardsWithAnalyzer(
    ClusterPtr cluster,
    const SelectQueryInfo & query_info,
    [[maybe_unused]] const StorageSnapshotPtr & storage_snapshot,
    ContextPtr local_context) const
{
    if (!query_info.filter_actions_dag)
        return nullptr;

    size_t limit = local_context->getSettingsRef()[Setting::optimize_skip_unused_shards_limit];
    if (!limit || limit > SSIZE_MAX)
    {
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "optimize_skip_unused_shards_limit out of range (0, {}]", SSIZE_MAX);
    }

    const auto & sharding_key_dag = sharding_key_expr->getActionsDAG();
    const auto * expr_node = sharding_key_dag.tryFindInOutputs(sharding_key_column_name);
    if (!expr_node)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Cannot find sharding key column {} in expression {}",
            sharding_key_column_name, sharding_key_dag.dumpDAG());

    const auto * predicate = query_info.filter_actions_dag->getOutputs().at(0);
    const auto variants = evaluateExpressionOverConstantCondition(predicate, {expr_node}, local_context, limit);

    // Can't get a definite answer if we can skip any shards
    if (!variants)
        return nullptr;

    std::set<int> shards;

    for (const auto & variant : *variants)
    {
        const auto selector = createSelector(cluster, variant.at(0));
        shards.insert(selector.begin(), selector.end());
    }

    return cluster->getClusterWithMultipleShards({shards.begin(), shards.end()});
}

/// Returns a new cluster with fewer shards if constant folding for `sharding_key_expr` is possible
/// using constraints from "PREWHERE" and "WHERE" conditions, otherwise returns `nullptr`
ClusterPtr StorageDistributed::skipUnusedShards(
    ClusterPtr cluster,
    const SelectQueryInfo & query_info,
    const TreeRewriterResultPtr & syntax_analyzer_result,
    const StorageSnapshotPtr & storage_snapshot,
    ContextPtr local_context) const
{
    if (local_context->getSettingsRef()[Setting::allow_experimental_analyzer])
        return skipUnusedShardsWithAnalyzer(cluster, query_info, storage_snapshot, local_context);

    const auto & select = query_info.query->as<ASTSelectQuery &>();
    if (!select.prewhere() && !select.where())
        return nullptr;

    /// FIXME: support analyzer
    if (!syntax_analyzer_result)
        return nullptr;

    ASTPtr condition_ast;
    /// Remove JOIN from the query since it may contain a condition for other tables.
    /// But only the conditions for the left table should be analyzed for shard skipping.
    {
        ASTPtr select_without_join_ptr = select.clone();
        ASTSelectQuery select_without_join = select_without_join_ptr->as<ASTSelectQuery &>();
        TreeRewriterResult analyzer_result_without_join = *syntax_analyzer_result;

        removeJoin(select_without_join, analyzer_result_without_join, local_context);
        if (!select_without_join.prewhere() && !select_without_join.where())
            return nullptr;

        if (select_without_join.prewhere() && select_without_join.where())
            condition_ast = makeASTFunction("and", select_without_join.prewhere()->clone(), select_without_join.where()->clone());
        else
            condition_ast = select_without_join.prewhere() ? select_without_join.prewhere()->clone() : select_without_join.where()->clone();
    }

    replaceConstantExpressions(condition_ast, local_context, storage_snapshot->metadata->getColumns().getAll(), shared_from_this(), storage_snapshot);

    size_t limit = local_context->getSettingsRef()[Setting::optimize_skip_unused_shards_limit];
    if (!limit || limit > SSIZE_MAX)
    {
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "optimize_skip_unused_shards_limit out of range (0, {}]", SSIZE_MAX);
    }
    // To interpret limit==0 as limit is reached
    ++limit;
    const auto blocks = evaluateExpressionOverConstantCondition(condition_ast, sharding_key_expr, limit);

    if (!limit)
    {
        LOG_DEBUG(
            log,
            "Number of values for sharding key exceeds optimize_skip_unused_shards_limit={}, "
            "try to increase it, but note that this may increase query processing time.",
            local_context->getSettingsRef()[Setting::optimize_skip_unused_shards_limit].value);
        return nullptr;
    }

    // Can't get a definite answer if we can skip any shards
    if (!blocks)
        return nullptr;

    std::set<int> shards;

    for (const auto & block : *blocks)
    {
        if (!block.has(sharding_key_column_name))
            throw Exception(ErrorCodes::TOO_MANY_ROWS, "sharding_key_expr should evaluate as a single row");

        const ColumnWithTypeAndName & result = block.getByName(sharding_key_column_name);
        const auto selector = createSelector(cluster, result);

        shards.insert(selector.begin(), selector.end());
    }

    return cluster->getClusterWithMultipleShards({shards.begin(), shards.end()});
}

ActionLock StorageDistributed::getActionLock(StorageActionBlockType type)
{
    if (type == ActionLocks::DistributedSend)
        return async_insert_blocker.cancel();
    return {};
}

void StorageDistributed::flushAndPrepareForShutdown()
{
    try
    {
        flushClusterNodesAllDataImpl(getContext(), /* settings_changes= */ {}, (*distributed_settings)[DistributedSetting::flush_on_detach]);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Cannot flush");
    }
}

void StorageDistributed::flushClusterNodesAllData(ContextPtr local_context, const SettingsChanges & settings_changes)
{
    flushClusterNodesAllDataImpl(local_context, settings_changes, /* flush= */ true);
}

void StorageDistributed::flushClusterNodesAllDataImpl(ContextPtr local_context, const SettingsChanges & settings_changes, bool flush)
{
    /// Sync SYSTEM FLUSH DISTRIBUTED with TRUNCATE
    auto table_lock = lockForShare(local_context->getCurrentQueryId(), local_context->getSettingsRef()[Setting::lock_acquire_timeout]);

    std::vector<std::shared_ptr<DistributedAsyncInsertDirectoryQueue>> directory_queues;

    {
        std::lock_guard lock(cluster_nodes_mutex);

        directory_queues.reserve(cluster_nodes_data.size());
        for (auto & node : cluster_nodes_data)
            directory_queues.push_back(node.second.directory_queue);
    }

    if (flush)
    {
        LOG_INFO(log, "Flushing pending INSERT blocks");

        Stopwatch watch;
        ThreadPool pool(CurrentMetrics::StorageDistributedThreads, CurrentMetrics::StorageDistributedThreadsActive, CurrentMetrics::StorageDistributedThreadsScheduled, directory_queues.size());
        ThreadPoolCallbackRunnerLocal<void> runner(pool, "DistFlush");

        for (const auto & node : directory_queues)
        {
            runner([node_to_flush = node, &settings_changes]
            {
                node_to_flush->flushAllData(settings_changes);
            });
        }

        runner.waitForAllToFinishAndRethrowFirstError();

        LOG_INFO(log, "Pending INSERT blocks flushed, took {} ms.", watch.elapsedMilliseconds());
    }
    else
    {
        LOG_INFO(log, "Skip flushing data (due to flush_on_detach=0)");

        for (auto & node : directory_queues)
            node->shutdownWithoutFlush();
    }
}

void StorageDistributed::rename(const String & new_path_to_table_data, const StorageID & new_table_id)
{
    assert(relative_data_path != new_path_to_table_data);
    if (!relative_data_path.empty())
        renameOnDisk(new_path_to_table_data);
    renameInMemory(new_table_id);
}


size_t StorageDistributed::getRandomShardIndex(const Cluster::ShardsInfo & shards)
{

    UInt32 total_weight = 0;
    for (const auto & shard : shards)
        total_weight += shard.weight;

    assert(total_weight > 0);

    size_t res;
    {
        std::lock_guard lock(rng_mutex);
        res = std::uniform_int_distribution<size_t>(0, total_weight - 1)(rng);
    }

    for (auto i = 0ul, s = shards.size(); i < s; ++i)
    {
        if (shards[i].weight > res)
            return i;
        res -= shards[i].weight;
    }

    UNREACHABLE();
}


void StorageDistributed::renameOnDisk(const String & new_path_to_table_data)
{
    for (const DiskPtr & disk : data_volume->getDisks())
    {
        disk->createDirectories(new_path_to_table_data);
        disk->moveDirectory(relative_data_path, new_path_to_table_data);

        auto new_path = disk->getPath() + new_path_to_table_data;
        LOG_DEBUG(log, "Updating path to {}", new_path);

        std::lock_guard lock(cluster_nodes_mutex);
        for (auto & node : cluster_nodes_data)
            node.second.directory_queue->updatePath(new_path_to_table_data);
    }

    relative_data_path = new_path_to_table_data;
}

void StorageDistributed::delayInsertOrThrowIfNeeded() const
{
    if (!(*distributed_settings)[DistributedSetting::bytes_to_throw_insert] &&
        !(*distributed_settings)[DistributedSetting::bytes_to_delay_insert])
        return;

    UInt64 total_bytes = *totalBytes(getContext());

    if ((*distributed_settings)[DistributedSetting::bytes_to_throw_insert] && total_bytes > (*distributed_settings)[DistributedSetting::bytes_to_throw_insert])
    {
        ProfileEvents::increment(ProfileEvents::DistributedRejectedInserts);
        throw Exception(ErrorCodes::DISTRIBUTED_TOO_MANY_PENDING_BYTES,
            "Too many bytes pending for async INSERT: {} (bytes_to_throw_insert={})",
            formatReadableSizeWithBinarySuffix(total_bytes),
            formatReadableSizeWithBinarySuffix((*distributed_settings)[DistributedSetting::bytes_to_throw_insert]));
    }

    if ((*distributed_settings)[DistributedSetting::bytes_to_delay_insert] && total_bytes > (*distributed_settings)[DistributedSetting::bytes_to_delay_insert])
    {
        /// Step is 5% of the delay and minimal one second.
        /// NOTE: max_delay_to_insert is in seconds, and step is in ms.
        const size_t step_ms = static_cast<size_t>(std::min<double>(1., static_cast<double>((*distributed_settings)[DistributedSetting::max_delay_to_insert]) * 1'000 * 0.05));
        UInt64 delayed_ms = 0;

        do {
            delayed_ms += step_ms;
            std::this_thread::sleep_for(std::chrono::milliseconds(step_ms));
        } while (*totalBytes(getContext()) > (*distributed_settings)[DistributedSetting::bytes_to_delay_insert] && delayed_ms < (*distributed_settings)[DistributedSetting::max_delay_to_insert]*1000);

        ProfileEvents::increment(ProfileEvents::DistributedDelayedInserts);
        ProfileEvents::increment(ProfileEvents::DistributedDelayedInsertsMilliseconds, delayed_ms);

        UInt64 new_total_bytes = *totalBytes(getContext());
        LOG_INFO(log, "Too many bytes pending for async INSERT: was {}, now {}, INSERT was delayed to {} ms",
            formatReadableSizeWithBinarySuffix(total_bytes),
            formatReadableSizeWithBinarySuffix(new_total_bytes),
            delayed_ms);

        if (new_total_bytes > (*distributed_settings)[DistributedSetting::bytes_to_delay_insert])
        {
            ProfileEvents::increment(ProfileEvents::DistributedRejectedInserts);
            throw Exception(ErrorCodes::DISTRIBUTED_TOO_MANY_PENDING_BYTES,
                "Too many bytes pending for async INSERT: {} (bytes_to_delay_insert={})",
                formatReadableSizeWithBinarySuffix(new_total_bytes),
                formatReadableSizeWithBinarySuffix((*distributed_settings)[DistributedSetting::bytes_to_delay_insert]));
        }
    }
}

void registerStorageDistributed(StorageFactory & factory)
{
    factory.registerStorage("Distributed", [](const StorageFactory::Arguments & args)
    {
        /** Arguments of engine is following:
          * - name of cluster in configuration;
          * - name of remote database;
          * - name of remote table;
          * - policy to store data in;
          *
          * Remote database may be specified in following form:
          * - identifier;
          * - constant expression with string result, like currentDatabase();
          * -- string literal as specific case;
          * - empty string means 'use default database from cluster'.
          *
          * Distributed engine also supports SETTINGS clause.
          */

        ASTs & engine_args = args.engine_args;

        if (engine_args.size() < 3 || engine_args.size() > 5)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Storage Distributed requires from 3 "
                            "to 5 parameters - name of configuration section with list "
                            "of remote servers, name of remote database, name "
                            "of remote table, sharding key expression (optional), policy to store data in (optional).");

        String cluster_name = getClusterNameAndMakeLiteral(engine_args[0]);

        const ContextPtr & context = args.getContext();
        const ContextPtr & local_context = args.getLocalContext();

        engine_args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[1], local_context);
        engine_args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[2], local_context);

        String remote_database = checkAndGetLiteralArgument<String>(engine_args[1], "remote_database");
        String remote_table = checkAndGetLiteralArgument<String>(engine_args[2], "remote_table");

        const auto & sharding_key = engine_args.size() >= 4 ? engine_args[3] : nullptr;
        String storage_policy = "default";
        if (engine_args.size() >= 5)
        {
            engine_args[4] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[4], local_context);
            storage_policy = checkAndGetLiteralArgument<String>(engine_args[4], "storage_policy");
        }

        /// Check that sharding_key exists in the table and has numeric type.
        if (sharding_key)
        {
            auto sharding_expr = buildShardingKeyExpression(sharding_key, context, args.columns.getAllPhysical(), true);
            const Block & block = sharding_expr->getSampleBlock();

            if (block.columns() != 1)
                throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS, "Sharding expression must return exactly one column");

            auto type = block.getByPosition(0).type;

            if (!type->isValueRepresentedByInteger())
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Sharding expression has type {}, but should be one of integer type",
                    type->getName());
        }

        /// TODO: move some arguments from the arguments to the SETTINGS.
        DistributedSettings distributed_settings = context->getDistributedSettings();
        if (args.storage_def->settings)
        {
            distributed_settings.loadFromQuery(*args.storage_def);
        }

        if (distributed_settings[DistributedSetting::max_delay_to_insert] < 1)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                "max_delay_to_insert cannot be less then 1");

        if (distributed_settings[DistributedSetting::bytes_to_throw_insert] && distributed_settings[DistributedSetting::bytes_to_delay_insert] &&
            distributed_settings[DistributedSetting::bytes_to_throw_insert] <= distributed_settings[DistributedSetting::bytes_to_delay_insert])
        {
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                "bytes_to_throw_insert cannot be less or equal to bytes_to_delay_insert (since it is handled first)");
        }

        /// Set default values from the distributed_background_insert_* global context settings.
        if (!distributed_settings[DistributedSetting::background_insert_batch].changed)
            distributed_settings[DistributedSetting::background_insert_batch] = context->getSettingsRef()[Setting::distributed_background_insert_batch];
        if (!distributed_settings[DistributedSetting::background_insert_split_batch_on_failure].changed)
            distributed_settings[DistributedSetting::background_insert_split_batch_on_failure]
                = context->getSettingsRef()[Setting::distributed_background_insert_split_batch_on_failure];
        if (!distributed_settings[DistributedSetting::background_insert_sleep_time_ms].changed)
            distributed_settings[DistributedSetting::background_insert_sleep_time_ms] = context->getSettingsRef()[Setting::distributed_background_insert_sleep_time_ms];
        if (!distributed_settings[DistributedSetting::background_insert_max_sleep_time_ms].changed)
            distributed_settings[DistributedSetting::background_insert_max_sleep_time_ms]
                = context->getSettingsRef()[Setting::distributed_background_insert_max_sleep_time_ms];

        return std::make_shared<StorageDistributed>(
            args.table_id,
            args.columns,
            args.constraints,
            args.comment,
            remote_database,
            remote_table,
            cluster_name,
            context,
            sharding_key,
            storage_policy,
            args.relative_data_path,
            distributed_settings,
            args.mode);
    },
    {
        .supports_settings = true,
        .supports_parallel_insert = true,
        .supports_schema_inference = true,
        .source_access_type = AccessType::REMOTE,
        .has_builtin_setting_fn = DistributedSettings::hasBuiltin,
    });
}

bool StorageDistributed::initializeDiskOnConfigChange(const std::set<String> & new_added_disks)
{
    if (!storage_policy || !data_volume)
        return true;

    auto new_storage_policy = getContext()->getStoragePolicy(storage_policy->getName());
    auto new_data_volume = new_storage_policy->getVolume(0);
    if (new_storage_policy->getVolumes().size() > 1)
        LOG_WARNING(log, "Storage policy for Distributed table has multiple volumes. "
                            "Only {} volume will be used to store data. Other will be ignored.", data_volume->getName());

    std::atomic_store(&storage_policy, new_storage_policy);
    std::atomic_store(&data_volume, new_data_volume);

    for (auto & disk : data_volume->getDisks())
    {
        if (new_added_disks.contains(disk->getName()))
        {
            initializeDirectoryQueuesForDisk(disk);
        }
    }

    return true;
}
}
