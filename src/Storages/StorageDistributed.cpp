#include <Storages/StorageDistributed.h>

#include <Databases/IDatabase.h>

#include <Disks/IDisk.h>

#include <QueryPipeline/RemoteQueryExecutor.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/ObjectUtils.h>
#include <DataTypes/NestedUtils.h>

#include <Storages/Distributed/DistributedSink.h>
#include <Storages/StorageFactory.h>
#include <Storages/AlterCommands.h>
#include <Storages/getStructureOfRemoteTable.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/StorageDummy.h>
#include <Storages/removeGroupingFunctionSpecializations.h>

#include <Columns/ColumnConst.h>

#include <Common/Macros.h>
#include <Common/ProfileEvents.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <Common/quoteString.h>
#include <Common/randomSeed.h>
#include <Common/formatReadable.h>
#include <Common/CurrentMetrics.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/IAST.h>

#include <Analyzer/Utils.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/Passes/QueryAnalysisPass.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>

#include <Planner/Planner.h>
#include <Planner/Utils.h>

#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/JoinedTables.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/Context.h>
#include <Interpreters/createBlockSelector.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/getClusterName.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Interpreters/getCustomKeyFilterForParallelReplicas.h>
#include <Interpreters/getHeaderForProcessingStage.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <TableFunctions/TableFunctionView.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <Storages/buildQueryTreeForShard.h>
#include <Storages/IStorageCluster.h>

#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sources/RemoteSource.h>
#include <Processors/Sinks/EmptySink.h>

#include <Core/Settings.h>
#include <Core/SettingsEnums.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <IO/ConnectionTimeouts.h>

#include <memory>
#include <filesystem>
#include <optional>
#include <cassert>


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
}

namespace DB
{

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

        if (num > res)
            res = num;
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
            auto result = block_with_constants.getByName(name);
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
    return (num_remote_shards + num_local_shards) * settings.max_parallel_replicas;
}

}

/// For destruction of std::unique_ptr of type that is incomplete in class definition.
StorageDistributed::~StorageDistributed() = default;


NamesAndTypesList StorageDistributed::getVirtuals() const
{
    /// NOTE This is weird. Most of these virtual columns are part of MergeTree
    /// tables info. But Distributed is general-purpose engine.
    return NamesAndTypesList{
        NameAndTypePair("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())),
        NameAndTypePair("_part", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())),
        NameAndTypePair("_part_index", std::make_shared<DataTypeUInt64>()),
        NameAndTypePair("_part_uuid", std::make_shared<DataTypeUUID>()),
        NameAndTypePair("_partition_id", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())),
        NameAndTypePair("_sample_factor", std::make_shared<DataTypeFloat64>()),
        NameAndTypePair("_part_offset", std::make_shared<DataTypeUInt64>()),
        NameAndTypePair("_row_exists", std::make_shared<DataTypeUInt8>()),
        NameAndTypePair("_shard_num", std::make_shared<DataTypeUInt32>()), /// deprecated
    };
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
    bool attach_,
    ClusterPtr owned_cluster_,
    ASTPtr remote_table_function_ptr_)
    : IStorage(id_)
    , WithContext(context_->getGlobalContext())
    , remote_database(remote_database_)
    , remote_table(remote_table_)
    , remote_table_function_ptr(remote_table_function_ptr_)
    , log(&Poco::Logger::get("StorageDistributed (" + id_.table_name + ")"))
    , owned_cluster(std::move(owned_cluster_))
    , cluster_name(getContext()->getMacros()->expand(cluster_name_))
    , has_sharding_key(sharding_key_)
    , relative_data_path(relative_data_path_)
    , distributed_settings(distributed_settings_)
    , rng(randomSeed())
{
    if (!distributed_settings.flush_on_detach && distributed_settings.monitor_batch_inserts)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Settings flush_on_detach=0 and monitor_batch_inserts=1 are incompatible");

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
    if (!attach_)
    {
        if (remote_database.empty() && !remote_table_function_ptr && !getCluster()->maybeCrossReplication())
            LOG_WARNING(log, "Name of remote database is empty. Default database will be used implicitly.");

        size_t num_local_shards = getCluster()->getLocalShardCount();
        if (num_local_shards && (remote_database.empty() || remote_database == id_.database_name) && remote_table == id_.table_name)
            throw Exception(ErrorCodes::INFINITE_LOOP, "Distributed table {} looks at itself", id_.table_name);
    }

    initializeFromDisk();
}


StorageDistributed::StorageDistributed(
    const StorageID & id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    ASTPtr remote_table_function_ptr_,
    const String & cluster_name_,
    ContextPtr context_,
    const ASTPtr & sharding_key_,
    const String & storage_policy_name_,
    const String & relative_data_path_,
    const DistributedSettings & distributed_settings_,
    bool attach,
    ClusterPtr owned_cluster_)
    : StorageDistributed(
        id_,
        columns_,
        constraints_,
        String{},
        String{},
        String{},
        cluster_name_,
        context_,
        sharding_key_,
        storage_policy_name_,
        relative_data_path_,
        distributed_settings_,
        attach,
        std::move(owned_cluster_),
        remote_table_function_ptr_)
{
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

    if (query_info.use_custom_key)
    {
        LOG_INFO(log, "Single shard cluster used with custom_key, transforming replicas into virtual shards");
        query_info.cluster = cluster->getClusterWithReplicasAsShards(settings, settings.max_parallel_replicas);
    }
    else
    {
        query_info.cluster = cluster;

        if (nodes > 1 && settings.optimize_skip_unused_shards)
        {
            /// Always calculate optimized cluster here, to avoid conditions during read()
            /// (Anyway it will be calculated in the read())
            ClusterPtr optimized_cluster = getOptimizedCluster(local_context, storage_snapshot, query_info);
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

    if (settings.distributed_group_by_no_merge)
    {
        if (settings.distributed_group_by_no_merge == DISTRIBUTED_GROUP_BY_NO_MERGE_AFTER_AGGREGATION)
        {
            if (settings.distributed_push_down_limit)
                return QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit;
            else
                return QueryProcessingStage::WithMergeableStateAfterAggregation;
        }
        else
        {
            /// NOTE: distributed_group_by_no_merge=1 does not respect distributed_push_down_limit
            /// (since in this case queries processed separately and the initiator is just a proxy in this case).
            if (to_stage != QueryProcessingStage::Complete)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Queries with distributed_group_by_no_merge=1 should be processed to Complete stage");
            return QueryProcessingStage::Complete;
        }
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
    else if (nodes == 0)
    {
        /// In case of 0 shards, the query should be processed fully on the initiator,
        /// since we need to apply aggregations.
        /// That's why we need to return FetchColumns.
        return QueryProcessingStage::FetchColumns;
    }

    auto optimized_stage = getOptimizedQueryProcessingStage(query_info, settings);
    if (optimized_stage)
    {
        if (*optimized_stage == QueryProcessingStage::Complete)
            return std::min(to_stage, *optimized_stage);
        return *optimized_stage;
    }

    return QueryProcessingStage::WithMergeableState;
}

std::optional<QueryProcessingStage::Enum> StorageDistributed::getOptimizedQueryProcessingStage(const SelectQueryInfo & query_info, const Settings & settings) const
{
    bool optimize_sharding_key_aggregation =
        settings.optimize_skip_unused_shards &&
        settings.optimize_distributed_group_by_sharding_key &&
        has_sharding_key &&
        (settings.allow_nondeterministic_optimize_skip_unused_shards || sharding_key_is_deterministic);

    QueryProcessingStage::Enum default_stage = QueryProcessingStage::WithMergeableStateAfterAggregation;
    if (settings.distributed_push_down_limit)
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
    if (settings.extremes)
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
    if (!hasDynamicSubcolumns(all_columns))
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

        if (column_in_storage && column_in_storage->type->hasDynamicSubcolumns())
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
        query_analysis_pass.run(table_function_node, query_context);

        replacement_table_expression = std::move(table_function_node);
    }
    else
    {
        auto resolved_remote_storage_id = remote_storage_id;
        // In case of cross-replication we don't know what database is used for the table.
        // `storage_id.hasDatabase()` can return false only on the initiator node.
        // Each shard will use the default database (in the case of cross-replication shards may have different defaults).
        if (remote_storage_id.hasDatabase())
            resolved_remote_storage_id = query_context->resolveStorageID(remote_storage_id);

        auto get_column_options = GetColumnsOptions(GetColumnsOptions::All).withExtendedObjects().withVirtuals();

        auto column_names_and_types = distributed_storage_snapshot->getColumns(get_column_options);

        auto storage = std::make_shared<StorageDummy>(resolved_remote_storage_id, ColumnsDescription{column_names_and_types});
        auto table_node = std::make_shared<TableNode>(std::move(storage), query_context);

        if (table_expression_modifiers)
            table_node->setTableExpressionModifiers(*table_expression_modifiers);

        replacement_table_expression = std::move(table_node);
    }

    replacement_table_expression->setAlias(query_info.table_expression->getAlias());

    auto query_tree_to_modify = query_info.query_tree->cloneAndReplace(query_info.table_expression, std::move(replacement_table_expression));

    return buildQueryTreeForShard(query_info, query_tree_to_modify);
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
    ASTPtr query_ast;

    if (local_context->getSettingsRef().allow_experimental_analyzer)
    {
        StorageID remote_storage_id = StorageID::createEmpty();
        if (!remote_table_function_ptr)
            remote_storage_id = StorageID{remote_database, remote_table};

        auto query_tree_distributed = buildQueryTreeDistributed(query_info,
            storage_snapshot,
            remote_storage_id,
            remote_table_function_ptr);
        header = InterpreterSelectQueryAnalyzer::getSampleBlock(query_tree_distributed, local_context, SelectQueryOptions(processed_stage).analyze());
        query_ast = queryNodeToSelectQuery(query_tree_distributed);
    }
    else
    {
        header =
            InterpreterSelectQuery(query_info.query, local_context, SelectQueryOptions(processed_stage).analyze()).getSampleBlock();
        query_ast = query_info.query;
    }

    const auto & modified_query_ast = ClusterProxy::rewriteSelectQuery(
        local_context, query_ast,
        remote_database, remote_table, remote_table_function_ptr);

    /// Return directly (with correct header) if no shard to query.
    if (query_info.getCluster()->getShardsInfo().empty())
    {
        Pipe pipe(std::make_shared<NullSource>(header));
        auto read_from_pipe = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
        read_from_pipe->setStepDescription("Read from NullSource (Distributed)");
        query_plan.addStep(std::move(read_from_pipe));

        return;
    }

    StorageID main_table = StorageID::createEmpty();
    if (!remote_table_function_ptr)
        main_table = StorageID{remote_database, remote_table};

    const auto & snapshot_data = assert_cast<const SnapshotData &>(*storage_snapshot->data);
    ClusterProxy::SelectStreamFactory select_stream_factory =
        ClusterProxy::SelectStreamFactory(
            header,
            snapshot_data.objects_by_shard,
            storage_snapshot,
            processed_stage);

    auto settings = local_context->getSettingsRef();

    ClusterProxy::AdditionalShardFilterGenerator additional_shard_filter_generator;
    if (query_info.use_custom_key)
    {
        if (auto custom_key_ast = parseCustomKeyForTable(settings.parallel_replicas_custom_key, *local_context))
        {
            if (query_info.getCluster()->getShardCount() == 1)
            {
                // we are reading from single shard with multiple replicas but didn't transform replicas
                // into virtual shards with custom_key set
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Replicas weren't transformed into virtual shards");
            }

            additional_shard_filter_generator =
                [&, my_custom_key_ast = std::move(custom_key_ast), shard_count = query_info.cluster->getShardCount()](uint64_t shard_num) -> ASTPtr
            {
                return getCustomKeyFilterForParallelReplica(
                    shard_count, shard_num - 1, my_custom_key_ast, settings.parallel_replicas_custom_key_filter_type, *this, local_context);
            };
        }
    }

    ClusterProxy::executeQuery(
        query_plan, header, processed_stage,
        main_table, remote_table_function_ptr,
        select_stream_factory, log, modified_query_ast,
        local_context, query_info,
        sharding_key_expr, sharding_key_column_name,
        query_info.cluster, additional_shard_filter_generator);

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
    if (!settings.insert_shard_id && !settings.insert_distributed_one_random_shard && !has_sharding_key && shard_num >= 2)
    {
        throw Exception(ErrorCodes::STORAGE_REQUIRES_PARAMETER,
                        "Method write is not supported by storage {} with more than one shard and no sharding key provided", getName());
    }

    if (settings.insert_shard_id && settings.insert_shard_id > shard_num)
    {
        throw Exception(ErrorCodes::INVALID_SHARD_ID, "Shard id should be range from 1 to shard number");
    }

    /// Force sync insertion if it is remote() table function
    bool insert_sync = settings.insert_distributed_sync || settings.insert_shard_id || owned_cluster;
    auto timeout = settings.insert_distributed_timeout;

    Names columns_to_send;
    if (settings.insert_allow_materialized_columns)
        columns_to_send = metadata_snapshot->getSampleBlock().getNames();
    else
        columns_to_send = metadata_snapshot->getSampleBlockNonMaterialized().getNames();

    /// DistributedSink will not own cluster, but will own ConnectionPools of the cluster
    return std::make_shared<DistributedSink>(
        local_context, *this, metadata_snapshot, cluster, insert_sync, timeout,
        StorageID{remote_database, remote_table}, columns_to_send);
}


std::optional<QueryPipeline> StorageDistributed::distributedWriteBetweenDistributedTables(const StorageDistributed & src_distributed, const ASTInsertQuery & query, ContextPtr local_context) const
{
    const auto & settings = local_context->getSettingsRef();
    auto new_query = std::dynamic_pointer_cast<ASTInsertQuery>(query.clone());

    /// Unwrap view() function.
    if (src_distributed.remote_table_function_ptr)
    {
        const TableFunctionPtr src_table_function =
            TableFunctionFactory::instance().get(src_distributed.remote_table_function_ptr, local_context);
        const TableFunctionView * view_function =
            assert_cast<const TableFunctionView *>(src_table_function.get());
        new_query->select = view_function->getSelectQuery().clone();
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

    const Cluster::AddressesWithFailover & src_addresses = src_distributed.getCluster()->getShardsAddresses();
    const Cluster::AddressesWithFailover & dst_addresses = getCluster()->getShardsAddresses();
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

    if (settings.parallel_distributed_insert_select == PARALLEL_DISTRIBUTED_INSERT_SELECT_ALL)
    {
        new_query->table_id = StorageID(getRemoteDatabaseName(), getRemoteTableName());
        /// Reset table function for INSERT INTO remote()/cluster()
        new_query->table_function.reset();
    }

    const auto & cluster = getCluster();
    const auto & shards_info = cluster->getShardsInfo();

    String new_query_str;
    {
        WriteBufferFromOwnString buf;
        IAST::FormatSettings ast_format_settings(buf, /*one_line*/ true, /*hilite*/ false, /*always_quote_identifiers_=*/ true);
        new_query->IAST::format(ast_format_settings);
        new_query_str = buf.str();
    }

    QueryPipeline pipeline;
    ContextMutablePtr query_context = Context::createCopy(local_context);
    query_context->increaseDistributedDepth();

    for (size_t shard_index : collections::range(0, shards_info.size()))
    {
        const auto & shard_info = shards_info[shard_index];
        if (shard_info.isLocal())
        {
            InterpreterInsertQuery interpreter(new_query, query_context);
            pipeline.addCompletedPipeline(interpreter.execute().pipeline);
        }
        else
        {
            auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(settings);
            auto connections = shard_info.pool->getMany(timeouts, &settings, PoolMode::GET_ONE);
            if (connections.empty() || connections.front().isNull())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected exactly one connection for shard {}",
                    shard_info.shard_num);

            ///  INSERT SELECT query returns empty block
            auto remote_query_executor
                = std::make_shared<RemoteQueryExecutor>(std::move(connections), new_query_str, Block{}, query_context);
            QueryPipeline remote_pipeline(std::make_shared<RemoteSource>(remote_query_executor, false, settings.async_socket_for_remote, settings.async_query_sending_for_remote));
            remote_pipeline.complete(std::make_shared<EmptySink>(remote_query_executor->getHeader()));

            pipeline.addCompletedPipeline(std::move(remote_pipeline));
        }
    }

    return pipeline;
}


std::optional<QueryPipeline> StorageDistributed::distributedWriteFromClusterStorage(const IStorageCluster & src_storage_cluster, const ASTInsertQuery & query, ContextPtr local_context) const
{
    const auto & settings = local_context->getSettingsRef();
    auto & select = query.select->as<ASTSelectWithUnionQuery &>();
    /// Select query is needed for pruining on virtual columns
    auto extension = src_storage_cluster.getTaskIteratorExtension(
        select.list_of_selects->children.at(0)->as<ASTSelectQuery>()->clone(),
        local_context);

    auto dst_cluster = getCluster();

    auto new_query = std::dynamic_pointer_cast<ASTInsertQuery>(query.clone());
    if (settings.parallel_distributed_insert_select == PARALLEL_DISTRIBUTED_INSERT_SELECT_ALL)
    {
        new_query->table_id = StorageID(getRemoteDatabaseName(), getRemoteTableName());
        /// Reset table function for INSERT INTO remote()/cluster()
        new_query->table_function.reset();
    }

    String new_query_str;
    {
        WriteBufferFromOwnString buf;
        IAST::FormatSettings ast_format_settings(buf, /*one_line*/ true, /*hilite*/ false, /*always_quote_identifiers*/ true);
        new_query->IAST::format(ast_format_settings);
        new_query_str = buf.str();
    }

    QueryPipeline pipeline;
    ContextMutablePtr query_context = Context::createCopy(local_context);
    query_context->increaseDistributedDepth();

    /// Here we take addresses from destination cluster and assume source table exists on these nodes
    for (const auto & replicas : getCluster()->getShardsAddresses())
    {
        /// There will be only one replica, because we consider each replica as a shard
        for (const auto & node : replicas)
        {
            auto connection = std::make_shared<Connection>(
                node.host_name, node.port, query_context->getGlobalContext()->getCurrentDatabase(),
                node.user, node.password, node.quota_key, node.cluster, node.cluster_secret,
                "ParallelInsertSelectInititiator",
                node.compression,
                node.secure
            );

            auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
                connection,
                new_query_str,
                Block{},
                query_context,
                /*throttler=*/nullptr,
                Scalars{},
                Tables{},
                QueryProcessingStage::Complete,
                extension);

            QueryPipeline remote_pipeline(std::make_shared<RemoteSource>(remote_query_executor, false, settings.async_socket_for_remote, settings.async_query_sending_for_remote));
            remote_pipeline.complete(std::make_shared<EmptySink>(remote_query_executor->getHeader()));

            pipeline.addCompletedPipeline(std::move(remote_pipeline));
        }
    }

    return pipeline;
}


std::optional<QueryPipeline> StorageDistributed::distributedWrite(const ASTInsertQuery & query, ContextPtr local_context)
{
    const Settings & settings = local_context->getSettingsRef();
    if (settings.max_distributed_depth && local_context->getClientInfo().distributed_depth >= settings.max_distributed_depth)
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
    ThreadPool pool(CurrentMetrics::StorageDistributedThreads, CurrentMetrics::StorageDistributedThreadsActive, disks.size());

    for (const DiskPtr & disk : disks)
    {
        pool.scheduleOrThrowOnError([&]()
        {
            initializeDirectoryQueuesForDisk(disk);
        });
    }
    pool.wait();

    const auto & paths = getDataPaths();
    std::vector<UInt64> last_increment(paths.size());
    for (size_t i = 0; i < paths.size(); ++i)
    {
        pool.scheduleOrThrowOnError([&, i]()
        {
            last_increment[i] = getMaximumFileNumber(paths[i]);
        });
    }
    pool.wait();

    for (const auto inc : last_increment)
    {
        if (inc > file_names_increment.value)
            file_names_increment.value.store(inc);
    }
    LOG_DEBUG(log, "Auto-increment is {}", file_names_increment.value);
}


void StorageDistributed::shutdown()
{
    monitors_blocker.cancelForever();

    std::lock_guard lock(cluster_nodes_mutex);

    LOG_DEBUG(log, "Joining background threads for async INSERT");
    cluster_nodes_data.clear();
    LOG_DEBUG(log, "Background threads for async INSERT joined");
}

void StorageDistributed::drop()
{
    // Some INSERT in-between shutdown() and drop() can call
    // getDirectoryQueue() again, so call shutdown() to clear them, but
    // when the drop() (this function) executed none of INSERT is allowed in
    // parallel.
    //
    // And second time shutdown() should be fast, since none of
    // DirectoryMonitor should do anything, because ActionBlocker is canceled
    // (in shutdown()).
    shutdown();

    // Distributed table without sharding_key does not allows INSERTs
    if (relative_data_path.empty())
        return;

    LOG_DEBUG(log, "Removing pending blocks for async INSERT from filesystem on DROP TABLE");

    auto disks = data_volume->getDisks();
    for (const auto & disk : disks)
    {
        if (!disk->exists(relative_data_path))
        {
            LOG_INFO(log, "Path {} is already removed from disk {}", relative_data_path, disk->getName());
            continue;
        }

        disk->removeRecursive(relative_data_path);
    }

    LOG_DEBUG(log, "Removed");
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
        it->second.directory_monitor->shutdownAndDropAllData();
        it = cluster_nodes_data.erase(it);
    }

    LOG_DEBUG(log, "Removed");
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
    if (!node_data.directory_monitor)
    {
        node_data.connection_pool = DistributedAsyncInsertDirectoryQueue::createPool(name, *this);
        node_data.directory_monitor = std::make_unique<DistributedAsyncInsertDirectoryQueue>(
            *this, disk, relative_data_path + name,
            node_data.connection_pool,
            monitors_blocker,
            getContext()->getDistributedSchedulePool());
    }
    return *node_data.directory_monitor;
}

std::vector<DistributedAsyncInsertDirectoryQueue::Status> StorageDistributed::getDirectoryQueueStatuses() const
{
    std::vector<DistributedAsyncInsertDirectoryQueue::Status> statuses;
    std::lock_guard lock(cluster_nodes_mutex);
    statuses.reserve(cluster_nodes_data.size());
    for (const auto & node : cluster_nodes_data)
        statuses.push_back(node.second.directory_monitor->getStatus());
    return statuses;
}

std::optional<UInt64> StorageDistributed::totalBytes(const Settings &) const
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
    ContextPtr local_context, const StorageSnapshotPtr & storage_snapshot, const SelectQueryInfo & query_info) const
{
    ClusterPtr cluster = getCluster();
    const Settings & settings = local_context->getSettingsRef();

    bool sharding_key_is_usable = settings.allow_nondeterministic_optimize_skip_unused_shards || sharding_key_is_deterministic;

    if (has_sharding_key && sharding_key_is_usable)
    {
        ClusterPtr optimized = skipUnusedShards(cluster, query_info, storage_snapshot, local_context);
        if (optimized)
            return optimized;
    }

    UInt64 force = settings.force_optimize_skip_unused_shards;
    if (force == FORCE_OPTIMIZE_SKIP_UNUSED_SHARDS_ALWAYS || (force == FORCE_OPTIMIZE_SKIP_UNUSED_SHARDS_HAS_SHARDING_KEY && has_sharding_key))
    {
        if (!has_sharding_key)
            throw Exception(ErrorCodes::UNABLE_TO_SKIP_UNUSED_SHARDS, "No sharding key");
        else if (!sharding_key_is_usable)
            throw Exception(ErrorCodes::UNABLE_TO_SKIP_UNUSED_SHARDS, "Sharding key is not deterministic");
        else
            throw Exception(ErrorCodes::UNABLE_TO_SKIP_UNUSED_SHARDS, "Sharding key {} is not used", sharding_key_column_name);
    }

    return {};
}

IColumn::Selector StorageDistributed::createSelector(const ClusterPtr cluster, const ColumnWithTypeAndName & result)
{
    const auto & slot_to_shard = cluster->getSlotToShard();

// If result.type is DataTypeLowCardinality, do shard according to its dictionaryType
#define CREATE_FOR_TYPE(TYPE)                                                                                       \
    if (typeid_cast<const DataType##TYPE *>(result.type.get()))                                                     \
        return createBlockSelector<TYPE>(*result.column, slot_to_shard);                                            \
    else if (auto * type_low_cardinality = typeid_cast<const DataTypeLowCardinality *>(result.type.get()))          \
        if (typeid_cast<const DataType ## TYPE *>(type_low_cardinality->getDictionaryType().get()))                 \
            return createBlockSelector<TYPE>(*result.column->convertToFullColumnIfLowCardinality(), slot_to_shard);

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

/// Returns a new cluster with fewer shards if constant folding for `sharding_key_expr` is possible
/// using constraints from "PREWHERE" and "WHERE" conditions, otherwise returns `nullptr`
ClusterPtr StorageDistributed::skipUnusedShards(
    ClusterPtr cluster,
    const SelectQueryInfo & query_info,
    const StorageSnapshotPtr & storage_snapshot,
    ContextPtr local_context) const
{
    const auto & select = query_info.query->as<ASTSelectQuery &>();
    if (!select.prewhere() && !select.where())
        return nullptr;

    /// FIXME: support analyzer
    if (!query_info.syntax_analyzer_result)
        return nullptr;

    ASTPtr condition_ast;
    /// Remove JOIN from the query since it may contain a condition for other tables.
    /// But only the conditions for the left table should be analyzed for shard skipping.
    {
        ASTPtr select_without_join_ptr = select.clone();
        ASTSelectQuery select_without_join = select_without_join_ptr->as<ASTSelectQuery &>();
        TreeRewriterResult analyzer_result_without_join = *query_info.syntax_analyzer_result;

        removeJoin(select_without_join, analyzer_result_without_join, local_context);
        if (!select_without_join.prewhere() && !select_without_join.where())
            return nullptr;

        if (select_without_join.prewhere() && select_without_join.where())
            condition_ast = makeASTFunction("and", select_without_join.prewhere()->clone(), select_without_join.where()->clone());
        else
            condition_ast = select_without_join.prewhere() ? select_without_join.prewhere()->clone() : select_without_join.where()->clone();
    }

    replaceConstantExpressions(condition_ast, local_context, storage_snapshot->metadata->getColumns().getAll(), shared_from_this(), storage_snapshot);

    size_t limit = local_context->getSettingsRef().optimize_skip_unused_shards_limit;
    if (!limit || limit > SSIZE_MAX)
    {
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "optimize_skip_unused_shards_limit out of range (0, {}]", SSIZE_MAX);
    }
    // To interpret limit==0 as limit is reached
    ++limit;
    const auto blocks = evaluateExpressionOverConstantCondition(condition_ast, sharding_key_expr, limit);

    if (!limit)
    {
        LOG_DEBUG(log,
            "Number of values for sharding key exceeds optimize_skip_unused_shards_limit={}, "
            "try to increase it, but note that this may increase query processing time.",
            local_context->getSettingsRef().optimize_skip_unused_shards_limit);
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
        return monitors_blocker.cancel();
    return {};
}

void StorageDistributed::flushAndPrepareForShutdown()
{
    try
    {
        flushClusterNodesAllData(getContext());
    }
    catch (...)
    {
        tryLogCurrentException(log, "Cannot flush");
    }
}

void StorageDistributed::flushClusterNodesAllData(ContextPtr local_context)
{
    /// Sync SYSTEM FLUSH DISTRIBUTED with TRUNCATE
    auto table_lock = lockForShare(local_context->getCurrentQueryId(), local_context->getSettingsRef().lock_acquire_timeout);

    std::vector<std::shared_ptr<DistributedAsyncInsertDirectoryQueue>> directory_monitors;

    {
        std::lock_guard lock(cluster_nodes_mutex);

        directory_monitors.reserve(cluster_nodes_data.size());
        for (auto & node : cluster_nodes_data)
            directory_monitors.push_back(node.second.directory_monitor);
    }

    bool need_flush = getDistributedSettingsRef().flush_on_detach;
    if (!need_flush)
        LOG_INFO(log, "Skip flushing data (due to flush_on_detach=0)");

    /// TODO: Maybe it should be executed in parallel
    for (auto & node : directory_monitors)
    {
        if (need_flush)
            node->flushAllData();
        else
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
            node.second.directory_monitor->updatePath(new_path_to_table_data);
    }

    relative_data_path = new_path_to_table_data;
}

void StorageDistributed::delayInsertOrThrowIfNeeded() const
{
    if (!distributed_settings.bytes_to_throw_insert &&
        !distributed_settings.bytes_to_delay_insert)
        return;

    UInt64 total_bytes = *totalBytes(getContext()->getSettingsRef());

    if (distributed_settings.bytes_to_throw_insert && total_bytes > distributed_settings.bytes_to_throw_insert)
    {
        ProfileEvents::increment(ProfileEvents::DistributedRejectedInserts);
        throw Exception(ErrorCodes::DISTRIBUTED_TOO_MANY_PENDING_BYTES,
            "Too many bytes pending for async INSERT: {} (bytes_to_throw_insert={})",
            formatReadableSizeWithBinarySuffix(total_bytes),
            formatReadableSizeWithBinarySuffix(distributed_settings.bytes_to_throw_insert));
    }

    if (distributed_settings.bytes_to_delay_insert && total_bytes > distributed_settings.bytes_to_delay_insert)
    {
        /// Step is 5% of the delay and minimal one second.
        /// NOTE: max_delay_to_insert is in seconds, and step is in ms.
        const size_t step_ms = static_cast<size_t>(std::min<double>(1., static_cast<double>(distributed_settings.max_delay_to_insert) * 1'000 * 0.05));
        UInt64 delayed_ms = 0;

        do {
            delayed_ms += step_ms;
            std::this_thread::sleep_for(std::chrono::milliseconds(step_ms));
        } while (*totalBytes(getContext()->getSettingsRef()) > distributed_settings.bytes_to_delay_insert && delayed_ms < distributed_settings.max_delay_to_insert*1000);

        ProfileEvents::increment(ProfileEvents::DistributedDelayedInserts);
        ProfileEvents::increment(ProfileEvents::DistributedDelayedInsertsMilliseconds, delayed_ms);

        UInt64 new_total_bytes = *totalBytes(getContext()->getSettingsRef());
        LOG_INFO(log, "Too many bytes pending for async INSERT: was {}, now {}, INSERT was delayed to {} ms",
            formatReadableSizeWithBinarySuffix(total_bytes),
            formatReadableSizeWithBinarySuffix(new_total_bytes),
            delayed_ms);

        if (new_total_bytes > distributed_settings.bytes_to_delay_insert)
        {
            ProfileEvents::increment(ProfileEvents::DistributedRejectedInserts);
            throw Exception(ErrorCodes::DISTRIBUTED_TOO_MANY_PENDING_BYTES,
                "Too many bytes pending for async INSERT: {} (bytes_to_delay_insert={})",
                formatReadableSizeWithBinarySuffix(new_total_bytes),
                formatReadableSizeWithBinarySuffix(distributed_settings.bytes_to_delay_insert));
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
        DistributedSettings distributed_settings;
        if (args.storage_def->settings)
        {
            distributed_settings.loadFromQuery(*args.storage_def);
        }

        if (distributed_settings.max_delay_to_insert < 1)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                "max_delay_to_insert cannot be less then 1");

        if (distributed_settings.bytes_to_throw_insert && distributed_settings.bytes_to_delay_insert &&
            distributed_settings.bytes_to_throw_insert <= distributed_settings.bytes_to_delay_insert)
        {
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                "bytes_to_throw_insert cannot be less or equal to bytes_to_delay_insert (since it is handled first)");
        }

        /// Set default values from the distributed_directory_monitor_* global context settings.
        if (!distributed_settings.monitor_batch_inserts.changed)
            distributed_settings.monitor_batch_inserts = context->getSettingsRef().distributed_directory_monitor_batch_inserts;
        if (!distributed_settings.monitor_split_batch_on_failure.changed)
            distributed_settings.monitor_split_batch_on_failure = context->getSettingsRef().distributed_directory_monitor_split_batch_on_failure;
        if (!distributed_settings.monitor_sleep_time_ms.changed)
            distributed_settings.monitor_sleep_time_ms = context->getSettingsRef().distributed_directory_monitor_sleep_time_ms;
        if (!distributed_settings.monitor_max_sleep_time_ms.changed)
            distributed_settings.monitor_max_sleep_time_ms = context->getSettingsRef().distributed_directory_monitor_max_sleep_time_ms;

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
            args.attach);
    },
    {
        .supports_settings = true,
        .supports_parallel_insert = true,
        .supports_schema_inference = true,
        .source_access_type = AccessType::REMOTE,
    });
}

}
