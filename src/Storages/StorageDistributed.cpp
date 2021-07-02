#include <Storages/StorageDistributed.h>

#include <Databases/IDatabase.h>

#include <Disks/IDisk.h>

#include <DataStreams/RemoteBlockInputStream.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>

#include <Storages/Distributed/DistributedBlockOutputStream.h>
#include <Storages/StorageFactory.h>
#include <Storages/AlterCommands.h>

#include <Columns/ColumnConst.h>

#include <Common/Macros.h>
#include <Common/ProfileEvents.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <Common/quoteString.h>
#include <Common/randomSeed.h>
#include <Common/formatReadable.h>

#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>

#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/InterpreterDescribeQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/JoinedTables.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/Context.h>
#include <Interpreters/createBlockSelector.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/getClusterName.h>
#include <Interpreters/getTableExpressions.h>
#include <Functions/IFunction.h>

#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/NullSink.h>

#include <Core/Field.h>
#include <Core/Settings.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <IO/ConnectionTimeoutsContext.h>

#include <Poco/DirectoryIterator.h>

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
}

namespace ActionLocks
{
    extern const StorageActionBlockType DistributedSend;
}

namespace
{

/// select query has database, table and table function names as AST pointers
/// Creates a copy of query, changes database, table and table function names.
ASTPtr rewriteSelectQuery(const ASTPtr & query, const std::string & database, const std::string & table, ASTPtr table_function_ptr = nullptr)
{
    auto modified_query_ast = query->clone();

    ASTSelectQuery & select_query = modified_query_ast->as<ASTSelectQuery &>();

    // Get rid of the settings clause so we don't send them to remote. Thus newly non-important
    // settings won't break any remote parser. It's also more reasonable since the query settings
    // are written into the query context and will be sent by the query pipeline.
    select_query.setExpression(ASTSelectQuery::Expression::SETTINGS, {});

    if (table_function_ptr)
        select_query.addTableFunction(table_function_ptr);
    else
        select_query.replaceDatabaseAndTable(database, table);

    /// Restore long column names (cause our short names are ambiguous).
    /// TODO: aliased table functions & CREATE TABLE AS table function cases
    if (!table_function_ptr)
    {
        RestoreQualifiedNamesVisitor::Data data;
        data.distributed_table = DatabaseAndTableWithAlias(*getTableExpression(query->as<ASTSelectQuery &>(), 0));
        data.remote_table.database = database;
        data.remote_table.table = table;
        data.rename = true;
        RestoreQualifiedNamesVisitor(data).visit(modified_query_ast);
    }

    return modified_query_ast;
}

/// The columns list in the original INSERT query is incorrect because inserted blocks are transformed
/// to the form of the sample block of the Distributed table. So we rewrite it and add all columns from
/// the sample block instead.
ASTPtr createInsertToRemoteTableQuery(const std::string & database, const std::string & table, const Block & sample_block)
{
    auto query = std::make_shared<ASTInsertQuery>();
    query->table_id = StorageID(database, table);

    auto columns = std::make_shared<ASTExpressionList>();
    query->columns = columns;
    query->children.push_back(columns);
    for (const auto & col : sample_block)
        columns->children.push_back(std::make_shared<ASTIdentifier>(col.name));

    return query;
}

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
    const StorageMetadataPtr & metadata_snapshot)
{
    auto syntax_result = TreeRewriter(context).analyze(node, columns, storage, metadata_snapshot);
    Block block_with_constants = KeyCondition::getBlockWithConstants(node, syntax_result, context);

    InDepthNodeVisitor<ReplacingConstantExpressionsMatcher, true> visitor(block_with_constants);
    visitor.visit(node);
}

/// This is the implementation of optimize_distributed_group_by_sharding_key.
/// It returns up to which stage the query can be processed on a shard, which
/// is one of the following:
/// - QueryProcessingStage::Complete
/// - QueryProcessingStage::WithMergeableStateAfterAggregation
/// - QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit
/// - none (in this case regular WithMergeableState should be used)
std::optional<QueryProcessingStage::Enum> getOptimizedQueryProcessingStage(const SelectQueryInfo & query_info, bool extremes, const Block & sharding_key_block)
{
    const auto & select = query_info.query->as<ASTSelectQuery &>();

    auto sharding_block_has = [&](const auto & exprs, size_t limit = SIZE_MAX) -> bool
    {
        size_t i = 0;
        for (auto & expr : exprs)
        {
            ++i;
            if (i > limit)
                break;

            auto id = expr->template as<ASTIdentifier>();
            if (!id)
                return false;
            /// TODO: if GROUP BY contains multiIf()/if() it should contain only columns from sharding_key
            if (!sharding_key_block.has(id->name()))
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
    if (extremes)
        return {};

    // DISTINCT
    if (select.distinct)
    {
        if (!sharding_block_has(select.select()->children))
            return {};
    }

    // GROUP BY
    const ASTPtr group_by = select.groupBy();
    if (!group_by)
    {
        if (!select.distinct)
            return {};
    }
    else
    {
        if (!sharding_block_has(group_by->children, 1))
            return {};
    }

    // ORDER BY
    const ASTPtr order_by = select.orderBy();
    if (order_by)
        return QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit;

    // LIMIT BY
    // LIMIT
    // OFFSET
    if (select.limitBy() || select.limitLength() || select.limitOffset())
        return QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit;

    // Only simple SELECT FROM GROUP BY sharding_key can use Complete state.
    return QueryProcessingStage::Complete;
}

size_t getClusterQueriedNodes(const Settings & settings, const ClusterPtr & cluster)
{
    size_t num_local_shards = cluster->getLocalShardCount();
    size_t num_remote_shards = cluster->getRemoteShardCount();
    return (num_remote_shards * settings.max_parallel_replicas) + num_local_shards;
}

}


/// For destruction of std::unique_ptr of type that is incomplete in class definition.
StorageDistributed::~StorageDistributed() = default;


NamesAndTypesList StorageDistributed::getVirtuals() const
{
    /// NOTE This is weird. Most of these virtual columns are part of MergeTree
    /// tables info. But Distributed is general-purpose engine.
    return NamesAndTypesList{
            NameAndTypePair("_table", std::make_shared<DataTypeString>()),
            NameAndTypePair("_part", std::make_shared<DataTypeString>()),
            NameAndTypePair("_part_index", std::make_shared<DataTypeUInt64>()),
            NameAndTypePair("_part_uuid", std::make_shared<DataTypeUUID>()),
            NameAndTypePair("_partition_id", std::make_shared<DataTypeString>()),
            NameAndTypePair("_sample_factor", std::make_shared<DataTypeFloat64>()),
            NameAndTypePair("_shard_num", std::make_shared<DataTypeUInt32>()),
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
    ClusterPtr owned_cluster_)
    : IStorage(id_)
    , WithContext(context_->getGlobalContext())
    , remote_database(remote_database_)
    , remote_table(remote_table_)
    , log(&Poco::Logger::get("StorageDistributed (" + id_.table_name + ")"))
    , owned_cluster(std::move(owned_cluster_))
    , cluster_name(getContext()->getMacros()->expand(cluster_name_))
    , has_sharding_key(sharding_key_)
    , relative_data_path(relative_data_path_)
    , distributed_settings(distributed_settings_)
    , rng(randomSeed())
{
    StorageInMemoryMetadata storage_metadata;
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
    if (!attach_ && !cluster_name.empty())
    {
        size_t num_local_shards = getContext()->getCluster(cluster_name)->getLocalShardCount();
        if (num_local_shards && remote_database == id_.database_name && remote_table == id_.table_name)
            throw Exception("Distributed table " + id_.table_name + " looks at itself", ErrorCodes::INFINITE_LOOP);
    }
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
        std::move(owned_cluster_))
{
    remote_table_function_ptr = std::move(remote_table_function_ptr_);
}

QueryProcessingStage::Enum StorageDistributed::getQueryProcessingStage(
    ContextPtr local_context,
    QueryProcessingStage::Enum to_stage,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info) const
{
    const auto & settings = local_context->getSettingsRef();

    ClusterPtr cluster = getCluster();
    query_info.cluster = cluster;

    /// Always calculate optimized cluster here, to avoid conditions during read()
    /// (Anyway it will be calculated in the read())
    if (getClusterQueriedNodes(settings, cluster) > 1 && settings.optimize_skip_unused_shards)
    {
        ClusterPtr optimized_cluster = getOptimizedCluster(local_context, metadata_snapshot, query_info.query);
        if (optimized_cluster)
        {
            LOG_DEBUG(log, "Skipping irrelevant shards - the query will be sent to the following shards of the cluster (shard numbers): {}",
                    makeFormattedListOfShards(optimized_cluster));
            cluster = optimized_cluster;
            query_info.optimized_cluster = cluster;
        }
        else
        {
            LOG_DEBUG(log, "Unable to figure out irrelevant shards from WHERE/PREWHERE clauses - the query will be sent to all shards of the cluster{}",
                    has_sharding_key ? "" : " (no sharding key)");
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
            return QueryProcessingStage::Complete;
        }
    }

    if (settings.distributed_push_down_limit)
        return QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit;

    /// Nested distributed query cannot return Complete stage,
    /// since the parent query need to aggregate the results after.
    if (to_stage == QueryProcessingStage::WithMergeableState)
        return QueryProcessingStage::WithMergeableState;

    /// If there is only one node, the query can be fully processed by the
    /// shard, initiator will work as a proxy only.
    if (getClusterQueriedNodes(settings, cluster) == 1)
        return QueryProcessingStage::Complete;

    if (settings.optimize_skip_unused_shards &&
        settings.optimize_distributed_group_by_sharding_key &&
        has_sharding_key &&
        (settings.allow_nondeterministic_optimize_skip_unused_shards || sharding_key_is_deterministic))
    {
        Block sharding_key_block = sharding_key_expr->getSampleBlock();
        auto stage = getOptimizedQueryProcessingStage(query_info, settings.extremes, sharding_key_block);
        if (stage)
        {
            LOG_DEBUG(log, "Force processing stage to {}", QueryProcessingStage::toString(*stage));
            return *stage;
        }
    }

    return QueryProcessingStage::WithMergeableState;
}

Pipe StorageDistributed::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    QueryPlan plan;
    read(plan, column_names, metadata_snapshot, query_info, local_context, processed_stage, max_block_size, num_streams);
    return plan.convertToPipe(
        QueryPlanOptimizationSettings::fromContext(local_context),
        BuildQueryPipelineSettings::fromContext(local_context));
}

void StorageDistributed::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    const auto & modified_query_ast = rewriteSelectQuery(
        query_info.query, remote_database, remote_table, remote_table_function_ptr);

    Block header =
        InterpreterSelectQuery(query_info.query, local_context, SelectQueryOptions(processed_stage).analyze()).getSampleBlock();

    /// Return directly (with correct header) if no shard to query.
    if (query_info.getCluster()->getShardsInfo().empty())
    {
        Pipe pipe(std::make_shared<NullSource>(header));
        auto read_from_pipe = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
        read_from_pipe->setStepDescription("Read from NullSource (Distributed)");
        query_plan.addStep(std::move(read_from_pipe));

        return;
    }

    const Scalars & scalars = local_context->hasQueryContext() ? local_context->getQueryContext()->getScalars() : Scalars{};

    bool has_virtual_shard_num_column = std::find(column_names.begin(), column_names.end(), "_shard_num") != column_names.end();
    if (has_virtual_shard_num_column && !isVirtualColumn("_shard_num", metadata_snapshot))
        has_virtual_shard_num_column = false;

    ClusterProxy::SelectStreamFactory select_stream_factory = remote_table_function_ptr
        ? ClusterProxy::SelectStreamFactory(
            header, processed_stage, remote_table_function_ptr, scalars, has_virtual_shard_num_column, local_context->getExternalTables())
        : ClusterProxy::SelectStreamFactory(
            header,
            processed_stage,
            StorageID{remote_database, remote_table},
            scalars,
            has_virtual_shard_num_column,
            local_context->getExternalTables());

    ClusterProxy::executeQuery(query_plan, select_stream_factory, log,
        modified_query_ast, local_context, query_info,
        sharding_key_expr, sharding_key_column_name,
        query_info.cluster);

    /// This is a bug, it is possible only when there is no shards to query, and this is handled earlier.
    if (!query_plan.isInitialized())
        throw Exception("Pipeline is not initialized", ErrorCodes::LOGICAL_ERROR);
}


BlockOutputStreamPtr StorageDistributed::write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context)
{
    auto cluster = getCluster();
    const auto & settings = local_context->getSettingsRef();

    /// Ban an attempt to make async insert into the table belonging to DatabaseMemory
    if (!storage_policy && !owned_cluster && !settings.insert_distributed_sync && !settings.insert_shard_id)
    {
        throw Exception("Storage " + getName() + " must have own data directory to enable asynchronous inserts",
                        ErrorCodes::BAD_ARGUMENTS);
    }

    auto shard_num = cluster->getLocalShardCount() + cluster->getRemoteShardCount();

    /// If sharding key is not specified, then you can only write to a shard containing only one shard
    if (!settings.insert_shard_id && !settings.insert_distributed_one_random_shard && !has_sharding_key && shard_num >= 2)
    {
        throw Exception(
            "Method write is not supported by storage " + getName() + " with more than one shard and no sharding key provided",
            ErrorCodes::STORAGE_REQUIRES_PARAMETER);
    }

    if (settings.insert_shard_id && settings.insert_shard_id > shard_num)
    {
        throw Exception("Shard id should be range from 1 to shard number", ErrorCodes::INVALID_SHARD_ID);
    }

    /// Force sync insertion if it is remote() table function
    bool insert_sync = settings.insert_distributed_sync || settings.insert_shard_id || owned_cluster;
    auto timeout = settings.insert_distributed_timeout;

    Block sample_block;
    if (!settings.insert_allow_materialized_columns)
        sample_block = metadata_snapshot->getSampleBlockNonMaterialized();
    else
        sample_block = metadata_snapshot->getSampleBlock();

    /// DistributedBlockOutputStream will not own cluster, but will own ConnectionPools of the cluster
    return std::make_shared<DistributedBlockOutputStream>(
        local_context, *this, metadata_snapshot,
        createInsertToRemoteTableQuery(remote_database, remote_table, sample_block),
        cluster, insert_sync, timeout, StorageID{remote_database, remote_table});
}


QueryPipelinePtr StorageDistributed::distributedWrite(const ASTInsertQuery & query, ContextPtr local_context)
{
    const Settings & settings = local_context->getSettingsRef();
    std::shared_ptr<StorageDistributed> storage_src;
    auto & select = query.select->as<ASTSelectWithUnionQuery &>();
    auto new_query = std::dynamic_pointer_cast<ASTInsertQuery>(query.clone());
    if (select.list_of_selects->children.size() == 1)
    {
        if (auto * select_query = select.list_of_selects->children.at(0)->as<ASTSelectQuery>())
        {
            JoinedTables joined_tables(Context::createCopy(local_context), *select_query);

            if (joined_tables.tablesCount() == 1)
            {
                storage_src = std::dynamic_pointer_cast<StorageDistributed>(joined_tables.getLeftTableStorage());
                if (storage_src)
                {
                    const auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
                    select_with_union_query->list_of_selects = std::make_shared<ASTExpressionList>();

                    auto new_select_query = std::dynamic_pointer_cast<ASTSelectQuery>(select_query->clone());
                    select_with_union_query->list_of_selects->children.push_back(new_select_query);

                    new_select_query->replaceDatabaseAndTable(storage_src->getRemoteDatabaseName(), storage_src->getRemoteTableName());

                    new_query->select = select_with_union_query;
                }
            }
        }
    }

    if (!storage_src || storage_src->getClusterName() != getClusterName())
    {
        return nullptr;
    }

    if (settings.parallel_distributed_insert_select == PARALLEL_DISTRIBUTED_INSERT_SELECT_ALL)
    {
        new_query->table_id = StorageID(getRemoteDatabaseName(), getRemoteTableName());
    }

    const auto & cluster = getCluster();
    const auto & shards_info = cluster->getShardsInfo();

    std::vector<std::unique_ptr<QueryPipeline>> pipelines;

    String new_query_str = queryToString(new_query);
    for (size_t shard_index : collections::range(0, shards_info.size()))
    {
        const auto & shard_info = shards_info[shard_index];
        if (shard_info.isLocal())
        {
            InterpreterInsertQuery interpreter(new_query, local_context);
            pipelines.emplace_back(std::make_unique<QueryPipeline>(interpreter.execute().pipeline));
        }
        else
        {
            auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(settings);
            auto connections = shard_info.pool->getMany(timeouts, &settings, PoolMode::GET_ONE);
            if (connections.empty() || connections.front().isNull())
                throw Exception(
                    "Expected exactly one connection for shard " + toString(shard_info.shard_num), ErrorCodes::LOGICAL_ERROR);

            ///  INSERT SELECT query returns empty block
            auto in_stream = std::make_shared<RemoteBlockInputStream>(std::move(connections), new_query_str, Block{}, local_context);
            pipelines.emplace_back(std::make_unique<QueryPipeline>());
            pipelines.back()->init(Pipe(std::make_shared<SourceFromInputStream>(std::move(in_stream))));
            pipelines.back()->setSinks([](const Block & header, QueryPipeline::StreamType) -> ProcessorPtr
            {
                return std::make_shared<EmptySink>(header);
            });
        }
    }

    return std::make_unique<QueryPipeline>(QueryPipeline::unitePipelines(std::move(pipelines)));
}


void StorageDistributed::checkAlterIsPossible(const AlterCommands & commands, ContextPtr local_context) const
{
    auto name_deps = getDependentViewsByColumn(local_context);
    for (const auto & command : commands)
    {
        if (command.type != AlterCommand::Type::ADD_COLUMN
            && command.type != AlterCommand::Type::MODIFY_COLUMN
            && command.type != AlterCommand::Type::DROP_COLUMN
            && command.type != AlterCommand::Type::COMMENT_COLUMN
            && command.type != AlterCommand::Type::RENAME_COLUMN)

            throw Exception("Alter of type '" + alterTypeToString(command.type) + "' is not supported by storage " + getName(),
                ErrorCodes::NOT_IMPLEMENTED);
        if (command.type == AlterCommand::DROP_COLUMN && !command.clear)
        {
            const auto & deps_mv = name_deps[command.column_name];
            if (!deps_mv.empty())
            {
                throw Exception(
                    "Trying to ALTER DROP column " + backQuoteIfNeed(command.column_name) + " which is referenced by materialized view "
                        + toString(deps_mv),
                    ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);
            }
        }
    }
}

void StorageDistributed::alter(const AlterCommands & params, ContextPtr local_context, TableLockHolder &)
{
    auto table_id = getStorageID();

    checkAlterIsPossible(params, local_context);
    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
    params.apply(new_metadata, local_context);
    DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(local_context, table_id, new_metadata);
    setInMemoryMetadata(new_metadata);
}


void StorageDistributed::startup()
{
    if (remote_database.empty() && !remote_table_function_ptr && !getCluster()->maybeCrossReplication())
        LOG_WARNING(log, "Name of remote database is empty. Default database will be used implicitly.");

    if (!storage_policy)
        return;

    const auto & disks = data_volume->getDisks();

    /// Make initialization for large number of disks parallel.
    ThreadPool pool(disks.size());

    for (const DiskPtr & disk : disks)
    {
        pool.scheduleOrThrowOnError([&]()
        {
            createDirectoryMonitors(disk);
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
    // requireDirectoryMonitor() again, so call shutdown() to clear them, but
    // when the drop() (this function) executed none of INSERT is allowed in
    // parallel.
    //
    // And second time shutdown() should be fast, since none of
    // DirectoryMonitor should do anything, because ActionBlocker is canceled
    // (in shutdown()).
    shutdown();

    // Distributed table w/o sharding_key does not allows INSERTs
    if (relative_data_path.empty())
        return;

    LOG_DEBUG(log, "Removing pending blocks for async INSERT from filesystem on DROP TABLE");

    auto disks = data_volume->getDisks();
    for (const auto & disk : disks)
        disk->removeRecursive(relative_data_path);

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

void StorageDistributed::createDirectoryMonitors(const DiskPtr & disk)
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
            const auto & tmp_path = dir_path / "tmp";

            /// "tmp" created by DistributedBlockOutputStream
            if (std::filesystem::is_directory(tmp_path) && std::filesystem::is_empty(tmp_path))
                std::filesystem::remove(tmp_path);

            if (std::filesystem::is_empty(dir_path))
            {
                LOG_DEBUG(log, "Removing {} (used for async INSERT into Distributed)", dir_path.string());
                /// Will be created by DistributedBlockOutputStream on demand.
                std::filesystem::remove(dir_path);
            }
            else
            {
                requireDirectoryMonitor(disk, dir_path.filename().string(), /* startup= */ true);
            }
        }
    }
}


StorageDistributedDirectoryMonitor& StorageDistributed::requireDirectoryMonitor(const DiskPtr & disk, const std::string & name, bool startup)
{
    const std::string & disk_path = disk->getPath();
    const std::string key(disk_path + name);

    auto create_node_data = [&]()
    {
        ClusterNodeData data;
        data.connection_pool = StorageDistributedDirectoryMonitor::createPool(name, *this);
        data.directory_monitor = std::make_unique<StorageDistributedDirectoryMonitor>(
            *this, disk, relative_data_path + name,
            data.connection_pool,
            monitors_blocker,
            getContext()->getDistributedSchedulePool());
        return data;
    };

    /// In case of startup the lock can be acquired later.
    if (startup)
    {
        auto tmp_node_data = create_node_data();
        std::lock_guard lock(cluster_nodes_mutex);
        auto & node_data = cluster_nodes_data[key];
        assert(!node_data.directory_monitor);
        node_data = std::move(tmp_node_data);
        return *node_data.directory_monitor;
    }
    else
    {
        std::lock_guard lock(cluster_nodes_mutex);
        auto & node_data = cluster_nodes_data[key];
        if (!node_data.directory_monitor)
        {
            node_data = create_node_data();
        }
        return *node_data.directory_monitor;
    }
}

std::vector<StorageDistributedDirectoryMonitor::Status> StorageDistributed::getDirectoryMonitorsStatuses() const
{
    std::vector<StorageDistributedDirectoryMonitor::Status> statuses;
    std::lock_guard lock(cluster_nodes_mutex);
    statuses.reserve(cluster_nodes_data.size());
    for (const auto & node : cluster_nodes_data)
        statuses.push_back(node.second.directory_monitor->getStatus());
    return statuses;
}

std::optional<UInt64> StorageDistributed::totalBytes(const Settings &) const
{
    UInt64 total_bytes = 0;
    for (const auto & status : getDirectoryMonitorsStatuses())
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
    ContextPtr local_context, const StorageMetadataPtr & metadata_snapshot, const ASTPtr & query_ptr) const
{
    ClusterPtr cluster = getCluster();
    const Settings & settings = local_context->getSettingsRef();

    bool sharding_key_is_usable = settings.allow_nondeterministic_optimize_skip_unused_shards || sharding_key_is_deterministic;

    if (has_sharding_key && sharding_key_is_usable)
    {
        ClusterPtr optimized = skipUnusedShards(cluster, query_ptr, metadata_snapshot, local_context);
        if (optimized)
            return optimized;
    }

    UInt64 force = settings.force_optimize_skip_unused_shards;
    if (force)
    {
        WriteBufferFromOwnString exception_message;
        if (!has_sharding_key)
            exception_message << "No sharding key";
        else if (!sharding_key_is_usable)
            exception_message << "Sharding key is not deterministic";
        else
            exception_message << "Sharding key " << sharding_key_column_name << " is not used";

        if (force == FORCE_OPTIMIZE_SKIP_UNUSED_SHARDS_ALWAYS)
            throw Exception(exception_message.str(), ErrorCodes::UNABLE_TO_SKIP_UNUSED_SHARDS);
        if (force == FORCE_OPTIMIZE_SKIP_UNUSED_SHARDS_HAS_SHARDING_KEY && has_sharding_key)
            throw Exception(exception_message.str(), ErrorCodes::UNABLE_TO_SKIP_UNUSED_SHARDS);
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

    throw Exception{"Sharding key expression does not evaluate to an integer type", ErrorCodes::TYPE_MISMATCH};
}

/// Returns a new cluster with fewer shards if constant folding for `sharding_key_expr` is possible
/// using constraints from "PREWHERE" and "WHERE" conditions, otherwise returns `nullptr`
ClusterPtr StorageDistributed::skipUnusedShards(
    ClusterPtr cluster,
    const ASTPtr & query_ptr,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr local_context) const
{
    const auto & select = query_ptr->as<ASTSelectQuery &>();

    if (!select.prewhere() && !select.where())
    {
        return nullptr;
    }

    ASTPtr condition_ast;
    if (select.prewhere() && select.where())
    {
        condition_ast = makeASTFunction("and", select.prewhere()->clone(), select.where()->clone());
    }
    else
    {
        condition_ast = select.prewhere() ? select.prewhere()->clone() : select.where()->clone();
    }

    replaceConstantExpressions(condition_ast, local_context, metadata_snapshot->getColumns().getAll(), shared_from_this(), metadata_snapshot);

    size_t limit = local_context->getSettingsRef().optimize_skip_unused_shards_limit;
    if (!limit || limit > SSIZE_MAX)
    {
        throw Exception("optimize_skip_unused_shards_limit out of range (0, {}]", ErrorCodes::ARGUMENT_OUT_OF_BOUND, SSIZE_MAX);
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

    // Can't get definite answer if we can skip any shards
    if (!blocks)
    {
        return nullptr;
    }

    std::set<int> shards;

    for (const auto & block : *blocks)
    {
        if (!block.has(sharding_key_column_name))
            throw Exception("sharding_key_expr should evaluate as a single row", ErrorCodes::TOO_MANY_ROWS);

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

void StorageDistributed::flushClusterNodesAllData(ContextPtr local_context)
{
    /// Sync SYSTEM FLUSH DISTRIBUTED with TRUNCATE
    auto table_lock = lockForShare(local_context->getCurrentQueryId(), local_context->getSettingsRef().lock_acquire_timeout);

    std::vector<std::shared_ptr<StorageDistributedDirectoryMonitor>> directory_monitors;

    {
        std::lock_guard lock(cluster_nodes_mutex);

        directory_monitors.reserve(cluster_nodes_data.size());
        for (auto & node : cluster_nodes_data)
            directory_monitors.push_back(node.second.directory_monitor);
    }

    /// TODO: Maybe it should be executed in parallel
    for (auto & node : directory_monitors)
        node->flushAllData();
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

    __builtin_unreachable();
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
        const size_t step_ms = std::min<double>(1., double(distributed_settings.max_delay_to_insert) * 1'000 * 0.05);
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
            throw Exception(
                "Storage Distributed requires from 3 to 5 parameters - "
                "name of configuration section with list of remote servers, "
                "name of remote database, "
                "name of remote table, "
                "sharding key expression (optional), "
                "policy to store data in (optional).",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        String cluster_name = getClusterNameAndMakeLiteral(engine_args[0]);

        engine_args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[1], args.getLocalContext());
        engine_args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[2], args.getLocalContext());

        String remote_database = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
        String remote_table = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();

        const auto & sharding_key = engine_args.size() >= 4 ? engine_args[3] : nullptr;
        const auto & storage_policy = engine_args.size() >= 5 ? engine_args[4]->as<ASTLiteral &>().value.safeGet<String>() : "default";

        /// Check that sharding_key exists in the table and has numeric type.
        if (sharding_key)
        {
            auto sharding_expr = buildShardingKeyExpression(sharding_key, args.getContext(), args.columns.getAllPhysical(), true);
            const Block & block = sharding_expr->getSampleBlock();

            if (block.columns() != 1)
                throw Exception("Sharding expression must return exactly one column", ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS);

            auto type = block.getByPosition(0).type;

            if (!type->isValueRepresentedByInteger())
                throw Exception("Sharding expression has type " + type->getName() +
                    ", but should be one of integer type", ErrorCodes::TYPE_MISMATCH);
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

        return StorageDistributed::create(
            args.table_id,
            args.columns,
            args.constraints,
            args.comment,
            remote_database,
            remote_table,
            cluster_name,
            args.getContext(),
            sharding_key,
            storage_policy,
            args.relative_data_path,
            distributed_settings,
            args.attach);
    },
    {
        .supports_settings = true,
        .supports_parallel_insert = true,
        .source_access_type = AccessType::REMOTE,
    });
}

}
