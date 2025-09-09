#include <Storages/IStorageCluster.h>

#include <pcg_random.hpp>
#include <Common/randomSeed.h>

#include <Common/Exception.h>
#include <Core/Settings.h>
#include <Core/QueryProcessingStage.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ConnectionTimeouts.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Planner/Utils.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sources/RemoteSource.h>
#include <QueryPipeline/narrowPipe.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageDictionary.h>
#include <Storages/StorageDistributed.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Storages/extractTableFunctionFromSelectQuery.h>

#include <algorithm>
#include <memory>
#include <string>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool async_query_sending_for_remote;
    extern const SettingsBool async_socket_for_remote;
    extern const SettingsBool skip_unavailable_shards;
    extern const SettingsBool parallel_replicas_local_plan;
    extern const SettingsString cluster_for_parallel_replicas;
    extern const SettingsNonZeroUInt64 max_parallel_replicas;
    extern const SettingsUInt64 object_storage_max_nodes;
    extern const SettingsBool object_storage_remote_initiator;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

IStorageCluster::IStorageCluster(
    const String & cluster_name_,
    const StorageID & table_id_,
    LoggerPtr log_)
    : IStorage(table_id_)
    , log(log_)
    , cluster_name(cluster_name_)
{
}

void ReadFromCluster::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

    const ActionsDAG::Node * predicate = nullptr;
    if (filter_actions_dag)
        predicate = filter_actions_dag->getOutputs().at(0);

    createExtension(predicate);
}

void ReadFromCluster::createExtension(const ActionsDAG::Node * predicate)
{
    if (extension)
        return;

    extension = storage->getTaskIteratorExtension(predicate, filter_actions_dag, context, cluster);
}

/// The code executes on initiator
void IStorageCluster::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    auto cluster_name_from_settings = getClusterName(context);

    if (cluster_name_from_settings.empty())
    {
        readFallBackToPure(query_plan, column_names, storage_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
        return;
    }

    storage_snapshot->check(column_names);

    const auto & settings = context->getSettingsRef();

    auto cluster = getClusterImpl(context, cluster_name_from_settings, settings[Setting::object_storage_max_nodes]);

    /// Calculate the header. This is significant, because some columns could be thrown away in some cases like query with count(*)

    Block sample_block;
    ASTPtr query_to_send = query_info.query;

    if (settings[Setting::allow_experimental_analyzer])
    {
        sample_block = InterpreterSelectQueryAnalyzer::getSampleBlock(query_info.query, context, SelectQueryOptions(processed_stage));
    }
    else
    {
        auto interpreter = InterpreterSelectQuery(query_info.query, context, SelectQueryOptions(processed_stage).analyze());
        sample_block = interpreter.getSampleBlock();
        query_to_send = interpreter.getQueryInfo().query->clone();
    }

    updateQueryToSendIfNeeded(query_to_send, storage_snapshot, context);

    if (settings[Setting::object_storage_remote_initiator])
    {
        auto storage_and_context = convertToRemote(cluster, context, cluster_name_from_settings, query_to_send);
        auto src_distributed = std::dynamic_pointer_cast<StorageDistributed>(storage_and_context.storage);
        auto modified_query_info = query_info;
        modified_query_info.cluster = src_distributed->getCluster();
        auto new_storage_snapshot = storage_and_context.storage->getStorageSnapshot(storage_snapshot->metadata, storage_and_context.context);
        storage_and_context.storage->read(query_plan, column_names, new_storage_snapshot, modified_query_info, storage_and_context.context, processed_stage, max_block_size, num_streams);
        return;
    }

    RestoreQualifiedNamesVisitor::Data data;
    data.distributed_table = DatabaseAndTableWithAlias(*getTableExpression(query_info.query->as<ASTSelectQuery &>(), 0));
    data.remote_table.database = context->getCurrentDatabase();
    data.remote_table.table = getName();
    RestoreQualifiedNamesVisitor(data).visit(query_to_send);
    AddDefaultDatabaseVisitor visitor(context, context->getCurrentDatabase(),
                                      /* only_replace_current_database_function_= */false,
                                      /* only_replace_in_join_= */true);
    visitor.visit(query_to_send);

    auto this_ptr = std::static_pointer_cast<IStorageCluster>(shared_from_this());

    auto reading = std::make_unique<ReadFromCluster>(
        column_names,
        query_info,
        storage_snapshot,
        context,
        sample_block,
        std::move(this_ptr),
        std::move(query_to_send),
        processed_stage,
        cluster,
        log);

    query_plan.addStep(std::move(reading));
}

IStorageCluster::RemoteCallVariables IStorageCluster::convertToRemote(
    ClusterPtr cluster,
    ContextPtr context,
    const std::string & cluster_name_from_settings,
    ASTPtr query_to_send)
{
    auto host_addresses = cluster->getShardsAddresses();
    if (host_addresses.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty cluster {}", cluster_name_from_settings);

    static pcg64 rng(randomSeed());
    size_t shard_num = rng() % host_addresses.size();
    auto shard_addresses = host_addresses[shard_num];
    /// After getClusterImpl each shard must have exactly 1 replica
    if (shard_addresses.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Size of shard {} in cluster {} is not equal 1", shard_num, cluster_name_from_settings);
    auto host_name = shard_addresses[0].toString();

    LOG_INFO(log, "Choose remote initiator '{}'", host_name);

    bool secure = shard_addresses[0].secure == Protocol::Secure::Enable;
    std::string remote_function_name = secure ? "remoteSecure" : "remote";

    /// Clean object_storage_remote_initiator setting to avoid infinite remote call
    auto new_context = Context::createCopy(context);
    new_context->setSetting("object_storage_remote_initiator", false);

    auto * select_query = query_to_send->as<ASTSelectQuery>();
    if (!select_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected SELECT query");

    auto query_settings = select_query->settings();
    if (query_settings)
    {
        auto & settings_ast = query_settings->as<ASTSetQuery &>();
        if (settings_ast.changes.removeSetting("object_storage_remote_initiator") && settings_ast.changes.empty())
        {
            select_query->setExpression(ASTSelectQuery::Expression::SETTINGS, {});
        }
    }

    ASTTableExpression * table_expression = extractTableExpressionASTPtrFromSelectQuery(query_to_send);
    if (!table_expression)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't find table expression");

    auto remote_query = makeASTFunction(remote_function_name, std::make_shared<ASTLiteral>(host_name), table_expression->table_function);

    table_expression->table_function = remote_query;

    auto remote_function = TableFunctionFactory::instance().get(remote_query, new_context);

    auto storage = remote_function->execute(query_to_send, new_context, remote_function_name);

    return RemoteCallVariables{storage, new_context};
}

SinkToStoragePtr IStorageCluster::write(
    const ASTPtr & query,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context,
    bool async_insert)
{
    auto cluster_name_from_settings = getClusterName(context);

    if (cluster_name_from_settings.empty())
        return writeFallBackToPure(query, metadata_snapshot, context, async_insert);

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method write is not supported by storage {}", getName());
}

void ReadFromCluster::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    const Scalars & scalars = context->hasQueryContext() ? context->getQueryContext()->getScalars() : Scalars{};
    const bool add_agg_info = processed_stage == QueryProcessingStage::WithMergeableState;

    Pipes pipes;
    auto new_context = updateSettings(context->getSettingsRef());
    const auto & current_settings = new_context->getSettingsRef();
    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(current_settings);

    size_t replica_index = 0;
    auto max_replicas_to_use = static_cast<UInt64>(cluster->getShardsInfo().size());
    if (current_settings[Setting::max_parallel_replicas] > 1)
        max_replicas_to_use = std::min(max_replicas_to_use, current_settings[Setting::max_parallel_replicas].value);

    createExtension(nullptr);

    for (const auto & shard_info : cluster->getShardsInfo())
    {
        if (pipes.size() >= max_replicas_to_use)
            break;

        /// We're taking all replicas as shards,
        /// so each shard will have only one address to connect to.
        auto try_results = shard_info.pool->getMany(
            timeouts,
            current_settings,
            PoolMode::GET_ONE,
            {},
            /*skip_unavailable_endpoints=*/true);

        if (try_results.empty())
            continue;

        IConnections::ReplicaInfo replica_info{.number_of_current_replica = replica_index++};

        auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
            std::vector<IConnectionPool::Entry>{try_results.front()},
            query_to_send->formatWithSecretsOneLine(),
            getOutputHeader(),
            new_context,
            /*throttler=*/nullptr,
            scalars,
            Tables(),
            processed_stage,
            nullptr,
            RemoteQueryExecutor::Extension{.task_iterator = extension->task_iterator, .replica_info = std::move(replica_info)});

        remote_query_executor->setLogger(log);
        Pipe pipe{std::make_shared<RemoteSource>(
            remote_query_executor,
            add_agg_info,
            current_settings[Setting::async_socket_for_remote],
            current_settings[Setting::async_query_sending_for_remote])};
        pipe.addSimpleTransform([&](const Block & header) { return std::make_shared<UnmarshallBlocksTransform>(header); });
        pipes.emplace_back(std::move(pipe));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));
    if (pipe.empty())
        pipe = Pipe(std::make_shared<NullSource>(getOutputHeader()));

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

QueryProcessingStage::Enum IStorageCluster::getQueryProcessingStage(
    ContextPtr context, QueryProcessingStage::Enum to_stage, const StorageSnapshotPtr &, SelectQueryInfo &) const
{
    /// Initiator executes query on remote node.
    if (context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
        if (to_stage >= QueryProcessingStage::Enum::WithMergeableState)
            return QueryProcessingStage::Enum::WithMergeableState;

    /// Follower just reads the data.
    return QueryProcessingStage::Enum::FetchColumns;
}

ContextPtr ReadFromCluster::updateSettings(const Settings & settings)
{
    Settings new_settings{settings};

    /// Cluster table functions should always skip unavailable shards.
    new_settings[Setting::skip_unavailable_shards] = true;

    auto new_context = Context::createCopy(context);
    new_context->setSettings(new_settings);
    return new_context;
}

ClusterPtr IStorageCluster::getClusterImpl(ContextPtr context, const String & cluster_name_, size_t max_hosts)
{
    return context->getCluster(cluster_name_)->getClusterWithReplicasAsShards(context->getSettingsRef(), /* max_replicas_from_shard */ 0, max_hosts);
}

}
