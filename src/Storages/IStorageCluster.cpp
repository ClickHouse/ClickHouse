#include "Storages/IStorageCluster.h"

#include "Common/Exception.h"
#include "Core/QueryProcessingStage.h"
#include <DataTypes/DataTypeString.h>
#include <IO/ConnectionTimeouts.h>
#include <Interpreters/Context.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <QueryPipeline/narrowPipe.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/Sources/RemoteSource.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Parsers/queryToString.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageDictionary.h>

#include <memory>
#include <string>

namespace DB
{

IStorageCluster::IStorageCluster(
    const String & cluster_name_,
    const StorageID & table_id_,
    Poco::Logger * log_,
    bool structure_argument_was_provided_)
    : IStorage(table_id_)
    , log(log_)
    , cluster_name(cluster_name_)
    , structure_argument_was_provided(structure_argument_was_provided_)
{
}


/// The code executes on initiator
Pipe IStorageCluster::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    updateBeforeRead(context);

    auto cluster = getCluster(context);
    auto extension = getTaskIteratorExtension(query_info.query, context);

    /// Calculate the header. This is significant, because some columns could be thrown away in some cases like query with count(*)

    Block sample_block;
    ASTPtr query_to_send = query_info.query;

    if (context->getSettingsRef().allow_experimental_analyzer)
    {
        sample_block = InterpreterSelectQueryAnalyzer::getSampleBlock(query_info.query, context, SelectQueryOptions(processed_stage));
    }
    else
    {
        auto interpreter = InterpreterSelectQuery(query_info.query, context, SelectQueryOptions(processed_stage).analyze());
        sample_block = interpreter.getSampleBlock();
        query_to_send = interpreter.getQueryInfo().query->clone();
    }

    const Scalars & scalars = context->hasQueryContext() ? context->getQueryContext()->getScalars() : Scalars{};

    Pipes pipes;

    const bool add_agg_info = processed_stage == QueryProcessingStage::WithMergeableState;

    if (!structure_argument_was_provided)
        addColumnsStructureToQuery(query_to_send, StorageDictionary::generateNamesAndTypesDescription(storage_snapshot->metadata->getColumns().getAll()), context);

    RestoreQualifiedNamesVisitor::Data data;
    data.distributed_table = DatabaseAndTableWithAlias(*getTableExpression(query_info.query->as<ASTSelectQuery &>(), 0));
    data.remote_table.database = context->getCurrentDatabase();
    data.remote_table.table = getName();
    RestoreQualifiedNamesVisitor(data).visit(query_to_send);
    AddDefaultDatabaseVisitor visitor(context, context->getCurrentDatabase(),
                                      /* only_replace_current_database_function_= */false,
                                      /* only_replace_in_join_= */true);
    visitor.visit(query_to_send);

    auto new_context = updateSettings(context, context->getSettingsRef());
    const auto & current_settings = new_context->getSettingsRef();
    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(current_settings);
    for (const auto & shard_info : cluster->getShardsInfo())
    {
        auto try_results = shard_info.pool->getMany(timeouts, &current_settings, PoolMode::GET_MANY);
        for (auto & try_result : try_results)
        {
            auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
                std::vector<IConnectionPool::Entry>{try_result},
                queryToString(query_to_send),
                sample_block,
                new_context,
                /*throttler=*/nullptr,
                scalars,
                Tables(),
                processed_stage,
                extension);

            remote_query_executor->setLogger(log);
            pipes.emplace_back(std::make_shared<RemoteSource>(remote_query_executor, add_agg_info, false, false));
        }
    }

    storage_snapshot->check(column_names);
    return Pipe::unitePipes(std::move(pipes));
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

ContextPtr IStorageCluster::updateSettings(ContextPtr context, const Settings & settings)
{
    Settings new_settings = settings;

    /// Cluster table functions should always skip unavailable shards.
    new_settings.skip_unavailable_shards = true;

    auto new_context = Context::createCopy(context);
    new_context->setSettings(new_settings);
    return new_context;
}

ClusterPtr IStorageCluster::getCluster(ContextPtr context) const
{
    return context->getCluster(cluster_name)->getClusterWithReplicasAsShards(context->getSettingsRef());
}

}
