#include "config.h"
#include "Interpreters/Context_fwd.h"

#if USE_HDFS

#include <Storages/HDFS/StorageHDFSCluster.h>

#include <Client/Connection.h>
#include <Core/QueryProcessingStage.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ConnectionTimeouts.h>
#include <Interpreters/Context.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <QueryPipeline/narrowPipe.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/RemoteQueryExecutor.h>

#include <Processors/Transforms/AddingDefaultsTransform.h>

#include <Processors/Sources/RemoteSource.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Storages/StorageDictionary.h>
#include <Storages/addColumnsStructureToQueryWithClusterEngine.h>

#include <memory>


namespace DB
{

StorageHDFSCluster::StorageHDFSCluster(
    ContextPtr context_,
    String cluster_name_,
    const String & uri_,
    const StorageID & table_id_,
    const String & format_name_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & compression_method_,
    bool structure_argument_was_provided_)
    : IStorageCluster(table_id_)
    , cluster_name(cluster_name_)
    , uri(uri_)
    , format_name(format_name_)
    , compression_method(compression_method_)
    , structure_argument_was_provided(structure_argument_was_provided_)
{
    checkHDFSURL(uri_);
    context_->getRemoteHostFilter().checkURL(Poco::URI(uri_));

    StorageInMemoryMetadata storage_metadata;

    if (columns_.empty())
    {
        auto columns = StorageHDFS::getTableStructureFromData(format_name, uri_, compression_method, context_);
        storage_metadata.setColumns(columns);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);
}

/// The code executes on initiator
Pipe StorageHDFSCluster::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    auto cluster = getCluster(context);
    auto extension = getTaskIteratorExtension(query_info.query, context);

    /// Calculate the header. This is significant, because some columns could be thrown away in some cases like query with count(*)
    Block header;

    if (context->getSettingsRef().allow_experimental_analyzer)
        header = InterpreterSelectQueryAnalyzer::getSampleBlock(query_info.query, context, SelectQueryOptions(processed_stage).analyze());
    else
        header = InterpreterSelectQuery(query_info.query, context, SelectQueryOptions(processed_stage).analyze()).getSampleBlock();

    const Scalars & scalars = context->hasQueryContext() ? context->getQueryContext()->getScalars() : Scalars{};

    Pipes pipes;

    const bool add_agg_info = processed_stage == QueryProcessingStage::WithMergeableState;

    auto query_to_send = query_info.original_query->clone();
    if (!structure_argument_was_provided)
        addColumnsStructureToQueryWithClusterEngine(
            query_to_send, StorageDictionary::generateNamesAndTypesDescription(storage_snapshot->metadata->getColumns().getAll()), 3, getName());

    const auto & current_settings = context->getSettingsRef();
    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(current_settings);
    for (const auto & shard_info : cluster->getShardsInfo())
    {
        auto try_results = shard_info.pool->getMany(timeouts, &current_settings, PoolMode::GET_MANY);
        for (auto & try_result : try_results)
        {
            auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
                std::vector<IConnectionPool::Entry>{try_result},
                queryToString(query_to_send),
                header,
                context,
                /*throttler=*/nullptr,
                scalars,
                Tables(),
                processed_stage,
                extension);

            pipes.emplace_back(std::make_shared<RemoteSource>(remote_query_executor, add_agg_info, false, false));
        }
    }

    storage_snapshot->check(column_names);
    return Pipe::unitePipes(std::move(pipes));
}

QueryProcessingStage::Enum StorageHDFSCluster::getQueryProcessingStage(
    ContextPtr context, QueryProcessingStage::Enum to_stage, const StorageSnapshotPtr &, SelectQueryInfo &) const
{
    /// Initiator executes query on remote node.
    if (context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
        if (to_stage >= QueryProcessingStage::Enum::WithMergeableState)
            return QueryProcessingStage::Enum::WithMergeableState;

    /// Follower just reads the data.
    return QueryProcessingStage::Enum::FetchColumns;
}


ClusterPtr StorageHDFSCluster::getCluster(ContextPtr context) const
{
    return context->getCluster(cluster_name)->getClusterWithReplicasAsShards(context->getSettingsRef());
}

RemoteQueryExecutor::Extension StorageHDFSCluster::getTaskIteratorExtension(ASTPtr, ContextPtr context) const
{
    auto iterator = std::make_shared<HDFSSource::DisclosedGlobIterator>(context, uri);
    auto callback = std::make_shared<HDFSSource::IteratorWrapper>([iter = std::move(iterator)]() mutable -> String { return iter->next(); });
    return RemoteQueryExecutor::Extension{.task_iterator = std::move(callback)};
}

NamesAndTypesList StorageHDFSCluster::getVirtuals() const
{
    return NamesAndTypesList{
        {"_path", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"_file", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())}};
}


}

#endif
