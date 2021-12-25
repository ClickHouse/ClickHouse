#include <Common/config.h>

#if USE_HDFS

#include <Common/Exception.h>
#include <Common/Throttler.h>
#include <Client/Connection.h>
#include <Core/QueryProcessingStage.h>
#include <Core/UUID.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/getTableExpressions.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <QueryPipeline/narrowBlockInputStreams.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Sources/RemoteSource.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/HDFS/StorageHDFSCluster.h>
#include <base/logger_useful.h>

#include <ios>
#include <memory>
#include <string>
#include <thread>
#include <cassert>

namespace DB
{
StorageHDFSCluster::StorageHDFSCluster(
    String cluster_name_,
    const String & uri_,
    const StorageID & table_id_,
    const String & format_name_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & compression_method_)
    : IStorage(table_id_)
    , cluster_name(cluster_name_)
    , uri(uri_)
    , format_name(format_name_)
    , compression_method(compression_method_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);
}

/// The code executes on initiator
Pipe StorageHDFSCluster::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t /*max_block_size*/,
    unsigned /*num_streams*/)
{
    auto cluster = context->getCluster(cluster_name)->getClusterWithReplicasAsShards(context->getSettings());

    auto iterator = std::make_shared<HDFSSource::DisclosedGlobIterator>(context, uri);
    auto callback = std::make_shared<HDFSSource::IteratorWrapper>([iterator]() mutable -> String
    {
        return iterator->next();
    });

    /// Calculate the header. This is significant, because some columns could be thrown away in some cases like query with count(*)
    Block header =
        InterpreterSelectQuery(query_info.query, context, SelectQueryOptions(processed_stage).analyze()).getSampleBlock();

    const Scalars & scalars = context->hasQueryContext() ? context->getQueryContext()->getScalars() : Scalars{};

    Pipes pipes;

    const bool add_agg_info = processed_stage == QueryProcessingStage::WithMergeableState;

    for (const auto & replicas : cluster->getShardsAddresses())
    {
        /// There will be only one replica, because we consider each replica as a shard
        for (const auto & node : replicas)
        {
            auto connection = std::make_shared<Connection>(
                node.host_name, node.port, context->getGlobalContext()->getCurrentDatabase(),
                node.user, node.password, node.cluster, node.cluster_secret,
                "HDFSClusterInititiator",
                node.compression,
                node.secure
            );


            /// For unknown reason global context is passed to IStorage::read() method
            /// So, task_identifier is passed as constructor argument. It is more obvious.
            auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
                connection,
                queryToString(query_info.query),
                header,
                context,
                /*throttler=*/nullptr,
                scalars,
                Tables(),
                processed_stage,
                RemoteQueryExecutor::Extension{.task_iterator = callback});

            pipes.emplace_back(std::make_shared<RemoteSource>(remote_query_executor, add_agg_info, false));
        }
    }

    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());
    return Pipe::unitePipes(std::move(pipes));
}

QueryProcessingStage::Enum StorageHDFSCluster::getQueryProcessingStage(
    ContextPtr context, QueryProcessingStage::Enum to_stage, const StorageMetadataPtr &, SelectQueryInfo &) const
{
    /// Initiator executes query on remote node.
    if (context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
        if (to_stage >= QueryProcessingStage::Enum::WithMergeableState)
            return QueryProcessingStage::Enum::WithMergeableState;

    /// Follower just reads the data.
    return QueryProcessingStage::Enum::FetchColumns;
}


NamesAndTypesList StorageHDFSCluster::getVirtuals() const
{
    return NamesAndTypesList{
        {"_path", std::make_shared<DataTypeString>()},
        {"_file", std::make_shared<DataTypeString>()}
    };
}


}

#endif
