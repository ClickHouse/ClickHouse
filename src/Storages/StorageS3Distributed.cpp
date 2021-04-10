#include "Storages/StorageS3Distributed.h"

#include <Common/config.h>
#include "Processors/Sources/SourceWithProgress.h"

#if USE_AWS_S3

#include "Common/Exception.h"
#include <Common/Throttler.h>
#include "Client/Connection.h"
#include "Core/QueryProcessingStage.h"
#include <Core/UUID.h>
#include "DataStreams/RemoteBlockInputStream.h"
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/getTableExpressions.h>
#include <Formats/FormatFactory.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/AddingDefaultsBlockInputStream.h>
#include <DataStreams/narrowBlockInputStreams.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>
#include <Processors/Pipe.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Sources/RemoteSource.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <common/logger_useful.h>

#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsV2Request.h>

#include <ios>
#include <memory>
#include <string>
#include <thread>
#include <cassert>

namespace DB
{


namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


StorageS3Distributed::StorageS3Distributed(
    const String & filename_,
    const String & access_key_id_,
    const String & secret_access_key_,
    const StorageID & table_id_,
    String cluster_name_,
    const String & format_name_,
    UInt64 max_connections_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const Context & context_,
    const String & compression_method_)
    : IStorage(table_id_)
    , client_auth{S3::URI{Poco::URI{filename_}}, access_key_id_, secret_access_key_, max_connections_, {}, {}} /// Client and settings will be updated later
    , filename(filename_)
    , cluster_name(cluster_name_)
    , cluster(context_.getCluster(cluster_name)->getClusterWithReplicasAsShards(context_.getSettings()))
    , format_name(format_name_)
    , compression_method(compression_method_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);
    StorageS3::updateClientAndAuthSettings(context_, client_auth);
}


Pipe StorageS3Distributed::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned /*num_streams*/)
{
    StorageS3::updateClientAndAuthSettings(context, client_auth);

    /// Secondary query, need to read from S3
    if (context.getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
    {
        bool need_path_column = false;
        bool need_file_column = false;
        for (const auto & column : column_names)
        {
            if (column == "_path")
                need_path_column = true;
            if (column == "_file")
                need_file_column = true;
        }
        
        /// Save callback not to capture context by reference of copy it.
        auto file_iterator = std::make_shared<StorageS3Source::IteratorWrapper>(
            [callback = context.getReadTaskCallback()]() -> std::optional<String> {
                return callback();
        });

        return Pipe(std::make_shared<StorageS3Source>(
            need_path_column, need_file_column, format_name, getName(),
            metadata_snapshot->getSampleBlock(), context,
            metadata_snapshot->getColumns(), max_block_size,
            compression_method,
            client_auth.client,
            client_auth.uri.bucket,
            file_iterator
        ));
    }

    /// The code from here and below executes on initiator
    S3::URI s3_uri(Poco::URI{filename});
    StorageS3::updateClientAndAuthSettings(context, client_auth);

    auto iterator = std::make_shared<StorageS3Source::DisclosedGlobIterator>(*client_auth.client, client_auth.uri);
    auto callback = std::make_shared<StorageS3Source::IteratorWrapper>([iterator]() mutable -> std::optional<String>
    {
        return iterator->next();
    });

    /// Calculate the header. This is significant, because some columns could be thrown away in some cases like query with count(*)
    Block header =
        InterpreterSelectQuery(query_info.query, context, SelectQueryOptions(processed_stage).analyze()).getSampleBlock();

    const Scalars & scalars = context.hasQueryContext() ? context.getQueryContext().getScalars() : Scalars{};

    Pipes pipes;
    connections.reserve(cluster->getShardCount());

    for (const auto & replicas : cluster->getShardsAddresses()) {
        /// There will be only one replica, because we consider each replica as a shard
        for (const auto & node : replicas)
        {
            connections.emplace_back(std::make_shared<Connection>(
                node.host_name, node.port, context.getGlobalContext().getCurrentDatabase(),
                node.user, node.password, node.cluster, node.cluster_secret,
                "S3DistributedInititiator",
                node.compression,
                node.secure
            ));

            /// For unknown reason global context is passed to IStorage::read() method
            /// So, task_identifier is passed as constructor argument. It is more obvious.
            auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
                    *connections.back(), queryToString(query_info.query), header, context, 
                    /*throttler=*/nullptr, scalars, Tables(), processed_stage, callback);

            pipes.emplace_back(std::make_shared<RemoteSource>(remote_query_executor, false, false)); 
        }
    }

    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());
    return Pipe::unitePipes(std::move(pipes));
}

QueryProcessingStage::Enum StorageS3Distributed::getQueryProcessingStage(
    const Context & context, QueryProcessingStage::Enum /*to_stage*/, SelectQueryInfo &) const
{
    /// Initiator executes query on remote node.
    if (context.getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY) {
        return QueryProcessingStage::Enum::WithMergeableState;
    }
    /// Follower just reads the data.
    return QueryProcessingStage::Enum::FetchColumns;
}


}

#endif
