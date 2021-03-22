#include "Storages/StorageS3Distributed.h"

#include "Common/Exception.h"
#include <Common/Throttler.h>
#include "Client/Connection.h"
#include "Core/QueryProcessingStage.h"
#include "DataStreams/RemoteBlockInputStream.h"
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

#include <IO/ReadBufferFromS3.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/WriteHelpers.h>

#include <Formats/FormatFactory.h>

#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/AddingDefaultsBlockInputStream.h>
#include <DataStreams/narrowBlockInputStreams.h>

#include <Processors/Formats/InputStreamFromInputFormat.h>


#include <Storages/IStorage.h>
#include <Processors/Pipe.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Interpreters/Context.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageS3.h>
#include <Parsers/queryToString.h>

#include <Poco/Logger.h>
#include <Poco/Net/TCPServerConnection.h>

#include <memory>
#include <string>
#include <thread>
#include <cassert>

#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsV2Request.h>

#include <Storages/StorageS3.h>


namespace DB
{


namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class StorageS3SequentialSource : public SourceWithProgress
{
public:

    static Block getHeader(Block sample_block, bool with_path_column, bool with_file_column)
    {
        if (with_path_column)
            sample_block.insert({DataTypeString().createColumn(), std::make_shared<DataTypeString>(), "_path"});
        if (with_file_column)
            sample_block.insert({DataTypeString().createColumn(), std::make_shared<DataTypeString>(), "_file"});

        return sample_block;
    }

    StorageS3SequentialSource(
        String initial_query_id_,
        bool need_path_,
        bool need_file_,
        const String & format_,
        String name_,
        const Block & sample_block_,
        const Context & context_,
        const ColumnsDescription & columns_,
        UInt64 max_block_size_,
        const CompressionMethod compression_method_,
        StorageS3::ClientAuthentificaiton & client_auth_)
        : SourceWithProgress(getHeader(sample_block_, need_path_, need_file_))
        , need_path(need_path_)
        , need_file(need_file_)
        , format(format_)
        , name(name_)
        , sample_block(sample_block_)
        , context(context_)
        , columns(columns_)
        , max_block_size(max_block_size_)
        , compression_method(compression_method_)
        , client_auth(client_auth_)
        , initial_query_id(initial_query_id_)
    {
        initiator_connection = std::make_shared<Connection>(
            /*host*/"127.0.0.1",
            /*port*/9000,
            /*default_database=*/context.getGlobalContext().getCurrentDatabase(),
            /*user=*/context.getClientInfo().initial_user,
            /*password=*/"",
            /*cluster=*/"",
            /*cluster_secret=*/""
        );

        createOrUpdateInnerSource();
    }

    String getName() const override
    {
        return name; 
    }

    Chunk generate() override
    {
        auto chunk = inner->generate();
        if (!chunk && !createOrUpdateInnerSource())
            return {};
        return inner->generate();
    }

private:

    String askAboutNextKey()
    {
        try
        {
            initiator_connection->connect(timeouts);
            initiator_connection->sendNextTaskRequest(initial_query_id);
            auto packet = initiator_connection->receivePacket();
            assert(packet.type = Protocol::Server::NextTaskReply);
            LOG_DEBUG(&Poco::Logger::get("StorageS3SequentialSource"), "Got new task {}", packet.next_task);
            return packet.next_task;
        }
        catch (...)
        {
            tryLogCurrentException(&Poco::Logger::get("StorageS3SequentialSource"));
            throw;
        }
    }


    bool createOrUpdateInnerSource()
    {
        auto next_uri = S3::URI(Poco::URI(askAboutNextKey()));

        if (next_uri.uri.empty())
            return false;

        assert(next_uri.bucket == client_auth.uri.bucket);

        inner = std::make_unique<StorageS3Source>(
            need_path,
            need_file,
            format,
            name,
            sample_block,
            context,
            columns,
            max_block_size,
            compression_method,
            client_auth.client,
            client_auth.uri.bucket,
            next_uri.key
        );
        return true;
    }

    bool need_path;
    bool need_file;
    String format;
    String name;
    Block sample_block;
    const Context & context;
    const ColumnsDescription & columns;
    UInt64 max_block_size;
    const CompressionMethod compression_method;

    std::unique_ptr<StorageS3Source> inner;
    StorageS3::ClientAuthentificaiton client_auth;

    ///  One second just in case
    ConnectionTimeouts timeouts{{1, 0}, {1, 0}, {1, 0}};
    std::shared_ptr<Connection> initiator_connection;
    /// This is used to ask about next task
    String initial_query_id;
};



StorageS3Distributed::StorageS3Distributed(
    const S3::URI & uri_,
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
    , cluster_name(cluster_name_)
    , cluster(context_.getCluster(cluster_name)->getClusterWithReplicasAsShards(context_.getSettings()))
    , client_auth{uri_, access_key_id_, secret_access_key_, max_connections_, {}, {}}
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
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned /*num_streams*/)
{
    /// Secondary query, need to read from S3
    if (context.getCurrentQueryId() != context.getInitialQueryId())
    {
        StorageS3::updateClientAndAuthSettings(context, client_auth);

        Pipes pipes;
        bool need_path_column = false;
        bool need_file_column = false;
        for (const auto & column : column_names)
        {
            if (column == "_path")
                need_path_column = true;
            if (column == "_file")
                need_file_column = true;
        }

        std::cout << metadata_snapshot->getSampleBlock().dumpStructure() << std::endl;

        return Pipe(std::make_shared<StorageS3SequentialSource>(
            context.getInitialQueryId(),
            need_path_column,
            need_file_column,
            format_name,
            getName(),
            metadata_snapshot->getSampleBlock(),
            context,
            metadata_snapshot->getColumns(),
            max_block_size,
            chooseCompressionMethod(client_auth.uri.key, compression_method),
            client_auth
        ));
    }


    Pipes pipes;
    connections.reserve(cluster->getShardCount());

    std::cout << "StorageS3Distributed::read" << std::endl;

    for (const auto & replicas : cluster->getShardsAddresses()) {
        /// There will be only one replica, because we consider each replica as a shard
        for (const auto & node : replicas)
        {
            connections.emplace_back(std::make_shared<Connection>(
                /*host=*/node.host_name,
                /*port=*/node.port,
                /*default_database=*/context.getGlobalContext().getCurrentDatabase(),
                /*user=*/node.user,
                /*password=*/node.password,
                /*cluster=*/node.cluster,
                /*cluster_secret=*/node.cluster_secret
            ));
            auto stream = std::make_shared<RemoteBlockInputStream>(
                /*connection=*/*connections.back(),
                /*query=*/queryToString(query_info.query),
                /*header=*/metadata_snapshot->getSampleBlock(),
                /*context=*/context,
                nullptr, Scalars(), Tables(), QueryProcessingStage::WithMergeableState
            );
            pipes.emplace_back(std::make_shared<SourceFromInputStream>(std::move(stream)));
        }
    }


    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());
    return Pipe::unitePipes(std::move(pipes));
}






}

