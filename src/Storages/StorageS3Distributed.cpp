#include "Storages/StorageS3Distributed.h"

#include <Common/config.h>
#include "Processors/Sources/SourceWithProgress.h"

#if USE_AWS_S3

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
#include <Storages/StorageS3.h>
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

struct StorageS3SourceBuilder
{
    bool need_path;
    bool need_file;
    String format;
    String name;
    Block sample_block;
    const Context & context;
    const ColumnsDescription & columns;
    UInt64 max_block_size;
    String compression_method;
};

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
        NextTaskCallback read_task_callback_,
        const ClientAuthentificationBuilder & client_auth_builder_,
        const StorageS3SourceBuilder & s3_builder_)
        : SourceWithProgress(getHeader(s3_builder_.sample_block, s3_builder_.need_path, s3_builder_.need_file))
        , initial_query_id(initial_query_id_)
        , s3_source_builder(s3_builder_)
        , cli_builder(client_auth_builder_)
        , read_task_callback(read_task_callback_)
    {
        createOrUpdateInnerSource();
    }

    String getName() const override
    {
        return "StorageS3SequentialSource"; 
    }

    Chunk generate() override
    {
        if (!inner)
            return {};

        auto chunk = inner->generate();
        if (!chunk) 
        {
            if (!createOrUpdateInnerSource())
                return {};
            else
                chunk = inner->generate();
        }
        return chunk;
    }

private:

    String askAboutNextKey()
    {
        try
        {
            return read_task_callback(initial_query_id);
        }
        catch (...)
        {
            tryLogCurrentException(&Poco::Logger::get("StorageS3SequentialSource"));
            throw;
        }
    }

    bool createOrUpdateInnerSource()
    {
        auto next_string = askAboutNextKey();
        if (next_string.empty())
            return false;

        auto next_uri = S3::URI(Poco::URI(next_string));

        auto client_auth = StorageS3::ClientAuthentificaiton{
            next_uri,
            cli_builder.access_key_id,
            cli_builder.secret_access_key,
            cli_builder.max_connections,
            {}, {}};
        StorageS3::updateClientAndAuthSettings(s3_source_builder.context, client_auth);

        inner = std::make_unique<StorageS3Source>(
            s3_source_builder.need_path,
            s3_source_builder.need_file,
            s3_source_builder.format,
            s3_source_builder.name,
            s3_source_builder.sample_block,
            s3_source_builder.context,
            s3_source_builder.columns,
            s3_source_builder.max_block_size,
            chooseCompressionMethod(client_auth.uri.key, ""),
            client_auth.client,
            client_auth.uri.bucket,
            next_uri.key
        );

        return true;
    }

    /// This is used to ask about next task
    String initial_query_id;

    StorageS3SourceBuilder s3_source_builder;
    ClientAuthentificationBuilder cli_builder;

    std::unique_ptr<StorageS3Source> inner;

    NextTaskCallback read_task_callback;
};



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
    , filename(filename_)
    , cluster_name(cluster_name_)
    , cluster(context_.getCluster(cluster_name)->getClusterWithReplicasAsShards(context_.getSettings()))
    , format_name(format_name_)
    , compression_method(compression_method_)
    , cli_builder{access_key_id_, secret_access_key_, max_connections_}
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);
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

        StorageS3SourceBuilder s3builder
        {
            need_path_column,
            need_file_column,
            format_name,
            getName(),
            metadata_snapshot->getSampleBlock(),
            context,
            metadata_snapshot->getColumns(),
            max_block_size,
            compression_method
        };

        return Pipe(std::make_shared<StorageS3SequentialSource>(
            context.getInitialQueryId(),
            context.getNextTaskCallback(),
            cli_builder,
            s3builder
        ));
    }


    /// The code from here and below executes on initiator

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
                "StorageS3DistributedInititiator",
                Protocol::Compression::Disable,
                Protocol::Secure::Disable
            ));

            auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
                    *connections.back(), queryToString(query_info.query), header, context, /*throttler=*/nullptr, scalars, Tables(), processed_stage);

            pipes.emplace_back(createRemoteSourcePipe(remote_query_executor, false, false, false, false)); 
        }
    }

    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());
    return Pipe::unitePipes(std::move(pipes));
}
}

#endif
