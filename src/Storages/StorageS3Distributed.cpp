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
    IAST::Hash tree_hash_,
    const String & address_hash_or_filename_,
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
    , tree_hash(tree_hash_)
    , address_hash_or_filename(address_hash_or_filename_)
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
        /// Find initiator in cluster
        Cluster::Address initiator;
        [&]() 
        {
            for (const auto & replicas : cluster->getShardsAddresses())
                for (const auto & node : replicas)
                    if (node.getHash() == address_hash_or_filename)
                    {
                        initiator = node;
                        return;
                    }
        }();


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

    String hash_of_address;
    [&]()
    {
        for (const auto & replicas : cluster->getShardsAddresses())
            for (const auto & node : replicas)
                /// Finding ourselves in cluster
                if (node.is_local && node.port == context.getTCPPort())
                {
                    hash_of_address = node.getHash();
                    break;
                }
    }();

    if (hash_of_address.empty())
        throw Exception(fmt::format("The initiator must be a part of a cluster {}", cluster_name), ErrorCodes::BAD_ARGUMENTS); 

    /// Our purpose to change some arguments of this function to store some relevant 
    /// information. Then we will send changed query to another hosts.
    /// We got a pointer to table function representation in AST (a pointer to subtree)
    /// as parameter of TableFunctionRemote::execute and saved its hash value.
    /// Here we find it in the AST of whole query, change parameter and format it to string.
    auto remote_query_ast = query_info.query->clone();
    auto table_expressions_from_whole_query = getTableExpressions(remote_query_ast->as<ASTSelectQuery &>());

    String remote_query;
    for (const auto & table_expression : table_expressions_from_whole_query)
    {
        const auto & table_function_ast = table_expression->table_function;
        if (table_function_ast->getTreeHash() == tree_hash)
        {
            auto & arguments = table_function_ast->children.at(0)->children;
            auto & bucket = arguments[1]->as<ASTLiteral &>().value.safeGet<String>();
            /// We rewrite query, and insert a port to connect as a first parameter
            /// So, we write hash_of_address here as buckey name to find initiator node
            /// in cluster from config on remote replica
            bucket = hash_of_address;
            remote_query = queryToString(remote_query_ast);
            break;
        }
    }

    if (remote_query.empty())
        throw Exception(fmt::format("There is no table function with hash of AST equals to {}", hash_of_address), ErrorCodes::LOGICAL_ERROR);

    /// Calculate the header. This is significant, because some columns could be thrown away in some cases like query with count(*)
    Block header =
        InterpreterSelectQuery(remote_query_ast, context, SelectQueryOptions(processed_stage).analyze()).getSampleBlock();

    const Scalars & scalars = context.hasQueryContext() ? context.getQueryContext().getScalars() : Scalars{};

    Pipes pipes;
    connections.reserve(cluster->getShardCount());

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
                /*cluster_secret=*/node.cluster_secret,
                "StorageS3DistributedInititiator",
                Protocol::Compression::Disable,
                Protocol::Secure::Disable
            ));
            auto stream = std::make_shared<RemoteBlockInputStream>(
                /*connection=*/*connections.back(),
                /*query=*/remote_query,
                /*header=*/header,
                /*context=*/context,
                /*throttler=*/nullptr,
                /*scalars*/scalars,
                /*external_tables*/Tables(),
                /*stage*/processed_stage
            );
            pipes.emplace_back(std::make_shared<SourceFromInputStream>(std::move(stream)));
        }
    }

    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());
    return Pipe::unitePipes(std::move(pipes));
}
}

#endif
