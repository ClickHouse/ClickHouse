#include <Storages/StorageAzure.h>


#if USE_AZURE_BLOB_STORAGE
#include <Formats/FormatFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>

#include <IO/ParallelReadBuffer.h>
#include <IO/SharedThreadPools.h>

#include <Parsers/ASTCreateQuery.h>
#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <re2/re2.h>

#include <azure/identity/managed_identity_credential.hpp>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/IInputFormat.h>

#include <Storages/StorageFactory.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/PartitionedSink.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/getVirtualsForStorage.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/StorageURL.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/ReadFromStorageProgress.h>

#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/Pipe.h>


using namespace Azure::Storage::Blobs;

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int DATABASE_ACCESS_DENIED;
}

bool isConnectionString(const std::string & candidate)
{
    return candidate.starts_with("DefaultEndpointsProtocol");
}

StorageAzure::Configuration StorageAzure::getConfiguration(ASTs & engine_args, ContextPtr local_context, bool get_format_from_file)
{
    LOG_INFO(&Poco::Logger::get("StorageAzure"), "get_format_from_file  = {}", get_format_from_file);

    StorageAzure::Configuration configuration;

    /// Supported signatures:
    ///
    /// Azure(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])

    if (engine_args.size() < 3 || engine_args.size() > 7)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Storage Azure requires 3 to 7 arguments: "
                        "Azure(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])");

    for (auto & engine_arg : engine_args)
        engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, local_context);

    std::unordered_map<std::string_view, size_t> engine_args_to_idx;

    configuration.connection_url = checkAndGetLiteralArgument<String>(engine_args[0], "connection_string/storage_account_url");
    configuration.is_connection_string = isConnectionString(configuration.connection_url);

    configuration.container = checkAndGetLiteralArgument<String>(engine_args[1], "container");
    configuration.blob_path = checkAndGetLiteralArgument<String>(engine_args[2], "blobpath");

    LOG_INFO(&Poco::Logger::get("StorageAzure"), "connection_url  = {}", configuration.connection_url);
    LOG_INFO(&Poco::Logger::get("StorageAzure"), "container  = {}", configuration.container);
    LOG_INFO(&Poco::Logger::get("StorageAzure"), "blobpath  = {}", configuration.blob_path);


    auto is_format_arg = [] (const std::string & s) -> bool
    {
        return s == "auto" || FormatFactory::instance().getAllFormats().contains(s);
    };

    if (engine_args.size() == 4)
    {
        //'c1 UInt64, c2 UInt64
        auto fourth_arg = checkAndGetLiteralArgument<String>(engine_args[3], "format/account_name");
        if (is_format_arg(fourth_arg))
        {
            configuration.format = fourth_arg;
        }
        else
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown format or account name specified without account key");
        }
    }
    else if (engine_args.size() == 5)
    {
        auto fourth_arg = checkAndGetLiteralArgument<String>(engine_args[3], "format/account_name");
        if (is_format_arg(fourth_arg))
        {
            configuration.format = fourth_arg;
            configuration.compression_method = checkAndGetLiteralArgument<String>(engine_args[4], "compression");
        }
        else
        {
            configuration.account_name = fourth_arg;
            configuration.account_key = checkAndGetLiteralArgument<String>(engine_args[4], "account_key");
        }
    }
    else if (engine_args.size() == 6)
    {
        auto fourth_arg = checkAndGetLiteralArgument<String>(engine_args[3], "format/account_name");
        if (fourth_arg == "auto" || FormatFactory::instance().getAllFormats().contains(fourth_arg))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Format and compression must be last arguments");
        }
        else
        {
            configuration.account_name = fourth_arg;
            configuration.account_key = checkAndGetLiteralArgument<String>(engine_args[4], "account_key");
            auto sixth_arg = checkAndGetLiteralArgument<String>(engine_args[5], "format/account_name");
            if (!is_format_arg(sixth_arg))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown format {}", sixth_arg);
            configuration.format = sixth_arg;
        }
    }
    else if (engine_args.size() == 7)
    {
        auto fourth_arg = checkAndGetLiteralArgument<String>(engine_args[3], "format/account_name");
        if (fourth_arg == "auto" || FormatFactory::instance().getAllFormats().contains(fourth_arg))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Format and compression must be last arguments");
        }
        else
        {
            configuration.account_name = fourth_arg;
            configuration.account_key = checkAndGetLiteralArgument<String>(engine_args[4], "account_key");
            auto sixth_arg = checkAndGetLiteralArgument<String>(engine_args[5], "format/account_name");
            if (!is_format_arg(sixth_arg))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown format {}", sixth_arg);
            configuration.format = sixth_arg;
            configuration.compression_method = checkAndGetLiteralArgument<String>(engine_args[6], "compression");
        }
    }

    configuration.blobs_paths = {configuration.blob_path};


    LOG_INFO(&Poco::Logger::get("StorageAzure"), "get_format_from_file  = {}", get_format_from_file);

//    if (configuration.format == "auto" && get_format_from_file)
//        configuration.format = FormatFactory::instance().getFormatFromFileName(configuration.blob_path, true);

    configuration.format = "TSV";

    return configuration;
}


void registerStorageAzure(StorageFactory & factory)
{
    factory.registerStorage("Azure", [](const StorageFactory::Arguments & args)
    {
        auto & engine_args = args.engine_args;
        if (engine_args.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "External data source must have arguments");

        auto configuration = StorageAzure::getConfiguration(engine_args, args.getLocalContext());
        auto client = StorageAzure::createClient(configuration);
        // Use format settings from global server context + settings from
        // the SETTINGS clause of the create query. Settings from current
        // session and user are ignored.
        std::optional<FormatSettings> format_settings;
        if (args.storage_def->settings)
        {
            FormatFactorySettings user_format_settings;

            // Apply changed settings from global context, but ignore the
            // unknown ones, because we only have the format settings here.
            const auto & changes = args.getContext()->getSettingsRef().changes();
            for (const auto & change : changes)
            {
                if (user_format_settings.has(change.name))
                    user_format_settings.set(change.name, change.value);
            }

            // Apply changes from SETTINGS clause, with validation.
            user_format_settings.applyChanges(args.storage_def->settings->changes);
            format_settings = getFormatSettings(args.getContext(), user_format_settings);
        }
        else
        {
            format_settings = getFormatSettings(args.getContext());
        }

        ASTPtr partition_by;
        if (args.storage_def->partition_by)
            partition_by = args.storage_def->partition_by->clone();

        return std::make_shared<StorageAzure>(
            std::move(configuration),
            std::make_unique<AzureObjectStorage>("AzureStorage", std::move(client), std::make_unique<AzureObjectStorageSettings>()),
            args.getContext(),
            args.table_id,
            args.columns,
            args.constraints,
            args.comment,
            format_settings,
            partition_by);
    },
    {
        .supports_settings = true,
        .supports_sort_order = true, // for partition by
        .supports_schema_inference = true,
        .source_access_type = AccessType::AZURE,
    });
}

AzureClientPtr StorageAzure::createClient(StorageAzure::Configuration configuration)
{
    AzureClientPtr result;

    if (configuration.is_connection_string)
    {
        LOG_INFO(&Poco::Logger::get("StorageAzure"), "createClient is_connection_string ");
        result = std::make_unique<BlobContainerClient>(BlobContainerClient::CreateFromConnectionString(configuration.connection_url, configuration.container));
    }
    else
    {
        if (configuration.account_name.has_value() && configuration.account_key.has_value())
        {
            auto storage_shared_key_credential = std::make_shared<Azure::Storage::StorageSharedKeyCredential>(*configuration.account_name, *configuration.account_key);
            result = std::make_unique<BlobContainerClient>(configuration.connection_url, storage_shared_key_credential);
        }

        auto managed_identity_credential = std::make_shared<Azure::Identity::ManagedIdentityCredential>();

        result = std::make_unique<BlobContainerClient>(configuration.connection_url, managed_identity_credential);

        LOG_INFO(&Poco::Logger::get("StorageAzure"), "createClient account_name & account_key ");
    }





    return result;
}

StorageAzure::StorageAzure(
    const Configuration & configuration_,
    std::unique_ptr<AzureObjectStorage> && object_storage_,
    ContextPtr context_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    std::optional<FormatSettings> format_settings_,
    ASTPtr partition_by_)
    : IStorage(table_id_)
    , name("AzureBlobStorage")
    , configuration(configuration_)
    , object_storage(std::move(object_storage_))
    , distributed_processing(false)
    , format_settings(format_settings_)
    , partition_by(partition_by_)
{
//    FormatFactory::instance().checkFormatName(configuration.format);
    context_->getGlobalContext()->getRemoteHostFilter().checkURL(Poco::URI(configuration.getConnectionURL()));

    StorageInMemoryMetadata storage_metadata;
    if (columns_.empty())
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Schema inference is not supported yet");
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);

    StoredObjects objects;
    for (const auto & key : configuration.blobs_paths)
        objects.emplace_back(key);

    for (auto obj : objects)
    {
        LOG_INFO(&Poco::Logger::get("StorageAzure"), "constructor obj.remote_paths  = {}", obj.remote_path);
        if (object_storage->exists(obj))
        {
            LOG_INFO(&Poco::Logger::get("StorageAzure"), "constructor exists  obj.remote_paths  = {}", obj.remote_path);
//            auto read_buffer = object_storage->readObject(obj);
//            LOG_INFO(&Poco::Logger::get("StorageAzure"), "constructor read size  obj.remote_paths  = {} , size = {}", obj.remote_path, read_buffer->getFileSize());
        }
    }


    auto default_virtuals = NamesAndTypesList{
        {"_path", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"_file", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())}};

    auto columns = storage_metadata.getSampleBlock().getNamesAndTypesList();

    virtual_columns = getVirtualsForStorage(columns, default_virtuals);
    for (const auto & column : virtual_columns)
        virtual_block.insert({column.type->createColumn(), column.type, column.name});
}

void StorageAzure::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &)
{

    if (configuration.withGlobs())
    {
        throw Exception(
            ErrorCodes::DATABASE_ACCESS_DENIED,
            "S3 key '{}' contains globs, so the table is in readonly mode",
            configuration.blob_path);
    }

    StoredObjects objects;
    for (const auto & key : configuration.blobs_paths)
        objects.emplace_back(key);

    object_storage->removeObjects(objects);
}

namespace
{

class StorageAzureSink : public SinkToStorage
{
public:
    StorageAzureSink(
        const String & format,
        const Block & sample_block_,
        ContextPtr context,
        std::optional<FormatSettings> format_settings_,
        const CompressionMethod compression_method,
        AzureObjectStorage * object_storage,
        const String & blob_path)
        : SinkToStorage(sample_block_)
        , sample_block(sample_block_)
        , format_settings(format_settings_)
    {
        StoredObject object(blob_path);
        write_buf = wrapWriteBufferWithCompressionMethod(object_storage->writeObject(object, WriteMode::Rewrite), compression_method, 3);
        writer = FormatFactory::instance().getOutputFormatParallelIfPossible(format, *write_buf, sample_block, context, format_settings);
    }

    String getName() const override { return "StorageS3Sink"; }

    void consume(Chunk chunk) override
    {
        std::lock_guard lock(cancel_mutex);
        if (cancelled)
            return;
        writer->write(getHeader().cloneWithColumns(chunk.detachColumns()));
    }

    void onCancel() override
    {
        std::lock_guard lock(cancel_mutex);
        finalize();
        cancelled = true;
    }

    void onException() override
    {
        std::lock_guard lock(cancel_mutex);
        finalize();
    }

    void onFinish() override
    {
        std::lock_guard lock(cancel_mutex);
        finalize();
    }

private:
    void finalize()
    {
        if (!writer)
            return;

        try
        {
            writer->finalize();
            writer->flush();
            write_buf->finalize();
        }
        catch (...)
        {
            /// Stop ParallelFormattingOutputFormat correctly.
            writer.reset();
            write_buf->finalize();
            throw;
        }
    }

    Block sample_block;
    std::optional<FormatSettings> format_settings;
    std::unique_ptr<WriteBuffer> write_buf;
    OutputFormatPtr writer;
    bool cancelled = false;
    std::mutex cancel_mutex;
};

class PartitionedStorageAzureSink : public PartitionedSink
{
public:
    PartitionedStorageAzureSink(
        const ASTPtr & partition_by,
        const String & format_,
        const Block & sample_block_,
        ContextPtr context_,
        std::optional<FormatSettings> format_settings_,
        const CompressionMethod compression_method_,
        AzureObjectStorage * object_storage_,
        const String & blob_)
        : PartitionedSink(partition_by, context_, sample_block_)
        , format(format_)
        , sample_block(sample_block_)
        , context(context_)
        , compression_method(compression_method_)
        , object_storage(object_storage_)
        , blob(blob_)
        , format_settings(format_settings_)
    {
    }

    SinkPtr createSinkForPartition(const String & partition_id) override
    {
        auto partition_key = replaceWildcards(blob, partition_id);
        validateKey(partition_key);

        return std::make_shared<StorageAzureSink>(
            format,
            sample_block,
            context,
            format_settings,
            compression_method,
            object_storage,
            partition_key
        );
    }

private:
    const String format;
    const Block sample_block;
    const ContextPtr context;
    const CompressionMethod compression_method;
    AzureObjectStorage * object_storage;
    const String blob;
    const std::optional<FormatSettings> format_settings;

    ExpressionActionsPtr partition_by_expr;

    static void validateKey(const String & str)
    {
        validatePartitionKey(str, true);
    }
};

}


Pipe StorageAzure::read(
    const Names & column_names ,
    const StorageSnapshotPtr &  storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    Pipes pipes;

    StoredObjects objects;
    for (const auto & key : configuration.blobs_paths)
        objects.emplace_back(key);

    auto reader = object_storage->readObjects(objects);
    auto columns_description = storage_snapshot->getDescriptionForColumns(column_names);
    auto block_for_format = storage_snapshot->getSampleBlockForColumns(columns_description.getNamesOfPhysical());


    pipes.emplace_back(std::make_shared<StorageAzureSource>(std::move(reader), context, block_for_format, max_block_size, columns_description));


    return Pipe::unitePipes(std::move(pipes));
}

SinkToStoragePtr StorageAzure::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context)
{
    auto sample_block = metadata_snapshot->getSampleBlock();
    auto chosen_compression_method = chooseCompressionMethod(configuration.blobs_paths.back(), configuration.compression_method);
    auto insert_query = std::dynamic_pointer_cast<ASTInsertQuery>(query);

    auto partition_by_ast = insert_query ? (insert_query->partition_by ? insert_query->partition_by : partition_by) : nullptr;
    bool is_partitioned_implementation = partition_by_ast && configuration.withWildcard();

    if (is_partitioned_implementation)
    {
        return std::make_shared<PartitionedStorageAzureSink>(
            partition_by_ast,
            configuration.format,
            sample_block,
            local_context,
            format_settings,
            chosen_compression_method,
            object_storage.get(),
            configuration.blobs_paths.back());
    }
    else
    {
        if (configuration.withGlobs())
            throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED,
                            "Azure key '{}' contains globs, so the table is in readonly mode", configuration.blob_path);

        bool truncate_in_insert = local_context->getSettingsRef().s3_truncate_on_insert;

        if (!truncate_in_insert && object_storage->exists(StoredObject(configuration.blob_path)))
        {
            if (local_context->getSettingsRef().s3_create_new_file_on_insert)
            {
                size_t index = configuration.blobs_paths.size();
                const auto & first_key = configuration.blobs_paths[0];
                auto pos = first_key.find_first_of('.');
                String new_key;
                do
                {
                    new_key = first_key.substr(0, pos) + "." + std::to_string(index) + (pos == std::string::npos ? "" : first_key.substr(pos));
                    ++index;
                }
                while (object_storage->exists(StoredObject(new_key)));

                configuration.blobs_paths.push_back(new_key);
            }
            else
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Object in bucket {} with key {} already exists. "
                    "If you want to overwrite it, enable setting s3_truncate_on_insert, if you "
                    "want to create a new file on each insert, enable setting s3_create_new_file_on_insert",
                    configuration.container, configuration.blobs_paths.back());
            }
        }

        return std::make_shared<StorageAzureSink>(
            configuration.format,
            sample_block,
            local_context,
            format_settings,
            chosen_compression_method,
            object_storage.get(),
            configuration.blobs_paths.back());
    }
}

NamesAndTypesList StorageAzure::getVirtuals() const
{
    return virtual_columns;
}

bool StorageAzure::supportsPartitionBy() const
{
    return true;
}


StorageAzureSource::StorageAzureSource (std::unique_ptr<ReadBufferFromFileBase> && read_buffer_, ContextPtr context_,
                                       const Block & sample_block_,UInt64 max_block_size_, const ColumnsDescription & columns_)
    :ISource(sample_block_)
    , WithContext(context_)
    , read_buffer(std::move(read_buffer_))
    , sample_block(sample_block_)
    , max_block_size(max_block_size_)
    , columns_desc(columns_)
{
    auto format = "TSV";

    auto input_format = FormatFactory::instance().getInput(
        format, *read_buffer, sample_block, getContext(), max_block_size);

    QueryPipelineBuilder builder;
    builder.init(Pipe(input_format));

    if (columns_desc.hasDefaults())
    {
        builder.addSimpleTransform(
            [&](const Block & header)
            { return std::make_shared<AddingDefaultsTransform>(header, columns_desc, *input_format, getContext()); });
    }

    pipeline = std::make_unique<QueryPipeline>(QueryPipelineBuilder::getPipeline(std::move(builder)));
    reader = std::make_unique<PullingPipelineExecutor>(*pipeline);
}


Chunk StorageAzureSource::generate()
{
    while(true)
    {
        Chunk chunk;
        if (reader->pull(chunk))
        {
            LOG_INFO(&Poco::Logger::get("StorageAzureSource"), "pulled chunk rows = {}", chunk.getNumRows());
        }
        return chunk;
    }
//    return {};
}

String StorageAzureSource::getName() const
{
    return "StorageAzureSource";
}

}

#endif
