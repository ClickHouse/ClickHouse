#include <Storages/StorageAzureBlob.h>


#if USE_AZURE_BLOB_STORAGE
#include <Formats/FormatFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>

#include <IO/SharedThreadPools.h>

#include <Parsers/ASTCreateQuery.h>
#include <Formats/ReadSchemaUtils.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <re2/re2.h>

#include <azure/storage/common/storage_credential.hpp>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/Transforms/ExtractColumnsTransform.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Sources/ConstChunkGenerator.h>

#include <Storages/StorageFactory.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/PartitionedSink.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/StorageURL.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Common/parseGlobs.h>
#include <Disks/ObjectStorages/ObjectStorageIterator.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>

#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/Pipe.h>

#include <Disks/IO/ReadBufferFromAzureBlobStorage.h>
#include <Disks/IO/WriteBufferFromAzureBlobStorage.h>


using namespace Azure::Storage::Blobs;

namespace CurrentMetrics
{
    extern const Metric ObjectStorageAzureThreads;
    extern const Metric ObjectStorageAzureThreadsActive;
}

namespace ProfileEvents
{
    extern const Event EngineFileLikeReadFiles;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int DATABASE_ACCESS_DENIED;
    extern const int CANNOT_COMPILE_REGEXP;
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;

}

namespace
{

const std::unordered_set<std::string_view> required_configuration_keys = {
    "blob_path",
    "container",
};

const std::unordered_set<std::string_view> optional_configuration_keys = {
    "format",
    "compression",
    "structure",
    "compression_method",
    "account_name",
    "account_key",
    "connection_string",
    "storage_account_url",
};

bool isConnectionString(const std::string & candidate)
{
    return !candidate.starts_with("http");
}

}

void StorageAzureBlob::processNamedCollectionResult(StorageAzureBlob::Configuration & configuration, const NamedCollection & collection)
{
    validateNamedCollection(collection, required_configuration_keys, optional_configuration_keys);

    if (collection.has("connection_string"))
    {
        configuration.connection_url = collection.get<String>("connection_string");
        configuration.is_connection_string = true;
    }

    if (collection.has("storage_account_url"))
    {
        configuration.connection_url = collection.get<String>("storage_account_url");
        configuration.is_connection_string = false;
    }

    configuration.container = collection.get<String>("container");
    configuration.blob_path = collection.get<String>("blob_path");

    if (collection.has("account_name"))
        configuration.account_name = collection.get<String>("account_name");

    if (collection.has("account_key"))
        configuration.account_key = collection.get<String>("account_key");

    configuration.structure = collection.getOrDefault<String>("structure", "auto");
    configuration.format = collection.getOrDefault<String>("format", configuration.format);
    configuration.compression_method = collection.getOrDefault<String>("compression_method", collection.getOrDefault<String>("compression", "auto"));
}


StorageAzureBlob::Configuration StorageAzureBlob::getConfiguration(ASTs & engine_args, ContextPtr local_context)
{
    StorageAzureBlob::Configuration configuration;

    /// Supported signatures:
    ///
    /// AzureBlobStorage(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])
    ///

    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, local_context))
    {
        processNamedCollectionResult(configuration, *named_collection);

        configuration.blobs_paths = {configuration.blob_path};

        if (configuration.format == "auto")
            configuration.format = FormatFactory::instance().getFormatFromFileName(configuration.blob_path, true);

        return configuration;
    }

    if (engine_args.size() < 3 || engine_args.size() > 7)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Storage AzureBlobStorage requires 3 to 7 arguments: "
                        "AzureBlobStorage(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])");

    for (auto & engine_arg : engine_args)
        engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, local_context);

    std::unordered_map<std::string_view, size_t> engine_args_to_idx;

    configuration.connection_url = checkAndGetLiteralArgument<String>(engine_args[0], "connection_string/storage_account_url");
    configuration.is_connection_string = isConnectionString(configuration.connection_url);

    configuration.container = checkAndGetLiteralArgument<String>(engine_args[1], "container");
    configuration.blob_path = checkAndGetLiteralArgument<String>(engine_args[2], "blobpath");

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

    if (configuration.format == "auto")
        configuration.format = FormatFactory::instance().getFormatFromFileName(configuration.blob_path, true);

    return configuration;
}


AzureObjectStorage::SettingsPtr StorageAzureBlob::createSettings(ContextPtr local_context)
{
    const auto & context_settings = local_context->getSettingsRef();
    auto settings_ptr = std::make_unique<AzureObjectStorageSettings>();
    settings_ptr->max_single_part_upload_size = context_settings.azure_max_single_part_upload_size;
    settings_ptr->max_single_read_retries = context_settings.azure_max_single_read_retries;
    settings_ptr->list_object_keys_size = static_cast<int32_t>(context_settings.azure_list_object_keys_size);

    return settings_ptr;
}

void registerStorageAzureBlob(StorageFactory & factory)
{
    factory.registerStorage("AzureBlobStorage", [](const StorageFactory::Arguments & args)
    {
        auto & engine_args = args.engine_args;
        if (engine_args.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "External data source must have arguments");

        auto configuration = StorageAzureBlob::getConfiguration(engine_args, args.getLocalContext());
        auto client = StorageAzureBlob::createClient(configuration, /* is_read_only */ false);
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

        auto settings = StorageAzureBlob::createSettings(args.getContext());

        return std::make_shared<StorageAzureBlob>(
            std::move(configuration),
            std::make_unique<AzureObjectStorage>("AzureBlobStorage", std::move(client), std::move(settings)),
            args.getContext(),
            args.table_id,
            args.columns,
            args.constraints,
            args.comment,
            format_settings,
            /* distributed_processing */ false,
            partition_by);
    },
    {
        .supports_settings = true,
        .supports_sort_order = true, // for partition by
        .supports_schema_inference = true,
        .source_access_type = AccessType::AZURE,
    });
}

static bool containerExists(std::unique_ptr<BlobServiceClient> &blob_service_client, std::string container_name)
{
    Azure::Storage::Blobs::ListBlobContainersOptions options;
    options.Prefix = container_name;
    options.PageSizeHint = 1;

    auto containers_list_response = blob_service_client->ListBlobContainers(options);
    auto containers_list = containers_list_response.BlobContainers;

    for (const auto & container : containers_list)
    {
        if (container_name == container.Name)
            return true;
    }
    return false;
}

AzureClientPtr StorageAzureBlob::createClient(StorageAzureBlob::Configuration configuration, bool is_read_only)
{
    AzureClientPtr result;

    if (configuration.is_connection_string)
    {
        std::unique_ptr<BlobServiceClient> blob_service_client = std::make_unique<BlobServiceClient>(BlobServiceClient::CreateFromConnectionString(configuration.connection_url));
        result = std::make_unique<BlobContainerClient>(BlobContainerClient::CreateFromConnectionString(configuration.connection_url, configuration.container));
        bool container_exists = containerExists(blob_service_client,configuration.container);

        if (!container_exists)
        {
            if (is_read_only)
                throw Exception(
                    ErrorCodes::DATABASE_ACCESS_DENIED,
                    "AzureBlobStorage container does not exist '{}'",
                    configuration.container);

            try
            {
                result->CreateIfNotExists();
            } catch (const Azure::Storage::StorageException & e)
            {
                if (!(e.StatusCode == Azure::Core::Http::HttpStatusCode::Conflict
                    && e.ReasonPhrase == "The specified container already exists."))
                {
                    throw;
                }
            }
        }
    }
    else
    {
        std::shared_ptr<Azure::Storage::StorageSharedKeyCredential> storage_shared_key_credential;
        if (configuration.account_name.has_value() && configuration.account_key.has_value())
        {
            storage_shared_key_credential
                = std::make_shared<Azure::Storage::StorageSharedKeyCredential>(*configuration.account_name, *configuration.account_key);
        }

        std::unique_ptr<BlobServiceClient> blob_service_client;
        if (storage_shared_key_credential)
        {
            blob_service_client = std::make_unique<BlobServiceClient>(configuration.connection_url, storage_shared_key_credential);
        }
        else
        {
            blob_service_client = std::make_unique<BlobServiceClient>(configuration.connection_url);
        }

        bool container_exists = containerExists(blob_service_client,configuration.container);

        std::string final_url;
        size_t pos = configuration.connection_url.find('?');
        if (pos != std::string::npos)
        {
            auto url_without_sas = configuration.connection_url.substr(0, pos);
            final_url = url_without_sas + (url_without_sas.back() == '/' ? "" : "/") + configuration.container
                + configuration.connection_url.substr(pos);
        }
        else
            final_url
                = configuration.connection_url + (configuration.connection_url.back() == '/' ? "" : "/") + configuration.container;

        if (container_exists)
        {
            if (storage_shared_key_credential)
                result = std::make_unique<BlobContainerClient>(final_url, storage_shared_key_credential);
            else
                result = std::make_unique<BlobContainerClient>(final_url);
        }
        else
        {
            if (is_read_only)
                throw Exception(
                    ErrorCodes::DATABASE_ACCESS_DENIED,
                    "AzureBlobStorage container does not exist '{}'",
                    configuration.container);
            try
            {
                result = std::make_unique<BlobContainerClient>(blob_service_client->CreateBlobContainer(configuration.container).Value);
            } catch (const Azure::Storage::StorageException & e)
            {
                if (e.StatusCode == Azure::Core::Http::HttpStatusCode::Conflict
                      && e.ReasonPhrase == "The specified container already exists.")
                {
                    if (storage_shared_key_credential)
                        result = std::make_unique<BlobContainerClient>(final_url, storage_shared_key_credential);
                    else
                        result = std::make_unique<BlobContainerClient>(final_url);
                }
                else
                {
                    throw;
                }
            }
        }
    }

    return result;
}

Poco::URI StorageAzureBlob::Configuration::getConnectionURL() const
{
    if (!is_connection_string)
        return Poco::URI(connection_url);

    auto parsed_connection_string = Azure::Storage::_internal::ParseConnectionString(connection_url);
    return Poco::URI(parsed_connection_string.BlobServiceUrl.GetAbsoluteUrl());
}


StorageAzureBlob::StorageAzureBlob(
    const Configuration & configuration_,
    std::unique_ptr<AzureObjectStorage> && object_storage_,
    ContextPtr context,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    std::optional<FormatSettings> format_settings_,
    bool distributed_processing_,
    ASTPtr partition_by_)
    : IStorage(table_id_)
    , name("AzureBlobStorage")
    , configuration(configuration_)
    , object_storage(std::move(object_storage_))
    , distributed_processing(distributed_processing_)
    , format_settings(format_settings_)
    , partition_by(partition_by_)
{
    FormatFactory::instance().checkFormatName(configuration.format);
    context->getGlobalContext()->getRemoteHostFilter().checkURL(configuration.getConnectionURL());

    StorageInMemoryMetadata storage_metadata;
    if (columns_.empty())
    {
        auto columns = getTableStructureFromData(object_storage.get(), configuration, format_settings, context, distributed_processing);
        storage_metadata.setColumns(columns);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);

    StoredObjects objects;
    for (const auto & key : configuration.blobs_paths)
        objects.emplace_back(key);

    virtual_columns = VirtualColumnUtils::getPathAndFileVirtualsForStorage(storage_metadata.getSampleBlock().getNamesAndTypesList());
}

void StorageAzureBlob::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &)
{
    if (configuration.withGlobs())
    {
        throw Exception(
            ErrorCodes::DATABASE_ACCESS_DENIED,
            "AzureBlobStorage key '{}' contains globs, so the table is in readonly mode",
            configuration.blob_path);
    }

    StoredObjects objects;
    for (const auto & key : configuration.blobs_paths)
        objects.emplace_back(key);

    object_storage->removeObjectsIfExist(objects);
}

namespace
{

class StorageAzureBlobSink : public SinkToStorage
{
public:
    StorageAzureBlobSink(
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

    String getName() const override { return "StorageAzureBlobSink"; }

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

    void onException(std::exception_ptr exception) override
    {
        std::lock_guard lock(cancel_mutex);
        try
        {
            std::rethrow_exception(exception);
        }
        catch (...)
        {
            /// An exception context is needed to proper delete write buffers without finalization
            release();
        }
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
            release();
            throw;
        }
    }

    void release()
    {
        writer.reset();
        write_buf->finalize();
    }

    Block sample_block;
    std::optional<FormatSettings> format_settings;
    std::unique_ptr<WriteBuffer> write_buf;
    OutputFormatPtr writer;
    bool cancelled = false;
    std::mutex cancel_mutex;
};

class PartitionedStorageAzureBlobSink : public PartitionedSink
{
public:
    PartitionedStorageAzureBlobSink(
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

        return std::make_shared<StorageAzureBlobSink>(
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

Pipe StorageAzureBlob::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    if (partition_by && configuration.withWildcard())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Reading from a partitioned Azure storage is not implemented yet");

    Pipes pipes;

    std::shared_ptr<StorageAzureBlobSource::IIterator> iterator_wrapper;
    if (distributed_processing)
    {
        iterator_wrapper = std::make_shared<StorageAzureBlobSource::ReadIterator>(local_context,
            local_context->getReadTaskCallback());
    }
    else if (configuration.withGlobs())
    {
        /// Iterate through disclosed globs and make a source for each file
        iterator_wrapper = std::make_shared<StorageAzureBlobSource::GlobIterator>(
            object_storage.get(), configuration.container, configuration.blob_path,
            query_info.query, virtual_columns, local_context, nullptr, local_context->getFileProgressCallback());
    }
    else
    {
        iterator_wrapper = std::make_shared<StorageAzureBlobSource::KeysIterator>(
            object_storage.get(), configuration.container, configuration.blobs_paths,
            query_info.query, virtual_columns, local_context, nullptr, local_context->getFileProgressCallback());
    }

    auto read_from_format_info = prepareReadingFromFormat(column_names, storage_snapshot, supportsSubsetOfColumns(), getVirtuals());
    bool need_only_count = (query_info.optimize_trivial_count || read_from_format_info.requested_columns.empty())
        && local_context->getSettingsRef().optimize_count_from_files;

    for (size_t i = 0; i < num_streams; ++i)
    {
        pipes.emplace_back(std::make_shared<StorageAzureBlobSource>(
            read_from_format_info,
            configuration.format,
            getName(),
            local_context,
            format_settings,
            max_block_size,
            configuration.compression_method,
            object_storage.get(),
            configuration.container,
            configuration.connection_url,
            iterator_wrapper,
            need_only_count,
            query_info));
    }

    return Pipe::unitePipes(std::move(pipes));
}

SinkToStoragePtr StorageAzureBlob::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, bool /*async_insert*/)
{
    auto sample_block = metadata_snapshot->getSampleBlock();
    auto chosen_compression_method = chooseCompressionMethod(configuration.blobs_paths.back(), configuration.compression_method);
    auto insert_query = std::dynamic_pointer_cast<ASTInsertQuery>(query);

    auto partition_by_ast = insert_query ? (insert_query->partition_by ? insert_query->partition_by : partition_by) : nullptr;
    bool is_partitioned_implementation = partition_by_ast && configuration.withWildcard();

    if (is_partitioned_implementation)
    {
        return std::make_shared<PartitionedStorageAzureBlobSink>(
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
                            "AzureBlobStorage key '{}' contains globs, so the table is in readonly mode", configuration.blob_path);

        bool truncate_in_insert = local_context->getSettingsRef().azure_truncate_on_insert;

        if (!truncate_in_insert && object_storage->exists(StoredObject(configuration.blob_path)))
        {

            if (local_context->getSettingsRef().azure_create_new_file_on_insert)
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
                    "If you want to overwrite it, enable setting azure_truncate_on_insert, if you "
                    "want to create a new file on each insert, enable setting azure_create_new_file_on_insert",
                    configuration.container, configuration.blobs_paths.back());
            }
        }

        return std::make_shared<StorageAzureBlobSink>(
            configuration.format,
            sample_block,
            local_context,
            format_settings,
            chosen_compression_method,
            object_storage.get(),
            configuration.blobs_paths.back());
    }
}

NamesAndTypesList StorageAzureBlob::getVirtuals() const
{
    return virtual_columns;
}

bool StorageAzureBlob::supportsPartitionBy() const
{
    return true;
}

bool StorageAzureBlob::supportsSubsetOfColumns() const
{
    return FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(configuration.format);
}

bool StorageAzureBlob::prefersLargeBlocks() const
{
    return FormatFactory::instance().checkIfOutputFormatPrefersLargeBlocks(configuration.format);
}

bool StorageAzureBlob::parallelizeOutputAfterReading(ContextPtr context) const
{
    return FormatFactory::instance().checkParallelizeOutputAfterReading(configuration.format, context);
}

StorageAzureBlobSource::GlobIterator::GlobIterator(
    AzureObjectStorage * object_storage_,
    const std::string & container_,
    String blob_path_with_globs_,
    ASTPtr query_,
    const NamesAndTypesList & virtual_columns_,
    ContextPtr context_,
    RelativePathsWithMetadata * outer_blobs_,
    std::function<void(FileProgress)> file_progress_callback_)
    : IIterator(context_)
    , object_storage(object_storage_)
    , container(container_)
    , blob_path_with_globs(blob_path_with_globs_)
    , query(query_)
    , virtual_columns(virtual_columns_)
    , outer_blobs(outer_blobs_)
    , file_progress_callback(file_progress_callback_)
{

    const String key_prefix = blob_path_with_globs.substr(0, blob_path_with_globs.find_first_of("*?{"));

    /// We don't have to list bucket, because there is no asterisks.
    if (key_prefix.size() == blob_path_with_globs.size())
    {
        ObjectMetadata object_metadata = object_storage->getObjectMetadata(blob_path_with_globs);
        blobs_with_metadata.emplace_back(blob_path_with_globs, object_metadata);
        if (outer_blobs)
            outer_blobs->emplace_back(blobs_with_metadata.back());
        is_finished = true;
        return;
    }

    object_storage_iterator = object_storage->iterate(key_prefix);

    matcher = std::make_unique<re2::RE2>(makeRegexpPatternFromGlobs(blob_path_with_globs));

    if (!matcher->ok())
        throw Exception(
            ErrorCodes::CANNOT_COMPILE_REGEXP, "Cannot compile regex from glob ({}): {}", blob_path_with_globs, matcher->error());

    recursive = blob_path_with_globs == "/**" ? true : false;
}

RelativePathWithMetadata StorageAzureBlobSource::GlobIterator::next()
{
    std::lock_guard lock(next_mutex);

    if (is_finished && index >= blobs_with_metadata.size())
    {
        return {};
    }

    bool need_new_batch = blobs_with_metadata.empty() || index >= blobs_with_metadata.size();

    if (need_new_batch)
    {
        RelativePathsWithMetadata new_batch;
        while (new_batch.empty())
        {
            auto result = object_storage_iterator->getCurrrentBatchAndScheduleNext();
            if (result.has_value())
            {
                new_batch = result.value();
            }
            else
            {
                is_finished = true;
                return {};
            }

            for (auto it = new_batch.begin(); it != new_batch.end();)
            {
                if (!recursive && !re2::RE2::FullMatch(it->relative_path, *matcher))
                    it = new_batch.erase(it);
                else
                    ++it;
            }
        }

        index = 0;
        if (!is_initialized)
        {
            filter_ast = VirtualColumnUtils::createPathAndFileFilterAst(query, virtual_columns, fs::path(container) / new_batch.front().relative_path, getContext());
            is_initialized = true;
        }

        if (filter_ast)
        {
            std::vector<String> paths;
            paths.reserve(new_batch.size());
            for (auto & path_with_metadata : new_batch)
                paths.push_back(fs::path(container) / path_with_metadata.relative_path);

            VirtualColumnUtils::filterByPathOrFile(new_batch, paths, query, virtual_columns, getContext(), filter_ast);
        }

        if (outer_blobs)
            outer_blobs->insert(outer_blobs->end(), new_batch.begin(), new_batch.end());

        blobs_with_metadata = std::move(new_batch);
        if (file_progress_callback)
        {
            for (const auto & [_, info] : blobs_with_metadata)
                file_progress_callback(FileProgress(0, info.size_bytes));
        }
    }

    size_t current_index = index++;
    if (current_index >= blobs_with_metadata.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index out of bound for blob metadata");
    return blobs_with_metadata[current_index];
}

StorageAzureBlobSource::KeysIterator::KeysIterator(
    AzureObjectStorage * object_storage_,
    const std::string & container_,
    const Strings & keys_,
    ASTPtr query_,
    const NamesAndTypesList & virtual_columns_,
    ContextPtr context_,
    RelativePathsWithMetadata * outer_blobs,
    std::function<void(FileProgress)> file_progress_callback)
    : IIterator(context_)
    , object_storage(object_storage_)
    , container(container_)
    , query(query_)
    , virtual_columns(virtual_columns_)
{
    Strings all_keys = keys_;

    ASTPtr filter_ast;
    if (!all_keys.empty())
        filter_ast = VirtualColumnUtils::createPathAndFileFilterAst(query, virtual_columns, fs::path(container) / all_keys[0], getContext());

    if (filter_ast)
    {
        Strings paths;
        paths.reserve(all_keys.size());
        for (const auto & key : all_keys)
            paths.push_back(fs::path(container) / key);

        VirtualColumnUtils::filterByPathOrFile(all_keys, paths, query, virtual_columns, getContext(), filter_ast);
    }

    for (auto && key : all_keys)
    {
        ObjectMetadata object_metadata = object_storage->getObjectMetadata(key);
        if (file_progress_callback)
            file_progress_callback(FileProgress(0, object_metadata.size_bytes));
        keys.emplace_back(RelativePathWithMetadata{key, object_metadata});
    }

    if (outer_blobs)
        *outer_blobs = keys;
}

RelativePathWithMetadata StorageAzureBlobSource::KeysIterator::next()
{
    size_t current_index = index.fetch_add(1, std::memory_order_relaxed);
    if (current_index >= keys.size())
        return {};

    return keys[current_index];
}

Chunk StorageAzureBlobSource::generate()
{
    while (true)
    {
        if (isCancelled() || !reader)
        {
            if (reader)
                reader->cancel();
            break;
        }

        Chunk chunk;
        if (reader->pull(chunk))
        {
            UInt64 num_rows = chunk.getNumRows();
            total_rows_in_file += num_rows;
            size_t chunk_size = 0;
            if (const auto * input_format = reader.getInputFormat())
                chunk_size = input_format->getApproxBytesReadForChunk();
            progress(num_rows, chunk_size ? chunk_size : chunk.bytes());
            VirtualColumnUtils::addRequestedPathAndFileVirtualsToChunk(chunk, requested_virtual_columns, fs::path(container) / reader.getRelativePath());
            return chunk;
        }

        if (reader.getInputFormat() && getContext()->getSettingsRef().use_cache_for_count_from_files)
            addNumRowsToCache(reader.getRelativePath(), total_rows_in_file);

        total_rows_in_file = 0;

        assert(reader_future.valid());
        reader = reader_future.get();

        if (!reader)
            break;

        /// Even if task is finished the thread may be not freed in pool.
        /// So wait until it will be freed before scheduling a new task.
        create_reader_pool.wait();
        reader_future = createReaderAsync();
    }

    return {};
}

void StorageAzureBlobSource::addNumRowsToCache(const DB::String & path, size_t num_rows)
{
    String source = fs::path(connection_url) / container / path;
    auto cache_key = getKeyForSchemaCache(source, format, format_settings, getContext());
    StorageAzureBlob::getSchemaCache(getContext()).addNumRows(cache_key, num_rows);
}

std::optional<size_t> StorageAzureBlobSource::tryGetNumRowsFromCache(const DB::RelativePathWithMetadata & path_with_metadata)
{
    String source = fs::path(connection_url) / container / path_with_metadata.relative_path;
    auto cache_key = getKeyForSchemaCache(source, format, format_settings, getContext());
    auto get_last_mod_time = [&]() -> std::optional<time_t>
    {
        auto last_mod = path_with_metadata.metadata.last_modified;
        if (last_mod)
            return last_mod->epochTime();
        return std::nullopt;
    };

    return StorageAzureBlob::getSchemaCache(getContext()).tryGetNumRows(cache_key, get_last_mod_time);
}

StorageAzureBlobSource::StorageAzureBlobSource(
    const ReadFromFormatInfo & info,
    const String & format_,
    String name_,
    ContextPtr context_,
    std::optional<FormatSettings> format_settings_,
    UInt64 max_block_size_,
    String compression_hint_,
    AzureObjectStorage * object_storage_,
    const String & container_,
    const String & connection_url_,
    std::shared_ptr<IIterator> file_iterator_,
    bool need_only_count_,
    const SelectQueryInfo & query_info_)
    :ISource(info.source_header, false)
    , WithContext(context_)
    , requested_columns(info.requested_columns)
    , requested_virtual_columns(info.requested_virtual_columns)
    , format(format_)
    , name(std::move(name_))
    , sample_block(info.format_header)
    , format_settings(format_settings_)
    , columns_desc(info.columns_description)
    , max_block_size(max_block_size_)
    , compression_hint(compression_hint_)
    , object_storage(std::move(object_storage_))
    , container(container_)
    , connection_url(connection_url_)
    , file_iterator(file_iterator_)
    , need_only_count(need_only_count_)
    , query_info(query_info_)
    , create_reader_pool(CurrentMetrics::ObjectStorageAzureThreads, CurrentMetrics::ObjectStorageAzureThreadsActive, 1)
    , create_reader_scheduler(threadPoolCallbackRunner<ReaderHolder>(create_reader_pool, "AzureReader"))
{
    reader = createReader();
    if (reader)
        reader_future = createReaderAsync();
}


StorageAzureBlobSource::~StorageAzureBlobSource()
{
    create_reader_pool.wait();
}

String StorageAzureBlobSource::getName() const
{
    return name;
}

StorageAzureBlobSource::ReaderHolder StorageAzureBlobSource::createReader()
{
    auto path_with_metadata = file_iterator->next();
    if (path_with_metadata.relative_path.empty())
        return {};

    if (path_with_metadata.metadata.size_bytes == 0)
        path_with_metadata.metadata = object_storage->getObjectMetadata(path_with_metadata.relative_path);

    QueryPipelineBuilder builder;
    std::shared_ptr<ISource> source;
    std::unique_ptr<ReadBuffer> read_buf;
    std::optional<size_t> num_rows_from_cache = need_only_count && getContext()->getSettingsRef().use_cache_for_count_from_files ? tryGetNumRowsFromCache(path_with_metadata) : std::nullopt;
    if (num_rows_from_cache)
    {
        /// We should not return single chunk with all number of rows,
        /// because there is a chance that this chunk will be materialized later
        /// (it can cause memory problems even with default values in columns or when virtual columns are requested).
        /// Instead, we use special ConstChunkGenerator that will generate chunks
        /// with max_block_size rows until total number of rows is reached.
        source = std::make_shared<ConstChunkGenerator>(sample_block, *num_rows_from_cache, max_block_size);
        builder.init(Pipe(source));
    }
    else
    {
        std::optional<size_t> max_parsing_threads;
        if (need_only_count)
            max_parsing_threads = 1;

        auto compression_method = chooseCompressionMethod(path_with_metadata.relative_path, compression_hint);
        read_buf = createAzureReadBuffer(path_with_metadata.relative_path, path_with_metadata.metadata.size_bytes);
        auto input_format = FormatFactory::instance().getInput(
                format, *read_buf, sample_block, getContext(), max_block_size,
                format_settings, max_parsing_threads, std::nullopt,
                /* is_remote_fs */ true, compression_method);
        input_format->setQueryInfo(query_info, getContext());

        if (need_only_count)
            input_format->needOnlyCount();

        builder.init(Pipe(input_format));

        if (columns_desc.hasDefaults())
        {
            builder.addSimpleTransform(
                [&](const Block & header)
                { return std::make_shared<AddingDefaultsTransform>(header, columns_desc, *input_format, getContext()); });
        }

        source = input_format;
    }

    /// Add ExtractColumnsTransform to extract requested columns/subcolumns
    /// from chunk read by IInputFormat.
    builder.addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<ExtractColumnsTransform>(header, requested_columns);
    });

    auto pipeline = std::make_unique<QueryPipeline>(QueryPipelineBuilder::getPipeline(std::move(builder)));
    auto current_reader = std::make_unique<PullingPipelineExecutor>(*pipeline);

    ProfileEvents::increment(ProfileEvents::EngineFileLikeReadFiles);

    return ReaderHolder{path_with_metadata, std::move(read_buf), std::move(source), std::move(pipeline), std::move(current_reader)};
}

std::future<StorageAzureBlobSource::ReaderHolder> StorageAzureBlobSource::createReaderAsync()
{
    return create_reader_scheduler([this] { return createReader(); }, Priority{});
}

std::unique_ptr<ReadBuffer> StorageAzureBlobSource::createAzureReadBuffer(const String & key, size_t object_size)
{
    auto read_settings = getContext()->getReadSettings().adjustBufferSize(object_size);
    read_settings.enable_filesystem_cache = false;
    auto download_buffer_size = getContext()->getSettings().max_download_buffer_size;
    const bool object_too_small = object_size <= 2 * download_buffer_size;

    // Create a read buffer that will prefetch the first ~1 MB of the file.
    // When reading lots of tiny files, this prefetching almost doubles the throughput.
    // For bigger files, parallel reading is more useful.
    if (object_too_small && read_settings.remote_fs_method == RemoteFSReadMethod::threadpool)
    {
        LOG_TRACE(log, "Downloading object of size {} from Azure with initial prefetch", object_size);
        return createAsyncAzureReadBuffer(key, read_settings, object_size);
    }

    return object_storage->readObject(StoredObject(key), read_settings, {}, object_size);
}

namespace
{
    class ReadBufferIterator : public IReadBufferIterator, WithContext
    {
    public:
        ReadBufferIterator(
            const std::shared_ptr<StorageAzureBlobSource::IIterator> & file_iterator_,
            AzureObjectStorage * object_storage_,
            const StorageAzureBlob::Configuration & configuration_,
            const std::optional<FormatSettings> & format_settings_,
            const RelativePathsWithMetadata & read_keys_,
            const ContextPtr & context_)
            : WithContext(context_)
            , file_iterator(file_iterator_)
            , object_storage(object_storage_)
            , configuration(configuration_)
            , format_settings(format_settings_)
            , read_keys(read_keys_)
            , prev_read_keys_size(read_keys_.size())
        {
        }

        std::unique_ptr<ReadBuffer> next() override
        {
            auto [key, metadata] = file_iterator->next();

            if (key.empty())
            {
                if (first)
                    throw Exception(
                        ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                        "Cannot extract table structure from {} format file, because there are no files with provided path "
                        "in AzureBlobStorage. You must specify table structure manually", configuration.format);

                return nullptr;
            }

            current_path = key;

            ///AzureBlobStorage file iterator could get new keys after new iteration, check them in schema cache.
            if (getContext()->getSettingsRef().schema_inference_use_cache_for_azure && read_keys.size() > prev_read_keys_size)
            {
                columns_from_cache = StorageAzureBlob::tryGetColumnsFromCache(read_keys.begin() + prev_read_keys_size, read_keys.end(), configuration, format_settings, getContext());
                prev_read_keys_size = read_keys.size();
                if (columns_from_cache)
                    return nullptr;
            }

            first = false;
            int zstd_window_log_max = static_cast<int>(getContext()->getSettingsRef().zstd_window_log_max);
            return wrapReadBufferWithCompressionMethod(
                object_storage->readObject(StoredObject(key), getContext()->getReadSettings(), {}, metadata.size_bytes),
                chooseCompressionMethod(key, configuration.compression_method),
                zstd_window_log_max);
        }

        std::optional<ColumnsDescription> getCachedColumns() override { return columns_from_cache; }

        void setNumRowsToLastFile(size_t num_rows) override
        {
            if (!getContext()->getSettingsRef().schema_inference_use_cache_for_s3)
                return;

            String source = fs::path(configuration.connection_url) / configuration.container / current_path;
            auto key = getKeyForSchemaCache(source, configuration.format, format_settings, getContext());
            StorageAzureBlob::getSchemaCache(getContext()).addNumRows(key, num_rows);
        }

    private:
        std::shared_ptr<StorageAzureBlobSource::IIterator> file_iterator;
        AzureObjectStorage * object_storage;
        const StorageAzureBlob::Configuration & configuration;
        const std::optional<FormatSettings> & format_settings;
        const RelativePathsWithMetadata & read_keys;
        std::optional<ColumnsDescription> columns_from_cache;
        size_t prev_read_keys_size;
        String current_path;
        bool first = true;
    };
}

ColumnsDescription StorageAzureBlob::getTableStructureFromData(
    AzureObjectStorage * object_storage,
    const Configuration & configuration,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr ctx,
    bool distributed_processing)
{
    RelativePathsWithMetadata read_keys;
    std::shared_ptr<StorageAzureBlobSource::IIterator> file_iterator;
    if (distributed_processing)
    {
        file_iterator = std::make_shared<StorageAzureBlobSource::ReadIterator>(ctx,
            ctx->getReadTaskCallback());
    }
    else if (configuration.withGlobs())
    {
        file_iterator = std::make_shared<StorageAzureBlobSource::GlobIterator>(
            object_storage, configuration.container, configuration.blob_path, nullptr, NamesAndTypesList{}, ctx, &read_keys);
    }
    else
    {
        file_iterator = std::make_shared<StorageAzureBlobSource::KeysIterator>(
            object_storage, configuration.container, configuration.blobs_paths, nullptr, NamesAndTypesList{}, ctx, &read_keys);
    }

    std::optional<ColumnsDescription> columns_from_cache;
    if (ctx->getSettingsRef().schema_inference_use_cache_for_azure)
        columns_from_cache = tryGetColumnsFromCache(read_keys.begin(), read_keys.end(), configuration, format_settings, ctx);

    ColumnsDescription columns;
    if (columns_from_cache)
    {
        columns = *columns_from_cache;
    }
    else
    {
        ReadBufferIterator read_buffer_iterator(file_iterator, object_storage, configuration, format_settings, read_keys, ctx);
        columns = readSchemaFromFormat(configuration.format, format_settings, read_buffer_iterator, configuration.withGlobs(), ctx);
    }

    if (ctx->getSettingsRef().schema_inference_use_cache_for_azure)
        addColumnsToCache(read_keys, columns, configuration, format_settings, configuration.format, ctx);

    return columns;

}

std::optional<ColumnsDescription> StorageAzureBlob::tryGetColumnsFromCache(
        const RelativePathsWithMetadata::const_iterator & begin,
        const RelativePathsWithMetadata::const_iterator & end,
        const StorageAzureBlob::Configuration & configuration,
        const std::optional<FormatSettings> & format_settings,
        const ContextPtr & ctx)
{
    auto & schema_cache = getSchemaCache(ctx);
    for (auto it = begin; it < end; ++it)
    {
        auto get_last_mod_time = [&] -> std::optional<time_t>
        {
            if (it->metadata.last_modified)
                return it->metadata.last_modified->epochTime();
            return std::nullopt;
        };

        auto host_and_bucket = configuration.connection_url + '/' + configuration.container;
        String source = host_and_bucket + '/' + it->relative_path;
        auto cache_key = getKeyForSchemaCache(source, configuration.format, format_settings, ctx);
        auto columns = schema_cache.tryGetColumns(cache_key, get_last_mod_time);
        if (columns)
            return columns;
    }

    return std::nullopt;

}

void StorageAzureBlob::addColumnsToCache(
    const RelativePathsWithMetadata & keys,
    const ColumnsDescription & columns,
    const StorageAzureBlob::Configuration & configuration,
    const std::optional<FormatSettings> & format_settings,
    const String & format_name,
    const ContextPtr & ctx)
{
    auto host_and_bucket = configuration.connection_url + '/' + configuration.container;
    Strings sources;
    sources.reserve(keys.size());
    std::transform(keys.begin(), keys.end(), std::back_inserter(sources), [&](const auto & elem){ return host_and_bucket + '/' + elem.relative_path; });
    auto cache_keys = getKeysForSchemaCache(sources, format_name, format_settings, ctx);
    auto & schema_cache = getSchemaCache(ctx);
    schema_cache.addManyColumns(cache_keys, columns);
}

SchemaCache & StorageAzureBlob::getSchemaCache(const ContextPtr & ctx)
{
    static SchemaCache schema_cache(ctx->getConfigRef().getUInt("schema_inference_cache_max_elements_for_azure", DEFAULT_SCHEMA_CACHE_ELEMENTS));
    return schema_cache;
}


std::unique_ptr<ReadBuffer> StorageAzureBlobSource::createAsyncAzureReadBuffer(
    const String & key, const ReadSettings & read_settings, size_t object_size)
{
    auto modified_settings{read_settings};
    modified_settings.remote_read_min_bytes_for_seek = modified_settings.remote_fs_buffer_size;
    auto async_reader = object_storage->readObjects(StoredObjects{StoredObject{key, object_size}}, modified_settings);

    async_reader->setReadUntilEnd();
    if (read_settings.remote_fs_prefetch)
        async_reader->prefetch(DEFAULT_PREFETCH_PRIORITY);

    return async_reader;
}

}

#endif
