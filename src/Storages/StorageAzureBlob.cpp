#include <Storages/StorageAzureBlob.h>


#if USE_AZURE_BLOB_STORAGE
#include <Formats/FormatFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>

#include <IO/ParallelReadBuffer.h>
#include <IO/SharedThreadPools.h>

#include <Parsers/ASTCreateQuery.h>
#include <Formats/ReadSchemaUtils.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <re2/re2.h>

#include <azure/identity/managed_identity_credential.hpp>
#include <azure/storage/common/storage_credential.hpp>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/Transforms/ExtractColumnsTransform.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/IInputFormat.h>

#include <Storages/StorageFactory.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/PartitionedSink.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/getVirtualsForStorage.h>
#include <Storages/StorageURL.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/ReadFromStorageProgress.h>
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
    return candidate.starts_with("DefaultEndpointsProtocol");
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
        auto client = StorageAzureBlob::createClient(configuration);
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
            partition_by);
    },
    {
        .supports_settings = true,
        .supports_sort_order = true, // for partition by
        .supports_schema_inference = true,
        .source_access_type = AccessType::AZURE,
    });
}

AzureClientPtr StorageAzureBlob::createClient(StorageAzureBlob::Configuration configuration)
{
    AzureClientPtr result;

    if (configuration.is_connection_string)
    {
        result = std::make_unique<BlobContainerClient>(BlobContainerClient::CreateFromConnectionString(configuration.connection_url, configuration.container));
        result->CreateIfNotExists();
    }
    else
    {
        if (configuration.account_name.has_value() && configuration.account_key.has_value())
        {
            auto storage_shared_key_credential = std::make_shared<Azure::Storage::StorageSharedKeyCredential>(*configuration.account_name, *configuration.account_key);
            auto blob_service_client = std::make_unique<BlobServiceClient>(configuration.connection_url, storage_shared_key_credential);
            try
            {
                result = std::make_unique<BlobContainerClient>(blob_service_client->CreateBlobContainer(configuration.container).Value);
            }
            catch (const Azure::Storage::StorageException & e)
            {
                if (e.StatusCode == Azure::Core::Http::HttpStatusCode::Conflict)
                {
                    auto final_url = configuration.connection_url
                        + (configuration.connection_url.back() == '/' ? "" : "/")
                        + configuration.container;

                    result = std::make_unique<BlobContainerClient>(final_url, storage_shared_key_credential);
                }
                else
                {
                    throw;
                }
            }
        }
        else
        {
            auto managed_identity_credential = std::make_shared<Azure::Identity::ManagedIdentityCredential>();
            auto blob_service_client = std::make_unique<BlobServiceClient>(configuration.connection_url, managed_identity_credential);
            try
            {
                result = std::make_unique<BlobContainerClient>(blob_service_client->CreateBlobContainer(configuration.container).Value);
            }
            catch (const Azure::Storage::StorageException & e)
            {
                if (e.StatusCode == Azure::Core::Http::HttpStatusCode::Conflict)
                {
                    auto final_url = configuration.connection_url
                        + (configuration.connection_url.back() == '/' ? "" : "/")
                        + configuration.container;

                    result = std::make_unique<BlobContainerClient>(final_url, managed_identity_credential);
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
    ASTPtr partition_by_)
    : IStorage(table_id_)
    , name("AzureBlobStorage")
    , configuration(configuration_)
    , object_storage(std::move(object_storage_))
    , distributed_processing(false)
    , format_settings(format_settings_)
    , partition_by(partition_by_)
{
    FormatFactory::instance().checkFormatName(configuration.format);
    context->getGlobalContext()->getRemoteHostFilter().checkURL(configuration.getConnectionURL());

    StorageInMemoryMetadata storage_metadata;
    if (columns_.empty())
    {
        auto columns = getTableStructureFromData(object_storage.get(), configuration, format_settings, context);
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

    auto default_virtuals = NamesAndTypesList{
        {"_path", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"_file", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())}};

    auto columns = storage_metadata.getSampleBlock().getNamesAndTypesList();

    virtual_columns = getVirtualsForStorage(columns, default_virtuals);
    for (const auto & column : virtual_columns)
        virtual_block.insert({column.type->createColumn(), column.type, column.name});
}

void StorageAzureBlob::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &)
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
    if (configuration.withGlobs())
    {
        /// Iterate through disclosed globs and make a source for each file
        iterator_wrapper = std::make_shared<StorageAzureBlobSource::GlobIterator>(
            object_storage.get(), configuration.container, configuration.blob_path,
            query_info.query, virtual_block, local_context, nullptr);
    }
    else
    {
        iterator_wrapper = std::make_shared<StorageAzureBlobSource::KeysIterator>(
            object_storage.get(), configuration.container, configuration.blobs_paths,
            query_info.query, virtual_block, local_context, nullptr);
    }

    auto read_from_format_info = prepareReadingFromFormat(column_names, storage_snapshot, configuration.format, getVirtuals());
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
            iterator_wrapper));
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

static void addPathToVirtualColumns(Block & block, const String & path, size_t idx)
{
    if (block.has("_path"))
        block.getByName("_path").column->assumeMutableRef().insert(path);

    if (block.has("_file"))
    {
        auto pos = path.find_last_of('/');
        assert(pos != std::string::npos);

        auto file = path.substr(pos + 1);
        block.getByName("_file").column->assumeMutableRef().insert(file);
    }

    block.getByName("_idx").column->assumeMutableRef().insert(idx);
}

StorageAzureBlobSource::GlobIterator::GlobIterator(
    AzureObjectStorage * object_storage_,
    const std::string & container_,
    String blob_path_with_globs_,
    ASTPtr query_,
    const Block & virtual_header_,
    ContextPtr context_,
    RelativePathsWithMetadata * outer_blobs_)
    : IIterator(context_)
    , object_storage(object_storage_)
    , container(container_)
    , blob_path_with_globs(blob_path_with_globs_)
    , query(query_)
    , virtual_header(virtual_header_)
    , outer_blobs(outer_blobs_)
{

    const String key_prefix = blob_path_with_globs.substr(0, blob_path_with_globs.find_first_of("*?{"));

    /// We don't have to list bucket, because there is no asterisks.
    if (key_prefix.size() == blob_path_with_globs.size())
    {
        ObjectMetadata object_metadata = object_storage->getObjectMetadata(blob_path_with_globs);
        blobs_with_metadata.emplace_back(blob_path_with_globs, object_metadata);
        if (outer_blobs)
            outer_blobs->emplace_back(blobs_with_metadata.back());
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

    if (is_finished)
        return {};

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
            createFilterAST(new_batch.front().relative_path);
            is_initialized = true;
        }

        if (filter_ast)
        {
            auto block = virtual_header.cloneEmpty();
            for (size_t i = 0; i < new_batch.size(); ++i)
                addPathToVirtualColumns(block, fs::path(container) / new_batch[i].relative_path, i);

            VirtualColumnUtils::filterBlockWithQuery(query, block, getContext(), filter_ast);
            const auto & idxs = typeid_cast<const ColumnUInt64 &>(*block.getByName("_idx").column);

            blobs_with_metadata.clear();
            for (UInt64 idx : idxs.getData())
            {
                total_size.fetch_add(new_batch[idx].metadata.size_bytes, std::memory_order_relaxed);
                blobs_with_metadata.emplace_back(std::move(new_batch[idx]));
                if (outer_blobs)
                    outer_blobs->emplace_back(blobs_with_metadata.back());
            }
        }
        else
        {
            if (outer_blobs)
                outer_blobs->insert(outer_blobs->end(), new_batch.begin(), new_batch.end());

            blobs_with_metadata = std::move(new_batch);
            for (const auto & [_, info] : blobs_with_metadata)
                total_size.fetch_add(info.size_bytes, std::memory_order_relaxed);
        }
    }

    size_t current_index = index++;
    if (current_index >= blobs_with_metadata.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index out of bound for blob metadata");
    return blobs_with_metadata[current_index];
}

size_t StorageAzureBlobSource::GlobIterator::getTotalSize() const
{
    return total_size.load(std::memory_order_relaxed);
}


void StorageAzureBlobSource::GlobIterator::createFilterAST(const String & any_key)
{
    if (!query || !virtual_header)
        return;

    /// Create a virtual block with one row to construct filter
    /// Append "idx" column as the filter result
    virtual_header.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "_idx"});

    auto block = virtual_header.cloneEmpty();
    addPathToVirtualColumns(block, fs::path(container) / any_key, 0);
    VirtualColumnUtils::prepareFilterBlockWithQuery(query, getContext(), block, filter_ast);
}


StorageAzureBlobSource::KeysIterator::KeysIterator(
    AzureObjectStorage * object_storage_,
    const std::string & container_,
    Strings keys_,
    ASTPtr query_,
    const Block & virtual_header_,
    ContextPtr context_,
    RelativePathsWithMetadata * outer_blobs_)
    : IIterator(context_)
    , object_storage(object_storage_)
    , container(container_)
    , query(query_)
    , virtual_header(virtual_header_)
    , outer_blobs(outer_blobs_)
{
    Strings all_keys = keys_;

    /// Create a virtual block with one row to construct filter
    if (query && virtual_header && !all_keys.empty())
    {
        /// Append "idx" column as the filter result
        virtual_header.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "_idx"});

        auto block = virtual_header.cloneEmpty();
        addPathToVirtualColumns(block, fs::path(container) / all_keys.front(), 0);

        VirtualColumnUtils::prepareFilterBlockWithQuery(query, getContext(), block, filter_ast);

        if (filter_ast)
        {
            block = virtual_header.cloneEmpty();
            for (size_t i = 0; i < all_keys.size(); ++i)
                addPathToVirtualColumns(block, fs::path(container) / all_keys[i], i);

            VirtualColumnUtils::filterBlockWithQuery(query, block, getContext(), filter_ast);
            const auto & idxs = typeid_cast<const ColumnUInt64 &>(*block.getByName("_idx").column);

            Strings filtered_keys;
            filtered_keys.reserve(block.rows());
            for (UInt64 idx : idxs.getData())
                filtered_keys.emplace_back(std::move(all_keys[idx]));

            all_keys = std::move(filtered_keys);
        }
    }

    for (auto && key : all_keys)
    {
        ObjectMetadata object_metadata = object_storage->getObjectMetadata(key);
        total_size += object_metadata.size_bytes;
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

size_t StorageAzureBlobSource::KeysIterator::getTotalSize() const
{
    return total_size.load(std::memory_order_relaxed);
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

            const auto & file_path = reader.getPath();
            if (num_rows && total_objects_size)
            {
                size_t chunk_size = reader.getFormat()->getApproxBytesReadForChunk();
                if (!chunk_size)
                    chunk_size = chunk.bytes();
                updateRowsProgressApprox(
                    *this, num_rows, chunk_size, total_objects_size, total_rows_approx_accumulated, total_rows_count_times, total_rows_approx_max);
            }

            for (const auto & virtual_column : requested_virtual_columns)
            {
                if (virtual_column.name == "_path")
                {
                    chunk.addColumn(virtual_column.type->createColumnConst(num_rows, file_path)->convertToFullColumnIfConst());
                }
                else if (virtual_column.name == "_file")
                {
                    size_t last_slash_pos = file_path.find_last_of('/');
                    auto column = virtual_column.type->createColumnConst(num_rows, file_path.substr(last_slash_pos + 1));
                    chunk.addColumn(column->convertToFullColumnIfConst());
                }
            }

            return chunk;
        }


        assert(reader_future.valid());
        reader = reader_future.get();

        if (!reader)
            break;

        size_t object_size = tryGetFileSizeFromReadBuffer(*reader.getReadBuffer()).value_or(0);
        /// Adjust total_rows_approx_accumulated with new total size.
        if (total_objects_size)
            total_rows_approx_accumulated = static_cast<size_t>(
                std::ceil(static_cast<double>(total_objects_size + object_size) / total_objects_size * total_rows_approx_accumulated));
        total_objects_size += object_size;

        /// Even if task is finished the thread may be not freed in pool.
        /// So wait until it will be freed before scheduling a new task.
        create_reader_pool.wait();
        reader_future = createReaderAsync();
    }

    return {};
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
    std::shared_ptr<IIterator> file_iterator_)
    :ISource(info.source_header)
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
    , file_iterator(file_iterator_)
    , create_reader_pool(CurrentMetrics::ObjectStorageAzureThreads, CurrentMetrics::ObjectStorageAzureThreadsActive, 1)
    , create_reader_scheduler(threadPoolCallbackRunner<ReaderHolder>(create_reader_pool, "AzureReader"))
{
    reader = createReader();
    if (reader)
    {
        const auto & read_buf = reader.getReadBuffer();
        if (read_buf)
            total_objects_size = tryGetFileSizeFromReadBuffer(*reader.getReadBuffer()).value_or(0);

        reader_future = createReaderAsync();
    }
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
    auto [current_key, info] = file_iterator->next();
    if (current_key.empty())
        return {};

    size_t object_size = info.size_bytes != 0 ? info.size_bytes : object_storage->getObjectMetadata(current_key).size_bytes;
    auto compression_method = chooseCompressionMethod(current_key, compression_hint);

    auto read_buf = createAzureReadBuffer(current_key, object_size);
    auto input_format = FormatFactory::instance().getInput(
            format, *read_buf, sample_block, getContext(), max_block_size,
            format_settings, std::nullopt, std::nullopt,
            /* is_remote_fs */ true, compression_method);

    QueryPipelineBuilder builder;
    builder.init(Pipe(input_format));

    if (columns_desc.hasDefaults())
    {
        builder.addSimpleTransform(
            [&](const Block & header)
            { return std::make_shared<AddingDefaultsTransform>(header, columns_desc, *input_format, getContext()); });
    }

    /// Add ExtractColumnsTransform to extract requested columns/subcolumns
    /// from chunk read by IInputFormat.
    builder.addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<ExtractColumnsTransform>(header, requested_columns);
    });

    auto pipeline = std::make_unique<QueryPipeline>(QueryPipelineBuilder::getPipeline(std::move(builder)));
    auto current_reader = std::make_unique<PullingPipelineExecutor>(*pipeline);

    return ReaderHolder{fs::path(container) / current_key, std::move(read_buf), input_format, std::move(pipeline), std::move(current_reader)};
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

ColumnsDescription StorageAzureBlob::getTableStructureFromData(
    AzureObjectStorage * object_storage,
    const Configuration & configuration,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr ctx)
{
    RelativePathsWithMetadata read_keys;
    std::shared_ptr<StorageAzureBlobSource::IIterator> file_iterator;
    if (configuration.withGlobs())
    {
        file_iterator = std::make_shared<StorageAzureBlobSource::GlobIterator>(
            object_storage, configuration.container, configuration.blob_path, nullptr, Block{}, ctx, &read_keys);
    }
    else
    {
        file_iterator = std::make_shared<StorageAzureBlobSource::KeysIterator>(
            object_storage, configuration.container, configuration.blobs_paths, nullptr, Block{}, ctx, &read_keys);
    }

    std::optional<ColumnsDescription> columns_from_cache;
    size_t prev_read_keys_size = read_keys.size();
    if (ctx->getSettingsRef().schema_inference_use_cache_for_azure)
        columns_from_cache = tryGetColumnsFromCache(read_keys.begin(), read_keys.end(), configuration, format_settings, ctx);

    ReadBufferIterator read_buffer_iterator = [&, first = true](ColumnsDescription & cached_columns) mutable -> std::unique_ptr<ReadBuffer>
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

        /// S3 file iterator could get new keys after new iteration, check them in schema cache.
        if (ctx->getSettingsRef().schema_inference_use_cache_for_azure && read_keys.size() > prev_read_keys_size)
        {
            columns_from_cache = tryGetColumnsFromCache(read_keys.begin() + prev_read_keys_size, read_keys.end(), configuration, format_settings, ctx);
            prev_read_keys_size = read_keys.size();
            if (columns_from_cache)
            {
                cached_columns = *columns_from_cache;
                return nullptr;
            }
        }

        first = false;
        int zstd_window_log_max = static_cast<int>(ctx->getSettingsRef().zstd_window_log_max);
        return wrapReadBufferWithCompressionMethod(
            object_storage->readObject(StoredObject(key), ctx->getReadSettings(), {}, metadata.size_bytes),
            chooseCompressionMethod(key, configuration.compression_method),
            zstd_window_log_max);
    };

    ColumnsDescription columns;
    if (columns_from_cache)
        columns = *columns_from_cache;
    else
        columns = readSchemaFromFormat(configuration.format, format_settings, read_buffer_iterator, configuration.withGlobs(), ctx);

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
        auto get_last_mod_time = [&] -> time_t
        {
            return it->metadata.last_modified->epochTime();
        };

        auto host_and_bucket = configuration.connection_url + '/' + configuration.container;
        String source = host_and_bucket + '/' + it->relative_path;
        auto cache_key = getKeyForSchemaCache(source, configuration.format, format_settings, ctx);
        auto columns = schema_cache.tryGet(cache_key, get_last_mod_time);
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
    schema_cache.addMany(cache_keys, columns);
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
