#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Core/ColumnWithTypeAndName.h>

#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Parsers/ASTInsertQuery.h>
#include <Formats/ReadSchemaUtils.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Processors/Sources/NullSource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Transforms/ExtractColumnsTransform.h>

#include <Storages/StorageFactory.h>
#include <Storages/Cache/SchemaCache.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/ObjectStorage/StorageObjectStorageSink.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/ReadBufferIterator.h>


namespace DB
{
namespace Setting
{
    extern const SettingsMaxThreads max_threads;
    extern const SettingsBool optimize_count_from_files;
    extern const SettingsBool use_hive_partitioning;
}

namespace ErrorCodes
{
    extern const int DATABASE_ACCESS_DENIED;
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

String StorageObjectStorage::getPathSample(StorageInMemoryMetadata metadata, ContextPtr context)
{
    auto query_settings = configuration->getQuerySettings(context);
    /// We don't want to throw an exception if there are no files with specified path.
    query_settings.throw_on_zero_files_match = false;

    bool local_distributed_processing = distributed_processing;
    if (context->getSettingsRef()[Setting::use_hive_partitioning])
        local_distributed_processing = false;

    auto file_iterator = StorageObjectStorageSource::createFileIterator(
        configuration,
        query_settings,
        object_storage,
        local_distributed_processing,
        context,
        {}, // predicate
        metadata.getColumns().getAll(), // virtual_columns
        nullptr, // read_keys
        {} // file_progress_callback
    );

    if (auto file = file_iterator->next(0))
        return file->getPath();
    return "";
}

StorageObjectStorage::StorageObjectStorage(
    ConfigurationPtr configuration_,
    ObjectStoragePtr object_storage_,
    ContextPtr context,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    std::optional<FormatSettings> format_settings_,
    bool distributed_processing_,
    ASTPtr partition_by_)
    : IStorage(table_id_)
    , configuration(configuration_)
    , object_storage(object_storage_)
    , format_settings(format_settings_)
    , partition_by(partition_by_)
    , distributed_processing(distributed_processing_)
    , log(getLogger(fmt::format("Storage{}({})", configuration->getEngineName(), table_id_.getFullTableName())))
{
    ColumnsDescription columns{columns_};

    std::string sample_path;
    resolveSchemaAndFormat(columns, configuration->format, object_storage, configuration, format_settings, sample_path, context);
    configuration->check(context);

    StorageInMemoryMetadata metadata;
    metadata.setColumns(columns);
    metadata.setConstraints(constraints_);
    metadata.setComment(comment);

    if (sample_path.empty() && context->getSettingsRef()[Setting::use_hive_partitioning])
        sample_path = getPathSample(metadata, context);

    setVirtuals(VirtualColumnUtils::getVirtualsForFileLikeStorage(metadata.columns, context, sample_path, format_settings));
    setInMemoryMetadata(metadata);
}

String StorageObjectStorage::getName() const
{
    return configuration->getEngineName();
}

bool StorageObjectStorage::prefersLargeBlocks() const
{
    return FormatFactory::instance().checkIfOutputFormatPrefersLargeBlocks(configuration->format);
}

bool StorageObjectStorage::parallelizeOutputAfterReading(ContextPtr context) const
{
    return FormatFactory::instance().checkParallelizeOutputAfterReading(configuration->format, context);
}

bool StorageObjectStorage::supportsSubsetOfColumns(const ContextPtr & context) const
{
    return FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(configuration->format, context, format_settings);
}

void StorageObjectStorage::updateConfiguration(ContextPtr context)
{
    IObjectStorage::ApplyNewSettingsOptions options{ .allow_client_change = !configuration->isStaticConfiguration() };
    object_storage->applyNewSettings(context->getConfigRef(), configuration->getTypeName() + ".", context, options);
}

namespace
{
class ReadFromObjectStorageStep : public SourceStepWithFilter
{
public:
    using ConfigurationPtr = StorageObjectStorage::ConfigurationPtr;

    ReadFromObjectStorageStep(
        ObjectStoragePtr object_storage_,
        ConfigurationPtr configuration_,
        const String & name_,
        const Names & columns_to_read,
        const NamesAndTypesList & virtual_columns_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const std::optional<DB::FormatSettings> & format_settings_,
        bool distributed_processing_,
        ReadFromFormatInfo info_,
        const bool need_only_count_,
        ContextPtr context_,
        size_t max_block_size_,
        size_t num_streams_)
        : SourceStepWithFilter(info_.source_header, columns_to_read, query_info_, storage_snapshot_, context_)
        , object_storage(object_storage_)
        , configuration(configuration_)
        , info(std::move(info_))
        , virtual_columns(virtual_columns_)
        , format_settings(format_settings_)
        , name(name_ + "Source")
        , need_only_count(need_only_count_)
        , max_block_size(max_block_size_)
        , num_streams(num_streams_)
        , distributed_processing(distributed_processing_)
    {
    }

    std::string getName() const override { return name; }

    void applyFilters(ActionDAGNodes added_filter_nodes) override
    {
        SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));
        const ActionsDAG::Node * predicate = nullptr;
        if (filter_actions_dag)
            predicate = filter_actions_dag->getOutputs().at(0);
        createIterator(predicate);
    }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        createIterator(nullptr);

        Pipes pipes;
        auto context = getContext();
        const size_t max_threads = context->getSettingsRef()[Setting::max_threads];
        size_t estimated_keys_count = iterator_wrapper->estimatedKeysCount();

        if (estimated_keys_count > 1)
            num_streams = std::min(num_streams, estimated_keys_count);
        else
        {
            /// The amount of keys (zero) was probably underestimated.
            /// We will keep one stream for this particular case.
            num_streams = 1;
        }

        const size_t max_parsing_threads = num_streams >= max_threads ? 1 : (max_threads / std::max(num_streams, 1ul));

        for (size_t i = 0; i < num_streams; ++i)
        {
            auto source = std::make_shared<StorageObjectStorageSource>(
                getName(), object_storage, configuration, info, format_settings,
                context, max_block_size, iterator_wrapper, max_parsing_threads, need_only_count);

            source->setKeyCondition(filter_actions_dag, context);
            pipes.emplace_back(std::move(source));
        }

        auto pipe = Pipe::unitePipes(std::move(pipes));
        if (pipe.empty())
            pipe = Pipe(std::make_shared<NullSource>(info.source_header));

        for (const auto & processor : pipe.getProcessors())
            processors.emplace_back(processor);

        pipeline.init(std::move(pipe));
    }

private:
    ObjectStoragePtr object_storage;
    ConfigurationPtr configuration;
    std::shared_ptr<StorageObjectStorageSource::IIterator> iterator_wrapper;

    const ReadFromFormatInfo info;
    const NamesAndTypesList virtual_columns;
    const std::optional<DB::FormatSettings> format_settings;
    const String name;
    const bool need_only_count;
    const size_t max_block_size;
    size_t num_streams;
    const bool distributed_processing;

    void createIterator(const ActionsDAG::Node * predicate)
    {
        if (iterator_wrapper)
            return;
        auto context = getContext();
        iterator_wrapper = StorageObjectStorageSource::createFileIterator(
            configuration, configuration->getQuerySettings(context), object_storage, distributed_processing,
            context, predicate, virtual_columns, nullptr, context->getFileProgressCallback());
    }
};
}

ReadFromFormatInfo StorageObjectStorage::prepareReadingFromFormat(
    const Strings & requested_columns,
    const StorageSnapshotPtr & storage_snapshot,
    bool supports_subset_of_columns,
    ContextPtr local_context)
{
    return DB::prepareReadingFromFormat(requested_columns, storage_snapshot, local_context, supports_subset_of_columns);
}

void StorageObjectStorage::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    updateConfiguration(local_context);
    if (partition_by && configuration->withPartitionWildcard())
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Reading from a partitioned {} storage is not implemented yet",
                        getName());
    }

    const auto read_from_format_info = prepareReadingFromFormat(
        column_names, storage_snapshot, supportsSubsetOfColumns(local_context), local_context);
    const bool need_only_count = (query_info.optimize_trivial_count || read_from_format_info.requested_columns.empty())
        && local_context->getSettingsRef()[Setting::optimize_count_from_files];

    auto read_step = std::make_unique<ReadFromObjectStorageStep>(
        object_storage,
        configuration,
        getName(),
        column_names,
        getVirtualsList(),
        query_info,
        storage_snapshot,
        format_settings,
        distributed_processing,
        read_from_format_info,
        need_only_count,
        local_context,
        max_block_size,
        num_streams);

    query_plan.addStep(std::move(read_step));
}

SinkToStoragePtr StorageObjectStorage::write(
    const ASTPtr & query,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr local_context,
    bool /* async_insert */)
{
    updateConfiguration(local_context);
    const auto sample_block = metadata_snapshot->getSampleBlock();
    const auto & settings = configuration->getQuerySettings(local_context);

    if (configuration->isArchive())
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Path '{}' contains archive. Write into archive is not supported",
                        configuration->getPath());
    }

    if (configuration->withGlobsIgnorePartitionWildcard())
    {
        throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED,
                        "Path '{}' contains globs, so the table is in readonly mode",
                        configuration->getPath());
    }

    if (configuration->withPartitionWildcard())
    {
        ASTPtr partition_by_ast = nullptr;
        if (auto insert_query = std::dynamic_pointer_cast<ASTInsertQuery>(query))
        {
            if (insert_query->partition_by)
                partition_by_ast = insert_query->partition_by;
            else
                partition_by_ast = partition_by;
        }

        if (partition_by_ast)
        {
            return std::make_shared<PartitionedStorageObjectStorageSink>(
                object_storage, configuration, format_settings, sample_block, local_context, partition_by_ast);
        }
    }

    auto paths = configuration->getPaths();
    if (auto new_key = checkAndGetNewFileOnInsertIfNeeded(
            *object_storage, *configuration, settings, paths.front(), paths.size()))
    {
        paths.push_back(*new_key);
    }
    configuration->setPaths(paths);

    return std::make_shared<StorageObjectStorageSink>(
        object_storage,
        configuration->clone(),
        format_settings,
        sample_block,
        local_context);
}

void StorageObjectStorage::truncate(
    const ASTPtr & /* query */,
    const StorageMetadataPtr & /* metadata_snapshot */,
    ContextPtr /* context */,
    TableExclusiveLockHolder & /* table_holder */)
{
    if (configuration->isArchive())
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Path '{}' contains archive. Table cannot be truncated",
                        configuration->getPath());
    }

    if (configuration->withGlobs())
    {
        throw Exception(
            ErrorCodes::DATABASE_ACCESS_DENIED,
            "{} key '{}' contains globs, so the table is in readonly mode and cannot be truncated",
            getName(), configuration->getPath());
    }

    StoredObjects objects;
    for (const auto & key : configuration->getPaths())
        objects.emplace_back(key);

    object_storage->removeObjectsIfExist(objects);
}

std::unique_ptr<ReadBufferIterator> StorageObjectStorage::createReadBufferIterator(
    const ObjectStoragePtr & object_storage,
    const ConfigurationPtr & configuration,
    const std::optional<FormatSettings> & format_settings,
    ObjectInfos & read_keys,
    const ContextPtr & context)
{
    auto file_iterator = StorageObjectStorageSource::createFileIterator(
        configuration,
        configuration->getQuerySettings(context),
        object_storage,
        false/* distributed_processing */,
        context,
        {}/* predicate */,
        {}/* virtual_columns */,
        &read_keys);

    return std::make_unique<ReadBufferIterator>(
        object_storage, configuration, file_iterator,
        format_settings, getSchemaCache(context, configuration->getTypeName()), read_keys, context);
}

ColumnsDescription StorageObjectStorage::resolveSchemaFromData(
    const ObjectStoragePtr & object_storage,
    const ConfigurationPtr & configuration,
    const std::optional<FormatSettings> & format_settings,
    std::string & sample_path,
    const ContextPtr & context)
{
    ObjectInfos read_keys;
    auto iterator = createReadBufferIterator(object_storage, configuration, format_settings, read_keys, context);
    auto schema = readSchemaFromFormat(configuration->format, format_settings, *iterator, context);
    sample_path = iterator->getLastFilePath();
    return schema;
}

std::string StorageObjectStorage::resolveFormatFromData(
    const ObjectStoragePtr & object_storage,
    const ConfigurationPtr & configuration,
    const std::optional<FormatSettings> & format_settings,
    std::string & sample_path,
    const ContextPtr & context)
{
    ObjectInfos read_keys;
    auto iterator = createReadBufferIterator(object_storage, configuration, format_settings, read_keys, context);
    auto format_and_schema = detectFormatAndReadSchema(format_settings, *iterator, context).second;
    sample_path = iterator->getLastFilePath();
    return format_and_schema;
}

std::pair<ColumnsDescription, std::string> StorageObjectStorage::resolveSchemaAndFormatFromData(
    const ObjectStoragePtr & object_storage,
    const ConfigurationPtr & configuration,
    const std::optional<FormatSettings> & format_settings,
    std::string & sample_path,
    const ContextPtr & context)
{
    ObjectInfos read_keys;
    auto iterator = createReadBufferIterator(object_storage, configuration, format_settings, read_keys, context);
    auto [columns, format] = detectFormatAndReadSchema(format_settings, *iterator, context);
    sample_path = iterator->getLastFilePath();
    configuration->format = format;
    return std::pair(columns, format);
}

SchemaCache & StorageObjectStorage::getSchemaCache(const ContextPtr & context, const std::string & storage_type_name)
{
    if (storage_type_name == "s3")
    {
        static SchemaCache schema_cache(
            context->getConfigRef().getUInt(
                "schema_inference_cache_max_elements_for_s3",
                DEFAULT_SCHEMA_CACHE_ELEMENTS));
        return schema_cache;
    }
    if (storage_type_name == "hdfs")
    {
        static SchemaCache schema_cache(
            context->getConfigRef().getUInt("schema_inference_cache_max_elements_for_hdfs", DEFAULT_SCHEMA_CACHE_ELEMENTS));
        return schema_cache;
    }
    if (storage_type_name == "azure")
    {
        static SchemaCache schema_cache(
            context->getConfigRef().getUInt("schema_inference_cache_max_elements_for_azure", DEFAULT_SCHEMA_CACHE_ELEMENTS));
        return schema_cache;
    }
    if (storage_type_name == "local")
    {
        static SchemaCache schema_cache(
            context->getConfigRef().getUInt("schema_inference_cache_max_elements_for_local", DEFAULT_SCHEMA_CACHE_ELEMENTS));
        return schema_cache;
    }
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported storage type: {}", storage_type_name);
}

void StorageObjectStorage::Configuration::initialize(
    Configuration & configuration,
    ASTs & engine_args,
    ContextPtr local_context,
    bool with_table_structure)
{
    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, local_context))
        configuration.fromNamedCollection(*named_collection, local_context);
    else
        configuration.fromAST(engine_args, local_context, with_table_structure);

    if (configuration.format == "auto")
    {
        configuration.format = FormatFactory::instance().tryGetFormatFromFileName(
            configuration.isArchive()
            ? configuration.getPathInArchive()
            : configuration.getPath()).value_or("auto");
    }
    else
        FormatFactory::instance().checkFormatName(configuration.format);

    configuration.initialized = true;
}

void StorageObjectStorage::Configuration::check(ContextPtr) const
{
    FormatFactory::instance().checkFormatName(format);
}

StorageObjectStorage::Configuration::Configuration(const Configuration & other)
{
    format = other.format;
    compression_method = other.compression_method;
    structure = other.structure;
    partition_columns = other.partition_columns;
}

bool StorageObjectStorage::Configuration::withPartitionWildcard() const
{
    static const String PARTITION_ID_WILDCARD = "{_partition_id}";
    return getPath().find(PARTITION_ID_WILDCARD) != String::npos
        || getNamespace().find(PARTITION_ID_WILDCARD) != String::npos;
}

bool StorageObjectStorage::Configuration::withGlobsIgnorePartitionWildcard() const
{
    if (!withPartitionWildcard())
        return withGlobs();
    return PartitionedSink::replaceWildcards(getPath(), "").find_first_of("*?{") != std::string::npos;
}

bool StorageObjectStorage::Configuration::isPathWithGlobs() const
{
    return getPath().find_first_of("*?{") != std::string::npos;
}

bool StorageObjectStorage::Configuration::isNamespaceWithGlobs() const
{
    return getNamespace().find_first_of("*?{") != std::string::npos;
}

std::string StorageObjectStorage::Configuration::getPathWithoutGlobs() const
{
    return getPath().substr(0, getPath().find_first_of("*?{"));
}

bool StorageObjectStorage::Configuration::isPathInArchiveWithGlobs() const
{
    return getPathInArchive().find_first_of("*?{") != std::string::npos;
}

std::string StorageObjectStorage::Configuration::getPathInArchive() const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Path {} is not archive", getPath());
}

void StorageObjectStorage::Configuration::assertInitialized() const
{
    if (!initialized)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Configuration was not initialized before usage");
    }
}
}
