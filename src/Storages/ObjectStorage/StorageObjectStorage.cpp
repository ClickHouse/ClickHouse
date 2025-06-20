#include <Core/ColumnWithTypeAndName.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>

#include <Common/logger_useful.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Parsers/ASTInsertQuery.h>
#include <Formats/ReadSchemaUtils.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Interpreters/Context.h>

#include <Processors/Sources/NullSource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromObjectStorageStep.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Transforms/ExtractColumnsTransform.h>

#include <Storages/Cache/SchemaCache.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/ObjectStorage/ReadBufferIterator.h>
#include <Storages/ObjectStorage/StorageObjectStorageSink.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Storages/StorageFactory.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/parseGlobs.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/HivePartitioningUtils.h>
#include <Storages/ObjectStorage/StorageObjectStorageSettings.h>

#include <Poco/Logger.h>

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
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
}

String StorageObjectStorage::getPathSample(ContextPtr context)
{
    auto query_settings = configuration->getQuerySettings(context);
    /// We don't want to throw an exception if there are no files with specified path.
    query_settings.throw_on_zero_files_match = false;
    query_settings.ignore_non_existent_file = true;

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
        {},
        {}, // virtual_columns
        {}, /* hive_columns */
        nullptr, // read_keys
        {} // file_progress_callback
    );

    const auto path = configuration->getRawPath();

    if (!configuration->isArchive() && !path.withGlobs() && !local_distributed_processing)
        return path.path;

    if (auto file = file_iterator->next(0))
        return file->getPath();
    return "";
}

StorageObjectStorage::StorageObjectStorage(
    ConfigurationPtr configuration_,
    ObjectStoragePtr object_storage_,
    ContextPtr context,
    const StorageID & table_id_,
    const ColumnsDescription & columns_in_table_or_function_definition,
    const ConstraintsDescription & constraints_,
    const String & comment,
    std::optional<FormatSettings> format_settings_,
    LoadingStrictnessLevel mode,
    bool distributed_processing_,
    ASTPtr partition_by_,
    bool is_table_function,
    bool lazy_init)
    : IStorage(table_id_)
    , configuration(configuration_)
    , object_storage(object_storage_)
    , format_settings(format_settings_)
    , distributed_processing(distributed_processing_)
    , log(getLogger(fmt::format("Storage{}({})", configuration->getEngineName(), table_id_.getFullTableName())))
{
    configuration->initPartitionStrategy(partition_by_, columns_in_table_or_function_definition, context);

    const bool need_resolve_columns_or_format = columns_in_table_or_function_definition.empty() || (configuration->format == "auto");
    const bool need_resolve_sample_path = context->getSettingsRef()[Setting::use_hive_partitioning]
        && !configuration->partition_strategy
        && !configuration->isDataLakeConfiguration();
    const bool do_lazy_init = lazy_init && !need_resolve_columns_or_format && !need_resolve_sample_path;

    bool updated_configuration = false;
    try
    {
        if (!do_lazy_init)
        {
            configuration->update(
                object_storage,
                context,
                /* if_not_updated_before */is_table_function,
                /* check_consistent_with_previous_metadata */true);

            updated_configuration = true;
        }
    }
    catch (...)
    {
        // If we don't have format or schema yet, we can't ignore failed configuration update,
        // because relevant configuration is crucial for format and schema inference
        if (mode <= LoadingStrictnessLevel::CREATE || need_resolve_columns_or_format)
        {
            throw;
        }
        tryLogCurrentException(log);
    }

    /// We always update configuration on read for table engine,
    /// but this is not needed for table function,
    /// which exists only for the duration of a single query
    /// (e.g. read always follows constructor immediately).
    update_configuration_on_read_write = !is_table_function || !updated_configuration;

    std::string sample_path;

    ColumnsDescription columns{columns_in_table_or_function_definition};
    if (need_resolve_columns_or_format)
        resolveSchemaAndFormat(columns, configuration->format, object_storage, configuration, format_settings, sample_path, context);
    else
        validateSupportedColumns(columns, *configuration);

    configuration->check(context);

    /// FIXME: We need to call getPathSample() lazily on select
    /// in case it failed to be initialized in constructor.
    if (updated_configuration && sample_path.empty() && need_resolve_sample_path && !configuration->partition_strategy)
    {
        try
        {
            sample_path = getPathSample(context);
        }
        catch (...)
        {
            LOG_WARNING(
                log,
                "Failed to list object storage, cannot use hive partitioning. "
                "Error: {}",
                getCurrentExceptionMessage(true));
        }
    }

    /*
     * If `partition_strategy=hive`, the partition columns shall be extracted from the `PARTITION BY` expression.
     * There is no need to read from the filepath.
     *
     * Otherwise, in case `use_hive_partitioning=1`, we can keep the old behavior of extracting it from the sample path.
     * And if the schema was inferred (not specified in the table definition), we need to enrich it with the path partition columns
     */
    if (configuration->partition_strategy && configuration->partition_strategy_type == PartitionStrategyFactory::StrategyType::HIVE)
    {
        hive_partition_columns_to_read_from_file_path = configuration->partition_strategy->getPartitionColumns();
    }
    else if (context->getSettingsRef()[Setting::use_hive_partitioning])
    {
        hive_partition_columns_to_read_from_file_path = HivePartitioningUtils::extractHivePartitionColumnsFromPath(columns, sample_path, format_settings, context);

        if (columns_in_table_or_function_definition.empty())
        {
            for (const auto & [name, type]: hive_partition_columns_to_read_from_file_path)
            {
                if (!columns.has(name))
                {
                    columns.add({name, type});
                }
            }
        }
    }

    if (hive_partition_columns_to_read_from_file_path.size() == columns.size())
    {
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "A hive partitioned file can't contain only partition columns. Try reading it with `partition_strategy=wildcard` and `use_hive_partitioning=0`");
    }

    if (configuration->partition_columns_in_data_file)
    {
        file_columns = columns;
    }
    else
    {
        std::unordered_set<String> hive_partition_columns_to_read_from_file_path_set;

        for (const auto & [name, type] : hive_partition_columns_to_read_from_file_path)
        {
            hive_partition_columns_to_read_from_file_path_set.insert(name);
        }

        for (const auto & [name, type] : columns.getAllPhysical())
        {
            if (!hive_partition_columns_to_read_from_file_path_set.contains(name))
            {
                file_columns.add({name, type});
            }
        }
    }

    // Assert file contains at least one column. The assertion only takes place if we were able to deduce the schema. The storage might be empty.
    if (!columns.empty())
    {
        if (file_columns.empty())
        {
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "File without physical columns is not supported. Give it a try with `use_hive_partitioning=0` and or `partition_strategy=wildcard`. File {}",
                sample_path);
        }
    }

    // todo arthur perhaps set partition key description?
    StorageInMemoryMetadata metadata;
    metadata.setColumns(columns);
    metadata.setConstraints(constraints_);
    metadata.setComment(comment);

    setVirtuals(VirtualColumnUtils::getVirtualsForFileLikeStorage(metadata.columns));
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

bool StorageObjectStorage::Configuration::update( ///NOLINT
    ObjectStoragePtr object_storage_ptr,
    ContextPtr context,
    bool /* if_not_updated_before */,
    bool /* check_consistent_with_previous_metadata */)
{
    IObjectStorage::ApplyNewSettingsOptions options{.allow_client_change = !isStaticConfiguration()};
    object_storage_ptr->applyNewSettings(context->getConfigRef(), getTypeName() + ".", context, options);
    return true;
}

IDataLakeMetadata * StorageObjectStorage::getExternalMetadata(ContextPtr query_context)
{
    configuration->update(
        object_storage,
        query_context,
        /* if_not_updated_before */false,
        /* check_consistent_with_previous_metadata */false);

    return configuration->getExternalMetadata();
}

bool StorageObjectStorage::updateExternalDynamicMetadataIfExists(ContextPtr query_context)
{
    bool updated = configuration->update(
        object_storage,
        query_context,
        /* if_not_updated_before */true,
        /* check_consistent_with_previous_metadata */false);

    if (!configuration->hasExternalDynamicMetadata())
        return false;

    if (!updated)
    {
        /// Force the update.
        configuration->update(
            object_storage,
            query_context,
            /* if_not_updated_before */false,
            /* check_consistent_with_previous_metadata */false);
    }

    auto columns = configuration->tryGetTableStructureFromMetadata();
    if (!columns.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No schema in table metadata");

    StorageInMemoryMetadata metadata;
    metadata.setColumns(std::move(columns.value()));
    setInMemoryMetadata(metadata);
    return true;
}

std::optional<UInt64> StorageObjectStorage::totalRows(ContextPtr query_context) const
{
    configuration->update(
        object_storage,
        query_context,
        /* if_not_updated_before */false,
        /* check_consistent_with_previous_metadata */true);

    return configuration->totalRows(query_context);
}

std::optional<UInt64> StorageObjectStorage::totalBytes(ContextPtr query_context) const
{
    configuration->update(
        object_storage,
        query_context,
        /* if_not_updated_before */false,
        /* check_consistent_with_previous_metadata */true);

    return configuration->totalBytes(query_context);
}

void StorageObjectStorage::Configuration::initPartitionStrategy(ASTPtr partition_by, const ColumnsDescription & columns, ContextPtr context)
{

    partition_strategy = PartitionStrategyFactory::get(
        partition_strategy_type,
        partition_by,
        columns.getOrdinary(),
        context,
        format,
        getRawPath().withGlobs(),
        partition_columns_in_data_file);
}

ReadFromFormatInfo StorageObjectStorage::Configuration::prepareReadingFromFormat(
    ObjectStoragePtr,
    const Strings & requested_columns,
    const StorageSnapshotPtr & storage_snapshot,
    bool supports_subset_of_columns,
    ContextPtr local_context,
    const PrepareReadingFromFormatHiveParams & hive_parameters)
{
    return DB::prepareReadingFromFormat(
        requested_columns,
        storage_snapshot,
        local_context,
        supports_subset_of_columns,
        hive_parameters);
}

std::optional<ColumnsDescription> StorageObjectStorage::Configuration::tryGetTableStructureFromMetadata() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method tryGetTableStructureFromMetadata is not implemented for basic configuration");
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
    /// We did configuration->update() in constructor,
    /// so in case of table function there is no need to do the same here again.
    if (update_configuration_on_read_write)
    {
        configuration->update(
            object_storage,
            local_context,
            /* if_not_updated_before */false,
            /* check_consistent_with_previous_metadata */true);
    }

    if (configuration->partition_strategy && configuration->partition_strategy_type != PartitionStrategyFactory::StrategyType::HIVE)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Reading from a partitioned {} storage is not implemented yet",
                        getName());
    }

    auto all_file_columns = file_columns.getAll();

    auto read_from_format_info = configuration->prepareReadingFromFormat(
        object_storage,
        column_names,
        storage_snapshot,
        supportsSubsetOfColumns(local_context),
        local_context,
        PrepareReadingFromFormatHiveParams { all_file_columns, hive_partition_columns_to_read_from_file_path.getNameToPairMap() });

    const bool need_only_count = (query_info.optimize_trivial_count || read_from_format_info.requested_columns.empty())
                                 && local_context->getSettingsRef()[Setting::optimize_count_from_files];

    auto modified_format_settings{format_settings};
    if (!modified_format_settings.has_value())
        modified_format_settings.emplace(getFormatSettings(local_context));

    configuration->modifyFormatSettings(modified_format_settings.value());

    auto read_step = std::make_unique<ReadFromObjectStorageStep>(
        object_storage,
        configuration,
        fmt::format("{}({})", getName(), getStorageID().getFullTableName()),
        column_names,
        getVirtualsList(),
        query_info,
        storage_snapshot,
        modified_format_settings,
        distributed_processing,
        read_from_format_info,
        need_only_count,
        local_context,
        max_block_size,
        num_streams);

    query_plan.addStep(std::move(read_step));
}

SinkToStoragePtr StorageObjectStorage::write(
    const ASTPtr &,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr local_context,
    bool /* async_insert */)
{
    if (update_configuration_on_read_write)
    {
        configuration->update(
            object_storage,
            local_context,
            /* if_not_updated_before */false,
            /* check_consistent_with_previous_metadata */true);
    }

    const auto sample_block = metadata_snapshot->getSampleBlock();
    const auto & settings = configuration->getQuerySettings(local_context);

    const auto raw_path = configuration->getRawPath();

    if (configuration->isArchive())
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Path '{}' contains archive. Write into archive is not supported",
                        raw_path.path);
    }

    if (raw_path.withGlobsIgnorePartitionWildcard())
    {
        throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED,
                        "Non partitioned table with path '{}' that contains globs, the table is in readonly mode",
                        configuration->getRawPath().path);
    }

    if (!configuration->supportsWrites())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Writes are not supported for engine");

    if (configuration->partition_strategy)
    {
        return std::make_shared<PartitionedStorageObjectStorageSink>(object_storage, configuration, format_settings, sample_block, local_context);
    }

    auto paths = configuration->getPaths();
    if (auto new_key = checkAndGetNewFileOnInsertIfNeeded(*object_storage, *configuration, settings, paths.front().path, paths.size()))
    {
        paths.push_back({*new_key});
    }
    configuration->setPaths(paths);

    return std::make_shared<StorageObjectStorageSink>(
        paths.back().path,
        object_storage,
        configuration,
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
    const auto path = configuration->getRawPath();

    if (configuration->isArchive())
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Path '{}' contains archive. Table cannot be truncated",
                        path.path);
    }

    if (path.withGlobs())
    {
        throw Exception(
            ErrorCodes::DATABASE_ACCESS_DENIED,
            "{} key '{}' contains globs, so the table is in readonly mode and cannot be truncated",
            getName(), path.path);
    }

    StoredObjects objects;
    for (const auto & key : configuration->getPaths())
        objects.emplace_back(key.path);

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
        {},
        {}/* virtual_columns */,
        {}, /* hive_columns */
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

void StorageObjectStorage::addInferredEngineArgsToCreateQuery(ASTs & args, const ContextPtr & context) const
{
    configuration->addStructureAndFormatToArgsIfNeeded(args, "", configuration->format, context, /*with_structure=*/false);
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
    Configuration & configuration_to_initialize,
    ASTs & engine_args,
    ContextPtr local_context,
    bool with_table_structure)
{
    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, local_context))
        configuration_to_initialize.fromNamedCollection(*named_collection, local_context);
    else
        configuration_to_initialize.fromAST(engine_args, local_context, with_table_structure);

    const auto raw_path = configuration_to_initialize.getRawPath();

    if (configuration_to_initialize.isNamespaceWithGlobs())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Expression can not have wildcards inside {} name", configuration_to_initialize.getNamespaceType());

    if (configuration_to_initialize.format == "auto")
    {
        if (configuration_to_initialize.isDataLakeConfiguration())
        {
            configuration_to_initialize.format = "Parquet";
        }
        else
        {
            configuration_to_initialize.format
                = FormatFactory::instance()
                      .tryGetFormatFromFileName(configuration_to_initialize.isArchive() ? configuration_to_initialize.getPathInArchive() : raw_path.path)
                      .value_or("auto");
        }
    }
    else
        FormatFactory::instance().checkFormatName(configuration_to_initialize.format);

    configuration_to_initialize.initialized = true;
}

void StorageObjectStorage::Configuration::check(ContextPtr) const
{
    FormatFactory::instance().checkFormatName(format);
}

StorageObjectStorage::Configuration::Path StorageObjectStorage::Configuration::getPathForRead() const
{
    auto raw_path = getRawPath();

    if (!partition_strategy)
    {
        return raw_path;
    }

    return Path {partition_strategy->getPathForRead(raw_path.path)};
}

StorageObjectStorage::Configuration::Path StorageObjectStorage::Configuration::getPathForWrite(const std::string & partition_id) const
{
    auto raw_path = getRawPath();

    if (!partition_strategy)
    {
        return raw_path;
    }

    return Path {partition_strategy->getPathForWrite(raw_path.path, partition_id)};
}

bool StorageObjectStorage::Configuration::Path::withPartitionWildcard() const
{
    static const String PARTITION_ID_WILDCARD = "{_partition_id}";
    return path.find(PARTITION_ID_WILDCARD) != String::npos;
}

bool StorageObjectStorage::Configuration::Path::withGlobsIgnorePartitionWildcard() const
{
    if (!withPartitionWildcard())
        return withGlobs();
    return PartitionedSink::replaceWildcards(path, "").find_first_of("*?{") != std::string::npos;
}

bool StorageObjectStorage::Configuration::Path::withGlobs() const
{
    return path.find_first_of("*?{") != std::string::npos;
}

std::string StorageObjectStorage::Configuration::Path::getWithoutGlobs() const
{
    if (allow_partial_prefix)
    {
        return path.substr(0, path.find_first_of("*?{"));
    }

    auto first_glob_pos = path.find_first_of("*?{");
    auto end_of_path_without_globs = path.substr(0, first_glob_pos).rfind('/');
    if (end_of_path_without_globs == std::string::npos || end_of_path_without_globs == 0)
        return "/";
    return path.substr(0, end_of_path_without_globs);
}

bool StorageObjectStorage::Configuration::isNamespaceWithGlobs() const
{
    return getNamespace().find_first_of("*?{") != std::string::npos;
}

bool StorageObjectStorage::Configuration::isPathInArchiveWithGlobs() const
{
    return getPathInArchive().find_first_of("*?{") != std::string::npos;
}

std::string StorageObjectStorage::Configuration::getPathInArchive() const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Path {} is not archive", getRawPath().path);
}

void StorageObjectStorage::Configuration::assertInitialized() const
{
    if (!initialized)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Configuration was not initialized before usage");
    }
}

}
