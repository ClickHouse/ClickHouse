#include <Storages/ObjectStorage/StorageObjectStorage.h>

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Core/LogsLevel.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromObjectStorageStep.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Executors/PullingPipelineExecutor.h>

#include <Storages/Cache/SchemaCache.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/ObjectStorage/ReadBufferIterator.h>
#include <Storages/ObjectStorage/StorageObjectStorageSink.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Storages/StorageFactory.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/ReadFromTableChangesStep.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableChanges.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadataDeltaKernel.h>
#include <Interpreters/StorageID.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Databases/DataLake/Common.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/HivePartitioningUtils.h>
#include <Storages/ObjectStorage/StorageObjectStorageSettings.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_count_from_files;
    extern const SettingsBool use_hive_partitioning;
    extern const SettingsInt64 delta_lake_snapshot_start_version;
    extern const SettingsInt64 delta_lake_snapshot_end_version;
    extern const SettingsUInt64 max_streams_for_files_processing_in_cluster_functions;
}

namespace ErrorCodes
{
    extern const int DATABASE_ACCESS_DENIED;
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DATA;
    extern const int BAD_ARGUMENTS;
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
        nullptr, // storage_metadata
        local_distributed_processing,
        context,
        {}, // predicate
        {},
        {}, // virtual_columns
        {}, // hive_columns
        nullptr, // read_keys
        {} // file_progress_callback
    );

    const auto path = configuration->getRawPath();

    if (!configuration->isArchive() && !path.hasGlobs() && !local_distributed_processing)
        return path.path;

    if (auto file = file_iterator->next(0))
        return file->getPath();
    return "";
}

StorageObjectStorage::StorageObjectStorage(
    StorageObjectStorageConfigurationPtr configuration_,
    ObjectStoragePtr object_storage_,
    ContextPtr context,
    const StorageID & table_id_,
    const ColumnsDescription & columns_in_table_or_function_definition,
    const ConstraintsDescription & constraints_,
    const String & comment,
    std::optional<FormatSettings> format_settings_,
    LoadingStrictnessLevel mode,
    std::shared_ptr<DataLake::ICatalog> catalog_,
    bool if_not_exists_,
    bool is_datalake_query,
    bool distributed_processing_,
    ASTPtr partition_by_,
    ASTPtr order_by_,
    bool is_table_function_,
    bool lazy_init)
    : IStorage(table_id_)
    , configuration(configuration_)
    , object_storage(object_storage_)
    , format_settings(format_settings_)
    , distributed_processing(distributed_processing_)
    , is_table_function(is_table_function_)
    , log(getLogger(fmt::format("Storage{}({})", configuration->getEngineName(), table_id_.getFullTableName())))
    , catalog(catalog_)
    , storage_id(table_id_)
{
    configuration->initPartitionStrategy(partition_by_, columns_in_table_or_function_definition, context);
    const bool need_resolve_columns_or_format = columns_in_table_or_function_definition.empty() || (configuration->format == "auto");
    const bool need_resolve_sample_path = context->getSettingsRef()[Setting::use_hive_partitioning]
        && !configuration->partition_strategy
        && !configuration->isDataLakeConfiguration();
    const bool do_lazy_init = lazy_init && !need_resolve_columns_or_format && !need_resolve_sample_path;

    LOG_DEBUG(
        log, "StorageObjectStorage: lazy_init={}, need_resolve_columns_or_format={}, "
        "need_resolve_sample_path={}, is_table_function={}, is_datalake_query={}, columns_in_table_or_function_definition={}",
        lazy_init, need_resolve_columns_or_format, need_resolve_sample_path, is_table_function,
        is_datalake_query, columns_in_table_or_function_definition.toString(true));

    bool is_delta_lake_cdf = context->getSettingsRef()[Setting::delta_lake_snapshot_start_version] != -1
            || context->getSettingsRef()[Setting::delta_lake_snapshot_start_version] != -1;

    if (!is_table_function && is_delta_lake_cdf)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Delta lake CDF is allowed only for deltaLake table function");
    }

    if (!is_table_function && !columns_in_table_or_function_definition.empty() && !is_datalake_query && mode == LoadingStrictnessLevel::CREATE)
    {
        LOG_DEBUG(log, "Creating new storage with specified columns");
        configuration->create(
            object_storage, context, columns_in_table_or_function_definition, partition_by_, order_by_, if_not_exists_, catalog, storage_id);
    }

    bool updated_configuration = false;
    try
    {
        if (!do_lazy_init)
        {
            configuration->update(
                object_storage,
                context,
                /* if_not_updated_before */ is_table_function);
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
        tryLogCurrentException(log, /*start of message = */ "", LogsLevel::warning);
    }

    std::string sample_path;

    ColumnsDescription columns{columns_in_table_or_function_definition};
    if (need_resolve_columns_or_format)
        resolveSchemaAndFormat(columns, configuration->format, object_storage, configuration, format_settings, sample_path, context);

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

    std::tie(hive_partition_columns_to_read_from_file_path, file_columns) = HivePartitioningUtils::setupHivePartitioningForObjectStorage(
        columns,
        configuration,
        sample_path,
        columns_in_table_or_function_definition.empty(),
        format_settings,
        context);

    bool validate_schema_with_remote = !need_resolve_columns_or_format
        && !configuration->isDataLakeConfiguration()
        && !columns_in_table_or_function_definition.empty()
        && !is_table_function
        && mode == LoadingStrictnessLevel::CREATE
        && !do_lazy_init;

    validateColumns(
        columns,
        configuration_,
        validate_schema_with_remote,
        object_storage_,
        &format_settings,
        &sample_path,
        context,
        &hive_partition_columns_to_read_from_file_path,
        &columns_in_table_or_function_definition,
        log);

    // Assert file contains at least one column. The assertion only takes place if we were able to deduce the schema. The storage might be empty.
    if (!columns.empty() && file_columns.empty())
    {
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "File without physical columns is not supported. Please try it with `use_hive_partitioning=0` and or `partition_strategy=wildcard`. File {}",
            sample_path);
    }

    bool format_supports_prewhere = FormatFactory::instance().checkIfFormatSupportsPrewhere(configuration->format, context, format_settings);

    /// TODO: Known problems with datalake prewhere:
    ///  * If the iceberg table went through schema evolution, columns read from file may need to
    ///    be renamed or typecast before applying prewhere. There's already a mechanism for
    ///    telling parquet reader to rename columns: ColumnMapper. And parquet reader already
    ///    automatically does type casts to requested types. But weirdly the iceberg reader uses
    ///    those mechanism to request the *old* name and type of the column, then has additional
    ///    code to do the renaming and casting as a separate step outside parquet reader.
    ///    We should probably change this and delete that additional code?
    ///  * Delta Lake can have "partition columns", which are columns with constant value specified
    ///    in the metadata, not present in parquet file. Like hive partitioning, but in metadata
    ///    files instead of path. Currently these columns are added to the block outside parquet
    ///    reader. If they appear in prewhere expression, parquet reader gets a "no column in block"
    ///    error. Unlike hive partitioning, we can't (?) just return these columns from
    ///    supportedPrewhereColumns() because at the time of the call the delta lake metadata hasn't
    ///    been read yet. So we should probably pass these columns to the parquet reader instead of
    ///    adding them outside.
    ///  * There's a bug in StorageObjectStorageSource::createReader: it makes a copy of
    ///    FormatFilterInfo, but for some reason unsets prewhere_info and row_level_filter_info.
    ///    There's probably no reason for this, and it should just copy those fields like the others.
    ///  * If the table contains files in different formats, with only some of them supporting
    ///    prewhere, things break.
    supports_prewhere = configuration->supportsPrewhere() && format_supports_prewhere;
    supports_tuple_elements = format_supports_prewhere;

    StorageInMemoryMetadata metadata;
    metadata.setColumns(columns);
    if (!do_lazy_init && is_table_function && configuration->isDataLakeConfiguration())
    {
        /// For datalake table functions, always pin the current snapshot version so that
        /// query execution uses the same snapshot as query analysis (logical-race fix).
        /// Additionally reload columns from the snapshot when the per-format setting is enabled.
        /// This is done eagerly because select queries for table functions may bypass
        /// updateExternalDynamicMetadataIfExists.
        configuration->update(object_storage, context, /* if_not_updated_before */true);
        if (auto state = configuration->getTableStateSnapshot(context))
        {
            metadata.setDataLakeTableState(*state);

            /// Reload schema state if needed.
            /// Schema reload for consistency can be disabled, because
            /// 1. user can want to define a table with only
            ///    a subset of columns from remote delta table
            /// 2. user want to override some data types
            ///    (for example, LawCardinality<String> instead of just String)
            if (configuration->shouldReloadSchemaForConsistency(context))
            {
                if (auto metadata_snapshot = configuration->buildStorageMetadataFromState(*state, context))
                    metadata = *metadata_snapshot;
            }
        }
    }

    metadata.setConstraints(constraints_);
    metadata.setComment(comment);
    if (configuration->partition_strategy)
        metadata.partition_key = configuration->partition_strategy->getPartitionKeyDescription();

    setVirtuals(VirtualColumnUtils::getVirtualsForFileLikeStorage(
        metadata.columns,
        context,
        format_settings,
        configuration->partition_strategy_type,
        sample_path));

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

bool StorageObjectStorage::supportsPrewhere() const
{
    return supports_prewhere;
}

bool StorageObjectStorage::canMoveConditionsToPrewhere() const
{
    return supports_prewhere;
}

std::optional<NameSet> StorageObjectStorage::supportedPrewhereColumns() const
{
    return getInMemoryMetadataPtr()->getColumnsWithoutDefaultExpressions(/*exclude=*/ hive_partition_columns_to_read_from_file_path);
}

IStorage::ColumnSizeByName StorageObjectStorage::getColumnSizes() const
{
    return getInMemoryMetadataPtr()->getFakeColumnSizes();
}

IDataLakeMetadata * StorageObjectStorage::getExternalMetadata(ContextPtr query_context)
{
    if (!configuration->isDataLakeConfiguration())
        return nullptr;

    configuration->update(
        object_storage,
        query_context,
        /* if_not_updated_before */ false);

    return configuration->getExternalMetadata();
}

void StorageObjectStorage::updateExternalDynamicMetadataIfExists(ContextPtr query_context)
{
    if (!configuration->isDataLakeConfiguration())
        return;

    /// Always force an update to pick up the latest snapshot version.
    /// Using if_not_updated_before=true would leave latest_snapshot_version
    /// stale from the first query and silently omit new files.
    configuration->update(object_storage, query_context, /* if_not_updated_before */ false);

    auto state = configuration->getTableStateSnapshot(query_context);
    if (!state)
        return;

    auto new_metadata = *getInMemoryMetadataPtr();
    /// Always pin the current snapshot version to prevent logical races between query
    /// analysis (which picks the schema) and query execution (which iterates files).
    new_metadata.setDataLakeTableState(*state);

    /// Optionally also refresh the columns (and other schema-derived fields such as the
    /// Iceberg sort key) when the per-format reload setting is enabled.
    if (configuration->shouldReloadSchemaForConsistency(query_context))
    {
        if (auto metadata_snapshot = configuration->buildStorageMetadataFromState(*state, query_context))
            new_metadata = *metadata_snapshot;
    }

    setInMemoryMetadata(new_metadata);
}


std::optional<UInt64> StorageObjectStorage::totalRows(ContextPtr query_context) const
{
    if (!configuration->supportsTotalRows(query_context, object_storage->getType()))
        return std::nullopt;

    /// Trivial count optimization can be applied only on initiator replica.
    /// (distributed_processing=true on non-initiator replicas).
    /// This is needed only for old analyzer.
    if (distributed_processing)
        return std::nullopt;

    configuration->update(
        object_storage,
        query_context,
        /* if_not_updated_before */ is_table_function);
    return configuration->totalRows(query_context);
}

std::optional<UInt64> StorageObjectStorage::totalBytes(ContextPtr query_context) const
{
    if (!configuration->supportsTotalBytes(query_context, object_storage->getType()))
        return std::nullopt;

    if (distributed_processing)
        return std::nullopt;

    configuration->update(
        object_storage,
        query_context,
        /* if_not_updated_before */ is_table_function);
    return configuration->totalBytes(query_context);
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
    if (distributed_processing && local_context->getSettingsRef()[Setting::max_streams_for_files_processing_in_cluster_functions])
        num_streams = local_context->getSettingsRef()[Setting::max_streams_for_files_processing_in_cluster_functions];

    /// For data lake we did update in getExternalDynamicMetadata.
    if (!is_table_function && !configuration->isDataLakeConfiguration())
    {
        configuration->update(
            object_storage,
            local_context,
            /* if_not_updated_before */ false);
    }


    if (configuration->partition_strategy && configuration->partition_strategy_type != PartitionStrategyFactory::StrategyType::HIVE)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Reading from a partitioned {} storage is not implemented yet",
                        getName());
    }

    const auto & settings = local_context->getSettingsRef();
#if USE_DELTA_KERNEL_RS
    if (configuration->isDataLakeConfiguration())
    {
        if (auto start_version = settings[Setting::delta_lake_snapshot_start_version].value;
            start_version != DeltaLake::TableSnapshot::LATEST_SNAPSHOT_VERSION)
        {
            if (const auto * delta_kernel_metadata = dynamic_cast<const DeltaLakeMetadataDeltaKernel *>(configuration->getExternalMetadata());
                delta_kernel_metadata != nullptr)
            {
                auto source_header = storage_snapshot->getSampleBlockForColumns(column_names);
                auto version_range = DeltaLake::TableChanges::getVersionRange(
                    start_version,
                    settings[Setting::delta_lake_snapshot_end_version].value);
                auto table_changes = delta_kernel_metadata->getTableChanges(
                    version_range,
                    source_header,
                    format_settings,
                    local_context);

                auto read_step = std::make_unique<ReadFromDeltaLakeTableChangesStep>(
                    std::move(table_changes),
                    source_header,
                    column_names,
                    query_info,
                    storage_snapshot,
                    num_streams,
                    local_context);
                query_plan.addStep(std::move(read_step));
                return;
            }
        }
        else if (auto end_version = settings[Setting::delta_lake_snapshot_start_version].value;
                 end_version != DeltaLake::TableSnapshot::LATEST_SNAPSHOT_VERSION)
        {
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "Cannot use delta_lake_snapshot_end_version without delta_lake_snapshot_start_version");
        }
    }
#endif
    auto read_from_format_info = configuration->prepareReadingFromFormat(
        object_storage,
        column_names,
        storage_snapshot,
        supportsSubsetOfColumns(local_context),
        supports_tuple_elements,
        local_context,
        PrepareReadingFromFormatHiveParams{ file_columns, hive_partition_columns_to_read_from_file_path.getNameToTypeMap() });


    if (query_info.prewhere_info || query_info.row_level_filter)
        read_from_format_info = updateFormatPrewhereInfo(read_from_format_info, query_info.row_level_filter, query_info.prewhere_info);

    const bool need_only_count = (query_info.optimize_trivial_count
                                  || (read_from_format_info.requested_columns.empty()
                                      && !read_from_format_info.prewhere_info
                                      && !read_from_format_info.row_level_filter))
        && settings[Setting::optimize_count_from_files];

    auto modified_format_settings{format_settings};
    if (!modified_format_settings.has_value())
        modified_format_settings.emplace(getFormatSettings(local_context));

    configuration->modifyFormatSettings(modified_format_settings.value(), *local_context);

    auto read_step = std::make_unique<ReadFromObjectStorageStep>(
        object_storage,
        configuration,
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
    /// For data lake we did update in getExternalDynamicMetadata.
    if (!is_table_function && !configuration->isDataLakeConfiguration())
    {
        configuration->update(
            object_storage,
            local_context,
            /* if_not_updated_before */ false);
    }

    const auto sample_block = std::make_shared<const Block>(metadata_snapshot->getSampleBlock());
    const auto & settings = configuration->getQuerySettings(local_context);

    const auto raw_path = configuration->getRawPath();

    if (configuration->isArchive())
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Path '{}' contains archive. Write into archive is not supported",
                        raw_path.path);
    }

    if (raw_path.hasGlobsIgnorePartitionWildcard())
    {
        throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED,
                        "Non partitioned table with path '{}' that contains globs, the table is in readonly mode",
                        configuration->getRawPath().path);
    }

    if (!configuration->supportsWrites())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Writes are not supported for engine");

    if (configuration->isDataLakeConfiguration() && configuration->supportsWrites())
        return configuration->write(sample_block, storage_id, object_storage, format_settings, local_context, catalog);

    /// Not a data lake, just raw object storage

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
        format_settings,
        sample_block,
        local_context,
        configuration->format,
        configuration->compression_method);
}

bool StorageObjectStorage::optimize(
    const ASTPtr & /*query*/,
    [[maybe_unused]] const StorageMetadataPtr & metadata_snapshot,
    const ASTPtr & /*partition*/,
    bool /*final*/,
    bool /*deduplicate*/,
    const Names & /* deduplicate_by_columns */,
    bool /*cleanup*/,
    [[maybe_unused]] ContextPtr context)
{
    return configuration->optimize(metadata_snapshot, context, format_settings);
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

    if (configuration->isDataLakeConfiguration())
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Truncate is not supported for data lake engine");
    }

    if (path.hasGlobs())
    {
        throw Exception(
            ErrorCodes::DATABASE_ACCESS_DENIED,
            "{} key '{}' contains globs, so the table is in readonly mode and cannot be truncated",
            getName(), path.path);
    }

    StoredObjects objects;
    for (const auto & key : configuration->getPaths())
    {
        objects.emplace_back(key.path);
    }
    object_storage->removeObjectsIfExist(objects);
}

void StorageObjectStorage::drop()
{
    if (catalog)
    {
        const auto [namespace_name, table_name] = DataLake::parseTableName(storage_id.getTableName());
        catalog->dropTable(namespace_name, table_name);
    }
    /// We cannot use query context here, because drop is executed in the background.
    configuration->drop(Context::getGlobalContextInstance());
}

std::unique_ptr<ReadBufferIterator> StorageObjectStorage::createReadBufferIterator(
    const ObjectStoragePtr & object_storage,
    const StorageObjectStorageConfigurationPtr & configuration,
    const std::optional<FormatSettings> & format_settings,
    ObjectInfos & read_keys,
    const ContextPtr & context)
{
    auto file_iterator = StorageObjectStorageSource::createFileIterator(
        configuration,
        configuration->getQuerySettings(context),
        object_storage,
        nullptr, /* storage_metadata */
        false, /* distributed_processing */
        context,
        {}, /* predicate*/
        {},
        {}, /* virtual_columns */
        {}, /* hive_columns */
        &read_keys);

    return std::make_unique<ReadBufferIterator>(
        object_storage, configuration, file_iterator,
        format_settings, getSchemaCache(context, configuration->getTypeName()), read_keys, context);
}

ColumnsDescription StorageObjectStorage::resolveSchemaFromData(
    const ObjectStoragePtr & object_storage,
    const StorageObjectStorageConfigurationPtr & configuration,
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
    const StorageObjectStorageConfigurationPtr & configuration,
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
    const StorageObjectStorageConfigurationPtr & configuration,
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

SchemaCache & StorageObjectStorage::getSchemaCache(const ContextPtr & context, const std::string & storage_engine_name)
{
    if (storage_engine_name == "s3")
    {
        static SchemaCache schema_cache(
            context->getConfigRef().getUInt(
                "schema_inference_cache_max_elements_for_s3",
                DEFAULT_SCHEMA_CACHE_ELEMENTS));
        return schema_cache;
    }
    if (storage_engine_name == "hdfs")
    {
        static SchemaCache schema_cache(
            context->getConfigRef().getUInt("schema_inference_cache_max_elements_for_hdfs", DEFAULT_SCHEMA_CACHE_ELEMENTS));
        return schema_cache;
    }
    if (storage_engine_name == "azure")
    {
        static SchemaCache schema_cache(
            context->getConfigRef().getUInt("schema_inference_cache_max_elements_for_azure", DEFAULT_SCHEMA_CACHE_ELEMENTS));
        return schema_cache;
    }
    if (storage_engine_name == "local")
    {
        static SchemaCache schema_cache(
            context->getConfigRef().getUInt("schema_inference_cache_max_elements_for_local", DEFAULT_SCHEMA_CACHE_ELEMENTS));
        return schema_cache;
    }
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported storage type: {}", storage_engine_name);
}

void StorageObjectStorage::mutate([[maybe_unused]] const MutationCommands & commands, [[maybe_unused]] ContextPtr context_)
{
    auto metadata_snapshot = getInMemoryMetadataPtr();
    auto storage = getStorageID();
    configuration->mutate(commands, context_, storage, metadata_snapshot, catalog, format_settings);
}

void StorageObjectStorage::checkMutationIsPossible(const MutationCommands & commands, const Settings & /* settings */) const
{
    configuration->checkMutationIsPossible(commands);
}

void StorageObjectStorage::alter(const AlterCommands & params, ContextPtr context, AlterLockHolder & /*alter_lock_holder*/)
{
    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
    params.apply(new_metadata, context);

    configuration->alter(params, context);

    DatabaseCatalog::instance()
        .getDatabase(storage_id.database_name)
        ->alterTable(context, storage_id, new_metadata, /*validate_new_create_query=*/true);
    setInMemoryMetadata(new_metadata);
}

void StorageObjectStorage::checkAlterIsPossible(const AlterCommands & commands, ContextPtr /*context*/) const
{
    configuration->checkAlterIsPossible(commands);
}


}
