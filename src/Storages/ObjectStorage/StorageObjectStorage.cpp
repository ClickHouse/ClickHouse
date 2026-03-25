#include <Storages/ObjectStorage/StorageObjectStorage.h>

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromObjectStorageStep.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Executors/PullingPipelineExecutor.h>

#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/ObjectStorage/StorageObjectStorageSink.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Storages/StorageFactory.h>
#include <Storages/VirtualColumnUtils.h>
#include <Interpreters/StorageID.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/HivePartitioningUtils.h>
#include <Storages/ObjectStorage/StorageObjectStorageSettings.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_count_from_files;
    extern const SettingsBool use_hive_partitioning;
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
    /// This iterator is used only to get a sample path for hive partitioning,
    /// not for actual data reading, so do not emit ProfileEvents.
    file_iterator->setEmitProfileEvents(false);

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
    bool distributed_processing_,
    ASTPtr partition_by_,
    ASTPtr /* order_by_ */,
    bool is_table_function_,
    bool lazy_init)
    : IStorage(table_id_)
    , configuration(configuration_)
    , object_storage(object_storage_)
    , format_settings(format_settings_)
    , distributed_processing(distributed_processing_)
    , is_table_function(is_table_function_)
    , log(getLogger(fmt::format("Storage{}({})", configuration->getEngineName(), table_id_.getFullTableName())))
{
    /// Copy storage metadata fields from configuration to table_options.
    table_options.format = configuration->format;
    table_options.compression_method = configuration->compression_method;
    table_options.structure = configuration->structure;
    table_options.partition_strategy_type = configuration->partition_strategy_type;
    table_options.partition_columns_in_data_file = configuration->partition_columns_in_data_file;

    table_options.initPartitionStrategy(partition_by_, columns_in_table_or_function_definition, context, configuration->getRawPath());
    const bool need_resolve_columns_or_format = columns_in_table_or_function_definition.empty() || (table_options.format == "auto");
    const bool need_resolve_sample_path = context->getSettingsRef()[Setting::use_hive_partitioning]
        && !table_options.partition_strategy;
    const bool do_lazy_init = lazy_init && !need_resolve_columns_or_format && !need_resolve_sample_path;

    LOG_DEBUG(
        log, "StorageObjectStorage: lazy_init={}, need_resolve_columns_or_format={}, "
        "need_resolve_sample_path={}, is_table_function={}, columns_in_table_or_function_definition={}",
        lazy_init, need_resolve_columns_or_format, need_resolve_sample_path, is_table_function,
        columns_in_table_or_function_definition.toString(true));

    bool updated_configuration = false;
    try
    {
        if (!do_lazy_init)
        {
            if (is_table_function)
                configuration->lazyInitializeIfNeeded(object_storage, context);
            else
                configuration->update(object_storage, context);
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

    if (configuration->getRawPath().hasSchemaHashWildcard())
    {
        if (table_options.partition_strategy_type == PartitionStrategyFactory::StrategyType::HIVE)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The _schema_hash placeholder is not supported with hive partition strategy");

        if (columns.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot use _schema_hash placeholder without explicitly specifying columns");

        configuration->setSchemaHash(StorageObjectStorageTableOptions::computeSchemaHash(columns));
    }

    if (need_resolve_columns_or_format)
        resolveSchemaAndFormat(columns, table_options.format, object_storage, configuration, format_settings, sample_path, context);
    else
        validateSupportedColumns(columns, *configuration);

    configuration->check(context);

    /// FIXME: We need to call getPathSample() lazily on select
    /// in case it failed to be initialized in constructor.
    if (updated_configuration && sample_path.empty() && need_resolve_sample_path && !table_options.partition_strategy)
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

    // Assert file contains at least one column. The assertion only takes place if we were able to deduce the schema. The storage might be empty.
    if (!columns.empty() && file_columns.empty())
    {
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "File without physical columns is not supported. Please try it with `use_hive_partitioning=0` and or `partition_strategy=wildcard`. File {}",
            sample_path);
    }

    bool format_supports_prewhere = FormatFactory::instance().checkIfFormatSupportsPrewhere(table_options.format, context, format_settings);

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

    metadata.setConstraints(constraints_);
    metadata.setComment(comment);
    if (table_options.partition_strategy)
        metadata.partition_key = table_options.partition_strategy->getPartitionKeyDescription();

    setVirtuals(VirtualColumnUtils::getVirtualsForFileLikeStorage(
        metadata.columns,
        context,
        format_settings,
        table_options.partition_strategy_type,
        sample_path));

    setInMemoryMetadata(metadata);
}

String StorageObjectStorage::getName() const
{
    return configuration->getEngineName();
}

bool StorageObjectStorage::prefersLargeBlocks() const
{
    return FormatFactory::instance().checkIfOutputFormatPrefersLargeBlocks(table_options.format);
}

bool StorageObjectStorage::parallelizeOutputAfterReading(ContextPtr context) const
{
    return FormatFactory::instance().checkParallelizeOutputAfterReading(table_options.format, context);
}

bool StorageObjectStorage::supportsSubsetOfColumns(const ContextPtr & context) const
{
    return FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(table_options.format, context, format_settings);
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

std::optional<UInt64> StorageObjectStorage::totalRows(ContextPtr) const
{
    return std::nullopt;
}

std::optional<UInt64> StorageObjectStorage::totalBytes(ContextPtr) const
{
    return std::nullopt;
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

    if (!is_table_function)
    {
        configuration->update(object_storage, local_context);
    }


    if (table_options.partition_strategy && table_options.partition_strategy_type != PartitionStrategyFactory::StrategyType::HIVE)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Reading from a partitioned {} storage is not implemented yet",
                        getName());
    }

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

    const auto & settings = local_context->getSettingsRef();
    const bool need_only_count = (query_info.optimize_trivial_count
                                  || (read_from_format_info.requested_columns.empty()
                                      && !read_from_format_info.prewhere_info
                                      && !read_from_format_info.row_level_filter))
        && settings[Setting::optimize_count_from_files]
        && !VirtualColumnUtils::hasRowDependentVirtualColumns(read_from_format_info.requested_virtual_columns);

    auto modified_format_settings{format_settings};
    if (!modified_format_settings.has_value())
        modified_format_settings.emplace(getFormatSettings(local_context));

    configuration->modifyFormatSettings(modified_format_settings.value(), *local_context);

    auto read_step = std::make_unique<ReadFromObjectStorageStep>(
        object_storage,
        configuration,
        table_options,
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
    if (!is_table_function)
    {
        configuration->update(object_storage, local_context);
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

    if (raw_path.hasGlobsIgnorePlaceholders())
    {
        throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED,
                        "Non partitioned table with path '{}' that contains globs, the table is in readonly mode",
                        configuration->getRawPath().path);
    }

    if (!configuration->supportsWrites())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Writes are not supported for engine");

    if (table_options.partition_strategy)
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
        table_options.format,
        table_options.compression_method);
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

    if (path.hasGlobsIgnorePlaceholders())
    {
        throw Exception(
            ErrorCodes::DATABASE_ACCESS_DENIED,
            "{} key '{}' contains globs, so the table is in readonly mode and cannot be truncated",
            getName(), path.path);
    }

    if (path.hasPartitionWildcard())
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Truncate is not supported for partitioned tables, the path is '{}'",
            path.path);
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
    /// We cannot use query context here, because drop is executed in the background.
    configuration->drop(Context::getGlobalContextInstance());
}

void StorageObjectStorage::addInferredEngineArgsToCreateQuery(ASTs & args, const ContextPtr & context) const
{
    configuration->addStructureAndFormatToArgsIfNeeded(args, "", table_options.format, context, /*with_structure=*/false);
}

}
