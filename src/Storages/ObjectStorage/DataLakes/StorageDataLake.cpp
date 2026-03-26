#include <Storages/ObjectStorage/DataLakes/StorageDataLake.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/StorageIceberg.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonMetadata.h>
#include <Storages/ObjectStorage/DataLakes/HudiMetadata.h>

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <Interpreters/Context.h>

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromObjectStorageStep.h>
#include <Processors/QueryPlan/ReadFromDataLakeStep.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Executors/PullingPipelineExecutor.h>

#include <Storages/NamedCollectionsHelpers.h>
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

#include <type_traits>

namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_count_from_files;
    extern const SettingsInt64 delta_lake_snapshot_start_version;
    extern const SettingsInt64 delta_lake_snapshot_end_version;
    extern const SettingsUInt64 max_streams_for_files_processing_in_cluster_functions;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
}

template <typename DataLakeMetadata>
StorageDataLake<DataLakeMetadata>::StorageDataLake(
    ObjectStorageConnectionConfigurationPtr configuration_,
    StorageObjectStorageTableOptions table_options_,
    ObjectStoragePtr object_storage_,
    ContextPtr context,
    const StorageID & table_id_,
    const ColumnsDescription & columns_in_table_or_function_definition,
    const ConstraintsDescription & constraints_,
    const String & comment,
    std::optional<FormatSettings> format_settings_,
    LoadingStrictnessLevel mode,
    DataLakeStorageSettingsPtr datalake_settings_,
    std::shared_ptr<DataLake::ICatalog> catalog_,
    bool distributed_processing_,
    ASTPtr partition_by_,
    ASTPtr order_by_,
    bool is_table_function_,
    bool lazy_init)
    : IStorage(table_id_)
    , configuration(configuration_)
    , table_options(std::move(table_options_))
    , object_storage(object_storage_)
    , format_settings(format_settings_)
    , distributed_processing(distributed_processing_)
    , is_table_function(is_table_function_)
    , log(getLogger(fmt::format("Storage{}({})", String(DataLakeMetadata::name) + configuration->getEngineName(), table_id_.getFullTableName())))
    , datalake_settings(std::move(datalake_settings_))
    , catalog(catalog_)
    , storage_id(table_id_)
{
    /// Ensure trailing slash on the raw path for data lake storages.
    auto path = configuration->getRawPath();
    if (!path.path.ends_with('/'))
        configuration->setRawPath(ObjectStorageConnectionConfiguration::Path(path.path + "/"));
    table_options.initPartitionStrategy(partition_by_, columns_in_table_or_function_definition, context, configuration->getRawPath());
    const bool need_resolve_columns_or_format = columns_in_table_or_function_definition.empty() || (table_options.format == "auto");
    const bool do_lazy_init = lazy_init && !need_resolve_columns_or_format;

    LOG_DEBUG(
        log, "StorageDataLake: lazy_init={}, need_resolve_columns_or_format={}, "
        "is_table_function={}, columns_in_table_or_function_definition={}",
        lazy_init, need_resolve_columns_or_format, is_table_function,
        columns_in_table_or_function_definition.toString(true));

    bool is_delta_lake_cdf = context->getSettingsRef()[Setting::delta_lake_snapshot_start_version] != -1
        || context->getSettingsRef()[Setting::delta_lake_snapshot_end_version] != -1;

    if (!is_table_function && is_delta_lake_cdf && !std::is_same_v<DataLakeMetadata, DeltaLakeMetadata>)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Delta lake CDF is allowed only for deltaLake table function");
    }

    if (!is_table_function && !columns_in_table_or_function_definition.empty() && mode == LoadingStrictnessLevel::CREATE)
    {
        LOG_DEBUG(log, "Creating new storage with specified columns");
        configuration->update(object_storage, context);
        DataLakeMetadata::createInitial(
            object_storage, configuration, context, columns_in_table_or_function_definition, partition_by_, order_by_, /*if_not_exists=*/ false, catalog, storage_id);
    }

    try
    {
        if (!do_lazy_init)
        {
            if (is_table_function)
                ensureMetadataInitialized(context);
            else
                updateMetadata(context);
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
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The _schema_hash placeholder is not supported for DataLake engines");

    if (need_resolve_columns_or_format)
    {
        /// For data lake storages, try to resolve schema from metadata first (e.g. Iceberg stores
        /// schema in its metadata files). Only fall through to reading actual data files if
        /// metadata doesn't provide a schema.
        if (columns.empty() && current_metadata)
        {
            auto schema = current_metadata->getTableSchema(context);
            if (!schema.empty())
                columns = ColumnsDescription(std::move(schema));
        }

        if (columns.empty() || table_options.format == "auto")
            resolveSchemaAndFormat(columns, table_options.format, table_options.compression_method, object_storage, configuration, format_settings, sample_path, context);
    }
    else
        validateSupportedColumns(columns, configuration->getTypeName());

    FormatFactory::instance().checkFormatName(table_options.format);

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
    if constexpr (std::is_same_v<DataLakeMetadata, IcebergMetadata>)
        supports_prewhere = format_supports_prewhere;
    else
        supports_prewhere = false;
    supports_tuple_elements = format_supports_prewhere;

    StorageInMemoryMetadata metadata;
    metadata.setColumns(columns);
    if (!do_lazy_init && is_table_function)
    {
        /// For datalake table functions, always pin the current snapshot version so that
        /// query execution uses the same snapshot as query analysis (logical-race fix).
        /// Additionally reload columns from the snapshot when the per-format setting is enabled.
        /// This is done eagerly because select queries for table functions may bypass
        /// updateExternalDynamicMetadataIfExists.
        ensureMetadataInitialized(context);
        if (auto state = current_metadata ? current_metadata->getTableStateSnapshot(context) : std::nullopt)
        {
            metadata.setDataLakeTableState(*state);

            /// Reload schema state if needed.
            /// Schema reload for consistency can be disabled, because
            /// 1. user can want to define a table with only
            ///    a subset of columns from remote delta table
            /// 2. user want to override some data types
            ///    (for example, LawCardinality<String> instead of just String)
            if (current_metadata->shouldReloadSchemaForConsistency(context))
            {
                if (auto metadata_snapshot = current_metadata->buildStorageMetadataFromState(*state, context))
                    metadata = *metadata_snapshot;
            }
        }
    }

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

template <typename DataLakeMetadata>
void StorageDataLake<DataLakeMetadata>::ensureMetadataInitialized(ContextPtr context) const
{
    if (current_metadata)
        return;
    configuration->update(object_storage, context);
    current_metadata = DataLakeMetadata::create(object_storage, configuration, context);
}

template <typename DataLakeMetadata>
void StorageDataLake<DataLakeMetadata>::updateMetadata(ContextPtr context) const
{
    configuration->update(object_storage, context);
    if (current_metadata && current_metadata->supportsUpdate())
    {
        current_metadata->update(context);
        return;
    }
    current_metadata = DataLakeMetadata::create(object_storage, configuration, context);
}

template <typename DataLakeMetadata>
String StorageDataLake<DataLakeMetadata>::getName() const
{
    return String(DataLakeMetadata::name) + configuration->getEngineName();
}

template <typename DataLakeMetadata>
bool StorageDataLake<DataLakeMetadata>::prefersLargeBlocks() const
{
    return FormatFactory::instance().checkIfOutputFormatPrefersLargeBlocks(table_options.format);
}

template <typename DataLakeMetadata>
bool StorageDataLake<DataLakeMetadata>::parallelizeOutputAfterReading(ContextPtr context) const
{
    return FormatFactory::instance().checkParallelizeOutputAfterReading(table_options.format, context);
}

template <typename DataLakeMetadata>
bool StorageDataLake<DataLakeMetadata>::supportsSubsetOfColumns(const ContextPtr & context) const
{
    return FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(table_options.format, context, format_settings);
}

template <typename DataLakeMetadata>
bool StorageDataLake<DataLakeMetadata>::supportsPrewhere() const
{
    return supports_prewhere;
}

template <typename DataLakeMetadata>
bool StorageDataLake<DataLakeMetadata>::canMoveConditionsToPrewhere() const
{
    return supports_prewhere;
}

template <typename DataLakeMetadata>
std::optional<NameSet> StorageDataLake<DataLakeMetadata>::supportedPrewhereColumns() const
{
    return getInMemoryMetadataPtr()->getColumnsWithoutDefaultExpressions(/*exclude=*/ {});
}

template <typename DataLakeMetadata>
IStorage::ColumnSizeByName StorageDataLake<DataLakeMetadata>::getColumnSizes() const
{
    return getInMemoryMetadataPtr()->getFakeColumnSizes();
}

template <typename DataLakeMetadata>
IDataLakeMetadata * StorageDataLake<DataLakeMetadata>::getExternalMetadata(ContextPtr query_context)
{
    updateMetadata(query_context);
    return current_metadata.get();
}

template <typename DataLakeMetadata>
void StorageDataLake<DataLakeMetadata>::updateExternalDynamicMetadataIfExists(ContextPtr query_context)
{
    /// Always force an update to pick up the latest snapshot version.
    /// Using if_not_updated_before=true would leave latest_snapshot_version
    /// stale from the first query and silently omit new files.
    updateMetadata(query_context);

    auto state = current_metadata ? current_metadata->getTableStateSnapshot(query_context) : std::nullopt;
    if (!state)
        return;

    auto new_metadata = *getInMemoryMetadataPtr();
    /// Always pin the current snapshot version to prevent logical races between query
    /// analysis (which picks the schema) and query execution (which iterates files).
    new_metadata.setDataLakeTableState(*state);

    /// Optionally also refresh the columns (and other schema-derived fields such as the
    /// Iceberg sort key) when the per-format reload setting is enabled.
    if (current_metadata->shouldReloadSchemaForConsistency(query_context))
    {
        if (auto metadata_snapshot = current_metadata->buildStorageMetadataFromState(*state, query_context))
            new_metadata = *metadata_snapshot;
    }

    setInMemoryMetadata(new_metadata);
}


template <typename DataLakeMetadata>
std::optional<UInt64> StorageDataLake<DataLakeMetadata>::totalRows(ContextPtr query_context) const
{
    if (!DataLakeMetadata::supportsTotalRows(query_context, object_storage->getType()))
        return std::nullopt;

    /// Trivial count optimization can be applied only on initiator replica.
    /// (distributed_processing=true on non-initiator replicas).
    /// This is needed only for old analyzer.
    if (distributed_processing)
        return std::nullopt;

    is_table_function ? ensureMetadataInitialized(query_context) : updateMetadata(query_context);

    return current_metadata->totalRows(query_context);
}

template <typename DataLakeMetadata>
std::optional<UInt64> StorageDataLake<DataLakeMetadata>::totalBytes(ContextPtr query_context) const
{
    if (!DataLakeMetadata::supportsTotalBytes(query_context, object_storage->getType()))
        return std::nullopt;

    if (distributed_processing)
        return std::nullopt;

    is_table_function ? ensureMetadataInitialized(query_context) : updateMetadata(query_context);
    return current_metadata->totalBytes(query_context);
}

template <typename DataLakeMetadata>
void StorageDataLake<DataLakeMetadata>::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    auto * read_metadata = getExternalMetadata(local_context);

    if (distributed_processing && local_context->getSettingsRef()[Setting::max_streams_for_files_processing_in_cluster_functions])
        num_streams = local_context->getSettingsRef()[Setting::max_streams_for_files_processing_in_cluster_functions];

    if (table_options.partition_strategy && table_options.partition_strategy_type != PartitionStrategyFactory::StrategyType::HIVE)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Reading from a partitioned {} storage is not implemented yet",
                        getName());
    }

    const auto & settings = local_context->getSettingsRef();
#if USE_DELTA_KERNEL_RS
    if (auto start_version = settings[Setting::delta_lake_snapshot_start_version].value;
        start_version != DeltaLake::TableSnapshot::LATEST_SNAPSHOT_VERSION)
    {
        if (const auto * delta_kernel_metadata = dynamic_cast<const DeltaLakeMetadataDeltaKernel *>(current_metadata.get());
            delta_kernel_metadata != nullptr)
        {
            auto source_header = storage_snapshot->getSampleBlockForColumns(column_names);
            auto version_range
                = DeltaLake::TableChanges::getVersionRange(start_version, settings[Setting::delta_lake_snapshot_end_version].value);
            auto table_changes = delta_kernel_metadata->getTableChanges(version_range, source_header, format_settings, local_context);

            auto read_step = std::make_unique<ReadFromDeltaLakeTableChangesStep>(
                std::move(table_changes), source_header, column_names, query_info, storage_snapshot, num_streams, local_context);
            query_plan.addStep(std::move(read_step));
            return;
        }
    }
    else if (auto end_version = settings[Setting::delta_lake_snapshot_end_version].value;
             end_version != DeltaLake::TableSnapshot::LATEST_SNAPSHOT_VERSION)
    {
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS, "Cannot use delta_lake_snapshot_end_version without delta_lake_snapshot_start_version");
    }
#endif
    auto read_from_format_info = read_metadata->prepareReadingFromFormat(
        column_names,
        storage_snapshot,
        local_context,
        supportsSubsetOfColumns(local_context),
        supports_tuple_elements);


    if (query_info.prewhere_info || query_info.row_level_filter)
        read_from_format_info = updateFormatPrewhereInfo(read_from_format_info, query_info.row_level_filter, query_info.prewhere_info);

    const bool need_only_count = (query_info.optimize_trivial_count
                                  || (read_from_format_info.requested_columns.empty()
                                      && !read_from_format_info.prewhere_info
                                      && !read_from_format_info.row_level_filter))
        && settings[Setting::optimize_count_from_files]
        && !VirtualColumnUtils::hasRowDependentVirtualColumns(read_from_format_info.requested_virtual_columns);

    auto modified_format_settings{format_settings};
    if (!modified_format_settings.has_value())
        modified_format_settings.emplace(getFormatSettings(local_context));

    read_metadata->modifyFormatSettings(modified_format_settings.value(), *local_context);

    auto read_step = std::make_unique<ReadFromDataLakeStep>(
        object_storage,
        configuration,
        table_options,
        column_names,
        getVirtualsList(),
        query_info,
        storage_snapshot,
        modified_format_settings,
        read_from_format_info,
        need_only_count,
        local_context,
        max_block_size,
        num_streams,
        read_metadata,
        distributed_processing);

    query_plan.addStep(std::move(read_step));
}

template <typename DataLakeMetadata>
void StorageDataLake<DataLakeMetadata>::truncate(
    const ASTPtr & /* query */,
    const StorageMetadataPtr & /* metadata_snapshot */,
    ContextPtr /* context */,
    TableExclusiveLockHolder & /* table_holder */)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Truncate is not supported for data lake engine");
}

template <typename DataLakeMetadata>
void StorageDataLake<DataLakeMetadata>::drop()
{
    if (catalog)
    {
        const auto [namespace_name, table_name] = DataLake::parseTableName(storage_id.getTableName());
        catalog->dropTable(namespace_name, table_name);
    }
    /// We cannot use query context here, because drop is executed in the background.
    if (current_metadata)
        current_metadata->drop(Context::getGlobalContextInstance());
}

template <typename DataLakeMetadata>
void StorageDataLake<DataLakeMetadata>::addInferredEngineArgsToCreateQuery(ASTs & args, const ContextPtr & context) const
{
    configuration->addStructureAndFormatToArgsIfNeeded(args, "", table_options.format, context, /*with_structure=*/false);
}

#if USE_AVRO
template class StorageDataLake<PaimonMetadata>;
#endif

#if USE_PARQUET && USE_DELTA_KERNEL_RS
template class StorageDataLake<DeltaLakeMetadata>;
#endif

#if USE_AWS_S3
template class StorageDataLake<HudiMetadata>;
#endif

}
