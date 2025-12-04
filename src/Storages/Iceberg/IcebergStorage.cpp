#include <thread>
#include <Core/ColumnWithTypeAndName.h>
#include <Processors/QueryPlan/ReadFromIcebergStorageStep.h>
#include <Storages/ObjectStorage/DataLakes/Common/Common.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Parsers/ASTInsertQuery.h>
#include <Formats/ReadSchemaUtils.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>

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
#include <Storages/ObjectStorage/DataLakes/Iceberg/Mutations.h>
#include <Interpreters/StorageID.h>
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Databases/DataLake/Common.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/HivePartitioningUtils.h>
#include <Storages/ObjectStorage/StorageObjectStorageSettings.h>
#include <Storages/Iceberg/IcebergStorage.h>

#include <Poco/Logger.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_count_from_files;
    extern const SettingsBool use_hive_partitioning;
}

namespace ErrorCodes
{
    extern const int DATABASE_ACCESS_DENIED;
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DATA;
}

namespace Setting
{
extern const SettingsNonZeroUInt64 max_block_size;
extern const SettingsUInt64 max_bytes_in_set;
extern const SettingsUInt64 max_rows_in_set;
extern const SettingsOverflowMode set_overflow_mode;
extern const SettingsInt64 iceberg_timestamp_ms;
extern const SettingsInt64 iceberg_snapshot_id;
extern const SettingsBool use_iceberg_metadata_files_cache;
extern const SettingsBool use_iceberg_partition_pruning;
extern const SettingsBool write_full_path_in_iceberg_metadata;
extern const SettingsBool use_roaring_bitmap_iceberg_positional_deletes;
extern const SettingsString iceberg_metadata_compression_method;
extern const SettingsBool allow_experimental_insert_into_iceberg;
extern const SettingsBool allow_experimental_iceberg_compaction;
extern const SettingsBool iceberg_delete_data_on_drop;
}


void data_lake_general_initialization_code() {
//     const bool need_resolve_columns_or_format = false;

//     LOG_DEBUG(log, "StorageObjectStorage: lazy_init={}, need_resolve_columns_or_format={}, need_resolve_sample_path={}, is_table_function={}, needs_data_table_create={}, columns_in_table_or_function_definition={}",
//         lazy_init, need_resolve_columns_or_format, false, is_table_function, needs_data_table_create, columns_in_table_or_function_definition.toString(true));

//     if (!is_table_function && !columns_in_table_or_function_definition.empty() && needs_data_table_create && mode == LoadingStrictnessLevel::CREATE)
//     {
            // if (object_storage->getType() == ObjectStorageType::Local)
            // {
            //     auto user_files_path = local_context->getUserFilesPath();
            //     if (!fileOrSymlinkPathStartsWith(this->getPathForRead().path, user_files_path))
            //         throw Exception(ErrorCodes::PATH_ACCESS_DENIED, "File path {} is not inside {}", this->getPathForRead().path, user_files_path);
            // }
            // BaseStorageConfiguration::update(object_storage, local_context, true);

            // DataLakeMetadata::createInitial(object_storage, weak_from_this(), local_context, columns, partition_by, if_not_exists, catalog, table_id_);
//     }

//     bool updated_configuration = false;
//     try
//     {
//         if (!lazy_init)
//         {
//             lazyInitializeIcebergMetadata(local_context);
//         }
//     }
//     catch (...)
//     {
//         // If we don't have format or schema yet, we can't ignore failed configuration update,
//         // because relevant configuration is crucial for format and schema inference
//         if (mode <= LoadingStrictnessLevel::CREATE)
//         {
//             throw;
//         }
//         tryLogCurrentException(log, /*start of message = */ "", LogsLevel::warning);
//     }

//     ColumnsDescription columns{columns_in_table_or_function_definition};
//     validateSupportedColumns(columns, *configuration);

//     configuration->check(context);

//         auto metadata_snapshot = configuration->getStorageSnapshotMetadata(context);
//         setInMemoryMetadata(metadata_snapshot);
}

IcebergStorage::IcebergStorage(
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
    bool needs_data_table_create,
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
    , catalog(catalog_)
    , storage_id(table_id_)
{
    data_lake_general_initialization_code();
}

String IcebergStorage::getName() const
{
    return "Iceberg" + configuration->getTypeName();
}

std::optional<NameSet> IcebergStorage::supportedPrewhereColumns() const
{
    return std::nullopt;
}

void IcebergStorage::updateExternalDynamicMetadataIfExists(ContextPtr query_context)
{
    lazyInitializeIcebergMetadata(query_context);
    auto metadata_snapshot = iceberg_metadata->getStorageSnapshotMetadata(query_context);
    setInMemoryMetadata(metadata_snapshot);
}

void IcebergStorage::lazyInitializeIcebergMetadata(ContextPtr context) const
{
    if (!iceberg_metadata)
    {
        iceberg_metadata = IcebergMetadata::create(object_storage, configuration, context);
    }
}

std::optional<UInt64> IcebergStorage::totalRows(ContextPtr query_context) const
{
    lazyInitializeIcebergMetadata(query_context);
    return iceberg_metadata->totalRows(query_context);
}

std::optional<UInt64> IcebergStorage::totalBytes(ContextPtr query_context) const
{
    lazyInitializeIcebergMetadata(query_context);
    return iceberg_metadata->totalBytes(query_context);
}

void IcebergStorage::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    chassert(iceberg_metadata);
    auto read_from_format_info = DB::prepareReadingFromFormat(column_names, storage_snapshot, local_context, false, false, {});

    if (query_info.prewhere_info || query_info.row_level_filter)
        read_from_format_info = updateFormatPrewhereInfo(read_from_format_info, query_info.row_level_filter, query_info.prewhere_info);

    const bool need_only_count = (query_info.optimize_trivial_count || (read_from_format_info.requested_columns.empty() && !read_from_format_info.prewhere_info && !read_from_format_info.row_level_filter))
        && local_context->getSettingsRef()[Setting::optimize_count_from_files];

    auto modified_format_settings{format_settings};
    if (!modified_format_settings.has_value())
        modified_format_settings.emplace(getFormatSettings(local_context));

    configuration->modifyFormatSettings(modified_format_settings.value(), *local_context);

    auto read_step = std::make_unique<ReadFromIcebergStorageStep>(
        object_storage,
        configuration,
        column_names,
        getVirtualsList(),
        query_info,
        storage_snapshot,
        modified_format_settings,
        distributed_processing,
        need_only_count,
        local_context,
        max_block_size,
        num_streams);

    query_plan.addStep(std::move(read_step));
}

SinkToStoragePtr IcebergStorage::write(
    const ASTPtr &,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr local_context,
    bool /* async_insert */)
{
    assert(iceberg_metadata);
    const auto sample_block = std::make_shared<const Block>(metadata_snapshot->getSampleBlock());
    return iceberg_metadata->write(
        sample_block,
        storage_id,
        object_storage,
        configuration,
        format_settings.has_value() ? *format_settings : FormatSettings{},
        local_context,
        catalog);
}

bool IcebergStorage::optimize(
    const ASTPtr & /*query*/,
    [[maybe_unused]] const StorageMetadataPtr & metadata_snapshot,
    const ASTPtr & /*partition*/,
    bool /*final*/,
    bool /*deduplicate*/,
    const Names & /* deduplicate_by_columns */,
    bool /*cleanup*/,
    [[maybe_unused]] ContextPtr context)
{
    return iceberg_metadata->optimize(metadata_snapshot, context, format_settings);
}

void IcebergStorage::drop()
{
    iceberg_metadata->drop(Context::getGlobalContext());
}

// I wonder if we need this method at all for IcebergStorage.
// void IcebergStorage::addInferredEngineArgsToCreateQuery(ASTs & args, const ContextPtr & context) const
// {
//     configuration->addStructureAndFormatToArgsIfNeeded(args, "", configuration->format, context, /*with_structure=*/false);
// }

void IcebergStorage::mutate([[maybe_unused]] const MutationCommands & commands, [[maybe_unused]] ContextPtr context_)
{
    auto metadata_snapshot = getInMemoryMetadataPtr();
    auto storage = getStorageID();
    iceberg_metadata->mutate(commands, context_, storage, metadata_snapshot, catalog, format_settings);
}

void IcebergStorage::checkMutationIsPossible(const MutationCommands & commands, const Settings & /* settings */) const
{
    iceberg_metadata->checkMutationIsPossible(commands);
}

void IcebergStorage::alter(const AlterCommands & params, ContextPtr context, AlterLockHolder & /*alter_lock_holder*/)
{
    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
    params.apply(new_metadata, context);

    iceberg_metadata->alter(params, context);

    DatabaseCatalog::instance()
        .getDatabase(storage_id.database_name)
        ->alterTable(context, storage_id, new_metadata, /*validate_new_create_query=*/true);
    setInMemoryMetadata(new_metadata);
}

IcebergMetadata::IcebergHistory IcebergStorage::getHistory(ContextPtr context)
{
    return iceberg_metadata->getHistory(context);
}

void IcebergStorage::checkAlterIsPossible(const AlterCommands & commands, ContextPtr /*context*/) const
{
    iceberg_metadata->checkAlterIsPossible(commands);
}

}
