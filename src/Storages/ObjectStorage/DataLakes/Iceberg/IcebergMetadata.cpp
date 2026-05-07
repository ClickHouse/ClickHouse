#include <base/defines.h>
#include <base/sleep.h>
#include "config.h"
#if USE_AVRO

#include <cstddef>
#include <memory>
#include <optional>
#include <sstream>
#include <Columns/ColumnSet.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatFilterInfo.h>
#include <Formats/FormatParserSharedResources.h>
#include <Formats/ReadSchemaUtils.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/tuple.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <Processors/Transforms/FilterTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>
#include <fmt/format.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Common/Exception.h>

#include <Interpreters/PreparedSets.h>
#include <Storages/ObjectStorage/Utils.h>

#include <Databases/DataLake/Common.h>
#include <Disks/DiskType.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Core/NamesAndTypes.h>
#include <Databases/DataLake/ICatalog.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTLiteral.h>
#include <Interpreters/Context.h>
#include <Interpreters/formatWithPossiblyHidingSecrets.h>
#include <Interpreters/IcebergMetadataLog.h>

#include <Storages/ObjectStorage/DataLakes/Common/Common.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/MetadataGenerator.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <IO/CompressedReadBufferWrapper.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Interpreters/StorageID.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ObjectStorage/DataLakes/Common/AvroForIcebergDeserializer.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Compaction.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ExecuteOptionsParser.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergIterator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergTableStateSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Mutations.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/StatelessMetadataFileGetter.h>
#include <Common/FailPoint.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Common/ProfileEvents.h>
#include <Common/SharedLockGuard.h>
#include <Common/logger_useful.h>

namespace ProfileEvents
{
extern const Event IcebergIteratorInitializationMicroseconds;
extern const Event IcebergMetadataUpdateMicroseconds;
extern const Event IcebergTrivialCountOptimizationApplied;
}

namespace DB
{

namespace DataLakeStorageSetting
{
extern const DataLakeStorageSettingsString iceberg_metadata_file_path;
extern const DataLakeStorageSettingsString iceberg_metadata_table_uuid;
extern const DataLakeStorageSettingsUInt32 iceberg_metadata_async_prefetch_period_ms;
extern const DataLakeStorageSettingsBool iceberg_recent_metadata_file_by_last_updated_ms_field;
extern const DataLakeStorageSettingsBool iceberg_use_version_hint;
extern const DataLakeStorageSettingsNonZeroUInt64 iceberg_format_version;
}

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
extern const int ICEBERG_SPECIFICATION_VIOLATION;
extern const int S3_ERROR;
extern const int TABLE_ALREADY_EXISTS;
extern const int SUPPORT_IS_DISABLED;
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
extern const SettingsBool allow_insert_into_iceberg;
extern const SettingsBool allow_experimental_iceberg_compaction;
extern const SettingsBool allow_experimental_expire_snapshots;
extern const SettingsBool iceberg_delete_data_on_drop;
}

static constexpr size_t MAX_TRANSACTION_RETRIES = 100;

namespace
{
String dumpMetadataObjectToString(const Poco::JSON::Object::Ptr & metadata_object)
{
    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::JSON::Stringifier::stringify(metadata_object, oss);
    return removeEscapedSlashes(oss.str());
}

/// Check if a previous attempt already committed this transaction the snapshot
/// (with our transaction_id embedded in its summary) is still present in the snapshots array
/// unless an external engine ran expireSnapshots in the meantime. If found, skip re-committing.
bool isExportPartitionTransactionAlreadyCommitted(const Poco::JSON::Object::Ptr & metadata, const String & transaction_id)
{
    const auto throw_error = [&](const std::string & missing_field_name)
    {
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "No {} found in metadata for iceberg file while trying to commit export partition transaction",
            missing_field_name);
    };

    const auto snapshots = metadata->getArray(Iceberg::f_snapshots);

    if (!snapshots)
    {
        throw_error(Iceberg::f_snapshots);
    }

    for (size_t i = 0; i < snapshots->size(); ++i)
    {
        const auto snap = snapshots->getObject(static_cast<UInt32>(i));
        const auto summary = snap->getObject(Iceberg::f_summary);

        if (!summary)
        {
            throw_error(Iceberg::f_summary);
        }

        if (summary->has(Iceberg::f_clickhouse_export_partition_transaction_id))
        {
            const auto tid = summary->getValue<String>(Iceberg::f_clickhouse_export_partition_transaction_id);

            if (tid == transaction_id)
            {
                return true;
            }
        }
    }

    return false;
}

}


using namespace Iceberg;

namespace
{
Iceberg::TableStateSnapshotPtr extractIcebergSnapshotIdFromMetadataObject(StorageMetadataPtr storage_metadata)
{
    if (!storage_metadata || !storage_metadata->datalake_table_state.has_value())
        return nullptr;
    chassert(std::holds_alternative<TableStateSnapshot>(storage_metadata->datalake_table_state.value()));
    return std::make_shared<TableStateSnapshot>(std::get<TableStateSnapshot>(storage_metadata->datalake_table_state.value()));
}
}

Iceberg::PersistentTableComponents IcebergMetadata::initializePersistentTableComponents(
    StorageObjectStorageConfigurationPtr configuration, IcebergMetadataFilesCachePtr cache_ptr, ContextPtr context_)
{
    const auto [metadata_version, metadata_file_path, compression_method]
        = getLatestOrExplicitMetadataFileAndVersion(object_storage, configuration->getPathForRead().path, configuration->getDataLakeSettings(), cache_ptr, context_, log.get(), std::nullopt, true);
    LOG_DEBUG(log, "Latest metadata file path is {}, version {}", metadata_file_path, metadata_version);
    auto metadata_object
        = getMetadataJSONObject(metadata_file_path, object_storage, cache_ptr, context_, log, compression_method, std::nullopt);
    Int32 format_version = metadata_object->getValue<Int32>(f_format_version);
    String table_location = metadata_object->getValue<String>(f_location);
    std::optional<String> table_uuid = std::nullopt;
    if (metadata_object->has(Iceberg::f_table_uuid))
    {
        table_uuid = normalizeUuid(metadata_object->getValue<String>(f_table_uuid));
    }
    else
    {
        if (format_version >= 2)
        {
            throw Exception(
                ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                "Iceberg table metadata file '{}' doesn't contain required field '{}'",
                metadata_file_path,
                Iceberg::f_table_uuid);
        }
    }
    return PersistentTableComponents{
        .schema_processor = std::make_shared<IcebergSchemaProcessor>(context_),
        .metadata_cache = cache_ptr,
        .format_version = format_version,
        .table_location = table_location,
        .metadata_compression_method = compression_method,
        .table_path = configuration->getPathForRead().path,
        .table_uuid = table_uuid,
        .common_namespace = configuration->getNamespace(),
    };
}

std::pair<IcebergDataSnapshotPtr, TableStateSnapshot> IcebergMetadata::getRelevantState(const ContextPtr & context, bool force_fetch_latest_metadata) const
{
    const auto [metadata_version, metadata_file_path, compression_method] = getLatestOrExplicitMetadataFileAndVersion(
        object_storage,
        persistent_components.table_path,
        data_lake_settings,
        persistent_components.metadata_cache,
        context,
        log.get(),
        persistent_components.table_uuid,
        force_fetch_latest_metadata);
    return getState(context, metadata_file_path, metadata_version);
}

IcebergMetadata::IcebergMetadata(
    ObjectStoragePtr object_storage_,
    StorageObjectStorageConfigurationPtr configuration_,
    const ContextPtr & context_,
    IcebergMetadataFilesCachePtr cache_ptr)
    : log(getLogger("IcebergMetadata"))
    , object_storage(std::move(object_storage_))
    , persistent_components(initializePersistentTableComponents(configuration_, cache_ptr, context_))
    , data_lake_settings(configuration_->getDataLakeSettings())
    , write_format(configuration_->getFormat())
{
    /// TODO: for now it's okay to start/stop the task via constructor/destructor. Once refactored, we'd need to plumb startup/shutdown and schedule the task from there
    if (cache_ptr && data_lake_settings[DataLakeStorageSetting::iceberg_metadata_async_prefetch_period_ms] != 0)
    {
        background_metadata_prefetch_task = context_->getIcebergSchedulePool().createTask(
            StorageID("", persistent_components.table_uuid ? *persistent_components.table_uuid : persistent_components.table_path),
            "backgroundMetadataPrefetcherThread",
            [this]
            {
                this->backgroundMetadataPrefetcherThread();
            }
        );
        background_metadata_prefetch_task->activateAndSchedule();
    }
}

IcebergMetadata::~IcebergMetadata()
{
    if (background_metadata_prefetch_task)
        background_metadata_prefetch_task->deactivate();
}

void IcebergMetadata::backgroundMetadataPrefetcherThread()
{
    size_t interval = data_lake_settings[DataLakeStorageSetting::iceberg_metadata_async_prefetch_period_ms];
    SCOPE_EXIT({
        background_metadata_prefetch_task->scheduleAfter(interval);
    });

    try
    {
        if (!Context::getGlobalContextInstance())
        {
            /// Should never happen, but if seen, this is clear indicator that the task should be started/stopped via startup/shutdown mechanism (check TODOs above)
            LOG_DEBUG(log, "backgroundMetadataPrefetcherThread: no global context - skipping");
            return;
        }

        Stopwatch watch;

        /// TODO: also we'd want to run all these download operations as separate scheduled tasks - to parallelize it and
        ///       to prevent running a heavy multi-step operation as: IO > deserialization > parsing > IO > deserialization > parsing > ...
        ///       We'll be able to achieve that after getting asyncIterator refactoring

        /// first, we fetch the latest metadata version and cache it;
        /// as a part of the same method, we download metadata.json of the latest metadata version
        /// and after parsing it, we fetch manifest lists, parse and cache them
        auto ctx = Context::getGlobalContextInstance()->getBackgroundContext();
        auto [actual_data_snapshot, actual_table_state_snapshot] = getRelevantState(ctx, true);
        if (actual_data_snapshot)
        {
            for (const auto & entry : actual_data_snapshot->manifest_list_entries)
            {
                /// second, we fetch, parse and cache each manifest file
                auto manifest_file_ptr = getManifestFileEntriesHandle(
                    object_storage, persistent_components, ctx, log, entry, actual_table_state_snapshot.schema_id);
            }
        }

        LOG_TRACE(log, "backgroundMetadataPrefetcherThread: interval={} prefetch_time_ms={} table_path={}/{} latest_metadata={}/{}",
                 interval,
                 watch.elapsedMilliseconds(),
                 persistent_components.table_path,
                 persistent_components.table_uuid ? *(persistent_components.table_uuid) : "no_uuid",
                 actual_table_state_snapshot.metadata_version,
                 actual_table_state_snapshot.metadata_file_path);
    }
    catch (...)
    {
        DB::tryLogCurrentException(log);
    }
}

Int32 IcebergMetadata::parseTableSchema(
    const Poco::JSON::Object::Ptr & metadata_object,
    IcebergSchemaProcessor & schema_processor,
    ContextPtr context_,
    LoggerPtr metadata_logger)
{
    const auto format_version = metadata_object->getValue<Int32>(f_format_version);
    if (format_version == 2)
    {
        auto [schema, current_schema_id] = parseTableSchemaV2Method(metadata_object);
        schema_processor.addIcebergTableSchema(schema, context_);
        return current_schema_id;
    }
    else
    {
        try
        {
            auto [schema, current_schema_id] = parseTableSchemaV1Method(metadata_object);
            schema_processor.addIcebergTableSchema(schema, context_);
            return current_schema_id;
        }
        catch (const Exception & first_error)
        {
            if (first_error.code() != ErrorCodes::BAD_ARGUMENTS)
                throw;
            try
            {
                auto [schema, current_schema_id] = parseTableSchemaV2Method(metadata_object);
                schema_processor.addIcebergTableSchema(schema, context_);
                LOG_WARNING(
                    metadata_logger,
                    "Iceberg table schema was parsed using v2 specification, but it was impossible to parse it using v1 "
                    "specification. Be "
                    "aware that you Iceberg writing engine violates Iceberg specification. Error during parsing {}",
                    first_error.displayText());
                return current_schema_id;
            }
            catch (const Exception & second_error)
            {
                if (first_error.code() != ErrorCodes::BAD_ARGUMENTS)
                    throw;
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Cannot parse Iceberg table schema both with v1 and v2 methods. Old method error: {}. New method error: {}",
                    first_error.displayText(),
                    second_error.displayText());
            }
        }
    }
}

Poco::JSON::Object::Ptr traverseMetadataAndFindNecessarySnapshotObject(
    Poco::JSON::Object::Ptr metadata_object,
    Int64 snapshot_id,
    IcebergSchemaProcessorPtr schema_processor,
    ContextPtr local_context)
{
    if (!metadata_object->has(f_snapshots))
        throw Exception(ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "No snapshot set found in metadata for iceberg file");
    auto schemas = metadata_object->get(f_schemas).extract<Poco::JSON::Array::Ptr>();
    for (UInt32 j = 0; j < schemas->size(); ++j)
    {
        auto schema = schemas->getObject(j);
        schema_processor->addIcebergTableSchema(schema, local_context);
    }
    Poco::JSON::Object::Ptr current_snapshot = nullptr;
    auto snapshots = metadata_object->get(f_snapshots).extract<Poco::JSON::Array::Ptr>();
    for (size_t i = 0; i < snapshots->size(); ++i)
    {
        const auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
        auto current_snapshot_id = snapshot->getValue<Int64>(f_metadata_snapshot_id);
        auto current_schema_id = snapshot->getValue<Int32>(f_schema_id);
        schema_processor->registerSnapshotWithSchemaId(current_snapshot_id, current_schema_id);
        if (snapshot->getValue<Int64>(f_metadata_snapshot_id) == snapshot_id)
        {
            current_snapshot = snapshot;
        }
    }
    return current_snapshot;
}

IcebergDataSnapshotPtr IcebergMetadata::createIcebergDataSnapshotFromSnapshotJSON(
    Poco::JSON::Object::Ptr snapshot_object, Int64 snapshot_id, ContextPtr local_context) const
{
    if (!snapshot_object->has(f_manifest_list))
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "Snapshot object doesn't contain a manifest list path for snapshot with id `{}`",
            snapshot_id);
    String manifest_list_file_path = snapshot_object->getValue<String>(f_manifest_list);
    std::optional<size_t> total_rows;
    std::optional<size_t> total_bytes;
    std::optional<size_t> total_position_deletes;

    if (snapshot_object->has(f_summary))
    {
        auto summary_object = snapshot_object->get(f_summary).extract<Poco::JSON::Object::Ptr>();
        if (summary_object->has(f_total_records))
            total_rows = summary_object->getValue<Int64>(f_total_records);

        if (summary_object->has(f_total_files_size))
            total_bytes = summary_object->getValue<Int64>(f_total_files_size);

        if (summary_object->has(f_total_position_deletes))
        {
            total_position_deletes = summary_object->getValue<Int64>(f_total_position_deletes);
        }
    }

    if (!snapshot_object->has(f_schema_id))
        throw Exception(ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "No schema id found for snapshot id `{}`", snapshot_id);
    Int32 schema_id = snapshot_object->getValue<Int32>(f_schema_id);


    return std::make_shared<IcebergDataSnapshot>(
        getManifestList(
            object_storage,
            persistent_components,
            local_context,
            getProperFilePathFromMetadataInfo(
                manifest_list_file_path, persistent_components.table_path, persistent_components.table_location),
            log),
        snapshot_id,
        schema_id,
        total_rows,
        total_bytes,
        total_position_deletes);
}

IcebergDataSnapshotPtr
IcebergMetadata::getIcebergDataSnapshot(Poco::JSON::Object::Ptr metadata_object, Int64 snapshot_id, ContextPtr local_context) const
{
    auto object = traverseMetadataAndFindNecessarySnapshotObject(
        metadata_object,
        snapshot_id,
        persistent_components.schema_processor,
        local_context);
    if (!object)
        throw Exception(ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "No snapshot found for id `{}`", snapshot_id);

    return createIcebergDataSnapshotFromSnapshotJSON(object, snapshot_id, local_context);
}

bool IcebergMetadata::optimize(
    const StorageMetadataPtr & metadata_snapshot, ContextPtr context, const std::optional<FormatSettings> & format_settings)
{
    if (context->getSettingsRef()[Setting::allow_experimental_iceberg_compaction])
    {
        const auto sample_block = std::make_shared<const Block>(metadata_snapshot->getSampleBlock());
        auto snapshots_info = getHistory(context);
        compactIcebergTable(
            snapshots_info,
            persistent_components,
            object_storage,
            data_lake_settings,
            format_settings,
            sample_block,
            context,
            write_format);
        return true;
    }
    else
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Enable 'allow_experimental_iceberg_compaction' setting to call optimize for iceberg tables.");
    }
}

std::pair<IcebergDataSnapshotPtr, Int32>
IcebergMetadata::getStateImpl(const ContextPtr & local_context, Poco::JSON::Object::Ptr metadata_object) const
{
    std::optional<String> manifest_list_file;

    bool timestamp_changed = local_context->getSettingsRef()[Setting::iceberg_timestamp_ms].changed;
    bool snapshot_id_changed = local_context->getSettingsRef()[Setting::iceberg_snapshot_id].changed;
    if (timestamp_changed && snapshot_id_changed)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Time travel with timestamp and snapshot id for iceberg table by path {} cannot be changed simultaneously",
            persistent_components.table_path);
    }
    if (timestamp_changed)
    {
        if (!metadata_object->has(f_snapshot_log))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "No snapshot log found in metadata for iceberg table {} so it is impossible to get relevant snapshot id using timestamp",
                persistent_components.table_path);
        std::optional<Int64> current_snapshot_id = std::nullopt;
        {
            Int64 closest_timestamp = 0;
            Int64 query_timestamp = local_context->getSettingsRef()[Setting::iceberg_timestamp_ms];
            auto snapshot_log = metadata_object->get(f_snapshot_log).extract<Poco::JSON::Array::Ptr>();
            for (size_t i = 0; i < snapshot_log->size(); ++i)
            {
                const auto snapshot = snapshot_log->getObject(static_cast<UInt32>(i));
                Int64 snapshot_timestamp = snapshot->getValue<Int64>(f_timestamp_ms);
                if (snapshot_timestamp <= query_timestamp && snapshot_timestamp > closest_timestamp)
                {
                    closest_timestamp = snapshot_timestamp;
                    current_snapshot_id = snapshot->getValue<Int64>(f_metadata_snapshot_id);
                }
            }
        }
        if (!current_snapshot_id)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "No snapshot found in snapshot log before requested timestamp for iceberg table {}",
                persistent_components.table_path);
        auto data_snapshot = getIcebergDataSnapshot(metadata_object, *current_snapshot_id, local_context);
        return {data_snapshot, data_snapshot->schema_id_on_snapshot_commit};
    }
    else if (snapshot_id_changed)
    {
        Int64 current_snapshot_id = local_context->getSettingsRef()[Setting::iceberg_snapshot_id];
        auto data_snapshot = getIcebergDataSnapshot(metadata_object, current_snapshot_id, local_context);
        return {data_snapshot, data_snapshot->schema_id_on_snapshot_commit};
    }
    else
    {
        auto schema_id = parseTableSchema(metadata_object, *persistent_components.schema_processor, local_context, log);
        if (!metadata_object->has(f_current_snapshot_id))
        {
            return {nullptr, schema_id};
        }
        auto current_snapshot_id = metadata_object->getValue<Int64>(f_current_snapshot_id);
        if (current_snapshot_id < 0)
        {
            return {nullptr, schema_id};
        }
        auto data_snapshot = getIcebergDataSnapshot(metadata_object, current_snapshot_id, local_context);
        return {data_snapshot, schema_id};
    }
}

std::pair<IcebergDataSnapshotPtr, TableStateSnapshot>
IcebergMetadata::getState(const ContextPtr & local_context, const String & metadata_path, Int32 metadata_version) const
{
    IcebergDataSnapshotPtr data_snapshot;
    TableStateSnapshot table_state_snapshot;
    auto metadata_object = getMetadataJSONObject(
        metadata_path, object_storage, persistent_components.metadata_cache, local_context, log, persistent_components.metadata_compression_method, persistent_components.table_uuid);

    auto dump_metadata = [&]()->String { return dumpMetadataObjectToString(metadata_object); };
    insertRowToLogTable(
        local_context,
        dump_metadata,
        DB::IcebergMetadataLogLevel::Metadata,
        persistent_components.table_path,
        metadata_path,
        std::nullopt,
        std::nullopt);

    chassert(persistent_components.format_version == metadata_object->getValue<int>(f_format_version));

    std::tie(data_snapshot, table_state_snapshot.schema_id) = getStateImpl(local_context, metadata_object);
    table_state_snapshot.snapshot_id = data_snapshot ? std::optional{data_snapshot->snapshot_id} : std::nullopt;
    table_state_snapshot.metadata_version = metadata_version;
    table_state_snapshot.metadata_file_path = metadata_path;
    return {data_snapshot, table_state_snapshot};
}

std::shared_ptr<NamesAndTypesList> IcebergMetadata::getInitialSchemaByPath(ContextPtr, ObjectInfoPtr object_info) const
{
    IcebergDataObjectInfo * iceberg_object_info = dynamic_cast<IcebergDataObjectInfo *>(object_info.get());
    if (!iceberg_object_info)
        return nullptr;
    /// if we need schema evolution or have equality deletes files, we need to read all the columns.
    return (iceberg_object_info->info.underlying_format_read_schema_id != iceberg_object_info->info.schema_id_relevant_to_iterator
            || (!iceberg_object_info->info.equality_deletes_objects.empty()))
        ? persistent_components.schema_processor->getClickhouseTableSchemaById(iceberg_object_info->info.underlying_format_read_schema_id)
        : nullptr;
}

std::shared_ptr<const ActionsDAG> IcebergMetadata::getSchemaTransformer(ContextPtr context_, ObjectInfoPtr object_info) const
{
    IcebergDataObjectInfo * iceberg_object_info = dynamic_cast<IcebergDataObjectInfo *>(object_info.get());
    if (!iceberg_object_info)
        return nullptr;
    return (iceberg_object_info->info.underlying_format_read_schema_id != iceberg_object_info->info.schema_id_relevant_to_iterator)
        ? persistent_components.schema_processor->getSchemaTransformationDagByIds(
              context_,
              iceberg_object_info->info.underlying_format_read_schema_id,
              iceberg_object_info->info.schema_id_relevant_to_iterator)
        : nullptr;
}

void IcebergMetadata::mutate(
    const MutationCommands & commands,
    StorageObjectStorageConfigurationPtr configuration,
    ContextPtr context,
    const StorageID & storage_id,
    StorageMetadataPtr metadata_snapshot,
    std::shared_ptr<DataLake::ICatalog> catalog,
    const std::optional<FormatSettings> & format_settings)
{
    if (!context->getSettingsRef()[Setting::allow_insert_into_iceberg].value)
    {
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "Iceberg mutations is experimental. "
            "To allow its usage, enable setting allow_insert_into_iceberg");
    }

    DB::Iceberg::mutate(
        commands,
        context,
        metadata_snapshot,
        storage_id,
        object_storage,
        data_lake_settings,
        persistent_components,
        write_format,
        format_settings,
        catalog,
        configuration->getTypeName(),
        configuration->getNamespace()
    );
}

void IcebergMetadata::checkMutationIsPossible(const MutationCommands & commands)
{
    for (const auto & command : commands)
        if (command.type != MutationCommand::DELETE && command.type != MutationCommand::UPDATE)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Iceberg supports only DELETE and UPDATE mutations");
}

void IcebergMetadata::checkAlterIsPossible(const AlterCommands & commands)
{
    for (const auto & command : commands)
    {
        if (command.type != AlterCommand::Type::ADD_COLUMN && command.type != AlterCommand::Type::DROP_COLUMN
            && command.type != AlterCommand::Type::MODIFY_COLUMN && command.type != AlterCommand::Type::RENAME_COLUMN)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter of type '{}' is not supported by Iceberg storage", command.type);

        if (command.type == AlterCommand::Type::MODIFY_COLUMN && command.to_remove != AlterCommand::RemoveProperty::NO_PROPERTY)
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Removing column property '{}' from column '{}' is not supported by Iceberg storage", command.to_remove, command.column_name);

        if (command.type == AlterCommand::Type::MODIFY_COLUMN && !command.data_type)
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Modifying column '{}' without changing its type is not supported by Iceberg storage", command.column_name);
    }
}

void IcebergMetadata::alter(const AlterCommands & params, ContextPtr context)
{
    if (!context->getSettingsRef()[Setting::allow_insert_into_iceberg].value)
    {
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "Alter iceberg is experimental. "
            "To allow its usage, enable setting allow_insert_into_iceberg");
    }

    Iceberg::alter(params, context, object_storage, data_lake_settings, persistent_components, write_format);
}

static Pipe expireSnapshotsResultToPipe(const Iceberg::ExpireSnapshotsResult & result)
{
    Block header{
        ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "metric_name"),
        ColumnWithTypeAndName(std::make_shared<DataTypeInt64>(), "metric_value"),
    };

    MutableColumns columns = header.cloneEmptyColumns();

    auto add = [&](const char * name, Int64 value)
    {
        columns[0]->insert(String(name));
        columns[1]->insert(value);
    };

    add("deleted_data_files_count", result.deleted_data_files_count);
    add("deleted_position_delete_files_count", result.deleted_position_delete_files_count);
    add("deleted_equality_delete_files_count", result.deleted_equality_delete_files_count);
    add("deleted_manifest_files_count", result.deleted_manifest_files_count);
    add("deleted_manifest_lists_count", result.deleted_manifest_lists_count);
    add("deleted_statistics_files_count", result.deleted_statistics_files_count);
    add("dry_run", result.dry_run ? 1 : 0);

    const size_t rows = columns[0]->size();
    Chunk chunk(std::move(columns), rows);
    return Pipe(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(std::move(header)), std::move(chunk)));
}

Pipe IcebergMetadata::executeCommand(
    const String & command_name,
    const ASTPtr & args,
    ObjectStoragePtr object_storage_,
    StorageObjectStorageConfigurationPtr configuration_,
    std::shared_ptr<DataLake::ICatalog> catalog_,
    ContextPtr context,
    const StorageID & storage_id)
{
    if (!context->getSettingsRef()[Setting::allow_insert_into_iceberg].value)
    {
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "Iceberg EXECUTE commands are experimental. "
            "To allow their usage, enable setting allow_insert_into_iceberg");
    }

    if (command_name == "expire_snapshots")
    {
        if (!context->getSettingsRef()[Setting::allow_experimental_expire_snapshots].value)
        {
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "Iceberg expire_snapshots is experimental. "
                "To allow its usage, enable setting allow_experimental_expire_snapshots");
        }

        auto options = parseExpireSnapshotsOptions(args, context);

        auto result = Iceberg::expireSnapshots(
            options,
            context,
            object_storage_,
            data_lake_settings,
            persistent_components,
            write_format,
            catalog_,
            configuration_->getTypeName(),
            configuration_->getNamespace(),
            storage_id.getTableName());

        return expireSnapshotsResultToPipe(result);
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown EXECUTE command '{}' for Iceberg table", command_name);
    }
}

void IcebergMetadata::createInitial(
    const ObjectStoragePtr & object_storage,
    const StorageObjectStorageConfigurationWeakPtr & configuration,
    const ContextPtr & local_context,
    const std::optional<ColumnsDescription> & columns,
    ASTPtr partition_by,
    ASTPtr order_by,
    bool if_not_exists,
    std::shared_ptr<DataLake::ICatalog> catalog,
    const StorageID & table_id_)
{
    auto configuration_ptr = configuration.lock();
    if (!configuration_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to create Iceberg table, but storage configuration is expired");

    std::vector<String> metadata_files;
    try
    {
        metadata_files = listFiles(*object_storage, configuration_ptr->getPathForRead().path, "metadata", ".metadata.json");
    }
    catch (const Exception & ex)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "NoSuchBucket: {}", ex.what());
    }
    if (!metadata_files.empty())
    {
        if (if_not_exists)
            return;
        else
            throw Exception(
                ErrorCodes::TABLE_ALREADY_EXISTS, "Iceberg table with path {} already exists", configuration_ptr->getPathForRead().path);
    }

    String location_path = configuration_ptr->getRawPath().path;
    if (local_context->getSettingsRef()[Setting::write_full_path_in_iceberg_metadata].value)
        location_path
            = configuration_ptr->getTypeName() + "://" + configuration_ptr->getNamespace() + "/" + configuration_ptr->getRawPath().path;
    auto [metadata_content_object, metadata_content] = createEmptyMetadataFile(
        location_path, *columns, partition_by, order_by, local_context, configuration_ptr->getDataLakeSettings()[DataLakeStorageSetting::iceberg_format_version]);
    auto compression_method_str = local_context->getSettingsRef()[Setting::iceberg_metadata_compression_method].value;
    auto compression_method = chooseCompressionMethod(compression_method_str, compression_method_str);

    auto compression_suffix = compression_method_str;
    if (!compression_suffix.empty())
        compression_suffix = "." + compression_suffix;

    auto filename = fmt::format("{}metadata/v1{}.metadata.json", configuration_ptr->getRawPath().path, compression_suffix);

    try
    {
        writeMessageToFile(metadata_content, filename, object_storage, local_context, "*", "", compression_method);
    }
    catch (const Exception & e)
    {
        /// The write uses `If-None-Match: *`, so S3 returns PreconditionFailed when the metadata file
        /// already exists (e.g. leftover data after `DROP TABLE` with `iceberg_delete_data_on_drop` off,
        /// or a concurrent creation). When `IF NOT EXISTS` was specified, this is expected.
        if (if_not_exists && e.code() == ErrorCodes::S3_ERROR
            && e.message().find("PreconditionFailed") != String::npos)
            return;
        throw;
    }

    if (configuration_ptr->getDataLakeSettings()[DataLakeStorageSetting::iceberg_use_version_hint].value)
    {
        auto filename_version_hint = configuration_ptr->getRawPath().path + "metadata/version-hint.text";
        writeMessageToFile(filename, filename_version_hint, object_storage, local_context, "*", "");
    }

    if (catalog)
    {
        auto catalog_filename = configuration_ptr->getTypeName() + "://" + configuration_ptr->getNamespace() + "/"
            + configuration_ptr->getRawPath().path + "metadata/v1.metadata.json";
        const auto & [namespace_name, table_name] = DataLake::parseTableName(table_id_.getTableName());
        catalog->createTable(namespace_name, table_name, catalog_filename, metadata_content_object);
    }
}

Iceberg::IcebergDataSnapshotPtr IcebergMetadata::getRelevantDataSnapshotFromTableStateSnapshot(
    Iceberg::TableStateSnapshot table_state_snapshot, ContextPtr local_context) const
{
    IcebergDataSnapshotPtr data_snapshot;
    auto metadata_object = getMetadataJSONObject(
        table_state_snapshot.metadata_file_path,
        object_storage,
        persistent_components.metadata_cache,
        local_context,
        log,
        persistent_components.metadata_compression_method,
        persistent_components.table_uuid);
    if (!table_state_snapshot.snapshot_id.has_value())
        return nullptr;
    Poco::JSON::Object::Ptr snapshot_object = traverseMetadataAndFindNecessarySnapshotObject(
        metadata_object, *table_state_snapshot.snapshot_id, persistent_components.schema_processor, local_context);

    return createIcebergDataSnapshotFromSnapshotJSON(snapshot_object, *table_state_snapshot.snapshot_id, local_context);
}

DataLakeMetadataPtr IcebergMetadata::create(
    const ObjectStoragePtr & object_storage,
    const StorageObjectStorageConfigurationWeakPtr & configuration,
    const ContextPtr & local_context)
{
    auto configuration_ptr = configuration.lock();
    if (!configuration_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to create Iceberg table, but storage configuration is expired");

    auto log = getLogger("IcebergMetadata");

    IcebergMetadataFilesCachePtr cache_ptr = nullptr;

    if (local_context->getSettingsRef()[Setting::use_iceberg_metadata_files_cache])
        cache_ptr = local_context->getIcebergMetadataFilesCache();
    else
        LOG_TRACE(
            log, "Not using in-memory cache for iceberg metadata files, because the setting use_iceberg_metadata_files_cache is false.");

    return std::make_unique<IcebergMetadata>(object_storage, configuration_ptr, local_context, cache_ptr);
}

IcebergMetadata::IcebergHistory IcebergMetadata::getHistory(ContextPtr local_context) const
{
    const auto [metadata_version, metadata_file_path, compression_method] = getLatestOrExplicitMetadataFileAndVersion(
        object_storage,
        persistent_components.table_path,
        data_lake_settings,
        persistent_components.metadata_cache,
        local_context,
        log.get(),
        persistent_components.table_uuid);

    auto metadata_object
        = getMetadataJSONObject(metadata_file_path, object_storage, persistent_components.metadata_cache, local_context, log, compression_method, persistent_components.table_uuid);
    chassert(persistent_components.format_version == metadata_object->getValue<int>(f_format_version));

    /// History
    std::vector<Iceberg::IcebergHistoryRecord> iceberg_history;

    auto snapshots = metadata_object->get(f_snapshots).extract<Poco::JSON::Array::Ptr>();
    auto snapshot_logs = metadata_object->get(f_snapshot_log).extract<Poco::JSON::Array::Ptr>();

    std::vector<Int64> ancestors;
    std::map<Int64, Int64> parents_list;
    for (size_t i = 0; i < snapshots->size(); ++i)
    {
        const auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
        auto snapshot_id = snapshot->getValue<Int64>(f_metadata_snapshot_id);

        if (snapshot->has(f_parent_snapshot_id) && !snapshot->isNull(f_parent_snapshot_id))
            parents_list[snapshot_id] = snapshot->getValue<Int64>(f_parent_snapshot_id);
        else
            parents_list[snapshot_id] = 0;
    }

    /// For empty table we may have no snapshots
    if (metadata_object->has(f_current_snapshot_id))
    {
        auto current_snapshot_id = metadata_object->getValue<Int64>(f_current_snapshot_id);
        /// Add current snapshot-id to ancestors list
        ancestors.push_back(current_snapshot_id);
        while (parents_list[current_snapshot_id] != 0)
        {
            ancestors.push_back(parents_list[current_snapshot_id]);
            current_snapshot_id = parents_list[current_snapshot_id];
        }
    }

    for (size_t i = 0; i < snapshots->size(); ++i)
    {
        IcebergHistoryRecord history_record;

        const auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
        history_record.snapshot_id = snapshot->getValue<Int64>(f_metadata_snapshot_id);
        history_record.manifest_list_path = snapshot->getValue<String>(f_manifest_list);
        const auto summary = snapshot->getObject(f_summary);
        if (summary->has(f_added_data_files))
            history_record.added_files = summary->getValue<Int32>(f_added_data_files);
        if (summary->has(f_added_records))
            history_record.added_records = summary->getValue<Int32>(f_added_records);
        history_record.added_files_size = summary->getValue<Int32>(f_added_files_size);
        history_record.num_partitions = summary->getValue<Int32>(f_changed_partition_count);

        if (snapshot->has(f_parent_snapshot_id) && !snapshot->isNull(f_parent_snapshot_id))
            history_record.parent_id = snapshot->getValue<Int64>(f_parent_snapshot_id);
        else
            history_record.parent_id = 0;

        for (size_t j = 0; j < snapshot_logs->size(); ++j)
        {
            const auto snapshot_log = snapshot_logs->getObject(static_cast<UInt32>(j));
            if (snapshot_log->getValue<Int64>(f_metadata_snapshot_id) == history_record.snapshot_id)
            {
                auto value = snapshot_log->getValue<std::string>(f_timestamp_ms);
                ReadBufferFromString in(value);
                DateTime64 time = 0;
                readDateTime64Text(time, 6, in);

                history_record.made_current_at = time;
                break;
            }
        }

        if (std::find(ancestors.begin(), ancestors.end(), history_record.snapshot_id) != ancestors.end())
            history_record.is_current_ancestor = true;
        else
            history_record.is_current_ancestor = false;

        iceberg_history.push_back(history_record);
    }

    return iceberg_history;
}

bool IcebergMetadata::isDataSortedBySortingKey(StorageMetadataPtr storage_metadata_snapshot, ContextPtr context) const
{
    if (!storage_metadata_snapshot->hasSortingKey())
        return false;

    auto sorting_key = storage_metadata_snapshot->getSortingKey();
    if (!sorting_key.sort_order_id.has_value())
        return false;

    auto table_state_snapshot = extractIcebergSnapshotIdFromMetadataObject(storage_metadata_snapshot);
    if (table_state_snapshot == nullptr)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Can't extract iceberg table state from storage snapshot for table location {}",
            persistent_components.table_location);
    }

    /// Empty table is sorted table
    auto data_snapshot = getRelevantDataSnapshotFromTableStateSnapshot(*table_state_snapshot, context);
    if (!data_snapshot)
        return true;

    for (const auto & manifest_list_entry : data_snapshot->manifest_list_entries)
    {
        auto files_handle = getManifestFileEntriesHandle(
            object_storage, persistent_components, context, log, manifest_list_entry, table_state_snapshot->schema_id);

        if (!files_handle.areAllDataFilesSortedBySortOrderID(sorting_key.sort_order_id.value()))
            return false;
    }
    return true;
}

std::optional<size_t> IcebergMetadata::totalRows(ContextPtr local_context) const
{
    auto [actual_data_snapshot, actual_table_state_snapshot] = getRelevantState(local_context);

    if (!actual_data_snapshot)
    {
        ProfileEvents::increment(ProfileEvents::IcebergTrivialCountOptimizationApplied);
        return 0;
    }


    /// All these "hints" with total rows or bytes are optional both in
    /// metadata files and in manifest files, so we try all of them one by one
    if (auto total_rows = actual_data_snapshot->getTotalRows(); total_rows.has_value())
    {
        ProfileEvents::increment(ProfileEvents::IcebergTrivialCountOptimizationApplied);
        return total_rows;
    }

    Int64 result = 0;
    for (const auto & manifest_list_entry : actual_data_snapshot->manifest_list_entries)
    {
        auto manifest_file_ptr = getManifestFileEntriesHandle(
            object_storage, persistent_components, local_context, log, manifest_list_entry, actual_table_state_snapshot.schema_id);
        auto data_count = manifest_file_ptr.getRowsCountInAllFilesExcludingDeleted(FileContentType::DATA);
        auto position_deletes_count = manifest_file_ptr.getRowsCountInAllFilesExcludingDeleted(FileContentType::POSITION_DELETE);
        if (!data_count.has_value() || !position_deletes_count.has_value())
            return {};

        result += data_count.value() - position_deletes_count.value();
    }

    ProfileEvents::increment(ProfileEvents::IcebergTrivialCountOptimizationApplied);
    return result;
}

std::optional<size_t> IcebergMetadata::totalBytes(ContextPtr local_context) const
{
    auto [actual_data_snapshot, actual_table_state_snapshot] = getRelevantState(local_context);

    if (!actual_data_snapshot)
        return 0;

    /// All these "hints" with total rows or bytes are optional both in
    /// metadata files and in manifest files, so we try all of them one by one
    if (actual_data_snapshot->total_bytes.has_value())
        return actual_data_snapshot->total_bytes;

    Int64 result = 0;
    for (const auto & manifest_list_entry : actual_data_snapshot->manifest_list_entries)
    {
        auto manifest_file_ptr = getManifestFileEntriesHandle(
            object_storage, persistent_components, local_context, log, manifest_list_entry, actual_table_state_snapshot.schema_id);
        auto count = manifest_file_ptr.getBytesCountInAllDataFilesExcludingDeleted();
        if (!count.has_value())
            return {};

        result += count.value();
    }

    return result;
}

std::optional<String> IcebergMetadata::partitionKey(ContextPtr context) const
{
    auto [actual_data_snapshot, actual_table_state_snapshot] = getRelevantState(context);
    if (!actual_data_snapshot)
        return std::nullopt;
    return getPartitionKey(context, actual_table_state_snapshot);
}

std::optional<String> IcebergMetadata::sortingKey(ContextPtr context) const
{
    auto [actual_data_snapshot, actual_table_state_snapshot] = getRelevantState(context);
    if (!actual_data_snapshot)
        return std::nullopt;
    auto metadata_object = getMetadataJSONObject(
        actual_table_state_snapshot.metadata_file_path,
        object_storage,
        persistent_components.metadata_cache,
        context,
        log,
        persistent_components.metadata_compression_method,
        persistent_components.table_uuid);
    auto [schema, current_schema_id] = parseTableSchemaV2Method(metadata_object);
    const auto & ch_schema = *persistent_components.schema_processor->getClickhouseTableSchemaById(current_schema_id);
    auto display = getSortingKeyDisplayStringFromMetadata(metadata_object, ch_schema);
    if (display)
        return display;
    auto key = getSortingKey(context, actual_table_state_snapshot);
    if (!key.expression_list_ast)
        return std::nullopt;
    return format({context, *key.expression_list_ast});
}


ObjectIterator IcebergMetadata::iterate(
    const ActionsDAG * filter_dag,
    FileProgressCallback callback,
    size_t /* list_batch_size */,
    StorageMetadataPtr storage_metadata,
    ContextPtr local_context) const
{
    auto iceberg_table_state = extractIcebergSnapshotIdFromMetadataObject(storage_metadata);
    if (iceberg_table_state == nullptr)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Can't extract iceberg table state from storage snapshot for table location {}",
            persistent_components.table_location);
    }

    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::IcebergIteratorInitializationMicroseconds);

    return std::make_shared<IcebergIterator>(
        object_storage,
        local_context,
        filter_dag,
        callback,
        iceberg_table_state,
        getRelevantDataSnapshotFromTableStateSnapshot(*iceberg_table_state, local_context),
        persistent_components);
}

NamesAndTypesList IcebergMetadata::getTableSchema(ContextPtr local_context) const
{
    auto [actual_data_snapshot, actual_table_state_snapshot] = getRelevantState(local_context);
    return *persistent_components.schema_processor->getClickhouseTableSchemaById(actual_table_state_snapshot.schema_id);
}

std::optional<DataLakeTableStateSnapshot> IcebergMetadata::getTableStateSnapshot(ContextPtr local_context) const
{
    auto [actual_data_snapshot, actual_table_state_snapshot] = getRelevantState(local_context);
    return DataLakeTableStateSnapshot{actual_table_state_snapshot};
}

std::unique_ptr<StorageInMemoryMetadata> IcebergMetadata::buildStorageMetadataFromState(
    const DataLakeTableStateSnapshot & state, ContextPtr local_context) const
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::IcebergMetadataUpdateMicroseconds);
    chassert(std::holds_alternative<Iceberg::TableStateSnapshot>(state));
    const auto & iceberg_state = std::get<Iceberg::TableStateSnapshot>(state);
    auto result = std::make_unique<StorageInMemoryMetadata>();
    result->setColumns(
        ColumnsDescription{*persistent_components.schema_processor->getClickhouseTableSchemaById(iceberg_state.schema_id)});
    result->setDataLakeTableState(state);
    result->sorting_key = getSortingKey(local_context, iceberg_state);
    return result;
}

bool IcebergMetadata::shouldReloadSchemaForConsistency(ContextPtr) const
{
    return true;
}

void IcebergMetadata::modifyFormatSettings(FormatSettings & format_settings, const Context & local_context) const
{
    if (!local_context.getSettingsRef()[Setting::use_roaring_bitmap_iceberg_positional_deletes].value)
        /// IcebergStreamingPositionDeleteTransform requires increasing row numbers from both the
        /// data reader and the deletes reader.
        format_settings.parquet.preserve_order = true;
}

void IcebergMetadata::addDeleteTransformers(
    ObjectInfoPtr object_info,
    QueryPipelineBuilder & builder,
    const std::optional<FormatSettings> & format_settings,
    FormatParserSharedResourcesPtr parser_shared_resources,
    ContextPtr local_context) const
{
    auto iceberg_object_info = std::dynamic_pointer_cast<IcebergDataObjectInfo>(object_info);
    if (!iceberg_object_info)
        return;

    if (!iceberg_object_info->info.position_deletes_objects.empty())
    {
        LOG_DEBUG(log, "Constructing filter transform for position delete, there are {} delete objects", iceberg_object_info->info.position_deletes_objects.size());
        builder.addSimpleTransform(
            [&](const SharedHeader & header)
            { return iceberg_object_info->getPositionDeleteTransformer(object_storage, header, format_settings, parser_shared_resources, local_context); });
    }
    const auto & delete_files = iceberg_object_info->info.equality_deletes_objects;
    if (!delete_files.empty())
        LOG_DEBUG(log, "Constructing filter transform for equality delete, there are {} delete files", delete_files.size());
    for (const EqualityDeleteObject & delete_file : delete_files)
    {
        auto simple_transform_adder = [&](const SharedHeader & header)
        {
            /// get header of delete file
            Block delete_file_header;
            RelativePathWithMetadata delete_file_object(delete_file.file_path);
            {
                auto schema_read_buffer = createReadBuffer(delete_file_object, object_storage, local_context, log);
                auto schema_reader = FormatFactory::instance().getSchemaReader(delete_file.file_format, *schema_read_buffer, local_context);
                auto columns_with_names = schema_reader->readSchema();
                ColumnsWithTypeAndName initial_header_data;
                for (const auto & elem : columns_with_names)
                {
                    initial_header_data.push_back(ColumnWithTypeAndName(elem.type, elem.name));
                }
                delete_file_header = Block(initial_header_data);
            }
            /// with equality ids, we can know which columns should be deleted, here we calculate the indexes.
            const std::vector<Int32> & equality_ids = *(delete_file.equality_ids);
            Block block_for_set;
            std::vector<size_t> equality_indexes_delete_file;
            for (Int32 col_id : equality_ids)
            {
                NameAndTypePair name_and_type
                    = persistent_components.schema_processor->getFieldCharacteristics(delete_file.schema_id, col_id);
                block_for_set.insert(ColumnWithTypeAndName(name_and_type.type, name_and_type.name));
                equality_indexes_delete_file.push_back(delete_file_header.getPositionByName(name_and_type.name));
            }
            /// Then we read the content of the delete file.
            auto mutable_columns_for_set = block_for_set.cloneEmptyColumns();
            std::unique_ptr<ReadBuffer> data_read_buffer = createReadBuffer(delete_file_object, object_storage, local_context, log);
            CompressionMethod compression_method = chooseCompressionMethod(delete_file.file_path, "auto");
            auto delete_format = FormatFactory::instance().getInput(
                delete_file.file_format,
                *data_read_buffer,
                delete_file_header,
                local_context,
                local_context->getSettingsRef()[DB::Setting::max_block_size],
                format_settings,
                parser_shared_resources,
                nullptr,
                true,
                compression_method);
            /// only get the delete columns and construct a set by 'block_for_set'
            while (true)
            {
                Chunk delete_chunk = delete_format->read();
                if (!delete_chunk)
                    break;
                size_t rows = delete_chunk.getNumRows();
                Columns delete_columns = delete_chunk.detachColumns();
                for (size_t i = 0; i < equality_indexes_delete_file.size(); i++)
                {
                    mutable_columns_for_set[i]->insertRangeFrom(*delete_columns[equality_indexes_delete_file[i]], 0, rows);
                }
            }
            block_for_set.setColumns(std::move(mutable_columns_for_set));
            /// we are constructing a 'not in' expression
            const auto & settings = local_context->getSettingsRef();
            SizeLimits size_limits_for_set
                = {settings[Setting::max_rows_in_set], settings[Setting::max_bytes_in_set], settings[Setting::set_overflow_mode]};
            FutureSetPtr future_set = std::make_shared<FutureSetFromTuple>(
                CityHash_v1_0_2::uint128(), nullptr, block_for_set.getColumnsWithTypeAndName(), true, size_limits_for_set);
            ColumnPtr set_col = ColumnSet::create(1, future_set);
            ActionsDAG dag(header->getColumnsWithTypeAndName());
            /// Construct right argument of 'not in' expression, it is the column set.
            const ActionsDAG::Node * in_rhs_arg = &dag.addColumn({set_col, std::make_shared<DataTypeSet>(), "set column"});
            /// Construct left argument of 'not in' expression
            ActionsDAG::NodeRawConstPtrs left_columns;
            std::unordered_map<std::string_view, const ActionsDAG::Node *> outputs;
            for (const auto & output : dag.getOutputs())
                outputs.emplace(output->result_name, output);
            /// select columns to use in 'notIn' function
            for (Int32 col_id : equality_ids)
            {
                NameAndTypePair name_and_type = persistent_components.schema_processor->getFieldCharacteristics(
                    iceberg_object_info->info.underlying_format_read_schema_id, col_id);
                auto it = outputs.find(name_and_type.name);
                if (it == outputs.end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find column {} in dag outputs", name_and_type.name);
                left_columns.push_back(it->second);
            }
            FunctionOverloadResolverPtr func_tuple_builder
                = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionTuple>());
            const ActionsDAG::Node * in_lhs_arg
                = left_columns.size() == 1 ? left_columns.front() : &dag.addFunction(func_tuple_builder, std::move(left_columns), {});
            /// we got the NOT IN function
            auto func_not_in = FunctionFactory::instance().get("notIn", nullptr);
            const auto & not_in_node = dag.addFunction(func_not_in, {in_lhs_arg, in_rhs_arg}, "notInResult");
            dag.getOutputs().push_back(&not_in_node);
            LOG_DEBUG(log, "Use expression {} in equality deletes", dag.dumpDAG());
            return std::make_shared<FilterTransform>(header, std::make_shared<ExpressionActions>(std::move(dag)), "notInResult", true);
        };
        builder.addSimpleTransform(simple_transform_adder);
    }
}

SinkToStoragePtr IcebergMetadata::write(
    SharedHeader sample_block,
    const StorageID & table_id,
    ObjectStoragePtr /*object_storage*/,
    StorageObjectStorageConfigurationPtr configuration,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr context,
    std::shared_ptr<DataLake::ICatalog> catalog)
{
    if (context->getSettingsRef()[Setting::allow_insert_into_iceberg])
    {
        return std::make_shared<IcebergStorageSink>(object_storage, configuration, format_settings, sample_block, context, catalog, persistent_components, table_id);
    }
    else
    {
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "Insert into iceberg is in beta."
            "To allow its usage, enable setting allow_insert_into_iceberg");
    }
}

void IcebergMetadata::drop(ContextPtr context)
{
    if (context->getSettingsRef()[Setting::iceberg_delete_data_on_drop].value)
    {
        auto files = listFiles(*object_storage, persistent_components.table_path, persistent_components.table_path, "");
        for (const auto & file : files)
            object_storage->removeObjectIfExists(StoredObject(file));
    }
}

ColumnMapperPtr IcebergMetadata::getColumnMapperForObject(ObjectInfoPtr object_info) const
{
    IcebergDataObjectInfo * iceberg_object_info = dynamic_cast<IcebergDataObjectInfo *>(object_info.get());
    if (!iceberg_object_info)
        return nullptr;
    chassert(object_info->getFileFormat().has_value());
    if (Poco::toLower(*object_info->getFileFormat()) != "parquet")
        return nullptr;

    return persistent_components.schema_processor->getColumnMapperById(iceberg_object_info->info.underlying_format_read_schema_id);
}

// (TODO): usage of this function is wrong and should be removed completely (it was done as a temporary workaround for a bug which occurred during supporting concurrent SELECTs).
ColumnMapperPtr IcebergMetadata::getColumnMapperForCurrentSchema(StorageMetadataPtr storage_metadata_snapshot, ContextPtr context) const
{
    if (Poco::toLower(write_format) != "parquet")
        return nullptr;
    auto iceberg_table_state = extractIcebergSnapshotIdFromMetadataObject(storage_metadata_snapshot);
    // This is a temporary cludge for cluster functions because now the state on replicas is not synchronized with the primary state on the coordinator
    if (!iceberg_table_state)
    {
        auto [actual_data_snapshot, actual_table_state_snapshot] = getRelevantState(context);
        iceberg_table_state = std::make_shared<TableStateSnapshot>(actual_table_state_snapshot);
    }
    return persistent_components.schema_processor->getColumnMapperById(iceberg_table_state->schema_id);
}

std::optional<String> IcebergMetadata::getPartitionKey(ContextPtr local_context, TableStateSnapshot actual_table_state_snapshot) const
{
    auto metadata_object = getMetadataJSONObject(
        actual_table_state_snapshot.metadata_file_path,
        object_storage,
        persistent_components.metadata_cache,
        local_context,
        log,
        persistent_components.metadata_compression_method,
        persistent_components.table_uuid);
    auto [schema, current_schema_id] = parseTableSchemaV2Method(metadata_object);
    return getPartitionKeyStringFromMetadata(
        metadata_object,
        *persistent_components.schema_processor->getClickhouseTableSchemaById(current_schema_id),
        local_context);
}

KeyDescription IcebergMetadata::getSortingKey(ContextPtr local_context, TableStateSnapshot actual_table_state_snapshot) const
{
    auto metadata_object = getMetadataJSONObject(
        actual_table_state_snapshot.metadata_file_path,
        object_storage,
        persistent_components.metadata_cache,
        local_context,
        log,
        persistent_components.metadata_compression_method,
        persistent_components.table_uuid);

    auto [schema, current_schema_id] = parseTableSchemaV2Method(metadata_object);
    auto result = getSortingKeyDescriptionFromMetadata(metadata_object, *persistent_components.schema_processor->getClickhouseTableSchemaById(current_schema_id), local_context);
    auto sort_order_id = metadata_object->getValue<Int64>(f_default_sort_order_id);
    result.sort_order_id = sort_order_id;
    return result;
}

SinkToStoragePtr IcebergMetadata::import(
    std::shared_ptr<DataLake::ICatalog> catalog,
    const std::function<void(const std::string &)> & new_file_path_callback,
    SharedHeader sample_block,
    const std::string & iceberg_metadata_json_string,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr context)
{
    Poco::JSON::Parser parser; /// For some reason base/base/JSON.h can not parse this json file
    Poco::Dynamic::Var json = parser.parse(iceberg_metadata_json_string);
    Poco::JSON::Object::Ptr metadata_json = json.extract<Poco::JSON::Object::Ptr>();

    return std::make_shared<IcebergImportSink>(
        catalog, persistent_components, metadata_json, object_storage,
        context, format_settings, write_format, sample_block, data_lake_settings, new_file_path_callback);
}

namespace FailPoints
{
    extern const char iceberg_writes_cleanup[];
    extern const char iceberg_writes_non_retry_cleanup[];
    extern const char iceberg_writes_post_publish_throw[];
}

namespace
{

/// Find the partition spec object with the given spec-id inside a metadata JSON document.
/// Throws BAD_ARGUMENTS if the spec is not found (indicates metadata/spec-id mismatch).
Poco::JSON::Object::Ptr lookupPartitionSpec(const Poco::JSON::Object::Ptr & meta, Int64 spec_id)
{
    auto specs = meta->getArray(Iceberg::f_partition_specs);
    for (size_t i = 0; i < specs->size(); ++i)
    {
        auto spec = specs->getObject(static_cast<UInt32>(i));
        if (spec->getValue<Int64>(Iceberg::f_spec_id) == spec_id)
            return spec;
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "Partition spec with id {} not found in table metadata", spec_id);
}

Poco::JSON::Object::Ptr lookupSchema(const Poco::JSON::Object::Ptr & meta, Int64 schema_id)
{
    auto schemas = meta->getArray(Iceberg::f_schemas);
    for (size_t i = 0; i < schemas->size(); ++i)
    {
        auto schema = schemas->getObject(static_cast<UInt32>(i));
        if (schema->getValue<Int32>(Iceberg::f_schema_id) == schema_id)
            return schema;
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "Schema with id {} not found in table metadata", schema_id);
}

}

bool IcebergMetadata::commitImportPartitionTransactionImpl(
    FileNamesGenerator & filename_generator,
    Poco::JSON::Object::Ptr & metadata,
    Poco::JSON::Object::Ptr & partition_spec,
    const String & transaction_id,
    Int64 original_schema_id,
    Int64 partition_spec_id,
    const std::vector<Field> & partition_values,
    const std::vector<String> & partition_columns,
    const std::vector<DataTypePtr> & partition_types,
    SharedHeader sample_block,
    const std::vector<String> & data_file_paths,
    const std::vector<IcebergSerializedFileStats> & per_file_stats,
    Int64 total_data_files,
    Int64 total_rows,
    Int64 total_chunks_size,
    std::shared_ptr<DataLake::ICatalog> catalog,
    const StorageID & table_id,
    const String & blob_storage_type_name,
    const String & blob_storage_namespace_name,
    ContextPtr context)
{
    /// this check also exists here because the metadata might have been updated upon retry attempts.
    if (isExportPartitionTransactionAlreadyCommitted(metadata, transaction_id))
    {
        LOG_INFO(log,
            "Export transaction {} already committed, skipping re-commit",
            transaction_id);
        return true;
    }

    CompressionMethod metadata_compression_method = persistent_components.metadata_compression_method;

    auto [metadata_name, storage_metadata_name] = filename_generator.generateMetadataName();

    Int64 parent_snapshot = -1;
    if (metadata->has(Iceberg::f_current_snapshot_id))
        parent_snapshot = metadata->getValue<Int64>(Iceberg::f_current_snapshot_id);

    auto [new_snapshot, manifest_list_name, storage_manifest_list_name] = MetadataGenerator(metadata).generateNextMetadata(
        filename_generator, metadata_name, parent_snapshot, total_data_files, total_rows, total_chunks_size, total_data_files, /* added_delete_files */0, /* num_deleted_rows */0);

    /// Embed the stable transaction identifier in the snapshot summary so that a retry after crash
    /// can detect the commit already happened by scanning the live snapshots array, without extra S3
    /// files. The field is a ClickHouse extension; Spark/Flink readers ignore unknown summary keys.
    new_snapshot->getObject(Iceberg::f_summary)->set(
        Iceberg::f_clickhouse_export_partition_transaction_id, transaction_id);

    String manifest_entry_name;
    String storage_manifest_entry_name;
    Int64 manifest_lengths = 0;

    /// Tracks whether the snapshot has become visible to readers.
    /// For the file-based layout that happens as soon as writeMetadataFileAndVersionHint
    /// succeeds; for a catalog layout it happens when catalog->updateMetadata succeeds.
    /// Once published, the manifest entry / manifest list are referenced by the live
    /// snapshot and must NOT be deleted by the outer failure cleanup, otherwise the
    /// already-published snapshot becomes unreadable.
    bool published = false;

    auto cleanup = [&](bool retry_because_of_metadata_conflict)
    {
        /// We can't cleanup the data files upon retry even if retry_because_of_metadata_conflict == false
        /// because this replica or some other replica might attempt to commit the same transaction later
        /// todo arthur: in the future, we should consider failing the entire task if retry_because_of_metadata_conflict = true

        object_storage->removeObjectIfExists(StoredObject(storage_manifest_entry_name));
        object_storage->removeObjectIfExists(StoredObject(storage_manifest_list_name));

        if (retry_because_of_metadata_conflict)
        {
            MetadataFileWithInfo latest_metadata_file_info;
            if (catalog && catalog->isTransactional())
            {
                const auto & [namespace_name, table_name] = DataLake::parseTableName(table_id.getTableName());
                DataLake::TableMetadata table_metadata = DataLake::TableMetadata().withLocation().withDataLakeSpecificProperties();
                catalog->getTableMetadata(namespace_name, table_name, context, table_metadata);

                auto table_specific_properties = table_metadata.getDataLakeSpecificProperties();
                if (!table_specific_properties.has_value() || table_specific_properties->iceberg_metadata_file_location.empty())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Catalog didn't return iceberg metadata location for table {}.{}", namespace_name, table_name);

                String metadata_path = table_metadata.getMetadataLocation(table_specific_properties->iceberg_metadata_file_location);
                if (!metadata_path.starts_with(persistent_components.table_path))
                    metadata_path = std::filesystem::path(persistent_components.table_path) / metadata_path;
                latest_metadata_file_info = Iceberg::getMetadataFileAndVersion(metadata_path);
            }
            else
            {
                latest_metadata_file_info = getLatestOrExplicitMetadataFileAndVersion(
                    object_storage,
                    persistent_components.table_path,
                    data_lake_settings,
                    persistent_components.metadata_cache,
                    context,
                    getLogger("IcebergWrites").get(),
                    persistent_components.table_uuid,
                    true);
            }

            auto [last_version, metadata_path, compression_method] = latest_metadata_file_info;

            LOG_DEBUG(log, "Rereading metadata file {} with version {}", metadata_path, last_version);

            metadata_compression_method = compression_method;
            filename_generator.setVersion(last_version + 1);

            metadata = getMetadataJSONObject(
                metadata_path,
                object_storage,
                persistent_components.metadata_cache,
                context,
                getLogger("IcebergMetadata"),
                compression_method,
                persistent_components.table_uuid);

            /// For the export path the schema and partition spec are fixed at the start of the
            /// operation (saved in ZooKeeper). If either changed we must fail immediately —
            /// the caller has to restart the export from scratch.
            const auto new_schema_id = metadata->getValue<Int64>(Iceberg::f_current_schema_id);
            if (new_schema_id != original_schema_id)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Table schema changed during export (expected schema {}, got {}). Restart the export operation.",
                    original_schema_id, new_schema_id);

            const Int64 new_partition_spec_id = metadata->getValue<Int64>(Iceberg::f_default_spec_id);
            if (new_partition_spec_id != partition_spec_id)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Partition spec changed during export (expected spec {}, got {}). Restart the export operation.",
                    partition_spec_id, new_partition_spec_id);

            partition_spec = lookupPartitionSpec(metadata, partition_spec_id);

            /// partition_values, partition_columns, partition_types, and
            /// data_file_paths are all fixed from the saved state — no update needed.
        }
    };

    try
    {
        {
            auto result = filename_generator.generateManifestEntryName();
            manifest_entry_name = result.path_in_metadata;
            storage_manifest_entry_name = result.path_in_storage;
        }

        auto buffer_manifest_entry = object_storage->writeObject(
            StoredObject(storage_manifest_entry_name), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());

        try
        {
            fiu_do_on(FailPoints::iceberg_writes_non_retry_cleanup,
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failpoint for cleanup enabled");
            });

            generateManifestFile(
                metadata,
                partition_columns,
                partition_values,
                partition_types,
                data_file_paths,
                std::nullopt,  /// per_file_stats is filled, no need for the generic aggregate
                sample_block,
                new_snapshot,
                write_format,
                partition_spec,
                partition_spec_id,
                *buffer_manifest_entry,
                Iceberg::FileContentType::DATA,
                per_file_stats);
            buffer_manifest_entry->finalize();
            manifest_lengths += buffer_manifest_entry->count();
        }
        catch (...)
        {
            cleanup(false);
            throw;
        }

        {
            auto buffer_manifest_list = object_storage->writeObject(
                StoredObject(storage_manifest_list_name), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());

            try
            {
                generateManifestList(
                    filename_generator, metadata, object_storage, context, {manifest_entry_name}, new_snapshot, manifest_lengths, *buffer_manifest_list, Iceberg::FileContentType::DATA, true);
                buffer_manifest_list->finalize();
            }
            catch (...)
            {
                cleanup(false);
                throw;
            }
        }

        {
            std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            Poco::JSON::Stringifier::stringify(metadata, oss, 4);
            std::string json_representation = removeEscapedSlashes(oss.str());

            LOG_DEBUG(log, "Writing new metadata file {}", storage_metadata_name);
            auto hint = filename_generator.generateVersionHint();
            if (!writeMetadataFileAndVersionHint(
                    storage_metadata_name,
                    json_representation,
                    hint.path_in_storage,
                    storage_metadata_name,
                    object_storage,
                    context,
                    metadata_compression_method,
                    data_lake_settings[DataLakeStorageSetting::iceberg_use_version_hint]))
            {
                LOG_DEBUG(log, "Failed to write metadata {}, retrying", storage_metadata_name);
                cleanup(true);
                return false;
            }

            LOG_DEBUG(log, "Metadata file {} written", storage_metadata_name);

            if (catalog)
            {
                String catalog_filename = metadata_name;
                if (!catalog_filename.starts_with(blob_storage_type_name))
                    catalog_filename = blob_storage_type_name + "://" + blob_storage_namespace_name + "/" + metadata_name;

                const auto & [namespace_name, table_name] = DataLake::parseTableName(table_id.getTableName());
                if (!catalog->updateMetadata(namespace_name, table_name, catalog_filename, new_snapshot))
                {
                    cleanup(true);
                    return false;
                }

                /// Catalog has accepted the commit - the new snapshot is now live and references
                /// storage_manifest_entry_name / storage_manifest_list_name. From here on, any
                /// failure must NOT delete those files.
                published = true;
            }
            else
            {
                /// File-based layout: the snapshot becomes visible via the metadata file and
                /// version hint that were just written above. From here on, any failure must
                /// NOT delete manifest entry / manifest list.
                published = true;
            }
        }

        /// Fault-injection hook that simulates an exception in the trailing post-publish
        /// region (e.g. failure in metadata-cache invalidation). Must be placed AFTER
        /// `published = true` to exercise the exception-safety guard in the outer catch.
        fiu_do_on(FailPoints::iceberg_writes_post_publish_throw,
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failpoint iceberg_writes_post_publish_throw enabled");
        });

        if (persistent_components.metadata_cache)
        {
            /// If there's an active metadata cache
            /// We can't just cache 'our' written version as latest, because it could've been overwritten by a concurrent catalog update
            /// This is why, we are safely invalidating the cache, and the very next reader will get the most up-to-date latest version
            persistent_components.metadata_cache->remove(persistent_components.table_path);
            if (persistent_components.table_uuid)
                persistent_components.metadata_cache->remove(*persistent_components.table_uuid);
        }
    }
    catch (...)
    {
        if (published)
        {
            /// Commit has already become visible to readers. The failure is in trailing
            /// post-publish work (e.g. metadata-cache invalidation). Running cleanup()
            /// here would delete manifest files referenced by the published snapshot
            /// and corrupt it. Log and swallow - any transient state (stale cache)
            /// is self-healing on subsequent reads.
            tryLogCurrentException(log,
                "Post-publish work failed after Iceberg snapshot was committed; "
                "skipping manifest cleanup to preserve published snapshot");
            return true;
        }

        LOG_ERROR(log, "Failed to commit import partition transaction: {}", getCurrentExceptionMessage(false));
        cleanup(false);
        throw;
    }

    return true;
}

void IcebergMetadata::commitExportPartitionTransaction(
    std::shared_ptr<DataLake::ICatalog> catalog,
    const StorageID & table_id,
    const String & transaction_id,
    Int64 original_schema_id,
    Int64 partition_spec_id,
    const std::vector<Field> & partition_values,
    SharedHeader sample_block,
    const std::vector<String> & data_file_paths,
    StorageObjectStorageConfigurationPtr configuration,
    ContextPtr context)
{

    MetadataFileWithInfo updated_metadata_file_info = getLatestOrExplicitMetadataFileAndVersion(
        object_storage,
        persistent_components.table_path,
        data_lake_settings,
        persistent_components.metadata_cache,
        context,
        getLogger("IcebergMetadata").get(),
        persistent_components.table_uuid,
        true);

    /// Latest metadata is ALWAYS necessary to commit - but we abort in case schema or partition spec changed
    Poco::JSON::Object::Ptr metadata = getMetadataJSONObject(
        updated_metadata_file_info.path,
        object_storage,
        persistent_components.metadata_cache,
        context,
        getLogger("IcebergMetadata"),
        updated_metadata_file_info.compression_method,
        persistent_components.table_uuid);

    if (isExportPartitionTransactionAlreadyCommitted(metadata, transaction_id))
    {
        LOG_INFO(log,
            "Export transaction {} already committed, skipping re-commit",
            transaction_id);
        return;
    }

    /// Fail fast if the table schema or partition spec changed between export-start and commit.
    /// The exported data files and partition values were produced against the original spec;
    const auto latest_schema_id = metadata->getValue<Int64>(Iceberg::f_current_schema_id);
    if (latest_schema_id != original_schema_id)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Table schema changed before export could commit (expected schema {}, got {}). "
            "Restart the export operation.",
            original_schema_id, latest_schema_id);

    const auto latest_spec_id = metadata->getValue<Int64>(Iceberg::f_default_spec_id);
    if (latest_spec_id != partition_spec_id)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Partition spec changed before export could commit (expected spec {}, got {}). "
            "Restart the export operation.",
            partition_spec_id, latest_spec_id);

    /// Derive partition_columns and partition_types from the schema and partition spec.
    /// The IDs are validated equal above so derivation from the latest metadata yields
    /// the same result as from the original ZK-pinned snapshot.

    const auto schema = lookupSchema(metadata, original_schema_id);

    auto partition_spec = lookupPartitionSpec(metadata, partition_spec_id);

    ChunkPartitioner partitioner(partition_spec->getArray(Iceberg::f_fields), schema, context, sample_block);

    const auto partition_columns = partitioner.getColumns();
    const auto partition_types = partitioner.getResultTypes();

    const auto metadata_compression_method = persistent_components.metadata_compression_method;
    auto config_path = persistent_components.table_path;
    if (config_path.empty() || config_path.back() != '/')
        config_path += "/";
    if (!config_path.starts_with('/'))
        config_path = '/' + config_path;

    FileNamesGenerator filename_generator;
    if (!context->getSettingsRef()[Setting::write_full_path_in_iceberg_metadata])
    {
        filename_generator = FileNamesGenerator(
            config_path, config_path, (catalog != nullptr && catalog->isTransactional()), metadata_compression_method, write_format);
    }
    else
    {
        auto bucket = metadata->getValue<String>(Iceberg::f_location);
        if (bucket.empty() || bucket.back() != '/')
            bucket += "/";
        filename_generator = FileNamesGenerator(
            bucket, config_path, (catalog != nullptr && catalog->isTransactional()), metadata_compression_method, write_format);
    }
    filename_generator.setVersion(updated_metadata_file_info.version + 1);

    /// Load per-file sidecar stats, necessary to populate the manifest file stats.
    std::vector<IcebergSerializedFileStats> per_file_stats;
    const Int64 total_data_files = static_cast<Int64>(data_file_paths.size());
    Int64 total_rows = 0;
    Int64 total_chunks_size = 0;
    per_file_stats.reserve(data_file_paths.size());
    for (const auto & path : data_file_paths)
    {
        const auto sidecar_path = getIcebergExportPartSidecarStoragePath(path);
        auto stats = readDataFileSidecar(sidecar_path, object_storage, context);
        total_rows += stats.record_count;
        total_chunks_size += stats.file_size_in_bytes;

        per_file_stats.push_back(std::move(stats));
    }

    size_t attempt = 0;
    while (attempt < MAX_TRANSACTION_RETRIES)
    {
        if (commitImportPartitionTransactionImpl(
                filename_generator,
                metadata,
                partition_spec,
                transaction_id,
                original_schema_id,
                partition_spec_id,
                partition_values,
                partition_columns,
                partition_types,
                sample_block,
                data_file_paths,
                per_file_stats,
                total_data_files,
                total_rows,
                total_chunks_size,
                catalog,
                table_id,
                configuration->getTypeName(),
                configuration->getNamespace(),
                context))
        {
            return;
        }

        ++attempt;
    }

    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
        "Failed to commit export partition transaction after {} attempts due to repeated metadata conflicts.",
        attempt);
}

Poco::JSON::Object::Ptr IcebergMetadata::getMetadataJSON(ContextPtr local_context) const
{
    auto [actual_data_snapshot, actual_table_state_snapshot] = getRelevantState(local_context);
    return getMetadataJSONObject(
        actual_table_state_snapshot.metadata_file_path,
        object_storage,
        persistent_components.metadata_cache,
        local_context,
        log,
        persistent_components.metadata_compression_method,
        persistent_components.table_uuid);
}

}

#endif
