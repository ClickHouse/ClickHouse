#include "config.h"
#if USE_AVRO

#include <cstddef>
#include <memory>
#include <optional>
#include <sstream>
#include <Columns/ColumnSet.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeSet.h>
#include <Formats/FormatFilterInfo.h>
#include <Formats/FormatParserSharedResources.h>
#include <Formats/ReadSchemaUtils.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/tuple.h>
#include <Processors/Formats/ISchemaReader.h>
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
#include <Core/Settings.h>
#include <Core/NamesAndTypes.h>
#include <Databases/DataLake/ICatalog.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/IcebergMetadataLog.h>

#include <Storages/ObjectStorage/DataLakes/Common/Common.h>
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
extern const SettingsBool iceberg_delete_data_on_drop;
}

namespace
{
String dumpMetadataObjectToString(const Poco::JSON::Object::Ptr & metadata_object)
{
    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::JSON::Stringifier::stringify(metadata_object, oss);
    return removeEscapedSlashes(oss.str());
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
        = getLatestOrExplicitMetadataFileAndVersion(object_storage, configuration->getPathForRead().path, configuration->getDataLakeSettings(), cache_ptr, context_, log.get(), std::nullopt);
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
        .schema_processor = std::make_shared<IcebergSchemaProcessor>(),
        .metadata_cache = cache_ptr,
        .format_version = format_version,
        .table_location = table_location,
        .metadata_compression_method = compression_method,
        .table_path = configuration->getPathForRead().path,
        .table_uuid = table_uuid,
    };
}

std::pair<IcebergDataSnapshotPtr, TableStateSnapshot> IcebergMetadata::getRelevantState(const ContextPtr & context) const
{
    const auto [metadata_version, metadata_file_path, compression_method] = getLatestOrExplicitMetadataFileAndVersion(
        object_storage,
        persistent_components.table_path,
        data_lake_settings,
        persistent_components.metadata_cache,
        context,
        log.get(),
        persistent_components.table_uuid);
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
    , write_format(configuration_->format)
{
}

Int32 IcebergMetadata::parseTableSchema(
    const Poco::JSON::Object::Ptr & metadata_object, IcebergSchemaProcessor & schema_processor, LoggerPtr metadata_logger)
{
    const auto format_version = metadata_object->getValue<Int32>(f_format_version);
    if (format_version == 2)
    {
        auto [schema, current_schema_id] = parseTableSchemaV2Method(metadata_object);
        schema_processor.addIcebergTableSchema(schema);
        return current_schema_id;
    }
    else
    {
        try
        {
            auto [schema, current_schema_id] = parseTableSchemaV1Method(metadata_object);
            schema_processor.addIcebergTableSchema(schema);
            return current_schema_id;
        }
        catch (const Exception & first_error)
        {
            if (first_error.code() != ErrorCodes::BAD_ARGUMENTS)
                throw;
            try
            {
                auto [schema, current_schema_id] = parseTableSchemaV2Method(metadata_object);
                schema_processor.addIcebergTableSchema(schema);
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
    Poco::JSON::Object::Ptr metadata_object, Int64 snapshot_id, IcebergSchemaProcessorPtr schema_processor)
{
    if (!metadata_object->has(f_snapshots))
        throw Exception(ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "No snapshot set found in metadata for iceberg file");
    auto schemas = metadata_object->get(f_schemas).extract<Poco::JSON::Array::Ptr>();
    for (UInt32 j = 0; j < schemas->size(); ++j)
    {
        auto schema = schemas->getObject(j);
        schema_processor->addIcebergTableSchema(schema);
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
    auto object = traverseMetadataAndFindNecessarySnapshotObject(metadata_object, snapshot_id, persistent_components.schema_processor);
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
        auto schema_id = parseTableSchema(metadata_object, *persistent_components.schema_processor, log);
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

    insertRowToLogTable(
        local_context,
        dumpMetadataObjectToString(metadata_object),
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

std::shared_ptr<const ActionsDAG> IcebergMetadata::getSchemaTransformer(ContextPtr, ObjectInfoPtr object_info) const
{
    IcebergDataObjectInfo * iceberg_object_info = dynamic_cast<IcebergDataObjectInfo *>(object_info.get());
    if (!iceberg_object_info)
        return nullptr;
    return (iceberg_object_info->info.underlying_format_read_schema_id != iceberg_object_info->info.schema_id_relevant_to_iterator)
        ? persistent_components.schema_processor->getSchemaTransformationDagByIds(
              iceberg_object_info->info.underlying_format_read_schema_id, iceberg_object_info->info.schema_id_relevant_to_iterator)
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
            && command.type != AlterCommand::Type::MODIFY_COLUMN)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter of type '{}' is not supported by Iceberg storage", command.type);
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
        metadata_object, *table_state_snapshot.snapshot_id, persistent_components.schema_processor);

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
        auto manifest_file_ptr = getManifestFile(
            object_storage,
            persistent_components,
            context,
            log,
            manifest_list_entry.manifest_file_path,
            manifest_list_entry.added_sequence_number,
            manifest_list_entry.added_snapshot_id);

        if (!manifest_file_ptr->areAllDataFilesSortedBySortOrderID(sorting_key.sort_order_id.value()))
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
        auto manifest_file_ptr = getManifestFile(
            object_storage,
            persistent_components,
            local_context,
            log,
            manifest_list_entry.manifest_file_path,
            manifest_list_entry.added_sequence_number,
            manifest_list_entry.added_snapshot_id);
        auto data_count = manifest_file_ptr->getRowsCountInAllFilesExcludingDeleted(FileContentType::DATA);
        auto position_deletes_count = manifest_file_ptr->getRowsCountInAllFilesExcludingDeleted(FileContentType::POSITION_DELETE);
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
        auto manifest_file_ptr = getManifestFile(
            object_storage,
            persistent_components,
            local_context,
            log,
            manifest_list_entry.manifest_file_path,
            manifest_list_entry.added_sequence_number,
            manifest_list_entry.added_snapshot_id);
        auto count = manifest_file_ptr->getBytesCountInAllDataFilesExcludingDeleted();
        if (!count.has_value())
            return {};

        result += count.value();
    }

    return result;
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

StorageInMemoryMetadata IcebergMetadata::getStorageSnapshotMetadata(ContextPtr local_context) const
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::IcebergMetadataUpdateMicroseconds);
    auto [actual_data_snapshot, actual_table_state_snapshot] = getRelevantState(local_context);
    StorageInMemoryMetadata result;
    result.setColumns(
        ColumnsDescription{*persistent_components.schema_processor->getClickhouseTableSchemaById(actual_table_state_snapshot.schema_id)});
    result.setDataLakeTableState(actual_table_state_snapshot);
    result.sorting_key = getSortingKey(local_context, actual_table_state_snapshot);
    return result;
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
        builder.addSimpleTransform(
            [&](const SharedHeader & header)
            { return iceberg_object_info->getPositionDeleteTransformer(object_storage, header, format_settings, parser_shared_resources, local_context); });
    }
    const auto & delete_files = iceberg_object_info->info.equality_deletes_objects;
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

}

#endif
