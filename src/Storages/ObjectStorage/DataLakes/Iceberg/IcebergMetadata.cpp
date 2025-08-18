
#include "config.h"
#if USE_AVRO

#include <cstddef>
#include <memory>
#include <optional>
#include <Formats/FormatFilterInfo.h>
#include <Formats/FormatParserSharedResources.h>
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Common/Exception.h>


#include <Databases/DataLake/Common.h>
#include <Core/Settings.h>
#include <Core/NamesAndTypes.h>
#include <Disks/ObjectStorages/StoredObject.h>
#include <Databases/DataLake/ICatalog.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>

#include <Storages/ObjectStorage/DataLakes/Common.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Interpreters/ExpressionActions.h>
#include <IO/CompressedReadBufferWrapper.h>

#include <Storages/ColumnsDescription.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/AvroForIcebergDeserializer.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergIterator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/StatelessMetadataFileGetter.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Compaction.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Mutations.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Interpreters/StorageID.h>

#include <Common/ProfileEvents.h>
#include <Common/SharedLockGuard.h>
#include <Common/logger_useful.h>

namespace ProfileEvents
{
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
    extern const DataLakeStorageSettingsInt64 iceberg_format_version;
}

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
extern const int ICEBERG_SPECIFICATION_VIOLATION;
extern const int TABLE_ALREADY_EXISTS;
extern const int SUPPORT_IS_DISABLED;
}

namespace Setting
{
extern const SettingsInt64 iceberg_timestamp_ms;
extern const SettingsInt64 iceberg_snapshot_id;
extern const SettingsBool use_iceberg_metadata_files_cache;
extern const SettingsBool use_iceberg_partition_pruning;
extern const SettingsBool write_full_path_in_iceberg_metadata;
extern const SettingsBool use_roaring_bitmap_iceberg_positional_deletes;
extern const SettingsString iceberg_metadata_compression_method;
extern const SettingsBool allow_experimental_insert_into_iceberg;
extern const SettingsBool allow_experimental_iceberg_compaction;
}


using namespace Iceberg;

IcebergMetadata::IcebergMetadata(
    ObjectStoragePtr object_storage_,
    StorageObjectStorageConfigurationWeakPtr configuration_,
    const ContextPtr & context_,
    Int32 metadata_version_,
    Int32 format_version_,
    const Poco::JSON::Object::Ptr & metadata_object_,
    IcebergMetadataFilesCachePtr cache_ptr,
    CompressionMethod metadata_compression_method_)
    : object_storage(std::move(object_storage_))
    , configuration(std::move(configuration_))
    , persistent_components(PersistentTableComponents{
          .schema_processor = std::make_shared<IcebergSchemaProcessor>(),
          .metadata_cache = cache_ptr,
          .format_version = format_version_,
          .table_location = metadata_object_->getValue<String>(f_location)
      })
    , log(getLogger("IcebergMetadata"))
    , last_metadata_version(metadata_version_)
    , relevant_snapshot_schema_id(-1)
    , metadata_compression_method(metadata_compression_method_)
{
    updateState(context_, metadata_object_);
}

void IcebergMetadata::addTableSchemaById(Int32 schema_id, Poco::JSON::Object::Ptr metadata_object) const
{
    if (persistent_components.schema_processor->hasClickhouseTableSchemaById(schema_id))
        return;
    if (!metadata_object->has(f_schemas))
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Cannot parse Iceberg table schema with id `{}`: 'schemas' field is missing in metadata", schema_id);
    }
    auto schemas = metadata_object->get(f_schemas).extract<Poco::JSON::Array::Ptr>();
    for (uint32_t i = 0; i != schemas->size(); ++i)
    {
        auto current_schema = schemas->getObject(i);
        if (current_schema->has(f_schema_id) && current_schema->getValue<int>(f_schema_id) == schema_id)
        {
            persistent_components.schema_processor->addIcebergTableSchema(current_schema);
            return;
        }
    }
    throw Exception(
        ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
        "Cannot parse Iceberg table schema with id `{}`: schema with such id is not found in metadata",
        schema_id);
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

bool IcebergMetadata::update(const ContextPtr & local_context)
{
    auto configuration_ptr = configuration.lock();

    std::lock_guard lock(mutex);

    const auto [metadata_version, metadata_file_path, compression_method]
        = getLatestOrExplicitMetadataFileAndVersion(object_storage, configuration_ptr, persistent_components.metadata_cache, local_context, log.get());

    if (last_metadata_version != metadata_version)
    {
        last_metadata_version = metadata_version;
    }

    auto metadata_object = getMetadataJSONObject(metadata_file_path, object_storage, configuration_ptr, persistent_components.metadata_cache, local_context, log, compression_method);
    chassert(persistent_components.format_version == metadata_object->getValue<int>(f_format_version));

    auto previous_snapshot_id = relevant_snapshot_id;
    auto previous_snapshot_schema_id = relevant_snapshot_schema_id;

    updateState(local_context, metadata_object);

    if (previous_snapshot_id != relevant_snapshot_id)
    {
        return true;
    }
    return previous_snapshot_schema_id != relevant_snapshot_schema_id;
}

void IcebergMetadata::updateSnapshot(ContextPtr local_context, Poco::JSON::Object::Ptr metadata_object)
{
    auto configuration_ptr = configuration.lock();
    if (!metadata_object->has(f_snapshots))
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "No snapshot set found in metadata for iceberg table `{}`, it is impossible to get manifest list by snapshot id `{}`",
            configuration_ptr->getPathForRead().path,
            relevant_snapshot_id);
    auto schemas = metadata_object->get(f_schemas).extract<Poco::JSON::Array::Ptr>();
    for (UInt32 j = 0; j < schemas->size(); ++j)
    {
        auto schema = schemas->getObject(j);
        persistent_components.schema_processor->addIcebergTableSchema(schema);
    }
    auto snapshots = metadata_object->get(f_snapshots).extract<Poco::JSON::Array::Ptr>();
    bool successfully_found_snapshot = false;
    for (size_t i = 0; i < snapshots->size(); ++i)
    {
        const auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
        auto current_snapshot_id = snapshot->getValue<Int64>(f_metadata_snapshot_id);
        auto current_schema_id = snapshot->getValue<Int32>(f_schema_id);
        persistent_components.schema_processor->registerSnapshotWithSchemaId(current_snapshot_id, current_schema_id);
        if (snapshot->getValue<Int64>(f_metadata_snapshot_id) == relevant_snapshot_id)
        {
            successfully_found_snapshot = true;
            if (!snapshot->has(f_manifest_list))
                throw Exception(
                    ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                    "No manifest list found for snapshot id `{}` for iceberg table `{}`",
                    relevant_snapshot_id,
                    configuration_ptr->getPathForRead().path);
            std::optional<size_t> total_rows;
            std::optional<size_t> total_bytes;
            std::optional<size_t> total_position_deletes;

            if (snapshot->has(f_summary))
            {
                auto summary_object = snapshot->get(f_summary).extract<Poco::JSON::Object::Ptr>();
                if (summary_object->has(f_total_records))
                    total_rows = summary_object->getValue<Int64>(f_total_records);

                if (summary_object->has(f_total_files_size))
                    total_bytes = summary_object->getValue<Int64>(f_total_files_size);

                if (summary_object->has(f_total_position_deletes))
                {
                    total_position_deletes = summary_object->getValue<Int64>(f_total_position_deletes);
                }
            }

#if USE_PARQUET
            if (configuration_ptr->format == "Parquet")
                column_mapper = std::make_shared<ColumnMapper>();

            if (column_mapper)
            {
                Int32 schema_id = snapshot->getValue<Int32>(f_schema_id);
                std::unordered_map<String, Int64> column_name_to_parquet_field_id;
                for (UInt32 j = 0; j < schemas->size(); ++j)
                {
                    auto schema = schemas->getObject(j);
                    if (schema->getValue<Int32>(f_schema_id) != schema_id)
                        continue;

                    column_name_to_parquet_field_id = IcebergSchemaProcessor::traverseSchema(schema->getArray(Iceberg::f_fields));
                }
                column_mapper->setStorageColumnEncoding(std::move(column_name_to_parquet_field_id));
            }
#endif

            relevant_snapshot = std::make_shared<IcebergDataSnapshot>(
                getManifestList(
                    object_storage,
                    configuration_ptr,
                    persistent_components,
                    local_context,
                    getProperFilePathFromMetadataInfo(
                        snapshot->getValue<String>(f_manifest_list), configuration_ptr->getPathForRead().path, persistent_components.table_location),
                    log),
                relevant_snapshot_id,
                total_rows,
                total_bytes,
                total_position_deletes);

            if (!snapshot->has(f_schema_id))
                throw Exception(
                    ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                    "No schema id found for snapshot id `{}` for iceberg table `{}`",
                    relevant_snapshot_id,
                    configuration_ptr->getPathForRead().path);
            relevant_snapshot_schema_id = snapshot->getValue<Int32>(f_schema_id);
            addTableSchemaById(relevant_snapshot_schema_id, metadata_object);
        }
    }
    if (!successfully_found_snapshot)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "No manifest list is found for snapshot id `{}` in metadata for iceberg table `{}`",
            relevant_snapshot_id,
            configuration_ptr->getPathForRead().path);
}

bool IcebergMetadata::optimize(const StorageMetadataPtr & metadata_snapshot, ContextPtr context, const std::optional<FormatSettings> & format_settings)
{
    if (context->getSettingsRef()[Setting::allow_experimental_iceberg_compaction])
    {
        auto configuration_ptr = configuration.lock();
        const auto sample_block = std::make_shared<const Block>(metadata_snapshot->getSampleBlock());
        auto snapshots_info = getHistory(context);
        compactIcebergTable(
            snapshots_info,
            persistent_components,
            object_storage,
            configuration_ptr,
            format_settings,
            sample_block,
            context,
            metadata_compression_method);
        return true;
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Enable 'allow_experimental_iceberg_compaction' setting to call optimize for iceberg tables.");
    }
}

void IcebergMetadata::updateState(const ContextPtr & local_context, Poco::JSON::Object::Ptr metadata_object)
{
    auto configuration_ptr = configuration.lock();

    std::optional<String> manifest_list_file;

    bool timestamp_changed = local_context->getSettingsRef()[Setting::iceberg_timestamp_ms].changed;
    bool snapshot_id_changed = local_context->getSettingsRef()[Setting::iceberg_snapshot_id].changed;
    if (timestamp_changed && snapshot_id_changed)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Time travel with timestamp and snapshot id for iceberg table by path {} cannot be changed simultaneously",
            configuration_ptr->getPathForRead().path);
    }
    if (timestamp_changed)
    {
        Int64 closest_timestamp = 0;
        Int64 query_timestamp = local_context->getSettingsRef()[Setting::iceberg_timestamp_ms];
        if (!metadata_object->has(f_snapshot_log))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No snapshot log found in metadata for iceberg table {} so it is impossible to get relevant snapshot id using timestamp", configuration_ptr->getPathForRead().path);
        auto snapshots = metadata_object->get(f_snapshot_log).extract<Poco::JSON::Array::Ptr>();
        relevant_snapshot_id = -1;
        for (size_t i = 0; i < snapshots->size(); ++i)
        {
            const auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
            Int64 snapshot_timestamp = snapshot->getValue<Int64>(f_timestamp_ms);
            if (snapshot_timestamp <= query_timestamp && snapshot_timestamp > closest_timestamp)
            {
                closest_timestamp = snapshot_timestamp;
                relevant_snapshot_id = snapshot->getValue<Int64>(f_metadata_snapshot_id);
            }
        }
        if (relevant_snapshot_id < 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No snapshot found in snapshot log before requested timestamp for iceberg table {}", configuration_ptr->getPathForRead().path);
        updateSnapshot(local_context, metadata_object);
    }
    else if (snapshot_id_changed)
    {
        relevant_snapshot_id = local_context->getSettingsRef()[Setting::iceberg_snapshot_id];
        updateSnapshot(local_context, metadata_object);
    }
    else
    {
        if (!metadata_object->has(f_current_snapshot_id))
            relevant_snapshot_id = -1;
        else
            relevant_snapshot_id = metadata_object->getValue<Int64>(f_current_snapshot_id);
        if (relevant_snapshot_id != -1)
        {
            updateSnapshot(local_context, metadata_object);
        }
        relevant_snapshot_schema_id = parseTableSchema(metadata_object, *persistent_components.schema_processor, log);
    }
}

std::shared_ptr<NamesAndTypesList> IcebergMetadata::getInitialSchemaByPath(ContextPtr, ObjectInfoPtr object_info) const
{
    SharedLockGuard lock(mutex);
    IcebergDataObjectInfo * iceberg_object_info = dynamic_cast<IcebergDataObjectInfo *>(object_info.get());
    if (!iceberg_object_info)
        return nullptr;
    return (iceberg_object_info->underlying_format_read_schema_id != relevant_snapshot_schema_id)
        ? persistent_components.schema_processor->getClickhouseTableSchemaById(iceberg_object_info->underlying_format_read_schema_id)
        : nullptr;
}

std::shared_ptr<const ActionsDAG> IcebergMetadata::getSchemaTransformer(ContextPtr, ObjectInfoPtr object_info) const
{
    IcebergDataObjectInfo * iceberg_object_info = dynamic_cast<IcebergDataObjectInfo *>(object_info.get());
    SharedLockGuard lock(mutex);
    if (!iceberg_object_info)
        return nullptr;
    return (iceberg_object_info->underlying_format_read_schema_id != relevant_snapshot_schema_id)
        ? persistent_components.schema_processor->getSchemaTransformationDagByIds(iceberg_object_info->underlying_format_read_schema_id, relevant_snapshot_schema_id)
        : nullptr;
}


void IcebergMetadata::mutate(
    const MutationCommands & commands,
    ContextPtr context,
    const StorageID & storage_id,
    StorageMetadataPtr metadata_snapshot,
    std::shared_ptr<DataLake::ICatalog> catalog,
    const std::optional<FormatSettings> & format_settings)
{
    auto configuration_ptr = configuration.lock();

    DB::Iceberg::mutate(commands, context, metadata_snapshot, storage_id, object_storage, configuration_ptr, format_settings, catalog);
}

void IcebergMetadata::checkMutationIsPossible(const MutationCommands & commands)
{
    for (const auto & command : commands)
        if (command.type != MutationCommand::DELETE)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Iceberg supports only DELETE mutations");
}

void IcebergMetadata::checkAlterIsPossible(const AlterCommands & commands)
{
    for (const auto & command : commands)
    {
        if (command.type != AlterCommand::Type::ADD_COLUMN && command.type != AlterCommand::Type::DROP_COLUMN && command.type != AlterCommand::Type::MODIFY_COLUMN)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter of type '{}' is not supported by Iceberg storage",
                command.type);
    }
}

void IcebergMetadata::alter(const AlterCommands & params, ContextPtr context)
{
    auto configuration_ptr = configuration.lock();

    Iceberg::alter(params, context, object_storage, configuration_ptr);
}

void IcebergMetadata::createInitial(
    const ObjectStoragePtr & object_storage,
    const StorageObjectStorageConfigurationWeakPtr & configuration,
    const ContextPtr & local_context,
    const std::optional<ColumnsDescription> & columns,
    ASTPtr partition_by,
    bool if_not_exists,
    std::shared_ptr<DataLake::ICatalog> catalog,
    const StorageID & table_id_)
{
    auto configuration_ptr = configuration.lock();

    std::vector<String> metadata_files;
    try
    {
        metadata_files = listFiles(*object_storage, *configuration_ptr, "metadata", ".metadata.json");
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
            throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Iceberg table with path {} already exists", configuration_ptr->getPathForRead().path);
    }

    String location_path = configuration_ptr->getRawPath().path;
    if (local_context->getSettingsRef()[Setting::write_full_path_in_iceberg_metadata].value)
        location_path = configuration_ptr->getTypeName() + "://" + configuration_ptr->getNamespace() + "/" + configuration_ptr->getRawPath().path;
    auto [metadata_content_object, metadata_content] = createEmptyMetadataFile(location_path, *columns, partition_by, configuration_ptr->getDataLakeSettings()[DataLakeStorageSetting::iceberg_format_version]);
    auto compression_method_str = local_context->getSettingsRef()[Setting::iceberg_metadata_compression_method].value;
    auto compression_method = chooseCompressionMethod(compression_method_str, compression_method_str);

    auto compression_suffix = compression_method_str;
    if (!compression_suffix.empty())
        compression_suffix = "." + compression_suffix;

    auto filename = fmt::format("{}metadata/v1{}.metadata.json", configuration_ptr->getRawPath().path, compression_suffix);
    auto cleanup = [&] ()
    {
        object_storage->removeObjectIfExists(StoredObject(filename));
    };

    writeMessageToFile(metadata_content, filename, object_storage, local_context, cleanup, compression_method);

    if (configuration_ptr->getDataLakeSettings()[DataLakeStorageSetting::iceberg_use_version_hint].value)
    {
        auto filename_version_hint = configuration_ptr->getRawPath().path + "metadata/version-hint.text";
        writeMessageToFile(filename, filename_version_hint, object_storage, local_context, cleanup);
    }
    if (catalog)
    {
        auto catalog_filename = configuration_ptr->getTypeName() + "://" + configuration_ptr->getNamespace() + "/" + configuration_ptr->getRawPath().path + "metadata/v1.metadata.json";
        const auto & [namespace_name, table_name] = DataLake::parseTableName(table_id_.getTableName());
        catalog->createTable(namespace_name, table_name, catalog_filename, metadata_content_object);
    }
}

DataLakeMetadataPtr IcebergMetadata::create(
    const ObjectStoragePtr & object_storage,
    const StorageObjectStorageConfigurationWeakPtr & configuration,
    const ContextPtr & local_context)
{
    auto configuration_ptr = configuration.lock();

    auto log = getLogger("IcebergMetadata");

    IcebergMetadataFilesCachePtr cache_ptr = nullptr;
    if (local_context->getSettingsRef()[Setting::use_iceberg_metadata_files_cache])
        cache_ptr = local_context->getIcebergMetadataFilesCache();
    else
        LOG_TRACE(log, "Not using in-memory cache for iceberg metadata files, because the setting use_iceberg_metadata_files_cache is false.");

    const auto [metadata_version, metadata_file_path, compression_method] = getLatestOrExplicitMetadataFileAndVersion(object_storage, configuration_ptr, cache_ptr, local_context, log.get());

    Poco::JSON::Object::Ptr object = getMetadataJSONObject(metadata_file_path, object_storage, configuration_ptr, cache_ptr, local_context, log, compression_method);

    auto format_version = object->getValue<int>(f_format_version);
    return std::make_unique<IcebergMetadata>(object_storage, configuration_ptr, local_context, metadata_version, format_version, object, cache_ptr, compression_method);
}

IcebergMetadata::IcebergHistory IcebergMetadata::getHistory(ContextPtr local_context) const
{
    auto configuration_ptr = configuration.lock();

    const auto [metadata_version, metadata_file_path, compression_method] = getLatestOrExplicitMetadataFileAndVersion(object_storage, configuration_ptr, persistent_components.metadata_cache, local_context, log.get());

    chassert([&]()
    {
        SharedLockGuard lock(mutex);
        return metadata_version == last_metadata_version;
    }());

    auto metadata_object = getMetadataJSONObject(metadata_file_path, object_storage, configuration_ptr, persistent_components.metadata_cache, local_context, log, compression_method);
    chassert([&]()
    {
        SharedLockGuard lock(mutex);
        return persistent_components.format_version == metadata_object->getValue<int>(f_format_version);
    }());

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


std::optional<size_t> IcebergMetadata::totalRows(ContextPtr local_context) const
{
    auto configuration_ptr = configuration.lock();
    if (!configuration_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Configuration is expired");

    SharedLockGuard lock(mutex);
    if (!relevant_snapshot)
    {
        ProfileEvents::increment(ProfileEvents::IcebergTrivialCountOptimizationApplied);
        return 0;
    }


    /// All these "hints" with total rows or bytes are optional both in
    /// metadata files and in manifest files, so we try all of them one by one
    if (relevant_snapshot->getTotalRows())
    {
        ProfileEvents::increment(ProfileEvents::IcebergTrivialCountOptimizationApplied);
        return relevant_snapshot->getTotalRows();
    }

    Int64 result = 0;
    for (const auto & manifest_list_entry : relevant_snapshot->manifest_list_entries)
    {
        auto manifest_file_ptr = getManifestFile(
            object_storage,
            configuration.lock(),
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
    auto configuration_ptr = configuration.lock();
    if (!configuration_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Configuration is expired");

    SharedLockGuard lock(mutex);
    if (!relevant_snapshot)
        return 0;

    /// All these "hints" with total rows or bytes are optional both in
    /// metadata files and in manifest files, so we try all of them one by one
    if (relevant_snapshot->total_bytes.has_value())
        return relevant_snapshot->total_bytes;

    Int64 result = 0;
    for (const auto & manifest_list_entry : relevant_snapshot->manifest_list_entries)
    {
        auto manifest_file_ptr = getManifestFile(
            object_storage,
            configuration.lock(),
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
     ContextPtr local_context) const
{
    SharedLockGuard lock(mutex);

    auto table_snapshot
        = std::make_shared<IcebergTableStateSnapshot>(last_metadata_version, relevant_snapshot_schema_id, relevant_snapshot_id);
    return std::make_shared<IcebergIterator>(
        object_storage,
        local_context,
        configuration.lock(),
        filter_dag,
        callback,
        table_snapshot,
        relevant_snapshot,
        persistent_components);
}

NamesAndTypesList IcebergMetadata::getTableSchema() const
{
    SharedLockGuard lock(mutex);
    return *persistent_components.schema_processor->getClickhouseTableSchemaById(relevant_snapshot_schema_id);
}

std::tuple<Int64, Int32> IcebergMetadata::getVersion() const
{
    SharedLockGuard lock(mutex);
    return std::make_tuple(relevant_snapshot_id, relevant_snapshot_schema_id);
}

SinkToStoragePtr IcebergMetadata::write(
    SharedHeader sample_block,
    const StorageID & table_id,
    ObjectStoragePtr /*object_storage*/,
    StorageObjectStorageConfigurationPtr /*configuration*/,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr context,
    std::shared_ptr<DataLake::ICatalog> catalog)
{
    if (context->getSettingsRef()[Setting::allow_experimental_insert_into_iceberg])
    {
        return std::make_shared<IcebergStorageSink>(
            object_storage, configuration.lock(), format_settings, sample_block, context, catalog, table_id);
    }
    else
    {
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "Insert into iceberg is experimental. "
            "To allow its usage, enable setting allow_experimental_insert_into_iceberg");
    }
}
}

#endif
