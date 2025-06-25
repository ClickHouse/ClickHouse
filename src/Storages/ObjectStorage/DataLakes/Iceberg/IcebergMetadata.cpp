#include "config.h"

#if USE_AVRO

#include <Core/Settings.h>
#include <Core/NamesAndTypes.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>

#include <Storages/ObjectStorage/DataLakes/Common.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include "Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h"
#include <Interpreters/ExpressionActions.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/AvroForIcebergDeserializer.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>

#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event IcebergTrivialCountOptimizationApplied;
    extern const Event IcebergVersionHintUsed;
}

namespace DB
{

namespace DataLakeStorageSetting
{
    extern const DataLakeStorageSettingsString iceberg_metadata_file_path;
    extern const DataLakeStorageSettingsString iceberg_metadata_table_uuid;
    extern const DataLakeStorageSettingsBool iceberg_recent_metadata_file_by_last_updated_ms_field;
    extern const DataLakeStorageSettingsBool iceberg_use_version_hint;
}

namespace ErrorCodes
{
extern const int FILE_DOESNT_EXIST;
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
extern const int ICEBERG_SPECIFICATION_VIOLATION;
}

namespace Setting
{
extern const SettingsInt64 iceberg_timestamp_ms;
extern const SettingsInt64 iceberg_snapshot_id;
extern const SettingsBool use_iceberg_metadata_files_cache;
extern const SettingsBool use_iceberg_partition_pruning;
}


using namespace Iceberg;

namespace
{

std::pair<Int32, Poco::JSON::Object::Ptr>
parseTableSchemaFromManifestFile(const AvroForIcebergDeserializer & deserializer, const String & manifest_file_name)
{
    auto schema_json_string = deserializer.tryGetAvroMetadataValue(f_schema);
    if (!schema_json_string.has_value())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot read Iceberg table: manifest file '{}' doesn't have field '{}' in its metadata",
            manifest_file_name, f_schema);
    Poco::JSON::Parser parser;
    Poco::Dynamic::Var json = parser.parse(*schema_json_string);
    const Poco::JSON::Object::Ptr & schema_object = json.extract<Poco::JSON::Object::Ptr>();
    Int32 schema_object_id = schema_object->getValue<int>(f_schema_id);
    return {schema_object_id, schema_object};
}

}


IcebergMetadata::IcebergMetadata(
    ObjectStoragePtr object_storage_,
    ConfigurationObserverPtr configuration_,
    const ContextPtr & context_,
    Int32 metadata_version_,
    Int32 format_version_,
    const Poco::JSON::Object::Ptr & metadata_object_,
    IcebergMetadataFilesCachePtr cache_ptr)
    : WithContext(context_)
    , object_storage(std::move(object_storage_))
    , configuration(std::move(configuration_))
    , schema_processor(IcebergSchemaProcessor())
    , log(getLogger("IcebergMetadata"))
    , manifest_cache(cache_ptr)
    , last_metadata_version(metadata_version_)
    , format_version(format_version_)
    , relevant_snapshot_schema_id(-1)
    , table_location(metadata_object_->getValue<String>(f_location))
{
    updateState(context_, metadata_object_, true);
}

std::pair<Poco::JSON::Object::Ptr, Int32> parseTableSchemaV2Method(const Poco::JSON::Object::Ptr & metadata_object)
{
    Poco::JSON::Object::Ptr schema;
    if (!metadata_object->has(f_current_schema_id))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse Iceberg table schema: '{}' field is missing in metadata", f_current_schema_id);
    auto current_schema_id = metadata_object->getValue<int>(f_current_schema_id);
    if (!metadata_object->has(f_schemas))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse Iceberg table schema: '{}' field is missing in metadata", f_schemas);
    auto schemas = metadata_object->get(f_schemas).extract<Poco::JSON::Array::Ptr>();
    if (schemas->size() == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse Iceberg table schema: '{}' field is empty", f_schemas);
    for (uint32_t i = 0; i != schemas->size(); ++i)
    {
        auto current_schema = schemas->getObject(i);
        if (!current_schema->has(f_schema_id))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse Iceberg table schema: '{}' field is missing in schema", f_schema_id);
        }
        if (current_schema->getValue<int>(f_schema_id) == current_schema_id)
        {
            schema = current_schema;
            break;
        }
    }

    if (!schema)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, R"(There is no schema with "{}" that matches "{}" in metadata)", f_schema_id, f_current_schema_id);
    if (schema->getValue<int>(f_schema_id) != current_schema_id)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, R"(Field "{}" of the schema doesn't match "{}" in metadata)", f_schema_id, f_current_schema_id);
    return {schema, current_schema_id};
}

std::pair<Poco::JSON::Object::Ptr, Int32> parseTableSchemaV1Method(const Poco::JSON::Object::Ptr & metadata_object)
{
    if (!metadata_object->has(f_schema))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse Iceberg table schema: '{}' field is missing in metadata", f_schema);
    Poco::JSON::Object::Ptr schema = metadata_object->getObject(f_schema);
    if (!metadata_object->has(f_schema_id))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse Iceberg table schema: '{}' field is missing in schema", f_schema_id);
    auto current_schema_id = schema->getValue<int>(f_schema_id);
    return {schema, current_schema_id};
}


void IcebergMetadata::addTableSchemaById(Int32 schema_id, Poco::JSON::Object::Ptr metadata_object)
{
    if (schema_processor.hasClickhouseTableSchemaById(schema_id))
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
            schema_processor.addIcebergTableSchema(current_schema);
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

    const auto [metadata_version, metadata_file_path]
        = getLatestOrExplicitMetadataFileAndVersion(object_storage, configuration_ptr, manifest_cache, local_context, log.get());

    bool metadata_file_changed = false;
    if (last_metadata_version != metadata_version)
    {
        last_metadata_version = metadata_version;
        metadata_file_changed = true;
    }

    auto metadata_object = getMetadataJSONObject(metadata_file_path, object_storage, configuration_ptr, manifest_cache, local_context, log);
    chassert(format_version == metadata_object->getValue<int>(f_format_version));

    auto previous_snapshot_id = relevant_snapshot_id;
    auto previous_snapshot_schema_id = relevant_snapshot_schema_id;

    updateState(local_context, metadata_object, metadata_file_changed);

    if (previous_snapshot_id != relevant_snapshot_id)
    {
        cached_unprunned_files_for_last_processed_snapshot = std::nullopt;
        schema_id_by_data_file_initialized.store(false);
        return true;
    }
    return previous_snapshot_schema_id != relevant_snapshot_schema_id;
}

void IcebergMetadata::updateSnapshot(Poco::JSON::Object::Ptr metadata_object)
{
    auto configuration_ptr = configuration.lock();
    if (!metadata_object->has(f_snapshots))
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "No snapshot set found in metadata for iceberg table `{}`, it is impossible to get manifest list by snapshot id `{}`",
            configuration_ptr->getPath(),
            relevant_snapshot_id);
    auto snapshots = metadata_object->get(f_snapshots).extract<Poco::JSON::Array::Ptr>();
    for (size_t i = 0; i < snapshots->size(); ++i)
    {
        const auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
        if (snapshot->getValue<Int64>(f_snapshot_id) == relevant_snapshot_id)
        {
            if (!snapshot->has(f_manifest_list))
                throw Exception(
                    ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                    "No manifest list found for snapshot id `{}` for iceberg table `{}`",
                    relevant_snapshot_id,
                    configuration_ptr->getPath());
            std::optional<size_t> total_rows;
            std::optional<size_t> total_bytes;

            if (snapshot->has(f_summary))
            {
                auto summary_object = snapshot->get(f_summary).extract<Poco::JSON::Object::Ptr>();
                if (summary_object->has(f_total_records))
                    total_rows = summary_object->getValue<Int64>(f_total_records);

                if (summary_object->has(f_total_files_size))
                    total_bytes = summary_object->getValue<Int64>(f_total_files_size);
            }

            relevant_snapshot = IcebergSnapshot{
                getManifestList(getProperFilePathFromMetadataInfo(
                    snapshot->getValue<String>(f_manifest_list), configuration_ptr->getPath(), table_location)),
                relevant_snapshot_id, total_rows, total_bytes};

            if (!snapshot->has(f_schema_id))
                throw Exception(
                    ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                    "No schema id found for snapshot id `{}` for iceberg table `{}`",
                    relevant_snapshot_id,
                    configuration_ptr->getPath());
            relevant_snapshot_schema_id = snapshot->getValue<Int32>(f_schema_id);
            addTableSchemaById(relevant_snapshot_schema_id, metadata_object);
            return;
        }
    }
    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "No manifest list is found for snapshot id `{}` in metadata for iceberg table `{}`",
        relevant_snapshot_id,
        configuration_ptr->getPath());
}

void IcebergMetadata::updateState(const ContextPtr & local_context, Poco::JSON::Object::Ptr metadata_object, bool metadata_file_changed)
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
            configuration_ptr->getPath());
    }
    if (timestamp_changed)
    {
        Int64 closest_timestamp = 0;
        Int64 query_timestamp = local_context->getSettingsRef()[Setting::iceberg_timestamp_ms];
        if (!metadata_object->has(f_snapshot_log))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No snapshot log found in metadata for iceberg table {} so it is impossible to get relevant snapshot id using timestamp", configuration_ptr->getPath());
        auto snapshots = metadata_object->get(f_snapshot_log).extract<Poco::JSON::Array::Ptr>();
        relevant_snapshot_id = -1;
        for (size_t i = 0; i < snapshots->size(); ++i)
        {
            const auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
            Int64 snapshot_timestamp = snapshot->getValue<Int64>(f_timestamp_ms);
            if (snapshot_timestamp <= query_timestamp && snapshot_timestamp > closest_timestamp)
            {
                closest_timestamp = snapshot_timestamp;
                relevant_snapshot_id = snapshot->getValue<Int64>(f_snapshot_id);
            }
        }
        if (relevant_snapshot_id < 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No snapshot found in snapshot log before requested timestamp for iceberg table {}", configuration_ptr->getPath());
        updateSnapshot(metadata_object);
    }
    else if (snapshot_id_changed)
    {
        relevant_snapshot_id = local_context->getSettingsRef()[Setting::iceberg_snapshot_id];
        updateSnapshot(metadata_object);
    }
    else if (metadata_file_changed)
    {
        if (!metadata_object->has(f_current_snapshot_id))
            relevant_snapshot_id = -1;
        else
            relevant_snapshot_id = metadata_object->getValue<Int64>(f_current_snapshot_id);
        if (relevant_snapshot_id != -1)
        {
            updateSnapshot(metadata_object);
        }
        relevant_snapshot_schema_id = parseTableSchema(metadata_object, schema_processor, log);
    }
}

std::optional<Int32> IcebergMetadata::getSchemaVersionByFileIfOutdated(String data_path) const
{
    if (!schema_id_by_data_file_initialized.load())
    {
        std::lock_guard lock(schema_id_by_data_file_mutex);
        if (!schema_id_by_data_file_initialized.load())
        {
            initializeSchemasFromManifestList(relevant_snapshot->manifest_list_entries);
            schema_id_by_data_file_initialized.store(true);
        }
    }
    auto schema_id_it = schema_id_by_data_file.find(data_path);
    if (schema_id_it == schema_id_by_data_file.end())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot find manifest file for data file: {}", data_path);
    }
    auto schema_id = schema_id_it->second;
    if (schema_id == relevant_snapshot_schema_id)
        return std::nullopt;
    return std::optional{schema_id};
}


DataLakeMetadataPtr IcebergMetadata::create(
    const ObjectStoragePtr & object_storage,
    const ConfigurationObserverPtr & configuration,
    const ContextPtr & local_context)
{
    auto configuration_ptr = configuration.lock();

    auto log = getLogger("IcebergMetadata");

    IcebergMetadataFilesCachePtr cache_ptr = nullptr;
    if (local_context->getSettingsRef()[Setting::use_iceberg_metadata_files_cache])
        cache_ptr = local_context->getIcebergMetadataFilesCache();
    else
        LOG_TRACE(log, "Not using in-memory cache for iceberg metadata files, because the setting use_iceberg_metadata_files_cache is false.");

    const auto [metadata_version, metadata_file_path] = getLatestOrExplicitMetadataFileAndVersion(object_storage, configuration_ptr, cache_ptr, local_context, log.get());

    Poco::JSON::Object::Ptr object = getMetadataJSONObject(metadata_file_path, object_storage, configuration_ptr, cache_ptr, local_context, log);

    IcebergSchemaProcessor schema_processor;

    auto format_version = object->getValue<int>(f_format_version);

    auto ptr
        = std::make_unique<IcebergMetadata>(object_storage, configuration_ptr, local_context, metadata_version, format_version, object, cache_ptr);

    return ptr;
}

void IcebergMetadata::initializeSchemasFromManifestList(ManifestFileCacheKeys manifest_list_ptr) const
{
    for (const auto & manifest_list_entry : manifest_list_ptr)
    {
        auto manifest_file_ptr = getManifestFile(manifest_list_entry.manifest_file_path, manifest_list_entry.added_sequence_number);
        initializeSchemasFromManifestFile(manifest_file_ptr);
    }
}

void IcebergMetadata::initializeSchemasFromManifestFile(ManifestFilePtr manifest_file_ptr) const
{
    for (const auto & manifest_file_entry : manifest_file_ptr->getFiles())
    {
        if (std::holds_alternative<DataFileEntry>(manifest_file_entry.file))
            schema_id_by_data_file.emplace(std::get<DataFileEntry>(manifest_file_entry.file).file_name, manifest_file_ptr->getSchemaId());
    }
}

ManifestFileCacheKeys IcebergMetadata::getManifestList(const String & filename) const
{
    auto configuration_ptr = configuration.lock();
    if (configuration_ptr == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Configuration is expired");

    auto create_fn = [&]()
    {
        StorageObjectStorage::ObjectInfo object_info(filename);
        auto manifest_list_buf = StorageObjectStorageSource::createReadBuffer(object_info, object_storage, getContext(), log);
        AvroForIcebergDeserializer manifest_list_deserializer(std::move(manifest_list_buf), filename, getFormatSettings(getContext()));

        ManifestFileCacheKeys manifest_file_cache_keys;

        for (size_t i = 0; i < manifest_list_deserializer.rows(); ++i)
        {
            const std::string file_path = manifest_list_deserializer.getValueFromRowByName(i, f_manifest_path, TypeIndex::String).safeGet<std::string>();
            const auto manifest_file_name = getProperFilePathFromMetadataInfo(file_path, configuration_ptr->getPath(), table_location);
            Int64 added_sequence_number = 0;
            if (format_version > 1)
                added_sequence_number = manifest_list_deserializer.getValueFromRowByName(i, f_sequence_number, TypeIndex::Int64).safeGet<Int64>();
            manifest_file_cache_keys.emplace_back(manifest_file_name, added_sequence_number);
        }
        /// We only return the list of {file name, seq number} for cache.
        /// Because ManifestList holds a list of ManifestFilePtr which consume much memory space.
        /// ManifestFilePtr is shared pointers can be held for too much time, so we cache ManifestFile separately.
        return manifest_file_cache_keys;
    };

    ManifestFileCacheKeys manifest_file_cache_keys;
    if (manifest_cache)
    {
        manifest_file_cache_keys = manifest_cache->getOrSetManifestFileCacheKeys(IcebergMetadataFilesCache::getKey(configuration_ptr, filename), create_fn);
    }
    else
    {
        manifest_file_cache_keys = create_fn();
    }
    return manifest_file_cache_keys;
}

IcebergMetadata::IcebergHistory IcebergMetadata::getHistory() const
{
    auto configuration_ptr = configuration.lock();

    const auto [metadata_version, metadata_file_path] = getLatestOrExplicitMetadataFileAndVersion(object_storage, configuration_ptr, manifest_cache, getContext(), log.get());

    chassert(metadata_version == last_metadata_version);

    auto metadata_object = getMetadataJSONObject(metadata_file_path, object_storage, configuration_ptr, manifest_cache, getContext(), log);

    chassert(format_version == metadata_object->getValue<int>(f_format_version));

    /// History
    std::vector<Iceberg::IcebergHistoryRecord> iceberg_history;

    auto snapshots = metadata_object->get(f_snapshots).extract<Poco::JSON::Array::Ptr>();
    auto snapshot_logs = metadata_object->get(f_snapshot_log).extract<Poco::JSON::Array::Ptr>();

    std::vector<Int64> ancestors;
    std::map<Int64, Int64> parents_list;
    for (size_t i = 0; i < snapshots->size(); ++i)
    {
        const auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
        auto snapshot_id = snapshot->getValue<Int64>(f_snapshot_id);

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
        history_record.snapshot_id = snapshot->getValue<Int64>(f_snapshot_id);

        if (snapshot->has(f_parent_snapshot_id) && !snapshot->isNull(f_parent_snapshot_id))
            history_record.parent_id = snapshot->getValue<Int64>(f_parent_snapshot_id);
        else
            history_record.parent_id = 0;

        for (size_t j = 0; j < snapshot_logs->size(); ++j)
        {
            const auto snapshot_log = snapshot_logs->getObject(static_cast<UInt32>(j));
            if (snapshot_log->getValue<Int64>(f_snapshot_id) == history_record.snapshot_id)
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

ManifestFilePtr IcebergMetadata::getManifestFile(const String & filename, Int64 inherited_sequence_number) const
{
    auto configuration_ptr = configuration.lock();

    auto create_fn = [&]()
    {
        ObjectInfo manifest_object_info(filename);
        auto buffer = StorageObjectStorageSource::createReadBuffer(manifest_object_info, object_storage, getContext(), log);
        AvroForIcebergDeserializer manifest_file_deserializer(std::move(buffer), filename, getFormatSettings(getContext()));
        auto [schema_id, schema_object] = parseTableSchemaFromManifestFile(manifest_file_deserializer, filename);
        schema_processor.addIcebergTableSchema(schema_object);
        return std::make_shared<ManifestFileContent>(
            manifest_file_deserializer,
            format_version,
            configuration_ptr->getPath(),
            schema_id,
            schema_object,
            schema_processor,
            inherited_sequence_number,
            table_location,
            getContext());
    };

    if (manifest_cache)
    {
        auto manifest_file = manifest_cache->getOrSetManifestFile(IcebergMetadataFilesCache::getKey(configuration_ptr, filename), create_fn);
        schema_processor.addIcebergTableSchema(manifest_file->getSchemaObject());
        return manifest_file;
    }
    return create_fn();
}

Strings IcebergMetadata::getDataFiles(const ActionsDAG * filter_dag, ContextPtr local_context) const
{
    if (!relevant_snapshot)
        return {};

    bool use_partition_pruning = filter_dag && local_context->getSettingsRef()[Setting::use_iceberg_partition_pruning];

    if (!use_partition_pruning && cached_unprunned_files_for_last_processed_snapshot.has_value())
        return cached_unprunned_files_for_last_processed_snapshot.value();

    Strings data_files;
    for (const auto & manifest_list_entry : relevant_snapshot->manifest_list_entries)
    {
        auto manifest_file_ptr = getManifestFile(manifest_list_entry.manifest_file_path, manifest_list_entry.added_sequence_number);
        initializeSchemasFromManifestFile(manifest_file_ptr);
        ManifestFilesPruner pruner(
            schema_processor, relevant_snapshot_schema_id,
            use_partition_pruning ? filter_dag : nullptr,
            *manifest_file_ptr, local_context);
        const auto & data_files_in_manifest = manifest_file_ptr->getFiles();
        for (const auto & manifest_file_entry : data_files_in_manifest)
        {
            if (manifest_file_entry.status != ManifestEntryStatus::DELETED)
            {
                if (!pruner.canBePruned(manifest_file_entry))
                {
                    if (std::holds_alternative<DataFileEntry>(manifest_file_entry.file))
                        data_files.push_back(std::get<DataFileEntry>(manifest_file_entry.file).file_name);
                }
            }
        }
    }

    schema_id_by_data_file_initialized = true;
    if (!use_partition_pruning)
    {
        cached_unprunned_files_for_last_processed_snapshot = data_files;
        return cached_unprunned_files_for_last_processed_snapshot.value();
    }

    return data_files;
}

std::optional<size_t> IcebergMetadata::totalRows() const
{
    auto configuration_ptr = configuration.lock();
    if (!configuration_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Configuration is expired");

    if (!relevant_snapshot)
    {
        ProfileEvents::increment(ProfileEvents::IcebergTrivialCountOptimizationApplied);
        return 0;
    }

    /// All these "hints" with total rows or bytes are optional both in
    /// metadata files and in manifest files, so we try all of them one by one
    if (relevant_snapshot->total_rows.has_value())
    {
        ProfileEvents::increment(ProfileEvents::IcebergTrivialCountOptimizationApplied);
        return relevant_snapshot->total_rows;
    }

    Int64 result = 0;
    for (const auto & manifest_list_entry : relevant_snapshot->manifest_list_entries)
    {
        auto manifest_file_ptr = getManifestFile(manifest_list_entry.manifest_file_path, manifest_list_entry.added_sequence_number);
        auto count = manifest_file_ptr->getRowsCountInAllDataFilesExcludingDeleted();
        if (!count.has_value())
            return {};

        result += count.value();
    }

    ProfileEvents::increment(ProfileEvents::IcebergTrivialCountOptimizationApplied);
    return result;
}


std::optional<size_t> IcebergMetadata::totalBytes() const
{
    auto configuration_ptr = configuration.lock();
    if (!configuration_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Configuration is expired");

    if (!relevant_snapshot)
        return 0;

    /// All these "hints" with total rows or bytes are optional both in
    /// metadata files and in manifest files, so we try all of them one by one
    if (relevant_snapshot->total_bytes.has_value())
        return relevant_snapshot->total_bytes;

    Int64 result = 0;
    for (const auto & manifest_list_entry : relevant_snapshot->manifest_list_entries)
    {
        auto manifest_file_ptr = getManifestFile(manifest_list_entry.manifest_file_path, manifest_list_entry.added_sequence_number);
        auto count = manifest_file_ptr->getBytesCountInAllDataFiles();
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
    return createKeysIterator(getDataFiles(filter_dag, local_context), object_storage, callback);
}

}

#endif
