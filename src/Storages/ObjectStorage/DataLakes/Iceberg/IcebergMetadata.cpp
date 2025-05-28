#include "config.h"

#if USE_AVRO

#include <Core/Settings.h>
#include <Core/NamesAndTypes.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

#include <Storages/ObjectStorage/DataLakes/Common.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/StorageObjectStorageSettings.h>
#include <Interpreters/ExpressionActions.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/AvroForIcebergDeserializer.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>

#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event IcebergTrivialCountOptimizationApplied;
}

namespace DB
{

namespace StorageObjectStorageSetting
{
    extern const StorageObjectStorageSettingsString iceberg_metadata_file_path;
    extern const StorageObjectStorageSettingsString iceberg_metadata_table_uuid;
    extern const StorageObjectStorageSettingsBool iceberg_recent_metadata_file_by_last_updated_ms_field;
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


constexpr const char * SEQUENCE_NUMBER_COLUMN = "sequence_number";
constexpr const char * MANIFEST_FILE_PATH_COLUMN = "manifest_path";
constexpr const char * FORMAT_VERSION_FIELD = "format-version";
constexpr const char * CURRENT_SNAPSHOT_ID_FIELD_IN_METADATA_FILE = "current-snapshot-id";
constexpr const char * SNAPSHOT_ID_FIELD_IN_SNAPSHOT = "snapshot-id";
constexpr const char * MANIFEST_LIST_PATH_FIELD = "manifest-list";
constexpr const char * SNAPSHOT_LOG_FIELD = "snapshot-log";
constexpr const char * TIMESTAMP_FIELD_INSIDE_SNAPSHOT = "timestamp-ms";
constexpr const char * TABLE_LOCATION_FIELD = "location";
constexpr const char * SNAPSHOTS_FIELD = "snapshots";
constexpr const char * LAST_UPDATED_MS_FIELD = "last-updated-ms";

namespace
{

std::pair<Int32, Poco::JSON::Object::Ptr>
parseTableSchemaFromManifestFile(const AvroForIcebergDeserializer & deserializer, const String & manifest_file_name)
{
    auto schema_json_string = deserializer.tryGetAvroMetadataValue("schema");
    if (!schema_json_string.has_value())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot read Iceberg table: manifest file '{}' doesn't have table schema in its metadata",
            manifest_file_name);
    Poco::JSON::Parser parser;
    Poco::Dynamic::Var json = parser.parse(*schema_json_string);
    const Poco::JSON::Object::Ptr & schema_object = json.extract<Poco::JSON::Object::Ptr>();
    Int32 schema_object_id = schema_object->getValue<int>("schema-id");
    return {schema_object_id, schema_object};
}


std::string normalizeUuid(const std::string & uuid)
{
    std::string result;
    result.reserve(uuid.size());
    for (char c : uuid)
    {
        if (std::isalnum(c))
        {
            result.push_back(std::tolower(c));
        }
    }
    return result;
}

Poco::JSON::Object::Ptr
readJSON(const String & metadata_file_path, ObjectStoragePtr object_storage, const ContextPtr & local_context, LoggerPtr log)
{
    ObjectInfo object_info(metadata_file_path);
    auto buf = StorageObjectStorageSource::createReadBuffer(object_info, object_storage, local_context, log);

    String json_str;
    readJSONObjectPossiblyInvalid(json_str, *buf);

    Poco::JSON::Parser parser; /// For some reason base/base/JSON.h can not parse this json file
    Poco::Dynamic::Var json = parser.parse(json_str);
    return json.extract<Poco::JSON::Object::Ptr>();
}


}


IcebergMetadata::IcebergMetadata(
    ObjectStoragePtr object_storage_,
    ConfigurationObserverPtr configuration_,
    const DB::ContextPtr & context_,
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
    , last_metadata_object(metadata_object_)
    , format_version(format_version_)
    , relevant_snapshot_schema_id(-1)
    , table_location(last_metadata_object->getValue<String>(TABLE_LOCATION_FIELD))
{
    updateState(context_, true);
}

std::pair<Poco::JSON::Object::Ptr, Int32> parseTableSchemaV2Method(const Poco::JSON::Object::Ptr & metadata_object)
{
    Poco::JSON::Object::Ptr schema;
    if (!metadata_object->has("current-schema-id"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse Iceberg table schema: 'current-schema-id' field is missing in metadata");
    auto current_schema_id = metadata_object->getValue<int>("current-schema-id");
    if (!metadata_object->has("schemas"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse Iceberg table schema: 'schemas' field is missing in metadata");
    auto schemas = metadata_object->get("schemas").extract<Poco::JSON::Array::Ptr>();
    if (schemas->size() == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse Iceberg table schema: schemas field is empty");
    for (uint32_t i = 0; i != schemas->size(); ++i)
    {
        auto current_schema = schemas->getObject(i);
        if (!current_schema->has("schema-id"))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse Iceberg table schema: 'schema-id' field is missing in schema");
        }
        if (current_schema->getValue<int>("schema-id") == current_schema_id)
        {
            schema = current_schema;
            break;
        }
    }

    if (!schema)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, R"(There is no schema with "schema-id" that matches "current-schema-id" in metadata)");
    if (schema->getValue<int>("schema-id") != current_schema_id)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, R"(Field "schema-id" of the schema doesn't match "current-schema-id" in metadata)");
    return {schema, current_schema_id};
}

std::pair<Poco::JSON::Object::Ptr, Int32> parseTableSchemaV1Method(const Poco::JSON::Object::Ptr & metadata_object)
{
    if (!metadata_object->has("schema"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse Iceberg table schema: 'schema' field is missing in metadata");
    Poco::JSON::Object::Ptr schema = metadata_object->getObject("schema");
    if (!metadata_object->has("schema"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse Iceberg table schema: 'schema-id' field is missing in schema");
    auto current_schema_id = schema->getValue<int>("schema-id");
    return {schema, current_schema_id};
}


void IcebergMetadata::addTableSchemaById(Int32 schema_id)
{
    if (schema_processor.hasClickhouseTableSchemaById(schema_id))
        return;
    if (!last_metadata_object->has("schemas"))
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Cannot parse Iceberg table schema with id `{}`: 'schemas' field is missing in metadata", schema_id);
    }
    auto schemas = last_metadata_object->get("schemas").extract<Poco::JSON::Array::Ptr>();
    for (uint32_t i = 0; i != schemas->size(); ++i)
    {
        auto current_schema = schemas->getObject(i);
        if (current_schema->has("schema-id") && current_schema->getValue<int>("schema-id") == schema_id)
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
    const auto format_version = metadata_object->getValue<Int32>(FORMAT_VERSION_FIELD);
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

static std::pair<Int32, String> getMetadataFileAndVersion(const std::string & path)
{
    String file_name(path.begin() + path.find_last_of('/') + 1, path.end());
    String version_str;
    /// v<V>.metadata.json
    if (file_name.starts_with('v'))
        version_str = String(file_name.begin() + 1, file_name.begin() + file_name.find_first_of('.'));
    /// <V>-<random-uuid>.metadata.json
    else
        version_str = String(file_name.begin(), file_name.begin() + file_name.find_first_of('-'));

    if (!std::all_of(version_str.begin(), version_str.end(), isdigit))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Bad metadata file name: {}. Expected vN.metadata.json where N is a number", file_name);

    return std::make_pair(std::stoi(version_str), path);
}

enum class MostRecentMetadataFileSelectionWay
{
    BY_LAST_UPDATED_MS_FIELD,
    BY_METADATA_FILE_VERSION
};

struct ShortMetadataFileInfo
{
    UInt32 version;
    UInt64 last_updated_ms;
    String path;
};


/**
 * Each version of table metadata is stored in a `metadata` directory and
 * has one of 2 formats:
 *   1) v<V>.metadata.json, where V - metadata version.
 *   2) <V>-<random-uuid>.metadata.json, where V - metadata version
 */
static std::pair<Int32, String> getLatestMetadataFileAndVersion(
    const ObjectStoragePtr & object_storage,
    const StorageObjectStorage::Configuration & configuration,
    const ContextPtr & local_context,
    const std::optional<String> & table_uuid)
{
    auto log = getLogger("IcebergMetadataFileResolver");
    MostRecentMetadataFileSelectionWay selection_way
        = configuration.getSettingsRef()[StorageObjectStorageSetting::iceberg_recent_metadata_file_by_last_updated_ms_field].value
        ? MostRecentMetadataFileSelectionWay::BY_LAST_UPDATED_MS_FIELD
        : MostRecentMetadataFileSelectionWay::BY_METADATA_FILE_VERSION;
    bool need_all_metadata_files_parsing
        = (selection_way == MostRecentMetadataFileSelectionWay::BY_LAST_UPDATED_MS_FIELD) || table_uuid.has_value();
    const auto metadata_files = listFiles(*object_storage, configuration, "metadata", ".metadata.json");
    if (metadata_files.empty())
    {
        throw Exception(
            ErrorCodes::FILE_DOESNT_EXIST, "The metadata file for Iceberg table with path {} doesn't exist", configuration.getPath());
    }
    std::vector<ShortMetadataFileInfo> metadata_files_with_versions;
    metadata_files_with_versions.reserve(metadata_files.size());
    for (const auto & path : metadata_files)
    {
        auto [version, metadata_file_path] = getMetadataFileAndVersion(path);
        if (need_all_metadata_files_parsing)
        {
            auto metadata_file_object = readJSON(metadata_file_path, object_storage, local_context, log);
            if (table_uuid.has_value())
            {
                if (metadata_file_object->has("table-uuid"))
                {
                    auto current_table_uuid = metadata_file_object->getValue<String>("table-uuid");
                    if (normalizeUuid(table_uuid.value()) == normalizeUuid(current_table_uuid))
                    {
                        metadata_files_with_versions.emplace_back(
                            version, metadata_file_object->getValue<UInt64>(LAST_UPDATED_MS_FIELD), metadata_file_path);
                    }
                }
                else
                {
                    Int64 format_version = metadata_file_object->getValue<Int64>(FORMAT_VERSION_FIELD);
                    throw Exception(
                        format_version == 1 ? ErrorCodes::BAD_ARGUMENTS : ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                        "Table UUID is not specified in some metadata files for table by path {}",
                        metadata_file_path);
                }
            }
            else
            {
                metadata_files_with_versions.emplace_back(version, metadata_file_object->getValue<UInt64>(LAST_UPDATED_MS_FIELD), metadata_file_path);
            }
        }
        else
        {
            metadata_files_with_versions.emplace_back(version, 0, metadata_file_path);
        }
    }

    /// Get the latest version of metadata file: v<V>.metadata.json
    const ShortMetadataFileInfo & latest_metadata_file_info = [&]()
    {
        if (selection_way == MostRecentMetadataFileSelectionWay::BY_LAST_UPDATED_MS_FIELD)
        {
            return *std::max_element(
                metadata_files_with_versions.begin(),
                metadata_files_with_versions.end(),
                [](const ShortMetadataFileInfo & a, const ShortMetadataFileInfo & b) { return a.last_updated_ms < b.last_updated_ms; });
        }
        else
        {
            return *std::max_element(
                metadata_files_with_versions.begin(),
                metadata_files_with_versions.end(),
                [](const ShortMetadataFileInfo & a, const ShortMetadataFileInfo & b) { return a.version < b.version; });
        }
    }();
    return {latest_metadata_file_info.version, latest_metadata_file_info.path};
}

static std::pair<Int32, String> getLatestOrExplicitMetadataFileAndVersion(
    const ObjectStoragePtr & object_storage,
    const StorageObjectStorage::Configuration & configuration,
    const ContextPtr & local_context,
    Poco::Logger * log)
{
    if (configuration.getSettingsRef()[StorageObjectStorageSetting::iceberg_metadata_file_path].changed)
    {
        auto explicit_metadata_path = configuration.getSettingsRef()[StorageObjectStorageSetting::iceberg_metadata_file_path].value;
        try
        {
            LOG_TEST(log, "Explicit metadata file path is specified {}, will read from this metadata file", explicit_metadata_path);
            std::filesystem::path p(explicit_metadata_path);
            auto it = p.begin();
            if (it != p.end())
            {
                if (*it == "." || *it == "..")
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Relative paths are not allowed");
            }
            auto prefix_storage_path = configuration.getPath();
            if (!explicit_metadata_path.starts_with(prefix_storage_path))
                explicit_metadata_path = std::filesystem::path(prefix_storage_path) / explicit_metadata_path;
            return getMetadataFileAndVersion(explicit_metadata_path);
        }
        catch (const std::exception & ex)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid path {} specified for iceberg_metadata_file_path: '{}'", explicit_metadata_path, ex.what());
        }
    }
    else if (configuration.getSettingsRef()[StorageObjectStorageSetting::iceberg_metadata_table_uuid].changed)
    {
        std::optional<String> table_uuid = configuration.getSettingsRef()[StorageObjectStorageSetting::iceberg_metadata_table_uuid].value;
        return getLatestMetadataFileAndVersion(object_storage, configuration, local_context, table_uuid);
    }
    else
    {
        return getLatestMetadataFileAndVersion(object_storage, configuration, local_context, std::nullopt);
    }
}


bool IcebergMetadata::update(const ContextPtr & local_context)
{
    auto configuration_ptr = configuration.lock();

    const auto [metadata_version, metadata_file_path]
        = getLatestOrExplicitMetadataFileAndVersion(object_storage, *configuration_ptr, local_context, log.get());

    bool metadata_file_changed = false;
    if (last_metadata_version != metadata_version)
    {
        last_metadata_version = metadata_version;
        last_metadata_object = ::DB::readJSON(metadata_file_path, object_storage, local_context, log);
        metadata_file_changed = true;
    }

    chassert(format_version == last_metadata_object->getValue<int>(FORMAT_VERSION_FIELD));

    auto previous_snapshot_id = relevant_snapshot_id;
    auto previous_snapshot_schema_id = relevant_snapshot_schema_id;

    updateState(local_context, metadata_file_changed);

    if (previous_snapshot_id != relevant_snapshot_id)
    {
        cached_unprunned_files_for_last_processed_snapshot = std::nullopt;
        return true;
    }
    return previous_snapshot_schema_id != relevant_snapshot_schema_id;
}

void IcebergMetadata::updateSnapshot()
{
    auto configuration_ptr = configuration.lock();
    if (!last_metadata_object->has(SNAPSHOTS_FIELD))
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "No snapshot set found in metadata for iceberg table `{}`, it is impossible to get manifest list by snapshot id `{}`",
            configuration_ptr->getPath(),
            relevant_snapshot_id);
    auto snapshots = last_metadata_object->get(SNAPSHOTS_FIELD).extract<Poco::JSON::Array::Ptr>();
    for (size_t i = 0; i < snapshots->size(); ++i)
    {
        const auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
        if (snapshot->getValue<Int64>(SNAPSHOT_ID_FIELD_IN_SNAPSHOT) == relevant_snapshot_id)
        {
            if (!snapshot->has("manifest-list"))
                throw Exception(
                    ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                    "No manifest list found for snapshot id `{}` for iceberg table `{}`",
                    relevant_snapshot_id,
                    configuration_ptr->getPath());
            std::optional<size_t> total_rows;
            std::optional<size_t> total_bytes;

            if (snapshot->has("summary"))
            {
                auto summary_object = snapshot->get("summary").extract<Poco::JSON::Object::Ptr>();
                if (summary_object->has("total-records"))
                    total_rows = summary_object->getValue<Int64>("total-records");

                if (summary_object->has("total-files-size"))
                    total_bytes = summary_object->getValue<Int64>("total-files-size");
            }

            relevant_snapshot = IcebergSnapshot{
                getManifestList(getProperFilePathFromMetadataInfo(
                    snapshot->getValue<String>(MANIFEST_LIST_PATH_FIELD), configuration_ptr->getPath(), table_location)),
                relevant_snapshot_id, total_rows, total_bytes};

            if (!snapshot->has("schema-id"))
                throw Exception(
                    ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                    "No schema id found for snapshot id `{}` for iceberg table `{}`",
                    relevant_snapshot_id,
                    configuration_ptr->getPath());
            relevant_snapshot_schema_id = snapshot->getValue<Int32>("schema-id");
            addTableSchemaById(relevant_snapshot_schema_id);
            return;
        }
    }
    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "No manifest list is found for snapshot id `{}` in metadata for iceberg table `{}`",
        relevant_snapshot_id,
        configuration_ptr->getPath());
}

void IcebergMetadata::updateState(const ContextPtr & local_context, bool metadata_file_changed)
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
        if (!last_metadata_object->has(SNAPSHOT_LOG_FIELD))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No snapshot log found in metadata for iceberg table {} so it is impossible to get relevant snapshot id using timestamp", configuration_ptr->getPath());
        auto snapshots = last_metadata_object->get(SNAPSHOT_LOG_FIELD).extract<Poco::JSON::Array::Ptr>();
        relevant_snapshot_id = -1;
        for (size_t i = 0; i < snapshots->size(); ++i)
        {
            const auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
            Int64 snapshot_timestamp = snapshot->getValue<Int64>(TIMESTAMP_FIELD_INSIDE_SNAPSHOT);
            if (snapshot_timestamp <= query_timestamp && snapshot_timestamp > closest_timestamp)
            {
                closest_timestamp = snapshot_timestamp;
                relevant_snapshot_id = snapshot->getValue<Int64>(SNAPSHOT_ID_FIELD_IN_SNAPSHOT);
            }
        }
        if (relevant_snapshot_id < 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No snapshot found in snapshot log before requested timestamp for iceberg table {}", configuration_ptr->getPath());
        updateSnapshot();
    }
    else if (snapshot_id_changed)
    {
        relevant_snapshot_id = local_context->getSettingsRef()[Setting::iceberg_snapshot_id];
        updateSnapshot();
    }
    else if (metadata_file_changed)
    {
        if (!last_metadata_object->has(CURRENT_SNAPSHOT_ID_FIELD_IN_METADATA_FILE))
            relevant_snapshot_id = -1;
        else
            relevant_snapshot_id = last_metadata_object->getValue<Int64>(CURRENT_SNAPSHOT_ID_FIELD_IN_METADATA_FILE);
        if (relevant_snapshot_id != -1)
        {
            updateSnapshot();
        }
        relevant_snapshot_schema_id = parseTableSchema(last_metadata_object, schema_processor, log);
    }
}

std::optional<Int32> IcebergMetadata::getSchemaVersionByFileIfOutdated(String data_path) const
{
    auto manifest_file_it = manifest_file_by_data_file.find(data_path);
    if (manifest_file_it == manifest_file_by_data_file.end())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot find manifest file for data file: {}", data_path);
    }
    const ManifestFileContent & manifest_file = *manifest_file_it->second;
    auto schema_id = manifest_file.getSchemaId();
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

    Poco::JSON::Object::Ptr object = nullptr;
    IcebergMetadataFilesCachePtr cache_ptr = nullptr;
    if (local_context->getSettingsRef()[Setting::use_iceberg_metadata_files_cache])
        cache_ptr = local_context->getIcebergMetadataFilesCache();
    else
        LOG_TRACE(log, "Not using in-memory cache for iceberg metadata files, because the setting use_iceberg_metadata_files_cache is false.");

    const auto [metadata_version, metadata_file_path] = getLatestOrExplicitMetadataFileAndVersion(object_storage, *configuration_ptr, local_context, log.get());

    auto create_fn = [&]()
    {
        ObjectInfo object_info(metadata_file_path); // NOLINT
        auto buf = StorageObjectStorageSource::createReadBuffer(object_info, object_storage, local_context, log);

        String json_str;
        readJSONObjectPossiblyInvalid(json_str, *buf);

        Poco::JSON::Parser parser; /// For some reason base/base/JSON.h can not parse this json file
        Poco::Dynamic::Var json = parser.parse(json_str);
        return std::make_pair(json.extract<Poco::JSON::Object::Ptr>(), json_str.size());
    };

    if (cache_ptr)
        object = cache_ptr->getOrSetTableMetadata(IcebergMetadataFilesCache::getKey(configuration_ptr, metadata_file_path), create_fn);
    else
        object = create_fn().first;

    IcebergSchemaProcessor schema_processor;

    auto format_version = object->getValue<int>(FORMAT_VERSION_FIELD);

    auto ptr
        = std::make_unique<IcebergMetadata>(object_storage, configuration_ptr, local_context, metadata_version, format_version, object, cache_ptr);

    return ptr;
}

void IcebergMetadata::initializeDataFiles(ManifestListPtr manifest_list_ptr) const
{
    for (const auto & manifest_file_content : *manifest_list_ptr)
    {
        for (const auto & data_file_path : manifest_file_content->getFiles())
        {
            if (std::holds_alternative<DataFileEntry>(data_file_path.file))
                manifest_file_by_data_file.emplace(std::get<DataFileEntry>(data_file_path.file).file_name, manifest_file_content);
        }
    }
}

ManifestListPtr IcebergMetadata::getManifestList(const String & filename) const
{
    auto configuration_ptr = configuration.lock();
    if (configuration_ptr == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Configuration is expired");

    auto create_fn = [&]()
    {
        ManifestList manifest_list;
        StorageObjectStorage::ObjectInfo object_info(filename);
        auto manifest_list_buf = StorageObjectStorageSource::createReadBuffer(object_info, object_storage, getContext(), log);
        AvroForIcebergDeserializer manifest_list_deserializer(std::move(manifest_list_buf), filename, getFormatSettings(getContext()));

        ManifestFileCacheKeys manifest_file_cache_keys;

        for (size_t i = 0; i < manifest_list_deserializer.rows(); ++i)
        {
            const std::string file_path = manifest_list_deserializer.getValueFromRowByName(i, MANIFEST_FILE_PATH_COLUMN, TypeIndex::String).safeGet<std::string>();
            const auto manifest_file_name = getProperFilePathFromMetadataInfo(file_path, configuration_ptr->getPath(), table_location);
            Int64 added_sequence_number = 0;
            if (format_version > 1)
                added_sequence_number = manifest_list_deserializer.getValueFromRowByName(i, SEQUENCE_NUMBER_COLUMN, TypeIndex::Int64).safeGet<Int64>();
            manifest_file_cache_keys.emplace_back(manifest_file_name, added_sequence_number);
        }
        /// We only return the list of {file name, seq number} for cache.
        /// Because ManifestList holds a list of ManifestFilePtr which consume much memory space.
        /// ManifestFilePtr is shared pointers can be held for too much time, so we cache ManifestFile separately.
        return manifest_file_cache_keys;
    };

    ManifestFileCacheKeys manifest_file_cache_keys;
    ManifestList manifest_list;
    if (manifest_cache)
    {
        manifest_file_cache_keys = manifest_cache->getOrSetManifestFileCacheKeys(IcebergMetadataFilesCache::getKey(configuration_ptr, filename), create_fn);
    }
    else
    {
        manifest_file_cache_keys = create_fn();
    }
    for (const auto & entry : manifest_file_cache_keys)
    {
        auto manifest_file_ptr = getManifestFile(entry.manifest_file_path, entry.added_sequence_number);
        manifest_list.push_back(manifest_file_ptr);
    }
    ManifestListPtr manifest_list_ptr = std::make_shared<ManifestList>(std::move(manifest_list));
    initializeDataFiles(manifest_list_ptr);
    return manifest_list_ptr;
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

Strings IcebergMetadata::getDataFiles(const ActionsDAG * filter_dag) const
{
    if (!relevant_snapshot)
        return {};

    bool use_partition_pruning = filter_dag && getContext()->getSettingsRef()[Setting::use_iceberg_partition_pruning];

    if (!use_partition_pruning && cached_unprunned_files_for_last_processed_snapshot.has_value())
        return cached_unprunned_files_for_last_processed_snapshot.value();

    Strings data_files;
    for (const auto & manifest_file_ptr : *(relevant_snapshot->manifest_list))
    {
        ManifestFilesPruner pruner(
            schema_processor, relevant_snapshot_schema_id,
            use_partition_pruning ? filter_dag : nullptr,
            *manifest_file_ptr, getContext());
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
    for (const auto & manifest_list_entry : *(relevant_snapshot->manifest_list))
    {
        auto count = manifest_list_entry->getRowsCountInAllDataFilesExcludingDeleted();
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
    for (const auto & manifest_list_entry : *(relevant_snapshot->manifest_list))
    {
        auto count = manifest_list_entry->getBytesCountInAllDataFiles();
        if (!count.has_value())
            return {};

        result += count.value();
    }

    return result;
}

ObjectIterator IcebergMetadata::iterate(
    const ActionsDAG * filter_dag,
    FileProgressCallback callback,
    size_t /* list_batch_size */) const
{
    return createKeysIterator(getDataFiles(filter_dag), object_storage, callback);
}

}

#endif
