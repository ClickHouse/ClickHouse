#include <config.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>

#if USE_AVRO

#include <algorithm>
#include <charconv>
#include <cstddef>
#include <cstring>
#include <filesystem>
#include <string>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/IStoragePolicy.h>
#include <Disks/ObjectStorages/StoredObject.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/DataLakes/Common/Common.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonClient.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonTableSchema.h>
#include <Storages/ObjectStorage/StorageObjectStorageSettings.h>
#include <base/types.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeTuple.h>
#include <Formats/FormatFactory.h>
#include <Storages/ObjectStorage/DataLakes/Common/AvroForIcebergDeserializer.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/Utils.h>
#include <boost/graph/properties.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>


namespace DB
{
namespace ErrorCodes
{
extern const int FILE_DOESNT_EXIST;
extern const int LOGICAL_ERROR;
extern const int CANNOT_PARSE_NUMBER;
}

PaimonSnapshot::PaimonSnapshot(const Poco::JSON::Object::Ptr & json_object)
{
    Paimon::getValueFromJSON(id, json_object, "id");
    Paimon::getValueFromJSON(schema_id, json_object, "schemaId");
    Paimon::getValueFromJSON(base_manifest_list, json_object, "baseManifestList");
    Paimon::getValueFromJSON(delta_manifest_list, json_object, "deltaManifestList");
    Paimon::getValueFromJSON(commit_user, json_object, "commitUser");
    Paimon::getValueFromJSON(commit_identifier, json_object, "commitIdentifier");
    Paimon::getValueFromJSON(commit_kind, json_object, "commitKind");
    Paimon::getValueFromJSON(time_millis, json_object, "timeMillis");
    Paimon::getOptionalValueFromJSON(version, json_object, "version");
    Paimon::getOptionalValueFromJSON(index_manifest, json_object, "indexManifest");
    Paimon::getOptionalValueFromJSON(base_manifest_list_size, json_object, "baseManifestListSize");
    Paimon::getOptionalValueFromJSON(delta_manifest_list_size, json_object, "deltaManifestListSize");
    Paimon::getOptionalValueFromJSON(changelog_manifest_list, json_object, "changelogManifestList");
    Paimon::getOptionalValueFromJSON(changelog_manifest_list_size, json_object, "changelogManifestListSize");
    Paimon::getOptionalValueFromJSON(total_record_count, json_object, "totalRecordCount");
    Paimon::getOptionalValueFromJSON(delta_record_count, json_object, "deltaRecordCount");
    Paimon::getOptionalValueFromJSON(changelog_record_count, json_object, "changelogRecordCount");
    Paimon::getOptionalValueFromJSON(watermark, json_object, "watermark");
    Paimon::getOptionalValueFromJSON(statistics, json_object, "statistics");

    if (json_object->has("logOffsets"))
    {
        log_offsets = std::unordered_map<Int32, Int64>();
        auto inner_map_json = json_object->getObject("logOffsets");
        log_offsets->reserve(inner_map_json->size());
        for (const auto & inner_key : inner_map_json->getNames())
        {
            Int32 key;
            auto [_, ec] = std::from_chars(inner_key.data(), inner_key.data() + inner_key.size(), key);
            if (ec != std::errc())
            {
                throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "The Paimon snapshot logOffsets key: {} is invalid.", inner_key);
            }
            log_offsets->emplace(key, inner_map_json->getValue<Int64>(inner_key));
        }
    }
}

PaimonTableClient::PaimonTableClient(
    ObjectStoragePtr object_storage_, StorageObjectStorageConfigurationWeakPtr configuration_, const DB::ContextPtr & context_)
    : WithContext(context_)
    , object_storage(object_storage_)
    , configuration(configuration_)
    , table_location(configuration.lock()->getPathForRead().path)
    , log(getLogger("PaimonTableClient"))
{}

std::pair<Int32, String> PaimonTableClient::getLastestTableSchemaInfo()
{
    auto configuration_ptr = configuration.lock();
    if (configuration_ptr == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Configuration is expired.");

    /// list all schema files
    const auto schema_files = listFiles(
        *object_storage,
        *configuration_ptr,
        PAIMON_SCHEMA_DIR,
        [](const RelativePathWithMetadata & path_with_metadata)
        {
            String relative_path = path_with_metadata.relative_path;
            String file_name(relative_path.begin() + relative_path.find_last_of('/') + 1, relative_path.end());
            return file_name.starts_with(PAIMON_SCHEMA_PREFIX);
        });
    if (schema_files.empty())
    {
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "The metadata file for Paimon table with path {} doesn't exist", table_location);
    }
    /// find max schema version
    std::vector<std::pair<UInt32, String>> schema_files_with_versions;
    schema_files_with_versions.reserve(schema_files.size());
    auto parse_version = [](const String & relative_file_path)
    {
        String file_name(relative_file_path.begin() + relative_file_path.find_last_of('/') + 1, relative_file_path.end());
        String version_string = file_name.substr(file_name.find(PAIMON_SCHEMA_PREFIX) + strlen(PAIMON_SCHEMA_PREFIX));
        size_t current_version;
        auto [_, ec] = std::from_chars(version_string.data(), version_string.data() + version_string.size(), current_version);
        if (ec != std::errc())
        {
            throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "The Paimon schema file: {} version: {} is invalid.", file_name, version_string);
        }
        return current_version;
    };
    for (const auto & path : schema_files)
    {
        schema_files_with_versions.emplace_back(std::make_pair(parse_version(path), path));
    }
    return *std::max_element(schema_files_with_versions.begin(), schema_files_with_versions.end());
}

/// schema
Poco::JSON::Object::Ptr PaimonTableClient::getTableSchemaJSON(const std::pair<Int32, String> & schema_meta_info)
{
    const auto [max_schema_version, max_schema_path] = schema_meta_info;
    /// parse schema json
    RelativePathWithMetadata object_info(max_schema_path);
    auto buf = createReadBuffer(object_info, object_storage, getContext(), log);
    String json_str;
    readJSONObjectPossiblyInvalid(json_str, *buf);
    Poco::JSON::Parser parser;
    Poco::Dynamic::Var json = parser.parse(json_str);
    const auto & shcema_json = json.extract<Poco::JSON::Object::Ptr>();

    return shcema_json;
}

std::pair<Int64, String> PaimonTableClient::getLastestTableSnapshotInfo()
{
    auto configuration_ptr = configuration.lock();
    if (configuration_ptr == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Configuration is expired.");
    /// read latest hint
    Int64 snapshot_version;
    RelativePathWithMetadata relative_path_with_metadata(
        std::filesystem::path(table_location) / PAIMON_SNAPSHOT_DIR / PAIMON_SNAPSHOT_LATEST_HINT);
    auto buf = createReadBuffer(relative_path_with_metadata, object_storage, getContext(), log);
    String hint_version_string;
    readStringUntilEOF(hint_version_string, *buf);
    {
        auto [_, ec]
            = std::from_chars(hint_version_string.data(), hint_version_string.data() + hint_version_string.size(), snapshot_version);
        if (ec != std::errc())
        {
            throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "The Paimon snapshot hint file content: {} is invalid.", hint_version_string);
        }
    }
    String latest_snapshot_path
        = std::filesystem::path(table_location) / (PAIMON_SNAPSHOT_DIR) / (PAIMON_SNAPSHOT_PRIFIX + std::to_string(snapshot_version));

    /// check latest hint is real latest snapshot, if not, find latest snapshot
    Int64 next_snapshot_version = snapshot_version + 1;
    StoredObject store_object(
        std::filesystem::path(table_location) / (PAIMON_SNAPSHOT_DIR) / (PAIMON_SNAPSHOT_PRIFIX + std::to_string(next_snapshot_version)));
    if (object_storage->exists(store_object))
    {
        auto snapshot_files = listFiles(
            *object_storage,
            *configuration_ptr,
            PAIMON_SNAPSHOT_DIR,
            [](const RelativePathWithMetadata & path_with_metadata)
            {
                String relative_path = path_with_metadata.relative_path;
                String file_name(relative_path.begin() + relative_path.find_last_of('/') + 1, relative_path.end());
                return file_name.starts_with(PAIMON_SNAPSHOT_PRIFIX);
            });

        std::vector<std::pair<Int64, String>> snapshot_files_with_versions;
        snapshot_files_with_versions.reserve(snapshot_files.size());

        auto parse_version = [](const String & relative_file_path)
        {
            String file_name(relative_file_path.begin() + relative_file_path.find_last_of('/') + 1, relative_file_path.end());
            String version_string = file_name.substr(file_name.find(PAIMON_SNAPSHOT_PRIFIX) + strlen(PAIMON_SNAPSHOT_PRIFIX));
            Int64 current_version;
            auto [_, ec] = std::from_chars(version_string.data(), version_string.data() + version_string.size(), current_version);
            if (ec != std::errc())
            {
                throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "The Paimon snapshot file: {} version: {} is invalid.", file_name, version_string);
            }
            return current_version;
        };

        for (const auto & path : snapshot_files)
        {
            snapshot_files_with_versions.emplace_back(std::make_pair(parse_version(path), path));
        }
        return *std::max_element(snapshot_files_with_versions.begin(), snapshot_files_with_versions.end());
    }
    else
    {
        return {snapshot_version, latest_snapshot_path};
    }
}

PaimonSnapshot PaimonTableClient::getSnapshot(const std::pair<Int64, String> & snapshot_meta_info)
{
    /// read latest hint
    const auto [latest_snapshot_version, latest_snapshot_path] = snapshot_meta_info;

    /// read snapshot and parse
    RelativePathWithMetadata snapshot_object(latest_snapshot_path);
    auto snapshot_buf = createReadBuffer(snapshot_object, object_storage, getContext(), log);
    String json_str;
    readJSONObjectPossiblyInvalid(json_str, *snapshot_buf);
    Poco::JSON::Parser parser;
    Poco::Dynamic::Var json = parser.parse(json_str);
    const auto & snapshot_json = json.extract<Poco::JSON::Object::Ptr>();
    return PaimonSnapshot(snapshot_json);
}

std::vector<PaimonManifestFileMeta> PaimonTableClient::getManifestMeta(String manifest_list_path)
{
    /// read manifest list file
    auto context = getContext();
    RelativePathWithMetadata relative_path(std::filesystem::path(table_location) / (PAIMON_MANIFEST_DIR) / manifest_list_path);
    auto manifest_list_buf = createReadBuffer(relative_path, object_storage, context, log);
    Iceberg::AvroForIcebergDeserializer manifest_list_deserializer(
        std::move(manifest_list_buf), manifest_list_path, getFormatSettings(getContext()));

    std::vector<PaimonManifestFileMeta> paimon_manifest_file_meta_vec;
    paimon_manifest_file_meta_vec.reserve(manifest_list_deserializer.rows());

    for (size_t i = 0; i < manifest_list_deserializer.rows(); ++i)
    {
        paimon_manifest_file_meta_vec.emplace_back(manifest_list_deserializer, "", i);
    }
    return paimon_manifest_file_meta_vec;
}

PaimonManifest
PaimonTableClient::getDataManifest(String manifest_path, const PaimonTableSchema & table_schema, const String & partition_default_name)
{
    String manifest_file_name(manifest_path.begin() + manifest_path.find_last_of('/') + 1, manifest_path.end());
    if (manifest_file_name.starts_with("index-manifest-"))
        return {};

    auto context = getContext();
    RelativePathWithMetadata object_info(std::filesystem::path(table_location) / (PAIMON_MANIFEST_DIR) / manifest_path);
    auto manifest_buf = createReadBuffer(object_info, object_storage, context, log);
    Iceberg::AvroForIcebergDeserializer manifest_deserializer(std::move(manifest_buf), manifest_path, getFormatSettings(getContext()));

    PaimonManifest paimon_manifest;
    paimon_manifest.entries.reserve(manifest_deserializer.rows());
    for (size_t i = 0; i < manifest_deserializer.rows(); ++i)
    {
        paimon_manifest.entries.emplace_back(manifest_deserializer, "", i, table_schema, partition_default_name);
    }
    return paimon_manifest;
}

}

#endif
