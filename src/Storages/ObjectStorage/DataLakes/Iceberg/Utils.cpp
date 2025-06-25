
#include <typeinfo>
#include <Poco/UUIDGenerator.h>
#include "Common/DateLUT.h"
#include "Core/ColumnsWithTypeAndName.h"
#include "Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h"
#include "config.h"

#if USE_AVRO

#include <Processors/Formats/Impl/AvroRowInputFormat.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <IO/ReadHelpers.h>
#include <filesystem>

#include <Storages/ObjectStorage/DataLakes/Common.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include "Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h"
#include <Interpreters/Context.h>

using namespace DB;


#include <Columns/IColumn.h>

namespace DB::ErrorCodes
{

extern const int FILE_DOESNT_EXIST;
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
extern const int ICEBERG_SPECIFICATION_VIOLATION;

}

namespace DB::DataLakeStorageSetting
{
    extern const DataLakeStorageSettingsString iceberg_metadata_file_path;
    extern const DataLakeStorageSettingsString iceberg_metadata_table_uuid;
    extern const DataLakeStorageSettingsBool iceberg_recent_metadata_file_by_last_updated_ms_field;
    extern const DataLakeStorageSettingsBool iceberg_use_version_hint;
}

namespace ProfileEvents
{
    extern const Event IcebergVersionHintUsed;
}

namespace Iceberg
{

using namespace DB;

// This function is used to get the file path inside the directory which corresponds to iceberg table from the full blob path which is written in manifest and metadata files.
// For example, if the full blob path is s3://bucket/table_name/data/00000-1-1234567890.avro, the function will return table_name/data/00000-1-1234567890.avro
// Common path should end with "<table_name>" or "<table_name>/".
std::string getProperFilePathFromMetadataInfo(std::string_view data_path, std::string_view common_path, std::string_view table_location)
{
    auto trim_backward_slash = [](std::string_view str) -> std::string_view
    {
        if (str.ends_with('/'))
        {
            return str.substr(0, str.size() - 1);
        }
        return str;
    };
    auto trim_forward_slash = [](std::string_view str) -> std::string_view
    {
        if (str.starts_with('/'))
        {
            return str.substr(1);
        }
        return str;
    };
    common_path = trim_backward_slash(common_path);
    table_location = trim_backward_slash(table_location);
    if (data_path.starts_with(table_location) && table_location.ends_with(common_path))
    {
        return std::filesystem::path{common_path} / trim_forward_slash(data_path.substr(table_location.size()));
    }


    auto pos = data_path.find(common_path);
    size_t good_pos = std::string::npos;
    while (pos != std::string::npos)
    {
        auto potential_position = pos + common_path.size();
        if ((std::string_view(data_path.data() + potential_position, 6) == "/data/")
            || (std::string_view(data_path.data() + potential_position, 10) == "/metadata/"))
        {
            good_pos = pos;
            break;
        }
        size_t new_pos = data_path.find(common_path, pos + 1);
        if (new_pos == std::string::npos)
        {
            break;
        }
        pos = new_pos;
    }


    if (good_pos != std::string::npos)
    {
        return std::string{data_path.substr(good_pos)};
    }
    else if (pos != std::string::npos)
    {
        return std::string{data_path.substr(pos)};
    }
    else
    {
        throw ::DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Expected to find '{}' in data path: '{}'", common_path, data_path);
    }
}

}

namespace DB
{

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

Poco::JSON::Object::Ptr getMetadataJSONObject(
    const String & metadata_file_path,
    ObjectStoragePtr object_storage,
    StorageObjectStorage::ConfigurationPtr configuration_ptr,
    IcebergMetadataFilesCachePtr cache_ptr,
    const ContextPtr & local_context,
    LoggerPtr log)
{
    std::cerr << "metadata_file_path " << metadata_file_path << '\n';

    auto create_fn = [&]()
    {
        ObjectInfo object_info(metadata_file_path);
        {
            auto buf = StorageObjectStorageSource::createReadBuffer(object_info, object_storage, local_context, log);
            for (int i = 0; i < 10; ++i)
            {
                char c;
                if (!buf->read(c))
                {
                    std::cerr << "EOF!\n";
                    break;
                }
                std::cerr << c;
            }
            std::cerr << '\n';
        }
        auto buf = StorageObjectStorageSource::createReadBuffer(object_info, object_storage, local_context, log);

        String json_str;
        std::cerr << "available before reading " << buf->buffer().size() << '\n';
        readJSONObjectPossiblyInvalid(json_str, *buf); // Почему-то мой json не читается
        
        return json_str;
    };

    String metadata_json_str;
    if (cache_ptr)
        metadata_json_str = cache_ptr->getOrSetTableMetadata(IcebergMetadataFilesCache::getKey(configuration_ptr, metadata_file_path), create_fn);
    else
        metadata_json_str = create_fn();

    std::cerr << "metadata_json_str " << metadata_json_str << '\n';
    Poco::JSON::Parser parser; /// For some reason base/base/JSON.h can not parse this json file
    Poco::Dynamic::Var json = parser.parse(metadata_json_str);
    return json.extract<Poco::JSON::Object::Ptr>();
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

String createDefaultMetadataPath(
    const ObjectStoragePtr & object_storage,
    const NamesAndTypesList & columns,
    StorageObjectStorage::ConfigurationPtr configuration_ptr,
    const String & table_uuid,
    ContextPtr context
)
{
    std::cerr << "columns size " << columns.size() << '\n';
    FileNamesGenerator filename_generator(configuration_ptr->getPath(), configuration_ptr->getPath());
    auto filename = filename_generator.generateMetadataName();

    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();

    Poco::JSON::Object::Ptr root = new Poco::JSON::Object;

    root->set("format-version", 2);
    root->set("table-uuid", table_uuid);
    root->set("location", filename);
    root->set("last-updated-ms", duration_cast<std::chrono::milliseconds>(duration).count());
    root->set("last-column-id", 1);
    root->set("current-schema-id", 0);

    Poco::JSON::Object::Ptr schema = new Poco::JSON::Object;
    schema->set("type", "struct");
    schema->set("schema-id", 0);

    Poco::JSON::Array::Ptr fields = new Poco::JSON::Array;
    for (const auto & column : columns)
    {
        Poco::JSON::Object::Ptr field = new Poco::JSON::Object;
        field->set("id", column.name);
        field->set("name", "id");
        field->set("required", true);
        // TODO: convert ch type into spark
        field->set("type", column.type->getName());

        fields->add(field);
    }
    schema->set("fields", fields);

    root->set("schema", schema);

    Poco::JSON::Array::Ptr partition_spec = new Poco::JSON::Array;
    Poco::JSON::Object::Ptr spec = new Poco::JSON::Object;
    spec->set("spec-id", 0);
    //spec->set("fields", "");
    partition_spec->add(spec);
    root->set("partition-spec", partition_spec);
    root->set("default-spec-id", 0);

    Poco::JSON::Array::Ptr schemas = new Poco::JSON::Array;
    schemas->add(schema);
    root->set("schemas", schemas);

    Poco::JSON::Array::Ptr partition_specs = new Poco::JSON::Array;
    partition_specs->add(spec);
    root->set("partition-specs", partition_specs);
    
    std::ostringstream oss;
    Poco::JSON::Stringifier::stringify(root, oss, 4);

    std::string json_representation = removeEscapedSlashes(oss.str());
    std::cerr << "init meta json_representation " << json_representation << '\n';

    auto buffer_metadata = object_storage->writeObject(
        StoredObject(filename), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());                    
    buffer_metadata->write(json_representation.data(), json_representation.size());

    buffer_metadata->finalize();

    return filename;
}

/**
 * Each version of table metadata is stored in a `metadata` directory and
 * has one of 2 formats:
 *   1) v<V>.metadata.json, where V - metadata version.
 *   2) <V>-<random-uuid>.metadata.json, where V - metadata version
 */
static std::pair<Int32, String> getLatestMetadataFileAndVersion(
    const ObjectStoragePtr & object_storage,
    StorageObjectStorage::ConfigurationPtr configuration_ptr,
    IcebergMetadataFilesCachePtr cache_ptr,
    const ContextPtr & local_context,
    const std::optional<String> & table_uuid)
{
    auto log = getLogger("IcebergMetadataFileResolver");
    MostRecentMetadataFileSelectionWay selection_way
        = configuration_ptr->getDataLakeSettings()[DataLakeStorageSetting::iceberg_recent_metadata_file_by_last_updated_ms_field].value
        ? MostRecentMetadataFileSelectionWay::BY_LAST_UPDATED_MS_FIELD
        : MostRecentMetadataFileSelectionWay::BY_METADATA_FILE_VERSION;
    bool need_all_metadata_files_parsing
        = (selection_way == MostRecentMetadataFileSelectionWay::BY_LAST_UPDATED_MS_FIELD) || table_uuid.has_value();
    auto metadata_files = listFiles(*object_storage, *configuration_ptr, "metadata", ".metadata.json");
    if (metadata_files.empty())
    {
        auto columns = configuration_ptr->getInitialColumns();
        Poco::UUIDGenerator gen;
        auto filename = createDefaultMetadataPath(object_storage, columns, configuration_ptr, table_uuid.has_value() ? *table_uuid : gen.createRandom().toString(), local_context);
        metadata_files.push_back(filename);
    }
    std::vector<ShortMetadataFileInfo> metadata_files_with_versions;
    metadata_files_with_versions.reserve(metadata_files.size());
    for (const auto & path : metadata_files)
    {
        std::cerr << "strange way " << path << '\n';
        auto [version, metadata_file_path] = getMetadataFileAndVersion(path);
        if (need_all_metadata_files_parsing)
        {
            auto metadata_file_object = getMetadataJSONObject(metadata_file_path, object_storage, configuration_ptr, cache_ptr, local_context, log);
            if (table_uuid.has_value())
            {
                if (metadata_file_object->has(Iceberg::f_table_uuid))
                {
                    auto current_table_uuid = metadata_file_object->getValue<String>(Iceberg::f_table_uuid);
                    if (normalizeUuid(table_uuid.value()) == normalizeUuid(current_table_uuid))
                    {
                        metadata_files_with_versions.emplace_back(
                            version, metadata_file_object->getValue<UInt64>(Iceberg::f_last_updated_ms), metadata_file_path);
                    }
                }
                else
                {
                    Int64 format_version = metadata_file_object->getValue<Int64>(Iceberg::f_format_version);
                    throw Exception(
                        format_version == 1 ? ErrorCodes::BAD_ARGUMENTS : ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                        "Table UUID is not specified in some metadata files for table by path {}",
                        metadata_file_path);
                }
            }
            else
            {
                metadata_files_with_versions.emplace_back(version, metadata_file_object->getValue<UInt64>(Iceberg::f_last_updated_ms), metadata_file_path);
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

std::pair<Int32, String> getLatestOrExplicitMetadataFileAndVersion(
    const ObjectStoragePtr & object_storage,
    StorageObjectStorage::ConfigurationPtr configuration_ptr,
    IcebergMetadataFilesCachePtr cache_ptr,
    const ContextPtr & local_context,
    Poco::Logger * log)
{
    const auto & data_lake_settings = configuration_ptr->getDataLakeSettings();
    if (data_lake_settings[DataLakeStorageSetting::iceberg_metadata_file_path].changed)
    {
        auto explicit_metadata_path = data_lake_settings[DataLakeStorageSetting::iceberg_metadata_file_path].value;
        std::cerr << "explicit_metadata_path " << explicit_metadata_path << '\n';
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
            auto prefix_storage_path = configuration_ptr->getPath();
            if (!explicit_metadata_path.starts_with(prefix_storage_path))
                explicit_metadata_path = std::filesystem::path(prefix_storage_path) / explicit_metadata_path;
            return getMetadataFileAndVersion(explicit_metadata_path);
        }
        catch (const std::exception & ex)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid path {} specified for iceberg_metadata_file_path: '{}'", explicit_metadata_path, ex.what());
        }
    }
    else if (data_lake_settings[DataLakeStorageSetting::iceberg_metadata_table_uuid].changed)
    {
        std::optional<String> table_uuid = data_lake_settings[DataLakeStorageSetting::iceberg_metadata_table_uuid].value;
        return getLatestMetadataFileAndVersion(object_storage, configuration_ptr, cache_ptr, local_context, table_uuid);
    }
    else if (data_lake_settings[DataLakeStorageSetting::iceberg_use_version_hint].value)
    {
        auto prefix_storage_path = configuration_ptr->getPath();
        auto version_hint_path = std::filesystem::path(prefix_storage_path) / "metadata" / "version-hint.text";
        std::string metadata_file;
        StoredObject version_hint(version_hint_path);
        auto buf = object_storage->readObject(version_hint, ReadSettings{});
        readString(metadata_file, *buf);
        if (!metadata_file.ends_with(".metadata.json"))
        {
            if (std::all_of(metadata_file.begin(), metadata_file.end(), isdigit))
                metadata_file = "v" + metadata_file + ".metadata.json";
            else
                metadata_file = metadata_file + ".metadata.json";
        }
        LOG_TEST(log, "Version hint file points to {}, will read from this metadata file", metadata_file);
        ProfileEvents::increment(ProfileEvents::IcebergVersionHintUsed);
        return getMetadataFileAndVersion(std::filesystem::path(prefix_storage_path) / "metadata" / metadata_file);
    }
    else
    {
        return getLatestMetadataFileAndVersion(object_storage, configuration_ptr, cache_ptr, local_context, std::nullopt);
    }
}

}

#endif
