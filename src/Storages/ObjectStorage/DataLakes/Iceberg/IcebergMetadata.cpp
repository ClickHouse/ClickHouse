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
#include <Storages/ObjectStorage/DataLakes/Iceberg/PartitionPruning.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>

#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>


namespace ProfileEvents
{
    extern const Event IcebergPartitionPrunnedFiles;
}

namespace DB
{

namespace StorageObjectStorageSetting
{
    extern const StorageObjectStorageSettingsString iceberg_metadata_file_path;
}

namespace ErrorCodes
{
extern const int FILE_DOESNT_EXIST;
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}

using namespace Iceberg;


constexpr const char * COLUMN_SEQ_NUMBER_NAME = "sequence_number";
constexpr const char * COLUMN_MANIFEST_FILE_PATH_NAME = "manifest_path";
constexpr const char * FIELD_FORMAT_VERSION_NAME = "format-version";

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


IcebergMetadata::IcebergMetadata(
    ObjectStoragePtr object_storage_,
    ConfigurationObserverPtr configuration_,
    const DB::ContextPtr & context_,
    Int32 metadata_version_,
    Int32 format_version_,
    const Poco::JSON::Object::Ptr & object)
    : WithContext(context_)
    , object_storage(std::move(object_storage_))
    , configuration(std::move(configuration_))
    , schema_processor(IcebergSchemaProcessor())
    , log(getLogger("IcebergMetadata"))
    , current_metadata_version(metadata_version_)
    , format_version(format_version_)
    , table_location(object->getValue<String>("location"))
{
    auto manifest_list_file = getRelevantManifestList(object);
    if (manifest_list_file)
    {
        current_snapshot = getSnapshot(manifest_list_file.value());
    }
    current_schema_id = parseTableSchema(object, schema_processor, log);
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

Int32 IcebergMetadata::parseTableSchema(
    const Poco::JSON::Object::Ptr & metadata_object, IcebergSchemaProcessor & schema_processor, LoggerPtr metadata_logger)
{
    Int32 format_version = metadata_object->getValue<Int32>(FIELD_FORMAT_VERSION_NAME);
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

/**
 * Each version of table metadata is stored in a `metadata` directory and
 * has one of 2 formats:
 *   1) v<V>.metadata.json, where V - metadata version.
 *   2) <V>-<random-uuid>.metadata.json, where V - metadata version
 */
static std::pair<Int32, String>
getLatestMetadataFileAndVersion(const ObjectStoragePtr & object_storage, const StorageObjectStorage::Configuration & configuration)
{
    const auto metadata_files = listFiles(*object_storage, configuration, "metadata", ".metadata.json");
    if (metadata_files.empty())
    {
        throw Exception(
            ErrorCodes::FILE_DOESNT_EXIST, "The metadata file for Iceberg table with path {} doesn't exist", configuration.getPath());
    }
    std::vector<std::pair<UInt32, String>> metadata_files_with_versions;
    metadata_files_with_versions.reserve(metadata_files.size());
    for (const auto & path : metadata_files)
    {
        metadata_files_with_versions.emplace_back(getMetadataFileAndVersion(path));
    }

    /// Get the latest version of metadata file: v<V>.metadata.json
    return *std::max_element(metadata_files_with_versions.begin(), metadata_files_with_versions.end());
}

static std::pair<Int32, String> getLatestOrExplicitMetadataFileAndVersion(const ObjectStoragePtr & object_storage, const StorageObjectStorage::Configuration & configuration, Poco::Logger * log)
{
    auto explicit_metadata_path = configuration.getSettingsRef()[StorageObjectStorageSetting::iceberg_metadata_file_path].value;
    std::pair<Int32, String> result;
    if (!explicit_metadata_path.empty())
    {
        LOG_TEST(log, "Explicit metadata file path is specified {}, will read from this metadata file", explicit_metadata_path);
        auto prefix_storage_path = configuration.getPath();
        if (!explicit_metadata_path.starts_with(prefix_storage_path))
            explicit_metadata_path = std::filesystem::path(prefix_storage_path) / explicit_metadata_path;
        result = getMetadataFileAndVersion(explicit_metadata_path);
    }
    else
    {
        result = getLatestMetadataFileAndVersion(object_storage, configuration);
    }

    return result;
}


Poco::JSON::Object::Ptr IcebergMetadata::readJSON(const String & metadata_file_path, const ContextPtr & local_context) const
{
    ObjectInfo object_info(metadata_file_path);
    auto buf = StorageObjectStorageSource::createReadBuffer(object_info, object_storage, local_context, log);

    String json_str;
    readJSONObjectPossiblyInvalid(json_str, *buf);

    Poco::JSON::Parser parser; /// For some reason base/base/JSON.h can not parse this json file
    Poco::Dynamic::Var json = parser.parse(json_str);
    return json.extract<Poco::JSON::Object::Ptr>();
}

bool IcebergMetadata::update(const ContextPtr & local_context)
{
    auto configuration_ptr = configuration.lock();

    const auto [metadata_version, metadata_file_path] = getLatestOrExplicitMetadataFileAndVersion(object_storage, *configuration_ptr, log.get());

    if (metadata_version == current_metadata_version)
        return false;

    current_metadata_version = metadata_version;

    auto metadata_object = readJSON(metadata_file_path, local_context);

    chassert(format_version == metadata_object->getValue<int>(FIELD_FORMAT_VERSION_NAME));


    auto manifest_list_file = getRelevantManifestList(metadata_object);
    if (manifest_list_file
        && (!current_snapshot.has_value() || (manifest_list_file.value() != current_snapshot->manifest_list_iterator.getName())))
    {
        current_snapshot = getSnapshot(manifest_list_file.value());
        cached_unprunned_files_for_current_snapshot = std::nullopt;
    }
    current_schema_id = parseTableSchema(metadata_object, schema_processor, log);
    return true;
}

std::optional<String> IcebergMetadata::getRelevantManifestList(const Poco::JSON::Object::Ptr & metadata)
{
    auto configuration_ptr = configuration.lock();

    auto snapshots = metadata->get("snapshots").extract<Poco::JSON::Array::Ptr>();

    auto current_snapshot_id = metadata->getValue<Int64>("current-snapshot-id");

    for (size_t i = 0; i < snapshots->size(); ++i)
    {
        const auto snapshot = snapshots->getObject(static_cast<UInt32>(i));

        if (snapshot->getValue<Int64>("snapshot-id") == current_snapshot_id)
        {
            const auto path = snapshot->getValue<String>("manifest-list");
            return getProperFilePathFromMetadataInfo(std::string_view(path), configuration_ptr->getPath(), table_location);
        }
    }
    return std::nullopt;
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
    if (schema_id == current_schema_id)
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

    const auto [metadata_version, metadata_file_path] = getLatestOrExplicitMetadataFileAndVersion(object_storage, *configuration_ptr, log.get());

    ObjectInfo object_info(metadata_file_path);
    auto buf = StorageObjectStorageSource::createReadBuffer(object_info, object_storage, local_context, log);

    String json_str;
    readJSONObjectPossiblyInvalid(json_str, *buf);

    Poco::JSON::Parser parser; /// For some reason base/base/JSON.h can not parse this json file
    Poco::Dynamic::Var json = parser.parse(json_str);
    const Poco::JSON::Object::Ptr & object = json.extract<Poco::JSON::Object::Ptr>();

    IcebergSchemaProcessor schema_processor;

    auto format_version = object->getValue<int>(FIELD_FORMAT_VERSION_NAME);

    auto ptr
        = std::make_unique<IcebergMetadata>(object_storage, configuration_ptr, local_context, metadata_version, format_version, object);

    return ptr;
}

ManifestList IcebergMetadata::initializeManifestList(const String & filename) const
{
    auto configuration_ptr = configuration.lock();
    if (configuration_ptr == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Configuration is expired");

    StorageObjectStorage::ObjectInfo object_info(filename);
    auto manifest_list_buf = StorageObjectStorageSource::createReadBuffer(object_info, object_storage, getContext(), log);
    AvroForIcebergDeserializer manifest_list_deserializer(std::move(manifest_list_buf), filename, getFormatSettings(getContext()));

    ManifestList manifest_list;

    for (size_t i = 0; i < manifest_list_deserializer.rows(); ++i)
    {
        const std::string file_path = manifest_list_deserializer.getValueFromRowByName(i, COLUMN_MANIFEST_FILE_PATH_NAME, TypeIndex::String).safeGet<std::string>();
        const auto manifest_file_name = getProperFilePathFromMetadataInfo(file_path, configuration_ptr->getPath(), table_location);
        Int64 added_sequence_number = 0;
        if (format_version > 1)
            added_sequence_number = manifest_list_deserializer.getValueFromRowByName(i, COLUMN_SEQ_NUMBER_NAME, TypeIndex::Int64).safeGet<Int64>();

        /// We can't encapsulate this logic in getManifestFile because we need not only the name of the file, but also an inherited sequence number which is known only during the parsing of ManifestList
        auto manifest_file_content = initializeManifestFile(manifest_file_name, added_sequence_number);
        auto [iterator, _inserted] = manifest_files_by_name.emplace(manifest_file_name, std::move(manifest_file_content));
        auto manifest_file_iterator = ManifestFileIterator{iterator};
        for (const auto & data_file_path : manifest_file_iterator->getFiles())
        {
            if (std::holds_alternative<DataFileEntry>(data_file_path.file))
                manifest_file_by_data_file.emplace(std::get<DataFileEntry>(data_file_path.file).file_name, manifest_file_iterator);
        }
        manifest_list.push_back(ManifestListFileEntry{manifest_file_iterator, added_sequence_number});
    }

    return manifest_list;
}

ManifestFileContent IcebergMetadata::initializeManifestFile(const String & filename, Int64 inherited_sequence_number) const
{
    auto configuration_ptr = configuration.lock();

    ObjectInfo manifest_object_info(filename);
    auto buffer = StorageObjectStorageSource::createReadBuffer(manifest_object_info, object_storage, getContext(), log);
    AvroForIcebergDeserializer manifest_file_deserializer(std::move(buffer), filename, getFormatSettings(getContext()));
    auto [schema_id, schema_object] = parseTableSchemaFromManifestFile(manifest_file_deserializer, filename);
    schema_processor.addIcebergTableSchema(schema_object);
    return ManifestFileContent(
        manifest_file_deserializer,
        format_version,
        configuration_ptr->getPath(),
        schema_id,
        schema_processor,
        inherited_sequence_number,
        table_location,
        getContext());
}

ManifestFileIterator IcebergMetadata::getManifestFile(const String & filename) const
{
    auto manifest_file_it = manifest_files_by_name.find(filename);
    if (manifest_file_it != manifest_files_by_name.end())
        return ManifestFileIterator{manifest_file_it};
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot find manifest file: {}", filename);
}

std::optional<ManifestFileIterator> IcebergMetadata::tryGetManifestFile(const String & filename) const
{
    auto manifest_file_it = manifest_files_by_name.find(filename);
    if (manifest_file_it != manifest_files_by_name.end())
        return ManifestFileIterator{manifest_file_it};
    return std::nullopt;
}

ManifestListIterator IcebergMetadata::getManifestList(const String & filename) const
{
    auto manifest_file_it = manifest_lists_by_name.find(filename);
    if (manifest_file_it != manifest_lists_by_name.end())
        return ManifestListIterator{manifest_file_it};
    auto configuration_ptr = configuration.lock();
    auto [manifest_file_iterator, _inserted] = manifest_lists_by_name.emplace(filename, initializeManifestList(filename));
    return ManifestListIterator{manifest_file_iterator};
}

IcebergSnapshot IcebergMetadata::getSnapshot(const String & filename) const
{
    return IcebergSnapshot{getManifestList(filename)};
}

Strings IcebergMetadata::getDataFilesImpl(const ActionsDAG * filter_dag) const
{
    if (!current_snapshot)
        return {};

    if (!filter_dag && cached_unprunned_files_for_current_snapshot.has_value())
        return cached_unprunned_files_for_current_snapshot.value();

    Strings data_files;
    for (const auto & manifest_list_entry : *(current_snapshot->manifest_list_iterator))
    {
        PartitionPruner pruner(schema_processor, current_schema_id, filter_dag, *manifest_list_entry.manifest_file, getContext());
        const auto & data_files_in_manifest = manifest_list_entry.manifest_file->getFiles();
        for (const auto & manifest_file_entry : data_files_in_manifest)
        {
            if (manifest_file_entry.status != ManifestEntryStatus::DELETED)
            {
                if (pruner.canBePruned(manifest_file_entry))
                {
                    ProfileEvents::increment(ProfileEvents::IcebergPartitionPrunnedFiles);
                }
                else
                {
                    if (std::holds_alternative<DataFileEntry>(manifest_file_entry.file))
                        data_files.push_back(std::get<DataFileEntry>(manifest_file_entry.file).file_name);
                }
            }
        }
    }

    if (!filter_dag)
    {
        cached_unprunned_files_for_current_snapshot = data_files;
        return cached_unprunned_files_for_current_snapshot.value();
    }

    return data_files;
}

Strings IcebergMetadata::makePartitionPruning(const ActionsDAG & filter_dag)
{
    auto configuration_ptr = configuration.lock();
    if (!configuration_ptr)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Configuration is expired");
    }
    return getDataFilesImpl(&filter_dag);
}
}

#endif
