#include <memory>
#include <Poco/Logger.h>
#include "Common/Config/ConfigProcessor.h"
#include "Common/DateLUT.h"
#include "Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h"
#include "config.h"

#if USE_AVRO

#    include <Columns/ColumnString.h>
#    include <Columns/ColumnTuple.h>
#    include <Columns/IColumn.h>
#    include <Core/Settings.h>
#    include <DataTypes/DataTypeArray.h>
#    include <DataTypes/DataTypeDate.h>
#    include <DataTypes/DataTypeDateTime64.h>
#    include <DataTypes/DataTypeFactory.h>
#    include <DataTypes/DataTypeFixedString.h>
#    include <DataTypes/DataTypeMap.h>
#    include <DataTypes/DataTypeNullable.h>
#    include <DataTypes/DataTypeString.h>
#    include <DataTypes/DataTypeTuple.h>
#    include <DataTypes/DataTypeUUID.h>
#    include <DataTypes/DataTypesDecimal.h>
#    include <DataTypes/DataTypesNumber.h>
#    include <Formats/FormatFactory.h>
#    include <IO/ReadBufferFromFileBase.h>
#    include <IO/ReadBufferFromString.h>
#    include <IO/ReadHelpers.h>
#    include <Processors/Formats/Impl/AvroRowInputFormat.h>
#    include <Storages/ObjectStorage/DataLakes/Common.h>
#    include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#    include <Common/logger_useful.h>

#    include <filesystem>
#    include <sstream>

#    include "Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h"
#    include "Storages/ObjectStorage/DataLakes/Iceberg/Utils.h"

#    include "Storages/ObjectStorage/DataLakes/Iceberg/ManifestFileImpl.h"
#    include "Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h"

namespace DB
{
namespace Setting
{
extern const SettingsBool allow_data_lake_dynamic_schema;
}

namespace ErrorCodes
{
extern const int FILE_DOESNT_EXIST;
extern const int ILLEGAL_COLUMN;
extern const int BAD_ARGUMENTS;
extern const int UNSUPPORTED_METHOD;
extern const int LOGICAL_ERROR;
}

namespace Iceberg
{

Int32 parseTableSchema(
    const Poco::JSON::Object::Ptr & metadata_object, IcebergSchemaProcessor & schema_processor, const LoggerPtr & metadata_logger);

std::pair<Int32, Poco::JSON::Object::Ptr>
parseTableSchemaFromManifestFile(const avro::DataFileReaderBase & manifest_file_reader, const String & manifest_file_name)
{
    auto avro_metadata = manifest_file_reader.metadata();
    auto avro_schema_it = avro_metadata.find("schema");
    if (avro_schema_it == avro_metadata.end())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot read Iceberg table: manifest file {} doesn't have table schema in its metadata",
            manifest_file_name);
    std::vector<uint8_t> schema_json = avro_schema_it->second;
    String schema_json_string = String(reinterpret_cast<char *>(schema_json.data()), schema_json.size());
    Poco::JSON::Parser parser;
    Poco::Dynamic::Var json = parser.parse(schema_json_string);
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
    const String & manifest_list_file_,
    const Poco::JSON::Object::Ptr & object)
    : WithContext(context_)
    , object_storage(std::move(object_storage_))
    , configuration(std::move(configuration_))
    , schema_processor(IcebergSchemaProcessor())
    , log(getLogger("IcebergMetadata"))
    , metadata_version(metadata_version_)
    , format_version(format_version_)
    , current_snapshot(manifest_list_file_.empty() ? std::nullopt : std::optional{getSnapshot(manifest_list_file_)})
{
    auto schema_id = parseTableSchema(object, schema_processor, log);
    schema = *(schema_processor.getClickhouseTableSchemaById(schema_id));
    current_schema_id = schema_id;
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

Int32 parseTableSchema(
    const Poco::JSON::Object::Ptr & metadata_object, IcebergSchemaProcessor & schema_processor, const LoggerPtr & metadata_logger)
{
    Int32 format_version = metadata_object->getValue<Int32>("format-version");
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

/**
 * Each version of table metadata is stored in a `metadata` directory and
 * has one of 2 formats:
 *   1) v<V>.metadata.json, where V - metadata version.
 *   2) <V>-<random-uuid>.metadata.json, where V - metadata version
 */
std::pair<Int32, String>
getMetadataFileAndVersion(const ObjectStoragePtr & object_storage, const StorageObjectStorage::Configuration & configuration)
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
        metadata_files_with_versions.emplace_back(std::stoi(version_str), path);
    }

    /// Get the latest version of metadata file: v<V>.metadata.json
    return *std::max_element(metadata_files_with_versions.begin(), metadata_files_with_versions.end());
}


DataLakeMetadataPtr IcebergMetadata::create(
    const ObjectStoragePtr & object_storage, const ConfigurationObserverPtr & configuration, const ContextPtr & local_context)
{
    auto configuration_ptr = configuration.lock();

    const auto [metadata_version, metadata_file_path] = getMetadataFileAndVersion(object_storage, *configuration_ptr);

    auto log = getLogger("IcebergMetadata");
    LOG_DEBUG(log, "Parse metadata {}", metadata_file_path);

    StorageObjectStorageSource::ObjectInfo object_info(metadata_file_path);
    auto buf = StorageObjectStorageSource::createReadBuffer(object_info, object_storage, local_context, log);

    String json_str;
    readJSONObjectPossiblyInvalid(json_str, *buf);

    Poco::JSON::Parser parser; /// For some reason base/base/JSON.h can not parse this json file
    Poco::Dynamic::Var json = parser.parse(json_str);
    const Poco::JSON::Object::Ptr & object = json.extract<Poco::JSON::Object::Ptr>();

    IcebergSchemaProcessor schema_processor;

    auto format_version = object->getValue<int>("format-version");

    auto snapshots = object->get("snapshots").extract<Poco::JSON::Array::Ptr>();

    String manifest_list_file;
    auto current_snapshot_id = object->getValue<Int64>("current-snapshot-id");

    LOG_DEBUG(&Poco::Logger::get("IcebergMetadata initialize"), "Current snapshot id {}", current_snapshot_id);

    for (size_t i = 0; i < snapshots->size(); ++i)
    {
        const auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
        LOG_DEBUG(
            &Poco::Logger::get("IcebergMetadata initialize"),
            "Iterationg on snapshot with id {}",
            snapshot->getValue<Int64>("snapshot-id"));

        if (snapshot->getValue<Int64>("snapshot-id") == current_snapshot_id)
        {
            const auto path = snapshot->getValue<String>("manifest-list");
            manifest_list_file = std::filesystem::path(configuration_ptr->getPath()) / "metadata" / std::filesystem::path(path).filename();
            break;
        }
    }

    auto ptr = std::make_unique<IcebergMetadata>(
        object_storage, configuration_ptr, local_context, metadata_version, format_version, manifest_list_file, object);


    return ptr;
}

/**
 * Manifest file has the following format: '/iceberg_data/db/table_name/metadata/c87bfec7-d36c-4075-ad04-600b6b0f2020-m0.avro'
 *
 * `manifest file` is different in format version V1 and V2 and has the following contents:
 *                        v1     v2
 * status                 req    req
 * snapshot_id            req    opt
 * sequence_number               opt
 * file_sequence_number          opt
 * data_file              req    req
 * Example format version V1:
 * ┌─status─┬─────────snapshot_id─┬─data_file───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
 * │      1 │ 2819310504515118887 │ ('/iceberg_data/db/table_name/data/00000-1-3edca534-15a0-4f74-8a28-4733e0bf1270-00001.parquet','PARQUET',(),100,1070,67108864,[(1,233),(2,210)],[(1,100),(2,100)],[(1,0),(2,0)],[],[(1,'\0'),(2,'0')],[(1,'c'),(2,'99')],NULL,[4],0) │
 * └────────┴─────────────────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
 * Example format version V2:
 * ┌─status─┬─────────snapshot_id─┬─sequence_number─┬─file_sequence_number─┬─data_file───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
 * │      1 │ 5887006101709926452 │            ᴺᵁᴸᴸ │                 ᴺᵁᴸᴸ │ (0,'/iceberg_data/db/table_name/data/00000-1-c8045c90-8799-4eac-b957-79a0484e223c-00001.parquet','PARQUET',(),100,1070,[(1,233),(2,210)],[(1,100),(2,100)],[(1,0),(2,0)],[],[(1,'\0'),(2,'0')],[(1,'c'),(2,'99')],NULL,[4],[],0) │
 * └────────┴─────────────────────┴─────────────────┴──────────────────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
 * In case of partitioned data we'll have extra directory partition=value:
 * ─status─┬─────────snapshot_id─┬─data_file──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
 * │      1 │ 2252246380142525104 │ ('/iceberg_data/db/table_name/data/a=0/00000-1-c9535a00-2f4f-405c-bcfa-6d4f9f477235-00001.parquet','PARQUET',(0),1,631,67108864,[(1,46),(2,48)],[(1,1),(2,1)],[(1,0),(2,0)],[],[(1,'\0\0\0\0\0\0\0\0'),(2,'1')],[(1,'\0\0\0\0\0\0\0\0'),(2,'1')],NULL,[4],0) │
 * │      1 │ 2252246380142525104 │ ('/iceberg_data/db/table_name/data/a=1/00000-1-c9535a00-2f4f-405c-bcfa-6d4f9f477235-00002.parquet','PARQUET',(1),1,631,67108864,[(1,46),(2,48)],[(1,1),(2,1)],[(1,0),(2,0)],[],[(1,'\0\0\0\0\0\0\0'),(2,'2')],[(1,'\0\0\0\0\0\0\0'),(2,'2')],NULL,[4],0) │
 * │      1 │ 2252246380142525104 │ ('/iceberg_data/db/table_name/data/a=2/00000-1-c9535a00-2f4f-405c-bcfa-6d4f9f477235-00003.parquet','PARQUET',(2),1,631,67108864,[(1,46),(2,48)],[(1,1),(2,1)],[(1,0),(2,0)],[],[(1,'\0\0\0\0\0\0\0'),(2,'3')],[(1,'\0\0\0\0\0\0\0'),(2,'3')],NULL,[4],0) │
 * └────────┴─────────────────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
 */

ManifestList IcebergMetadata::initializeManifestList(const String & manifest_list_file) const
{
    auto configuration_ptr = configuration.lock();
    if (configuration_ptr == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Configuration is expired");

    auto context = getContext();
    StorageObjectStorageSource::ObjectInfo object_info(manifest_list_file);
    auto manifest_list_buf = StorageObjectStorageSource::createReadBuffer(object_info, object_storage, context, log);

    LOG_DEBUG(&Poco::Logger::get("initializeManifestList"), "Parse manifest list {}", manifest_list_file);
    auto manifest_list_file_reader
        = std::make_unique<avro::DataFileReaderBase>(std::make_unique<AvroInputStreamReadBufferAdapter>(*manifest_list_buf));

    LOG_DEBUG(&Poco::Logger::get("initializeManifestList"), "Parsed manifest list {}", manifest_list_file);

    auto data_type = AvroSchemaReader::avroNodeToDataType(manifest_list_file_reader->dataSchema().root()->leafAt(0));
    Block header{{data_type->createColumn(), data_type, "manifest_path"}};
    auto columns = parseAvro(*manifest_list_file_reader, header, getFormatSettings(context));
    auto & col = columns.at(0);

    if (col->getDataType() != TypeIndex::String)
    {
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "The parsed column from Avro file of `manifest_path` field should be String type, got {}",
            col->getFamilyName());
    }

    const auto * col_str = typeid_cast<ColumnString *>(col.get());
    std::vector<ManifestFileEntry> manifest_files;
    for (size_t i = 0; i < col_str->size(); ++i)
    {
        const auto file_path = col_str->getDataAt(i).toView();
        const auto filename = std::filesystem::path(file_path).filename();
        String manifest_file = std::filesystem::path(configuration_ptr->getPath()) / "metadata" / filename;
        auto manifest_file_it = manifest_files_by_name.find(manifest_file);
        if (manifest_file_it != manifest_files_by_name.end())
        {
            manifest_files.emplace_back(manifest_file_it);
            continue;
        }
        manifest_files.emplace_back(initializeManifestFile(filename, configuration_ptr));
    }

    return ManifestList{manifest_files};
}

ManifestFileEntry IcebergMetadata::initializeManifestFile(const String & filename, const ConfigurationPtr & configuration_ptr) const
{
    String manifest_file = std::filesystem::path(configuration_ptr->getPath()) / "metadata" / filename;

    StorageObjectStorageSource::ObjectInfo manifest_object_info(manifest_file);
    auto buffer = StorageObjectStorageSource::createReadBuffer(manifest_object_info, object_storage, getContext(), log);
    auto manifest_file_reader = std::make_unique<avro::DataFileReaderBase>(std::make_unique<AvroInputStreamReadBufferAdapter>(*buffer));
    auto [schema_id, schema_object] = parseTableSchemaFromManifestFile(*manifest_file_reader, filename);
    auto manifest_file_impl = std::make_unique<ManifestFileContentImpl>(
        std::move(manifest_file_reader), format_version, configuration_ptr->getPath(), getFormatSettings(getContext()), schema_id);
    auto [manifest_file_iterator, _inserted]
        = manifest_files_by_name.emplace(manifest_file, ManifestFileContent(std::move(manifest_file_impl)));
    ManifestFileEntry manifest_file_entry{manifest_file_iterator};
    for (const auto & data_file : manifest_file_entry.getContent().getDataFiles())
    {
        manifest_entry_by_data_file.emplace(data_file.data_file_name, manifest_file_entry);
    }
    LOG_DEBUG(&Poco::Logger::get("IcebergMetadata"), "Added manifest file {}", manifest_file);

    schema_processor.addIcebergTableSchema(schema_object);
    return manifest_file_entry;
}


IcebergSnapshot IcebergMetadata::getSnapshot(const String & manifest_list_file) const
{
    const auto manifest_list_file_it = manifest_lists_by_name.find(manifest_list_file);
    if (manifest_list_file_it != manifest_lists_by_name.end())
        return IcebergSnapshot(manifest_list_file_it);
    return IcebergSnapshot{manifest_lists_by_name.emplace(manifest_list_file, initializeManifestList(manifest_list_file)).first};
}


Strings IcebergMetadata::getDataFiles() const
{
    std::lock_guard lock(get_data_files_mutex);
    if (!data_files.empty())
        return data_files;

    if (!current_snapshot)
    {
        return {};
    }

    for (const auto & manifest_entry : current_snapshot->getManifestList().getManifestFiles())
    {
        for (const auto & data_file : manifest_entry.getContent().getDataFiles())
        {
            if (data_file.status != ManifestEntryStatus::DELETED)
            {
                data_files.push_back(data_file.data_file_name);
            }
        }
    }

    return data_files;
}

}

}

#endif
