#include "config.h"

#if USE_AVRO

#include <Common/logger_useful.h>
#include <Core/Settings.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <Processors/Formats/Impl/AvroRowInputFormat.h>
#include <Storages/ObjectStorage/DataLakes/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Common.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <filesystem>

namespace DB
{
namespace Setting
{
    extern const SettingsBool iceberg_engine_ignore_schema_evolution;
}

namespace ErrorCodes
{
extern const int FILE_DOESNT_EXIST;
extern const int ILLEGAL_COLUMN;
extern const int BAD_ARGUMENTS;
extern const int UNSUPPORTED_METHOD;
}

IcebergMetadata::IcebergMetadata(
    ObjectStoragePtr object_storage_,
    ConfigurationPtr configuration_,
    DB::ContextPtr context_,
    Int32 metadata_version_,
    Int32 format_version_,
    String manifest_list_file_,
    Int32 current_schema_id_,
    DB::NamesAndTypesList schema_)
    : WithContext(context_)
    , object_storage(object_storage_)
    , configuration(configuration_)
    , metadata_version(metadata_version_)
    , format_version(format_version_)
    , manifest_list_file(std::move(manifest_list_file_))
    , current_schema_id(current_schema_id_)
    , schema(std::move(schema_))
    , log(getLogger("IcebergMetadata"))
{
}

namespace
{

enum class ManifestEntryStatus : uint8_t
{
    EXISTING = 0,
    ADDED = 1,
    DELETED = 2,
};

enum class DataFileContent : uint8_t
{
    DATA = 0,
    POSITION_DELETES = 1,
    EQUALITY_DELETES = 2,
};

/**
 * Iceberg supports the next data types (see https://iceberg.apache.org/spec/#schemas-and-data-types):
 * - Primitive types:
 *   - boolean
 *   - int
 *   - long
 *   - float
 *   - double
 *   - decimal(P, S)
 *   - date
 *   - time (time of day in microseconds since midnight)
 *   - timestamp (in microseconds since 1970-01-01)
 *   - timestamptz (timestamp with timezone, stores values in UTC timezone)
 *   - string
 *   - uuid
 *   - fixed(L) (fixed-length byte array of length L)
 *   - binary
 * - Complex types:
 *   - struct(field1: Type1, field2: Type2, ...) (tuple of typed values)
 *   - list(nested_type)
 *   - map(Key, Value)
 *
 * Example of table schema in metadata:
 * {
 *     "type" : "struct",
 *     "schema-id" : 0,
 *     "fields" : [
 *     {
 *         "id" : 1,
 *         "name" : "id",
 *         "required" : false,
 *         "type" : "long"
 *     },
 *     {
 *         "id" : 2,
 *         "name" : "array",
 *         "required" : false,
 *         "type" : {
 *             "type" : "list",
 *             "element-id" : 5,
 *             "element" : "int",
 *             "element-required" : false
 *     },
 *     {
 *         "id" : 3,
 *         "name" : "data",
 *         "required" : false,
 *         "type" : "binary"
 *     }
 * }
 */

DataTypePtr getSimpleTypeByName(const String & type_name)
{
    if (type_name == "boolean")
        return DataTypeFactory::instance().get("Bool");
    if (type_name == "int")
        return std::make_shared<DataTypeInt32>();
    if (type_name == "long")
        return std::make_shared<DataTypeInt64>();
    if (type_name == "float")
        return std::make_shared<DataTypeFloat32>();
    if (type_name == "double")
        return std::make_shared<DataTypeFloat64>();
    if (type_name == "date")
        return std::make_shared<DataTypeDate>();
    /// Time type represents time of the day in microseconds since midnight.
    /// We don't have similar type for it, let's use just Int64.
    if (type_name == "time")
        return std::make_shared<DataTypeInt64>();
    if (type_name == "timestamp")
        return std::make_shared<DataTypeDateTime64>(6);
    if (type_name == "timestamptz")
        return std::make_shared<DataTypeDateTime64>(6, "UTC");
    if (type_name == "string" || type_name == "binary")
        return std::make_shared<DataTypeString>();
    if (type_name == "uuid")
        return std::make_shared<DataTypeUUID>();

    if (type_name.starts_with("fixed[") && type_name.ends_with(']'))
    {
        ReadBufferFromString buf(std::string_view(type_name.begin() + 6, type_name.end() - 1));
        size_t n;
        readIntText(n, buf);
        return std::make_shared<DataTypeFixedString>(n);
    }

    if (type_name.starts_with("decimal(") && type_name.ends_with(')'))
    {
        ReadBufferFromString buf(std::string_view(type_name.begin() + 8, type_name.end() - 1));
        size_t precision;
        size_t scale;
        readIntText(precision, buf);
        skipWhitespaceIfAny(buf);
        assertChar(',', buf);
        skipWhitespaceIfAny(buf);
        tryReadIntText(scale, buf);
        return createDecimal<DataTypeDecimal>(precision, scale);
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown Iceberg type: {}", type_name);
}

DataTypePtr getFieldType(const Poco::JSON::Object::Ptr & field, const String & type_key, bool required);

DataTypePtr getComplexTypeFromObject(const Poco::JSON::Object::Ptr & type)
{
    String type_name = type->getValue<String>("type");
    if (type_name == "list")
    {
        bool element_required = type->getValue<bool>("element-required");
        auto element_type = getFieldType(type, "element", element_required);
        return std::make_shared<DataTypeArray>(element_type);
    }

    if (type_name == "map")
    {
        auto key_type = getFieldType(type, "key", true);
        auto value_required = type->getValue<bool>("value-required");
        auto value_type = getFieldType(type, "value", value_required);
        return std::make_shared<DataTypeMap>(key_type, value_type);
    }

    if (type_name == "struct")
    {
        DataTypes element_types;
        Names element_names;
        auto fields = type->get("fields").extract<Poco::JSON::Array::Ptr>();
        element_types.reserve(fields->size());
        element_names.reserve(fields->size());
        for (size_t i = 0; i != fields->size(); ++i)
        {
            auto field = fields->getObject(static_cast<Int32>(i));
            element_names.push_back(field->getValue<String>("name"));
            auto required = field->getValue<bool>("required");
            element_types.push_back(getFieldType(field, "type", required));
        }

        return std::make_shared<DataTypeTuple>(element_types, element_names);
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown Iceberg type: {}", type_name);
}

DataTypePtr getFieldType(const Poco::JSON::Object::Ptr & field, const String & type_key, bool required)
{
    if (field->isObject(type_key))
        return getComplexTypeFromObject(field->getObject(type_key));

    auto type = field->get(type_key);
    if (type.isString())
    {
        const String & type_name = type.extract<String>();
        auto data_type = getSimpleTypeByName(type_name);
        return required ? data_type : makeNullable(data_type);
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected 'type' field: {}", type.toString());

}

std::pair<NamesAndTypesList, Int32> parseTableSchema(const Poco::JSON::Object::Ptr & metadata_object, int format_version, bool ignore_schema_evolution)
{
    Poco::JSON::Object::Ptr schema;
    Int32 current_schema_id;

    /// First check if schema was evolved, because we don't support it yet.
    /// For version 2 we can check it by using field schemas, but in version 1
    /// this field is optional and we will check it later during parsing manifest files
    /// (we will compare schema id from manifest file and currently used schema).
    if (format_version == 2)
    {
        current_schema_id = metadata_object->getValue<int>("current-schema-id");
        auto schemas = metadata_object->get("schemas").extract<Poco::JSON::Array::Ptr>();
        if (schemas->size() == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse Iceberg table schema: schemas field is empty");

        if (ignore_schema_evolution)
        {
            /// If we ignore schema evolution, we will just use latest schema for all data files.
            /// Find schema with 'schema-id' equal to 'current_schema_id'.
            for (uint32_t i = 0; i != schemas->size(); ++i)
            {
                auto current_schema = schemas->getObject(i);
                if (current_schema->getValue<int>("schema-id") == current_schema_id)
                {
                    schema = current_schema;
                    break;
                }
            }

            if (!schema)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, R"(There is no schema with "schema-id" that matches "current-schema-id" in metadata)");
        }
        else
        {
            if (schemas->size() != 1)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "Cannot read Iceberg table: the table schema has been changed at least 1 time, reading tables with evolved schema is "
                    "supported. If you want to ignore schema evolution and read all files using latest schema saved on table creation, enable setting "
                    "iceberg_engine_ignore_schema_evolution (Note: enabling this setting can lead to incorrect result)");

            /// Now we sure that there is only one schema.
            schema = schemas->getObject(0);
            if (schema->getValue<int>("schema-id") != current_schema_id)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, R"(Field "schema-id" of the schema doesn't match "current-schema-id" in metadata)");
        }
    }
    else
    {
        schema = metadata_object->getObject("schema");
        current_schema_id = schema->getValue<int>("schema-id");
        /// Field "schemas" is optional for version 1, but after version 2 was introduced,
        /// in most cases this field is added for new tables in version 1 as well.
        if (!ignore_schema_evolution && metadata_object->has("schemas") && metadata_object->get("schemas").extract<Poco::JSON::Array::Ptr>()->size() > 1)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Cannot read Iceberg table: the table schema has been changed at least 1 time, reading tables with evolved schema is not "
                "supported. If you want to ignore schema evolution and read all files using latest schema saved on table creation, enable setting "
                "iceberg_engine_ignore_schema_evolution (Note: enabling this setting can lead to incorrect result)");
    }

    NamesAndTypesList names_and_types;
    auto fields = schema->get("fields").extract<Poco::JSON::Array::Ptr>();
    for (size_t i = 0; i != fields->size(); ++i)
    {
        auto field = fields->getObject(static_cast<UInt32>(i));
        auto name = field->getValue<String>("name");
        bool required = field->getValue<bool>("required");
        names_and_types.push_back({name, getFieldType(field, "type", required)});
    }

    return {std::move(names_and_types), current_schema_id};
}

MutableColumns parseAvro(
    avro::DataFileReaderBase & file_reader,
    const Block & header,
    const FormatSettings & settings)
{
    auto deserializer = std::make_unique<AvroDeserializer>(header, file_reader.dataSchema(), true, true, settings);
    MutableColumns columns = header.cloneEmptyColumns();

    file_reader.init();
    RowReadExtension ext;
    while (file_reader.hasMore())
    {
        file_reader.decr();
        deserializer->deserializeRow(columns, file_reader.decoder(), ext);
    }
    return columns;
}

/**
 * Each version of table metadata is stored in a `metadata` directory and
 * has one of 2 formats:
 *   1) v<V>.metadata.json, where V - metadata version.
 *   2) <V>-<random-uuid>.metadata.json, where V - metadata version
 */
std::pair<Int32, String> getMetadataFileAndVersion(
    ObjectStoragePtr object_storage,
    const StorageObjectStorage::Configuration & configuration)
{
    const auto metadata_files = listFiles(*object_storage, configuration, "metadata", ".metadata.json");
    if (metadata_files.empty())
    {
        throw Exception(
            ErrorCodes::FILE_DOESNT_EXIST,
            "The metadata file for Iceberg table with path {} doesn't exist",
            configuration.getPath());
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
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bad metadata file name: {}. Expected vN.metadata.json where N is a number", file_name);
        metadata_files_with_versions.emplace_back(std::stoi(version_str), path);
    }

    /// Get the latest version of metadata file: v<V>.metadata.json
    return *std::max_element(metadata_files_with_versions.begin(), metadata_files_with_versions.end());
}

}

DataLakeMetadataPtr IcebergMetadata::create(
    ObjectStoragePtr object_storage,
    ConfigurationPtr configuration,
    ContextPtr local_context)
{
    const auto [metadata_version, metadata_file_path] = getMetadataFileAndVersion(object_storage, *configuration);

    auto log = getLogger("IcebergMetadata");
    LOG_DEBUG(log, "Parse metadata {}", metadata_file_path);

    StorageObjectStorageSource::ObjectInfo object_info(metadata_file_path);
    auto buf = StorageObjectStorageSource::createReadBuffer(object_info, object_storage, local_context, log);

    String json_str;
    readJSONObjectPossiblyInvalid(json_str, *buf);

    Poco::JSON::Parser parser; /// For some reason base/base/JSON.h can not parse this json file
    Poco::Dynamic::Var json = parser.parse(json_str);
    const Poco::JSON::Object::Ptr & object = json.extract<Poco::JSON::Object::Ptr>();

    auto format_version = object->getValue<int>("format-version");
    auto [schema, schema_id]
        = parseTableSchema(object, format_version, local_context->getSettingsRef()[Setting::iceberg_engine_ignore_schema_evolution]);

    auto current_snapshot_id = object->getValue<Int64>("current-snapshot-id");
    auto snapshots = object->get("snapshots").extract<Poco::JSON::Array::Ptr>();

    String manifest_list_file;
    for (size_t i = 0; i < snapshots->size(); ++i)
    {
        const auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
        if (snapshot->getValue<Int64>("snapshot-id") == current_snapshot_id)
        {
            const auto path = snapshot->getValue<String>("manifest-list");
            manifest_list_file = std::filesystem::path(configuration->getPath()) / "metadata" / std::filesystem::path(path).filename();
            break;
        }
    }

    return std::make_unique<IcebergMetadata>(object_storage, configuration, local_context, metadata_version, format_version, manifest_list_file, schema_id, schema);
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
Strings IcebergMetadata::getDataFiles() const
{
    if (!data_files.empty())
        return data_files;

    Strings manifest_files;
    if (manifest_list_file.empty())
        return data_files;

    LOG_TEST(log, "Collect manifest files from manifest list {}", manifest_list_file);

    auto context = getContext();
    StorageObjectStorageSource::ObjectInfo object_info(manifest_list_file);
    auto manifest_list_buf = StorageObjectStorageSource::createReadBuffer(object_info, object_storage, context, log);
    auto manifest_list_file_reader = std::make_unique<avro::DataFileReaderBase>(std::make_unique<AvroInputStreamReadBufferAdapter>(*manifest_list_buf));

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
    for (size_t i = 0; i < col_str->size(); ++i)
    {
        const auto file_path = col_str->getDataAt(i).toView();
        const auto filename = std::filesystem::path(file_path).filename();
        manifest_files.emplace_back(std::filesystem::path(configuration->getPath()) / "metadata" / filename);
    }

    NameSet files;
    LOG_TEST(log, "Collect data files");
    for (const auto & manifest_file : manifest_files)
    {
        LOG_TEST(log, "Process manifest file {}", manifest_file);

        StorageObjectStorageSource::ObjectInfo manifest_object_info(manifest_file);
        auto buffer = StorageObjectStorageSource::createReadBuffer(manifest_object_info, object_storage, context, log);
        auto manifest_file_reader = std::make_unique<avro::DataFileReaderBase>(std::make_unique<AvroInputStreamReadBufferAdapter>(*buffer));

        /// Manifest file should always have table schema in avro file metadata. By now we don't support tables with evolved schema,
        /// so we should check if all manifest files have the same schema as in table metadata.
        auto avro_metadata = manifest_file_reader->metadata();
        std::vector<uint8_t> schema_json = avro_metadata["schema"];
        String schema_json_string = String(reinterpret_cast<char *>(schema_json.data()), schema_json.size());
        Poco::JSON::Parser parser;
        Poco::Dynamic::Var json = parser.parse(schema_json_string);
        Poco::JSON::Object::Ptr schema_object = json.extract<Poco::JSON::Object::Ptr>();
        if (!context->getSettingsRef()[Setting::iceberg_engine_ignore_schema_evolution]
            && schema_object->getValue<int>("schema-id") != current_schema_id)
            throw Exception(
                ErrorCodes::UNSUPPORTED_METHOD,
                "Cannot read Iceberg table: the table schema has been changed at least 1 time, reading tables with evolved schema is not "
                "supported. If you want to ignore schema evolution and read all files using latest schema saved on table creation, enable setting "
                "iceberg_engine_ignore_schema_evolution (Note: enabling this setting can lead to incorrect result)");

        avro::NodePtr root_node = manifest_file_reader->dataSchema().root();
        size_t leaves_num = root_node->leaves();
        size_t expected_min_num = format_version == 1 ? 3 : 2;
        if (leaves_num < expected_min_num)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unexpected number of columns {}. Expected at least {}",
                root_node->leaves(), expected_min_num);
        }

        avro::NodePtr status_node = root_node->leafAt(0);
        if (status_node->type() != avro::Type::AVRO_INT)
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "The parsed column from Avro file of `status` field should be Int type, got {}",
                magic_enum::enum_name(status_node->type()));
        }

        avro::NodePtr data_file_node = root_node->leafAt(static_cast<int>(leaves_num) - 1);
        if (data_file_node->type() != avro::Type::AVRO_RECORD)
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "The parsed column from Avro file of `data_file` field should be Tuple type, got {}",
                magic_enum::enum_name(data_file_node->type()));
        }

        auto status_col_data_type = AvroSchemaReader::avroNodeToDataType(status_node);
        auto data_col_data_type = AvroSchemaReader::avroNodeToDataType(data_file_node);
        Block manifest_file_header
            = {{status_col_data_type->createColumn(), status_col_data_type, "status"},
               {data_col_data_type->createColumn(), data_col_data_type, "data_file"}};

        columns = parseAvro(*manifest_file_reader, manifest_file_header, getFormatSettings(getContext()));
        if (columns.size() != 2)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected number of columns. Expected 2, got {}", columns.size());

        if (columns.at(0)->getDataType() != TypeIndex::Int32)
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "The parsed column from Avro file of `status` field should be Int32 type, got {}",
                columns.at(0)->getFamilyName());
        }
        if (columns.at(1)->getDataType() != TypeIndex::Tuple)
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "The parsed column from Avro file of `file_path` field should be Tuple type, got {}",
                columns.at(1)->getFamilyName());
        }

        const auto * status_int_column = assert_cast<ColumnInt32 *>(columns.at(0).get());
        const auto & data_file_tuple_type = assert_cast<const DataTypeTuple &>(*data_col_data_type.get());
        const auto * data_file_tuple_column = assert_cast<ColumnTuple *>(columns.at(1).get());

        if (status_int_column->size() != data_file_tuple_column->size())
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "The parsed column from Avro file of `file_path` and `status` have different rows number: {} and {}",
                status_int_column->size(),
                data_file_tuple_column->size());
        }

        ColumnPtr file_path_column = data_file_tuple_column->getColumnPtr(data_file_tuple_type.getPositionByName("file_path"));

        if (file_path_column->getDataType() != TypeIndex::String)
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "The parsed column from Avro file of `file_path` field should be String type, got {}",
                file_path_column->getFamilyName());
        }

        const auto * file_path_string_column = assert_cast<const ColumnString *>(file_path_column.get());

        ColumnPtr content_column;
        const ColumnInt32 * content_int_column = nullptr;
        if (format_version == 2)
        {
            content_column = data_file_tuple_column->getColumnPtr(data_file_tuple_type.getPositionByName("content"));
            if (content_column->getDataType() != TypeIndex::Int32)
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "The parsed column from Avro file of `content` field should be Int type, got {}",
                    content_column->getFamilyName());
            }

            content_int_column = assert_cast<const ColumnInt32 *>(content_column.get());
        }

        for (size_t i = 0; i < data_file_tuple_column->size(); ++i)
        {
            if (format_version == 2)
            {
                Int32 content_type = content_int_column->getElement(i);
                if (DataFileContent(content_type) != DataFileContent::DATA)
                    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Cannot read Iceberg table: positional and equality deletes are not supported");
            }

            const auto status = status_int_column->getInt(i);
            const auto data_path = std::string(file_path_string_column->getDataAt(i).toView());
            const auto pos = data_path.find(configuration->getPath());
            if (pos == std::string::npos)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected to find {} in data path: {}", configuration->getPath(), data_path);

            const auto file_path = data_path.substr(pos);

            if (ManifestEntryStatus(status) == ManifestEntryStatus::DELETED)
            {
                LOG_TEST(log, "Processing delete file for path: {}", file_path);
                chassert(!files.contains(file_path));
            }
            else
            {
                LOG_TEST(log, "Processing data file for path: {}", file_path);
                files.insert(file_path);
            }
        }
    }

    data_files = std::vector<std::string>(files.begin(), files.end());
    return data_files;
}

}

#endif
