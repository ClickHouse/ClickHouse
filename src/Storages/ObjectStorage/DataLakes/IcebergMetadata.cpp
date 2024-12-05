#include <mutex>
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
#include <sstream>

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

Int32 parseTableSchema(
    const Poco::JSON::Object::Ptr & metadata_object, IcebergSchemaProcessor & schema_processor, LoggerPtr metadata_logger);

IcebergMetadata::IcebergMetadata(
    ObjectStoragePtr object_storage_,
    ConfigurationObserverPtr configuration_,
    const DB::ContextPtr & context_,
    Int32 metadata_version_,
    Int32 format_version_,
    String manifest_list_file_,
    const Poco::JSON::Object::Ptr & object)
    : WithContext(context_)
    , object_storage(std::move(object_storage_))
    , configuration(std::move(configuration_))
    , metadata_version(metadata_version_)
    , format_version(format_version_)
    , manifest_list_file(std::move(manifest_list_file_))
    , schema_processor(IcebergSchemaProcessor())
    , log(getLogger("IcebergMetadata"))
{
    auto schema_id = parseTableSchema(object, schema_processor, log);
    schema = *(schema_processor.getClickhouseTableSchemaById(schema_id));
    current_schema_id = schema_id;
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

std::pair<size_t, size_t> parseDecimal(const String & type_name)
{
    ReadBufferFromString buf(std::string_view(type_name.begin() + 8, type_name.end() - 1));
    size_t precision;
    size_t scale;
    readIntText(precision, buf);
    skipWhitespaceIfAny(buf);
    assertChar(',', buf);
    skipWhitespaceIfAny(buf);
    tryReadIntText(scale, buf);
    return {precision, scale};
}

bool operator==(const Poco::JSON::Object & first, const Poco::JSON::Object & second)
{
    std::stringstream first_string_stream; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    std::stringstream second_string_stream; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    first.stringify(first_string_stream);
    if (!first_string_stream)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JSON Parsing failed");
    }
    second.stringify(second_string_stream);
    if (!second_string_stream)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JSON Parsing failed");
    }
    return first_string_stream.str() == second_string_stream.str();
}

bool operator!=(const Poco::JSON::Object & first, const Poco::JSON::Object & second)
{
    return !(first == second);
}
}


DataTypePtr IcebergSchemaProcessor::getSimpleType(const String & type_name)
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
        auto [precision, scale] = parseDecimal(type_name);
        return createDecimal<DataTypeDecimal>(precision, scale);
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown Iceberg type: {}", type_name);
}

DataTypePtr IcebergSchemaProcessor::getComplexTypeFromObject(const Poco::JSON::Object::Ptr & type)
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

DataTypePtr IcebergSchemaProcessor::getFieldType(const Poco::JSON::Object::Ptr & field, const String & type_key, bool required)
{
    if (field->isObject(type_key))
        return getComplexTypeFromObject(field->getObject(type_key));

    auto type = field->get(type_key);
    if (type.isString())
    {
        const String & type_name = type.extract<String>();
        auto data_type = getSimpleType(type_name);
        return required ? data_type : makeNullable(data_type);
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected 'type' field: {}", type.toString());

}


/**
* Iceberg allows only three types of primitive type conversion:
* int -> long
* float -> double
* decimal(P, S) -> decimal(P', S) where P' > P
* This function checks if `old_type` and `new_type` satisfy to one of these conditions.
**/
bool IcebergSchemaProcessor::allowPrimitiveTypeConversion(const String & old_type, const String & new_type)
{
    bool allowed_type_conversion = (old_type == new_type);
    allowed_type_conversion |= (old_type == "int") && (new_type == "long");
    allowed_type_conversion |= (old_type == "float") && (new_type == "double");
    if (old_type.starts_with("decimal(") && old_type.ends_with(')') && new_type.starts_with("decimal(") && new_type.ends_with(")"))
    {
        auto [old_precision, old_scale] = parseDecimal(old_type);
        auto [new_precision, new_scale] = parseDecimal(new_type);
        allowed_type_conversion |= (old_precision <= new_precision) && (old_scale == new_scale);
    }
    return allowed_type_conversion;
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
    const Poco::JSON::Object::Ptr & metadata_object, IcebergSchemaProcessor & schema_processor, LoggerPtr metadata_logger)
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


// Ids are passed only for error logging purposes
std::shared_ptr<ActionsDAG> IcebergSchemaProcessor::getSchemaTransformationDag(
    const Poco::JSON::Object::Ptr & old_schema, const Poco::JSON::Object::Ptr & new_schema, Int32 old_id, Int32 new_id)
{
    std::unordered_map<size_t, std::pair<Poco::JSON::Object::Ptr, const ActionsDAG::Node *>> old_schema_entries;
    auto old_schema_fields = old_schema->get("fields").extract<Poco::JSON::Array::Ptr>();
    std::shared_ptr<ActionsDAG> dag = std::make_shared<ActionsDAG>();
    auto & outputs = dag->getOutputs();
    for (size_t i = 0; i != old_schema_fields->size(); ++i)
    {
        auto field = old_schema_fields->getObject(static_cast<UInt32>(i));
        size_t id = field->getValue<size_t>("id");
        auto name = field->getValue<String>("name");
        bool required = field->getValue<bool>("required");
        old_schema_entries[id] = {field, &dag->addInput(name, getFieldType(field, "type", required))};
    }
    auto new_schema_fields = new_schema->get("fields").extract<Poco::JSON::Array::Ptr>();
    for (size_t i = 0; i != new_schema_fields->size(); ++i)
    {
        auto field = new_schema_fields->getObject(static_cast<UInt32>(i));
        size_t id = field->getValue<size_t>("id");
        auto name = field->getValue<String>("name");
        bool required = field->getValue<bool>("required");
        auto type = getFieldType(field, "type", required);
        auto old_node_it = old_schema_entries.find(id);
        if (old_node_it != old_schema_entries.end())
        {
            auto [old_json, old_node] = old_node_it->second;
            if (field->isObject("type"))
            {
                if (*old_json != *field)
                {
                    throw Exception(
                        ErrorCodes::UNSUPPORTED_METHOD,
                        "Schema evolution is not supported for complex types yet, field id is {}, old schema id is {}, new schema id "
                        "is {}",
                        id,
                        old_id,
                        new_id);
                }
                else
                {
                    outputs.push_back(old_node);
                }
            }
            else
            {
                if (old_json->isObject("type"))
                {
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Can't cast primitive type to the complex type, field id is {}, old schema id is {}, new schema id is {}",
                        id,
                        old_id,
                        new_id);
                }
                String old_type = old_json->getValue<String>("type");
                String new_type = field->getValue<String>("type");

                const ActionsDAG::Node * node = old_node;
                if (old_type == new_type)
                {
                    if (old_json->getValue<String>("name") != name)
                    {
                        node = &dag->addAlias(*old_node, name);
                    }
                }
                else if (allowPrimitiveTypeConversion(old_type, new_type))
                {
                    node = &dag->addCast(*old_node, getFieldType(field, "type", required), name);
                }
                outputs.push_back(node);
            }
        }
        else
        {
            if (field->isObject("type"))
            {
                throw Exception(
                    ErrorCodes::UNSUPPORTED_METHOD,
                    "Adding a default column with id {} and complex type is not supported yet. Old schema id is {}, new schema id is "
                    "{}",
                    id,
                    old_id,
                    new_id);
            }
            if (!type->isNullable())
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Cannot add a column with id {} with required values to the table during schema evolution. This is forbidden by "
                    "Iceberg format specification. Old schema id is {}, new "
                    "schema id is {}",
                    id,
                    old_id,
                    new_id);
            }
            ColumnPtr default_type_column = type->createColumnConstWithDefaultValue(0);
            const auto & constant = dag->addColumn({default_type_column, type, name});
            outputs.push_back(&constant);
        }
    }
    return dag;
}

std::shared_ptr<const ActionsDAG> IcebergSchemaProcessor::getSchemaTransformationDagByIds(Int32 old_id, Int32 new_id)
{
    if (old_id == new_id)
    {
        return nullptr;
    }
    std::lock_guard lock(mutex);
    auto required_transform_dag_it = transform_dags_by_ids.find({old_id, new_id});
    if (required_transform_dag_it != transform_dags_by_ids.end())
    {
        return required_transform_dag_it->second;
    }

    auto old_schema_it = iceberg_table_schemas_by_ids.find(old_id);
    if (old_schema_it == iceberg_table_schemas_by_ids.end())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Schema with schema-id {} is unknown", old_id);
    }
    auto new_schema_it = iceberg_table_schemas_by_ids.find(new_id);
    if (new_schema_it == iceberg_table_schemas_by_ids.end())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Schema with schema-id {} is unknown", new_id);
    }
    return transform_dags_by_ids[{old_id, new_id}]
        = getSchemaTransformationDag(old_schema_it->second, new_schema_it->second, old_id, new_id);
}

void IcebergSchemaProcessor::addIcebergTableSchema(Poco::JSON::Object::Ptr schema_ptr)
{
    Int32 schema_id = schema_ptr->getValue<Int32>("schema-id");
    if (iceberg_table_schemas_by_ids.contains(schema_id))
    {
        chassert(clickhouse_table_schemas_by_ids.contains(schema_id));
        chassert(*iceberg_table_schemas_by_ids.at(schema_id) == *schema_ptr);
    }
    else
    {
        iceberg_table_schemas_by_ids[schema_id] = schema_ptr;
        auto fields = schema_ptr->get("fields").extract<Poco::JSON::Array::Ptr>();
        auto clickhouse_schema = std::make_shared<NamesAndTypesList>();
        for (size_t i = 0; i != fields->size(); ++i)
        {
            auto field = fields->getObject(static_cast<UInt32>(i));
            auto name = field->getValue<String>("name");
            bool required = field->getValue<bool>("required");
            clickhouse_schema->push_back(NameAndTypePair{name, getFieldType(field, "type", required)});
        }
        clickhouse_table_schemas_by_ids[schema_id] = clickhouse_schema;
    }
}

std::shared_ptr<NamesAndTypesList> IcebergSchemaProcessor::getClickhouseTableSchemaById(Int32 id)
{
    auto it = clickhouse_table_schemas_by_ids.find(id);
    if (it == clickhouse_table_schemas_by_ids.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Schema with id {} is unknown", id);
    return it->second;
}

MutableColumns parseAvro(avro::DataFileReaderBase & file_reader, const Block & header, const FormatSettings & settings)
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
std::pair<Int32, String>
getMetadataFileAndVersion(const ObjectStoragePtr & object_storage, const StorageObjectStorage::Configuration & configuration)
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

    for (size_t i = 0; i < snapshots->size(); ++i)
    {
        const auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
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


Strings IcebergMetadata::getDataFiles() const
{
    std::lock_guard lock(get_data_files_mutex);
    if (!data_files.empty())
        return data_files;

    auto configuration_ptr = configuration.lock();
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
        manifest_files.emplace_back(std::filesystem::path(configuration_ptr->getPath()) / "metadata" / filename);
    }

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
        auto avro_schema_it = avro_metadata.find("schema");
        if (avro_schema_it == avro_metadata.end())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Cannot read Iceberg table: manifest file {} doesn't have table schema in its metadata",
                manifest_file);
        std::vector<uint8_t> schema_json = avro_schema_it->second;
        String schema_json_string = String(reinterpret_cast<char *>(schema_json.data()), schema_json.size());
        Poco::JSON::Parser parser;
        Poco::Dynamic::Var json = parser.parse(schema_json_string);
        const Poco::JSON::Object::Ptr & schema_object = json.extract<Poco::JSON::Object::Ptr>();
        Int32 schema_object_id = schema_object->getValue<int>("schema-id");
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
            const auto pos = data_path.find(configuration_ptr->getPath());
            if (pos == std::string::npos)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected to find {} in data path: {}", configuration_ptr->getPath(), data_path);

            const auto file_path = data_path.substr(pos);

            if (ManifestEntryStatus(status) == ManifestEntryStatus::DELETED)
            {
                LOG_TEST(log, "Processing delete file for path: {}", file_path);
                chassert(schema_id_by_data_file.contains(file_path) == 0);
            }
            else
            {
                LOG_TEST(log, "Processing data file for path: {}", file_path);
                schema_id_by_data_file[file_path] = schema_object_id;
            }
        }

        schema_processor.addIcebergTableSchema(schema_object);
    }

    for (const auto & [file_path, schema_object_id] : schema_id_by_data_file)
    {
        data_files.emplace_back(file_path);
    }
    return data_files;
}

}

#endif
