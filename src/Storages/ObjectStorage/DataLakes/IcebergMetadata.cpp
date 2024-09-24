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

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <filesystem>
#    include <sstream>


namespace DB
{

namespace ErrorCodes
{
extern const int FILE_DOESNT_EXIST;
extern const int ILLEGAL_COLUMN;
extern const int BAD_ARGUMENTS;
extern const int UNSUPPORTED_METHOD;
extern const int LOGICAL_ERROR;
}

IcebergMetadata::IcebergMetadata(
    ObjectStoragePtr object_storage_,
    ConfigurationPtr configuration_,
    DB::ContextPtr context_,
    Int32 metadata_version_,
    Int32 format_version_,
    String manifest_list_file_,
    Int32 current_schema_id_,
    IcebergSchemaProcessor schema_processor_)
    : WithContext(context_)
    , object_storage(object_storage_)
    , configuration(configuration_)
    , metadata_version(metadata_version_)
    , format_version(format_version_)
    , manifest_list_file(std::move(manifest_list_file_))
    , current_schema_id(current_schema_id_)
    , schema_processor(schema_processor_)
    , schema(*schema_processor.getClickhouseTableSchemaById(current_schema_id))
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

std::pair<size_t, size_t> parse_decimal(const String & type_name)
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
        auto [precision, scale] = parse_decimal(type_name);
        return createDecimal<DataTypeDecimal>(precision, scale);
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown Iceberg type: {}", type_name);
}

std::optional<NameAndTypePair> IcebergSchemaProcessor::getSimpleNameAndTypeByVersion(Int32 field_id, std::optional<Int32> schema_id)
{
    if (!schema_id.has_value())
    {
        schema_id = last_schema_id;
        if (!schema_id.has_value())
        {
            return std::nullopt;
        }
    }
    if (!simple_type_by_field_id.contains(field_id))
    {
        return std::nullopt;
    }
    auto representations = simple_type_by_field_id.at(field_id);
    size_t i = 0;
    for (; i < representations.size(); ++i)
    {
        if (representations[i].first > last_schema_id.value())
        {
            break;
        }
    }
    if (i == 0)
    {
        return std::nullopt;
    }
    else if (representations[i - 1].second.has_value())
    {
        const auto & x = representations[i - 1].second.value();
        if (!x.name.has_value())
        {
            return std::nullopt;
        }
        return NameAndTypePair{x.name.value(), x.required ? getSimpleType(x.type) : makeNullable(getSimpleType(x.type))};
    }
    else
    {
        return std::nullopt;
    }
}

DataTypePtr IcebergSchemaProcessor::getComplexTypeFromObject(const Poco::JSON::Object::Ptr & type, Int32 parent_id)
{
    String type_name = type->getValue<String>("type");
    if (type_name == "list")
    {
        bool element_required = type->getValue<bool>("element-required");
        auto element_type = getFieldType(type, "element", "element-id", element_required, parent_id);
        return std::make_shared<DataTypeArray>(element_type);
    }

    if (type_name == "map")
    {
        auto key_type = getFieldType(type, "key", "key-id", true, parent_id);
        auto value_required = type->getValue<bool>("value-required");
        auto value_type = getFieldType(type, "value", "value-id", value_required, parent_id);
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
            element_types.push_back(getFieldType(field, "type", "id", required, parent_id));
        }

        return std::make_shared<DataTypeTuple>(element_types, element_names);
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown Iceberg type: {}", type_name);
}

void IcebergSchemaProcessor::refreshParentInfo(Int32 parent_id, Int32 current_id)
{
    if (!parents.contains(current_id))
    {
        parents[current_id] = parent_id;
    }
    else
    {
        chassert(parents[current_id] == parent_id);
    }
}

DataTypePtr IcebergSchemaProcessor::getFieldType(
    const Poco::JSON::Object::Ptr & field, const String & type_key, const String & id_key, bool required, Int32 parent_id)
{
    if (field->isObject(type_key))
    {
        Int32 current_id = field->getValue<Int32>("id");
        if (current_schema_id.has_value())
        {
            refreshParentInfo(parent_id, current_id);
        }
        return getComplexTypeFromObject(field->getObject(type_key), current_id);
    }

    auto type = field->get(type_key);
    if (type.isString())
    {
        const String & type_name = type.extract<String>();
        if (current_schema_id.has_value())
        {
            Int32 id = field->getValue<Int32>(id_key);
            refreshParentInfo(parent_id, id);
            auto nullable_name = field->getNullableValue<String>("name");
            std::optional<String> name = nullable_name.isNull() ? std::nullopt : std::optional{nullable_name.value()};
            if (simple_type_by_field_id.contains(id))
            {
                chassert(simple_type_by_field_id[id].size() >= 2);
                chassert(!simple_type_by_field_id[id].back().second.has_value());
                simple_type_by_field_id[id].pop_back();
                chassert(simple_type_by_field_id[id].back().second.has_value());
                auto current_representation = SimpleTypeRepresentation{name, required, type_name};
                if (current_representation != simple_type_by_field_id[id].back().second.value())
                {
                    simple_type_by_field_id[id].emplace_back(current_schema_id.value(), current_representation);
                }
            }
            else
            {
                simple_type_by_field_id[id].emplace_back(current_schema_id.value(), SimpleTypeRepresentation{name, required, type_name});
            }
        }
        return required ? getSimpleType(type_name) : makeNullable(getSimpleType(type_name));
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
        auto [old_precision, old_scale] = parse_decimal(old_type);
        auto [new_precision, new_scale] = parse_decimal(new_type);
        allowed_type_conversion |= (old_precision <= new_precision) && (old_scale == new_scale);
    }
    return allowed_type_conversion;
}

Int32 parseTableSchema(const Poco::JSON::Object::Ptr & metadata_object, IcebergSchemaProcessor & schema_processor)
{
    Int32 format_version = metadata_object->getValue<Int32>("format-version");
    if (format_version == 2)
    {
        auto fields = metadata_object->get("schemas").extract<Poco::JSON::Array::Ptr>();
        for (size_t i = 0; i != fields->size(); ++i)
        {
            auto field = fields->getObject(static_cast<UInt32>(i));
            schema_processor.addIcebergTableSchema(field);
        }
        return metadata_object->getValue<int>("current-schema-id");
    }
    else
    {
        auto schema = metadata_object->getObject("schema");
        schema_processor.addIcebergTableSchema(schema);
        return schema->getValue<Int32>("schema-id");
    }
}

std::shared_ptr<ActionsDAG>
IcebergSchemaProcessor::getSchemaTransformationDag(const Poco::JSON::Object::Ptr & old_schema, const Poco::JSON::Object::Ptr & new_schema)
{
    std::map<size_t, std::pair<Poco::JSON::Object::Ptr, const ActionsDAG::Node *>> old_schema_entries;
    auto old_schema_fields = old_schema->get("fields").extract<Poco::JSON::Array::Ptr>();
    std::shared_ptr<ActionsDAG> dag = std::make_shared<ActionsDAG>();
    auto & outputs = dag->getOutputs();
    for (size_t i = 0; i != old_schema_fields->size(); ++i)
    {
        auto field = old_schema_fields->getObject(static_cast<UInt32>(i));
        size_t id = field->getValue<size_t>("id");
        auto name = field->getValue<String>("name");
        bool required = field->getValue<bool>("required");
        old_schema_entries[id] = {field, &dag->addInput(name, getFieldType(field, "type", "id", required, 0))};
    }
    auto new_schema_fields = new_schema->get("fields").extract<Poco::JSON::Array::Ptr>();
    for (size_t i = 0; i != new_schema_fields->size(); ++i)
    {
        auto field = new_schema_fields->getObject(static_cast<UInt32>(i));
        size_t id = field->getValue<size_t>("id");
        auto name = field->getValue<String>("name");
        bool required = field->getValue<bool>("required");
        auto type = getFieldType(field, "type", "id", required, 0);
        if (old_schema_entries.count(id))
        {
            auto [old_json, old_node] = old_schema_entries.find(id)->second;
            if (field->isObject("type"))
            {
                if (!(*old_json == *field))
                {
                    throw Exception(
                        ErrorCodes::UNSUPPORTED_METHOD,
                        "Schema evolution is not supported for complex types yet, field id is {}, old schema id is {}, new schema id is {}",
                        id,
                        *current_old_id,
                        *current_new_id);
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
                        ErrorCodes::UNSUPPORTED_METHOD,
                        "Can't cast primitive type to the complex type, field id is {}, old schema id is {}, new schema id is {}",
                        id,
                        *current_old_id,
                        *current_new_id);
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
                    node = &dag->addCast(*old_node, getSimpleType(new_type), name);
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
                    "Adding a default column with id {} and complex type is not supported yet, old schema id is {}, new schema id is {}",
                    id,
                    *current_old_id,
                    *current_new_id);
            }
            if (!type->isNullable())
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Canßnot add a column with id {} with required values to the table during schema evolution, old schema id is {}, new "
                    "schema id is {}",
                    id,
                    *current_old_id,
                    *current_new_id);
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
    current_old_id = old_id;
    current_new_id = new_id;
    SCOPE_EXIT({
        current_old_id = std::nullopt;
        current_new_id = std::nullopt;
    });
    Poco::JSON::Object::Ptr old_schema, new_schema;
    if (transform_dags_by_ids.count({old_id, new_id}))
    {
        return transform_dags_by_ids.at({old_id, new_id});
    }
    try
    {
        old_schema = iceberg_table_schemas_by_ids.at(old_id);
    }
    catch (std::exception &)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Schema with schema-id {} is unknown", old_id);
    }
    if (old_id == new_id)
    {
        return nullptr;
    }
    try
    {
        new_schema = iceberg_table_schemas_by_ids.at(new_id);
    }
    catch (std::exception &)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Schema with schema-id {} is unknown", new_id);
    }
    return transform_dags_by_ids[{old_id, new_id}] = getSchemaTransformationDag(old_schema, new_schema);
}

//Schemas should be added in the ascending order of schema-id
void IcebergSchemaProcessor::addIcebergTableSchema(Poco::JSON::Object::Ptr schema)
{
    Int32 schema_id = schema->getValue<Int32>("schema-id");
    current_schema_id = schema_id;
    SCOPE_EXIT({ current_schema_id = std::nullopt; });
    if (iceberg_table_schemas_by_ids.count(schema_id))
    {
        chassert(clickhouse_table_schemas_by_ids.count(schema_id) > 0);
        chassert(*iceberg_table_schemas_by_ids.at(schema_id) == *schema);
    }
    else
    {
        chassert(!last_schema_id.has_value() || (last_schema_id.value() < current_schema_id.value()));
        for (auto & [id, id_variable_type_evolution] : simple_type_by_field_id)
        {
            chassert(!id_variable_type_evolution.empty());
            if (id_variable_type_evolution.back().second.has_value())
            {
                chassert(id_variable_type_evolution.back().first < current_schema_id.value());
                id_variable_type_evolution.emplace_back(current_schema_id.value(), std::nullopt);
            }
        }
        iceberg_table_schemas_by_ids[schema_id] = schema;
        auto fields = schema->get("fields").extract<Poco::JSON::Array::Ptr>();
        auto clickhouse_schema = std::make_shared<NamesAndTypesList>();
        for (size_t i = 0; i != fields->size(); ++i)
        {
            auto field = fields->getObject(static_cast<UInt32>(i));
            auto name = field->getValue<String>("name");
            bool required = field->getValue<bool>("required");
            clickhouse_schema->push_back(NameAndTypePair{name, getFieldType(field, "type", "id", required, 0)});
        }
        clickhouse_table_schemas_by_ids[schema_id] = clickhouse_schema;
    }
    last_schema_id = schema_id;
    for (auto & [field_id, id_variable_type_evolution] : simple_type_by_field_id)
    {
        LOG_DEBUG(&Poco::Logger::get("Printing schema id"), "Field Id: {}", field_id);
        LOG_DEBUG(&Poco::Logger::get("Printing schema id"), "Types size: {}", id_variable_type_evolution.size());
        for (const auto & [schema_version_id, simple_type] : id_variable_type_evolution)
        {
            if (simple_type.has_value())
            {
                LOG_DEBUG(
                    &Poco::Logger::get("Printing schema id"),
                    "Schema id: {}, Simple repro: name: {}, required: {}, type: {}",
                    schema_version_id,
                    simple_type->name.has_value() ? simple_type->name.value() : "NONAME",
                    simple_type->required,
                    simple_type->type);
            }
            else
            {
                LOG_DEBUG(&Poco::Logger::get("Printing schema id"), "Schema id: {}, FINISHED", schema_id);
            }
        }
        std::optional<NameAndTypePair> kek = IcebergSchemaProcessor::getSimpleNameAndTypeByVersion(field_id);
        LOG_DEBUG(
            &Poco::Logger::get("Printing schema id after request"),
            "Name: {}, type: {}",
            kek.has_value() ? kek.value().name : "NONAME",
            kek.has_value() ? kek.value().type->getPrettyName() : "NOTYPE");
    }
}


std::shared_ptr<NamesAndTypesList> IcebergSchemaProcessor::getClickhouseTableSchemaById(Int32 id)
{
    try
    {
        return clickhouse_table_schemas_by_ids.at(id);
    }
    catch (std::exception &)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Schema with id {} is unknown", id);
    }
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


DataLakeMetadataPtr IcebergMetadata::create(
    ObjectStoragePtr object_storage,
    ConfigurationPtr configuration,
    ContextPtr local_context)
{
    const auto [metadata_version, metadata_file_path] = getMetadataFileAndVersion(object_storage, *configuration);
    auto read_settings = local_context->getReadSettings();
    auto buf = object_storage->readObject(StoredObject(metadata_file_path), read_settings);
    String json_str;
    readJSONObjectPossiblyInvalid(json_str, *buf);

    Poco::JSON::Parser parser; /// For some reason base/base/JSON.h can not parse this json file
    Poco::Dynamic::Var json = parser.parse(json_str);
    Poco::JSON::Object::Ptr object = json.extract<Poco::JSON::Object::Ptr>();

    IcebergSchemaProcessor schema_processor;

    auto schema_id = parseTableSchema(object, schema_processor);

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

    Int32 format_version = object->getValue<Int32>("format-version");

    return std::make_unique<IcebergMetadata>(
        object_storage, configuration, local_context, metadata_version, format_version, manifest_list_file, schema_id, schema_processor);
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
DataFileInfos IcebergMetadata::getDataFileInfos(const ActionsDAG * filter_dag) const
{
    // if (!data_file_infos.empty())
    //     return data_file_infos;

    Strings manifest_files;
    if (manifest_list_file.empty())
        return {};

    LOG_TEST(log, "Collect manifest files from manifest list {}", manifest_list_file);

    auto context = getContext();
    auto read_settings = context->getReadSettings();
    auto manifest_list_buf = object_storage->readObject(StoredObject(manifest_list_file), read_settings);
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

    std::map<String, Int32> files;
    LOG_TEST(log, "Collect data files");
    for (const auto & manifest_file : manifest_files)
    {
        LOG_TEST(log, "Process manifest file {}", manifest_file);

        auto buffer = object_storage->readObject(StoredObject(manifest_file), read_settings);
        auto manifest_file_reader = std::make_unique<avro::DataFileReaderBase>(std::make_unique<AvroInputStreamReadBufferAdapter>(*buffer));

        /// Manifest file should always have table schema in avro file metadata. By now we don't support tables with evolved schema,
        /// so we should check if all manifest files have the same schema as in table metadata.
        auto avro_metadata = manifest_file_reader->metadata();
        std::vector<uint8_t> schema_json = avro_metadata["schema"];
        String schema_json_string = String(reinterpret_cast<char *>(schema_json.data()), schema_json.size());
        Poco::JSON::Parser parser;
        Poco::Dynamic::Var json = parser.parse(schema_json_string);
        Poco::JSON::Object::Ptr schema_object = json.extract<Poco::JSON::Object::Ptr>();
        Int32 schema_object_id = schema_object->getValue<int>("schema-id");
        avro::NodePtr root_node = manifest_file_reader->dataSchema().root();
        schema_processor.addIcebergTableSchema(schema_object);
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
        LOG_DEBUG(&Poco::Logger::get("Node characteristics"), "Status node has name: {}", status_node->hasName());
        LOG_DEBUG(&Poco::Logger::get("Filter dag state"), "Filter dag is None: {}", filter_dag == nullptr);

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

        std::vector<ColumnPtr> partition_columns;
        std::vector<PartitionTransform> partition_transforms;
        NamesAndTypesList partition_names_and_types;
        if (filter_dag)
        {
            LOG_DEBUG(&Poco::Logger::get("Entered partitioning block"), "");
            ColumnPtr big_partition_column = data_file_tuple_column->getColumnPtr(data_file_tuple_type.getPositionByName("partition"));
            if (big_partition_column->getDataType() != TypeIndex::Tuple)
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "The parsed column from Avro file of `file_path` field should be Tuple type, got {}",
                    columns.at(1)->getFamilyName());
            }
            const auto * big_partition_tuple = assert_cast<const ColumnTuple *>(big_partition_column.get());
            std::vector<uint8_t> partition_spec_json_bytes = avro_metadata["partition-spec"];
            String partition_spec_json_string
                = String(reinterpret_cast<char *>(partition_spec_json_bytes.data()), partition_spec_json_bytes.size());
            Poco::Dynamic::Var partition_spec_json = parser.parse(partition_spec_json_string);
            Poco::JSON::Array::Ptr partition_spec = partition_spec_json.extract<Poco::JSON::Array::Ptr>();
            for (size_t i = 0; i != partition_spec->size(); ++i)
            {
                auto current_field = partition_spec->getObject(static_cast<UInt32>(i));

                auto source_id = current_field->getValue<Int32>("source-id");
                LOG_DEBUG(&Poco::Logger::get("Partition Spec"), "source_id: {}", source_id);
                auto name_and_type = schema_processor.getSimpleNameAndTypeByVersion(source_id);
                LOG_DEBUG(&Poco::Logger::get("Partition Spec"), "name_and_type has value: {}", name_and_type.has_value());
                if (!name_and_type.has_value())
                {
                    continue;
                }
                LOG_DEBUG(&Poco::Logger::get("Partition Spec"), "Type: {}", name_and_type.value().type);

                //NEED TO REMOVE THIS
                auto if_nullable = dynamic_cast<const DataTypeNullable *>(name_and_type->getTypeInStorage().get());
                if (!WhichDataType(if_nullable->getNestedType()).isDate())
                {
                    continue;
                }
                PartitionTransform transform = getTransform(current_field->getValue<String>("transform"));
                // LOG_DEBUG(&Poco::Logger::get("Partition Spec"), "Transform is year: {}", transform == PartitionTransform::Year);

                if (transform == PartitionTransform::Unsupported)
                {
                    continue;
                }
                auto partition_name = current_field->getValue<String>("name");
                LOG_DEBUG(&Poco::Logger::get("Partition Spec"), "Name: {}", partition_name);

                // NEED TO COMPILE THIS
                partition_columns.push_back(big_partition_tuple->getColumnPtr(i));
                partition_transforms.push_back(transform);
                partition_names_and_types.push_back(std::move(name_and_type).value());
            }
            LOG_DEBUG(&Poco::Logger::get("Exited partitioning block"), "Number of taken colums: {}", partition_columns.size());
        }


        // partition_name_types = partition_key_expr->getRequiredColumnsWithTypes();
        // auto partition_minmax_idx_expr
        //     = std::make_shared<ExpressionActions>(ActionsDAG(partition_name_types), ExpressionActionsSettings::fromContext(getContext()));
        // // std::vector<Range> ranges;
        // // ranges.reserve(partition_names.size());
        // // for (size_t i = 0; i < partition_names.size(); ++i)
        // //     ranges.emplace_back(fields[i]);

        // const KeyCondition partition_key_condition(filter_dag, getContext(), partition_names, partition_minmax_idx_expr);
        // if (!partition_key_condition.checkInHyperrectangle(ranges, partition_types).can_be_true)
        //     return {};
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

            auto partition_names = partition_names_and_types.getNames();
            auto partition_types = partition_names_and_types.getTypes();

            std::vector<Range> ranges;
            for (size_t j = 0; j < partition_transforms.size(); ++j)
            {
                chassert((partition_transforms[j] == PartitionTransform::Year) || (partition_transforms[j] == PartitionTransform::Month));
                auto type = partition_types[j];
                auto year_column = dynamic_cast<const ColumnNullable *>(partition_columns[j].get())->getNestedColumnPtr();

                // if (year_column->getDataType()->getNested() != TypeIndex::Int32)
                // {
                //     throw Exception(
                //         ErrorCodes::ILLEGAL_COLUMN,
                //         "The parsed column from Avro file of `{}` field should be Int type, got {}",
                //         partition_names[i],
                //         year_column->getFamilyName());
                // }
                auto year_int_column = assert_cast<const ColumnInt32 *>(year_column.get());
                auto year = year_int_column->getInt(i);

                const UInt64 year_beginning = DateLUT::instance().LUTIndexByYearSinceEpochStartsZeroIndexing(year);
                const UInt64 next_year_beginning = DateLUT::instance().LUTIndexByYearSinceEpochStartsZeroIndexing(year + 1);
                Field year_beginning_field(year_beginning);
                Field next_year_beginning_field(next_year_beginning);
                // ColumnVector<UInt16>(1, year_beginning)->get(0, year_beginning_field);
                // ColumnVector<UInt16>(1, next_year_beginning)->get(0, next_year_beginning_field);
                ranges.emplace_back(year_beginning_field, true, next_year_beginning_field, false);
                LOG_DEBUG(
                    &Poco::Logger::get("Partition years"),
                    "Print partition date years: file_path: {}, year from epoch: {}, year_begin: {}, year_exclusive_end: {}, "
                    "clickhouse_column_name: {}",
                    file_path,
                    year,
                    year_beginning,
                    next_year_beginning,
                    partition_names[j]);
            }


            if (!partition_transforms.empty())
            {
                LOG_DEBUG(&Poco::Logger::get("Range to check"), "Given ranges size: {}", ranges.size());
                for (const auto & range : ranges)
                {
                    LOG_DEBUG(&Poco::Logger::get("Range to check"), "Given range: {}", range.toString());
                }
                ExpressionActionsPtr partition_minmax_idx_expr = std::make_shared<ExpressionActions>(
                    ActionsDAG(partition_names_and_types), ExpressionActionsSettings::fromContext(getContext()));
                for (auto par_name : partition_names)
                {
                    LOG_DEBUG(&Poco::Logger::get("Partition names"), "{}", par_name);
                }
                const KeyCondition partition_key_condition(filter_dag, getContext(), partition_names, partition_minmax_idx_expr);
                Ranges debug_ranges;
                bool ranges_extracted = partition_key_condition.extractPlainRanges(debug_ranges);
                if (ranges_extracted)
                {
                    LOG_DEBUG(&Poco::Logger::get("Range extracting"), "Extracted ranges size: {}", debug_ranges.size());
                    for (const auto & range : debug_ranges)
                    {
                        LOG_DEBUG(&Poco::Logger::get("Range extracting"), "Extracted range: {}", range.toString());
                    }
                }
                else
                {
                    LOG_DEBUG(&Poco::Logger::get("Range extracting"), "Ranges were not extracted");
                }
                if (!partition_key_condition.checkInHyperrectangle(ranges, partition_types).can_be_true)
                {
                    LOG_DEBUG(&Poco::Logger::get("Partition pruning"), "Partition pruning was successful for file: {}", file_path);
                    continue;
                }
                else
                {
                    LOG_DEBUG(&Poco::Logger::get("Partition pruning"), "Partition pruning failed for file: {}", file_path);
                }
            }

            if (ManifestEntryStatus(status) == ManifestEntryStatus::DELETED)
            {
                LOG_TEST(log, "Processing delete file for path: {}", file_path);
                chassert(files.count(file_path) == 0);
            }
            else
            {
                LOG_TEST(log, "Processing data file for path: {}", file_path);
                files[file_path] = schema_object_id;
            }
        }
    }

    DataFileInfos data_file_infos;

    for (const auto & [file_path, schema_object_id] : files)
    {
        data_file_infos.emplace_back(
            file_path,
            schema_processor.getClickhouseTableSchemaById(schema_object_id),
            schema_processor.getSchemaTransformationDagByIds(schema_object_id, current_schema_id));
    }

    return data_file_infos;
}

}

#endif
