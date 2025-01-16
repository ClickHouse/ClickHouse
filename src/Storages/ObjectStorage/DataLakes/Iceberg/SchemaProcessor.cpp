#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <IO/ReadBufferFromString.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include "Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h"

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

#include <IO/ReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int BAD_ARGUMENTS;
extern const int UNSUPPORTED_METHOD;
}

namespace
{


template <typename T>
bool equals(const T & first, const T & second)
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

bool operator==(const Poco::JSON::Array & first, const Poco::JSON::Array & second)
{
    return equals(first, second);
}

bool schemasAreIdentical(const Poco::JSON::Object & first, const Poco::JSON::Object & second)
{
    static String fields_key = "fields";
    if (!first.isArray(fields_key) || !second.isArray(fields_key))
        return false;
    return *(first.getArray(fields_key)) == *(second.getArray(fields_key));
}

std::pair<size_t, size_t> parseDecimal(const String & type_name)
{
    DB::ReadBufferFromString buf(std::string_view(type_name.begin() + 8, type_name.end() - 1));
    size_t precision;
    size_t scale;
    readIntText(precision, buf);
    skipWhitespaceIfAny(buf);
    assertChar(',', buf);
    skipWhitespaceIfAny(buf);
    tryReadIntText(scale, buf);
    return {precision, scale};
}

}

void IcebergSchemaProcessor::addIcebergTableSchema(Poco::JSON::Object::Ptr schema_ptr)
{
    Int32 schema_id = schema_ptr->getValue<Int32>("schema-id");
    if (iceberg_table_schemas_by_ids.contains(schema_id))
    {
        chassert(clickhouse_table_schemas_by_ids.contains(schema_id));
        chassert(schemasAreIdentical(*iceberg_table_schemas_by_ids.at(schema_id), *schema_ptr));
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
                const ActionsDAG::Node * node = old_node;
                if (old_json->getValue<String>("name") != name)
                {
                    node = &dag->addAlias(*old_node, name);
                }

                outputs.push_back(node);
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
            if (!type->isNullable() && !field->isObject("type"))
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

std::shared_ptr<NamesAndTypesList> IcebergSchemaProcessor::getClickhouseTableSchemaById(Int32 id)
{
    auto it = clickhouse_table_schemas_by_ids.find(id);
    if (it == clickhouse_table_schemas_by_ids.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Schema with id {} is unknown", id);
    return it->second;
}

}
