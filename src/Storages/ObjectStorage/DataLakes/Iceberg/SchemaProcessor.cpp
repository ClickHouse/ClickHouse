#include <optional>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <IO/ReadBufferFromString.h>
#include <Common/Exception.h>
#include <Common/SharedLockGuard.h>
#include <base/scope_guard.h>

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

using namespace Iceberg;

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


bool operator==(const Poco::JSON::Object & first, const Poco::JSON::Object & second)
{
    return equals(first, second);
}

bool operator==(const Poco::JSON::Array & first, const Poco::JSON::Array & second)
{
    return equals(first, second);
}

bool schemasAreIdentical(const Poco::JSON::Object & first, const Poco::JSON::Object & second)
{
    if (!first.isArray(f_fields) || !second.isArray(f_fields))
        return false;
    return *(first.getArray(f_fields)) == *(second.getArray(f_fields));
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

std::string IcebergSchemaProcessor::default_link{};

void IcebergSchemaProcessor::addIcebergTableSchema(Poco::JSON::Object::Ptr schema_ptr)
{
    std::lock_guard lock(mutex);

    Int32 schema_id = schema_ptr->getValue<Int32>(f_schema_id);
    current_schema_id = schema_id;
    if (iceberg_table_schemas_by_ids.contains(schema_id))
    {
        chassert(clickhouse_table_schemas_by_ids.contains(schema_id));
        chassert(schemasAreIdentical(*iceberg_table_schemas_by_ids.at(schema_id), *schema_ptr));
    }
    else
    {
        iceberg_table_schemas_by_ids[schema_id] = schema_ptr;
        auto fields = schema_ptr->get(f_fields).extract<Poco::JSON::Array::Ptr>();
        auto clickhouse_schema = std::make_shared<NamesAndTypesList>();
        String current_full_name{};
        for (size_t i = 0; i != fields->size(); ++i)
        {
            auto field = fields->getObject(static_cast<UInt32>(i));
            auto name = field->getValue<String>(f_name);
            bool required = field->getValue<bool>(f_required);
            current_full_name = name;
            auto type = getFieldType(field, f_type, required, current_full_name, true);
            clickhouse_schema->push_back(NameAndTypePair{name, type});
            clickhouse_types_by_source_ids[{schema_id, field->getValue<Int32>(f_id)}] = NameAndTypePair{current_full_name, type};
            clickhouse_ids_by_source_names[{schema_id, current_full_name}] = field->getValue<Int32>(f_id);
        }
        clickhouse_table_schemas_by_ids[schema_id] = clickhouse_schema;
    }
    current_schema_id = std::nullopt;
}

NameAndTypePair IcebergSchemaProcessor::getFieldCharacteristics(Int32 schema_version, Int32 source_id) const
{
    SharedLockGuard lock(mutex);

    auto it = clickhouse_types_by_source_ids.find({schema_version, source_id});
    if (it == clickhouse_types_by_source_ids.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Field with source id {} is unknown", source_id);
    return it->second;
}

std::optional<NameAndTypePair> IcebergSchemaProcessor::tryGetFieldCharacteristics(Int32 schema_version, Int32 source_id) const
{
    SharedLockGuard lock(mutex);

    auto it = clickhouse_types_by_source_ids.find({schema_version, source_id});
    if (it == clickhouse_types_by_source_ids.end())
        return {};
    return it->second;
}

std::optional<Int32> IcebergSchemaProcessor::tryGetColumnIDByName(Int32 schema_id, const std::string & name) const
{
    SharedLockGuard lock(mutex);

    auto it = clickhouse_ids_by_source_names.find({schema_id, name});
    if (it == clickhouse_ids_by_source_names.end())
        return {};
    return it->second;
}

NamesAndTypesList IcebergSchemaProcessor::tryGetFieldsCharacteristics(Int32 schema_id, const std::vector<Int32> & source_ids) const
{
    SharedLockGuard lock(mutex);

    NamesAndTypesList fields;
    for (const auto & source_id : source_ids)
    {
        auto it = clickhouse_types_by_source_ids.find({schema_id, source_id});
        if (it != clickhouse_types_by_source_ids.end())
            fields.push_back(it->second);
    }
    return fields;
}

DataTypePtr IcebergSchemaProcessor::getSimpleType(const String & type_name)
{
    if (type_name == f_boolean)
        return DataTypeFactory::instance().get("Bool");
    if (type_name == f_int)
        return std::make_shared<DataTypeInt32>();
    if (type_name == f_long || type_name == f_bigint)
        return std::make_shared<DataTypeInt64>();
    if (type_name == f_float)
        return std::make_shared<DataTypeFloat32>();
    if (type_name == f_double)
        return std::make_shared<DataTypeFloat64>();
    if (type_name == f_date)
        return std::make_shared<DataTypeDate>();
    if (type_name == f_time)
        return std::make_shared<DataTypeInt64>();
    if (type_name == f_timestamp)
        return std::make_shared<DataTypeDateTime64>(6);
    if (type_name == f_timestamptz)
        return std::make_shared<DataTypeDateTime64>(6, "UTC");
    if (type_name == f_string || type_name == f_binary)
        return std::make_shared<DataTypeString>();
    if (type_name == f_uuid)
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

DataTypePtr
IcebergSchemaProcessor::getComplexTypeFromObject(const Poco::JSON::Object::Ptr & type, String & current_full_name, bool is_subfield_of_root)
{
    String type_name = type->getValue<String>(f_type);
    if (type_name == f_list)
    {
        bool element_required = type->getValue<bool>("element-required");
        auto element_type = getFieldType(type, f_element, element_required);
        return std::make_shared<DataTypeArray>(element_type);
    }

    if (type_name == f_map)
    {
        auto key_type = getFieldType(type, f_key, true);
        auto value_required = type->getValue<bool>("value-required");
        auto value_type = getFieldType(type, f_value, value_required);
        return std::make_shared<DataTypeMap>(key_type, value_type);
    }

    if (type_name == f_struct)
    {
        DataTypes element_types;
        Names element_names;
        auto fields = type->get(f_fields).extract<Poco::JSON::Array::Ptr>();
        element_types.reserve(fields->size());
        element_names.reserve(fields->size());
        for (size_t i = 0; i != fields->size(); ++i)
        {
            auto field = fields->getObject(static_cast<Int32>(i));
            element_names.push_back(field->getValue<String>(f_name));
            auto required = field->getValue<bool>(f_required);
            if (is_subfield_of_root)
            {
                /// NOTE: getComplexTypeFromObject() with is_subfield_of_root==true called only from addIcebergTableSchema(), which already holds the exclusive lock
                /// So it is OK to use TSA_SUPPRESS_WARNING_FOR_READ/TSA_SUPPRESS_WARNING_FOR_WRITE
                Int32 schema_id = TSA_SUPPRESS_WARNING_FOR_READ(current_schema_id).value();

                (current_full_name += ".").append(element_names.back());
                scope_guard guard([&] { current_full_name.resize(current_full_name.size() - element_names.back().size() - 1); });
                element_types.push_back(getFieldType(field, f_type, required, current_full_name, true));
                TSA_SUPPRESS_WARNING_FOR_WRITE(clickhouse_types_by_source_ids)[{schema_id, field->getValue<Int32>(f_id)}]
                    = NameAndTypePair{current_full_name, element_types.back()};

                TSA_SUPPRESS_WARNING_FOR_WRITE(clickhouse_ids_by_source_names)[{schema_id, current_full_name}] = field->getValue<Int32>(f_id);
            }
            else
            {
                element_types.push_back(getFieldType(field, f_type, required));
            }
        }

        return std::make_shared<DataTypeTuple>(element_types, element_names);
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown Iceberg type: {}", type_name);
}

DataTypePtr IcebergSchemaProcessor::getFieldType(
    const Poco::JSON::Object::Ptr & field, const String & type_key, bool required, String & current_full_name, bool is_subfield_of_root)
{
    if (field->isObject(type_key))
        return getComplexTypeFromObject(field->getObject(type_key), current_full_name, is_subfield_of_root);

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
    allowed_type_conversion |= (old_type == f_int) && (new_type == f_long);
    allowed_type_conversion |= (old_type == f_float) && (new_type == f_double);
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
    auto old_schema_fields = old_schema->get(f_fields).extract<Poco::JSON::Array::Ptr>();
    std::shared_ptr<ActionsDAG> dag = std::make_shared<ActionsDAG>();
    auto & outputs = dag->getOutputs();
    for (size_t i = 0; i != old_schema_fields->size(); ++i)
    {
        auto field = old_schema_fields->getObject(static_cast<UInt32>(i));
        size_t id = field->getValue<size_t>(f_id);
        auto name = field->getValue<String>(f_name);
        bool required = field->getValue<bool>(f_required);
        old_schema_entries[id] = {field, &dag->addInput(name, getFieldType(field, f_type, required))};
    }
    auto new_schema_fields = new_schema->get(f_fields).extract<Poco::JSON::Array::Ptr>();
    for (size_t i = 0; i != new_schema_fields->size(); ++i)
    {
        auto field = new_schema_fields->getObject(static_cast<UInt32>(i));
        size_t id = field->getValue<size_t>(f_id);
        auto name = field->getValue<String>(f_name);
        bool required = field->getValue<bool>(f_required);
        auto type = getFieldType(field, f_type, required);
        auto old_node_it = old_schema_entries.find(id);
        if (old_node_it != old_schema_entries.end())
        {
            auto [old_json, old_node] = old_node_it->second;
            if (field->isObject(f_type))
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
                if (old_json->isObject(f_type))
                {
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Can't cast primitive type to the complex type, field id is {}, old schema id is {}, new schema id is {}",
                        id,
                        old_id,
                        new_id);
                }
                String old_type = old_json->getValue<String>(f_type);
                String new_type = field->getValue<String>(f_type);

                const ActionsDAG::Node * node = old_node;
                if (old_type == new_type)
                {
                    if (old_json->getValue<String>(f_name) != name)
                    {
                        node = &dag->addAlias(*old_node, name);
                    }
                }
                else if (allowPrimitiveTypeConversion(old_type, new_type))
                {
                    node = &dag->addCast(*old_node, getFieldType(field, f_type, required), name);
                }
                outputs.push_back(node);
            }
        }
        else
        {
            if (field->isObject(f_type))
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
        return nullptr;

    std::lock_guard lock(mutex);
    auto required_transform_dag_it = transform_dags_by_ids.find({old_id, new_id});
    if (required_transform_dag_it != transform_dags_by_ids.end())
        return required_transform_dag_it->second;

    auto old_schema_it = iceberg_table_schemas_by_ids.find(old_id);
    if (old_schema_it == iceberg_table_schemas_by_ids.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Schema with schema-id {} is unknown", old_id);

    auto new_schema_it = iceberg_table_schemas_by_ids.find(new_id);
    if (new_schema_it == iceberg_table_schemas_by_ids.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Schema with schema-id {} is unknown", new_id);

    return transform_dags_by_ids[{old_id, new_id}]
        = getSchemaTransformationDag(old_schema_it->second, new_schema_it->second, old_id, new_id);
}

std::shared_ptr<NamesAndTypesList> IcebergSchemaProcessor::getClickhouseTableSchemaById(Int32 id)
{
    SharedLockGuard lock(mutex);

    auto it = clickhouse_table_schemas_by_ids.find(id);
    if (it == clickhouse_table_schemas_by_ids.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Schema with id {} is unknown", id);
    return it->second;
}

bool IcebergSchemaProcessor::hasClickhouseTableSchemaById(Int32 id) const
{
    SharedLockGuard lock(mutex);

    return clickhouse_table_schemas_by_ids.contains(id);
}
}
