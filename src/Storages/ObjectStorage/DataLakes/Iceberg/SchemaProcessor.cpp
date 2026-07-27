#include <algorithm>
#include <memory>
#include <optional>
#include <unordered_map>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>

#include <IO/ReadBufferFromString.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Columns/ColumnConst.h>
#include <Columns/IColumn.h>
#include <Functions/IFunction.h>
#include <base/types.h>
#include <Common/SharedLockGuard.h>
#include <Common/StringUtils.h>
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
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatFactory.h>

#include <IO/ReadHelpers.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/ComplexTypeSchemaProcessorFunctions.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int BAD_ARGUMENTS;
extern const int ICEBERG_SPECIFICATION_VIOLATION;
}


namespace
{

void traverseComplexType(Poco::JSON::Object::Ptr type, std::unordered_map<String, Int64> & result, const String & current_path)
{
    auto type_str = type->getValue<String>(Iceberg::f_type);
    if (type_str == "map")
    {
        auto key_id = type->getValue<Int64>(Iceberg::f_key_id);
        auto value_id = type->getValue<Int64>(Iceberg::f_value_id);
        auto key_name = Nested::concatenateName(current_path, "key");
        auto value_name = Nested::concatenateName(current_path, "value");
        if (type->isObject(Iceberg::f_key))
            traverseComplexType(type->getObject(Iceberg::f_key), result, key_name);
        result[key_name] = key_id;

        if (type->isObject(Iceberg::f_value))
            traverseComplexType(type->getObject(Iceberg::f_value), result, value_name);
        result[value_name] = value_id;
        return;
    }
    if (type_str == "list")
    {
        auto element_id = type->getValue<Int64>(Iceberg::f_element_id);
        auto element_name = Nested::concatenateName(current_path, "element");
        if (type->isObject(Iceberg::f_element))
            traverseComplexType(type->getObject(Iceberg::f_element), result, element_name);
        result[element_name] = element_id;
        return;
    }
    if (type_str == "struct")
    {
        auto fields = type->getArray(Iceberg::f_fields);
        for (UInt32 i = 0; i < fields->size(); ++i)
        {
            auto field = fields->getObject(i);
            auto field_id = field->getValue<Int32>(Iceberg::f_id);
            auto child_path = Nested::concatenateName(current_path, field->getValue<String>(Iceberg::f_name));
            if (field->isObject(Iceberg::f_type))
                traverseComplexType(field->getObject(Iceberg::f_type), result, child_path);
            result[child_path] = field_id;
        }
        return;
    }
}

/// Collects the paths whose Iceberg logical type is exactly `string` (both `string` and `binary`
/// read as DataTypeString). `type` is a JSON string (primitive leaf) or object (complex type).
void collectStringPaths(const Poco::Dynamic::Var & type, std::unordered_set<String> & result, const String & current_path)
{
    if (type.isString())
    {
        if (type.extract<String>() == Iceberg::f_string)
            result.insert(current_path);
        return;
    }

    const auto & type_object = type.extract<Poco::JSON::Object::Ptr>();
    auto type_str = type_object->getValue<String>(Iceberg::f_type);
    if (type_str == "map")
    {
        if (type_object->has(Iceberg::f_key))
            collectStringPaths(type_object->get(Iceberg::f_key), result, Nested::concatenateName(current_path, "key"));
        if (type_object->has(Iceberg::f_value))
            collectStringPaths(type_object->get(Iceberg::f_value), result, Nested::concatenateName(current_path, "value"));
    }
    else if (type_str == "list")
    {
        if (type_object->has(Iceberg::f_element))
            collectStringPaths(type_object->get(Iceberg::f_element), result, Nested::concatenateName(current_path, "element"));
    }
    else if (type_str == "struct")
    {
        auto fields = type_object->getArray(Iceberg::f_fields);
        for (UInt32 i = 0; i < fields->size(); ++i)
        {
            auto field = fields->getObject(i);
            auto child_path = Nested::concatenateName(current_path, field->getValue<String>(Iceberg::f_name));
            if (field->has(Iceberg::f_type))
                collectStringPaths(field->get(Iceberg::f_type), result, child_path);
        }
    }
}

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


bool schemasAreIdentical(const Poco::JSON::Object & first, const Poco::JSON::Object & second, const std::unordered_map<String, String> & type_mapping);

/// Canonicalize spacing in an Iceberg primitive type string by removing ASCII whitespace that is
/// only optional formatting around the delimiters '(', ')', '[', ']', ',' (and at the string edges).
/// Whitespace embedded inside a token (e.g. between the digits of "decimal(2 0,0)") is preserved so
/// that malformed spellings remain malformed and are still rejected by the parser.
String canonicalizeTypeSpacing(const String & s)
{
    auto is_delimiter = [](char c) { return c == '(' || c == ')' || c == '[' || c == ']' || c == ','; };
    String result;
    result.reserve(s.size());
    for (size_t i = 0; i < s.size(); ++i)
    {
        if (!isWhitespaceASCII(s[i]))
        {
            result.push_back(s[i]);
            continue;
        }
        const char prev = result.empty() ? '\0' : result.back();
        size_t j = i + 1;
        while (j < s.size() && isWhitespaceASCII(s[j]))
            ++j;
        const char next = j < s.size() ? s[j] : '\0';
        /// Drop whitespace only when it is next to a delimiter or at the start/end of the string;
        /// keep it when it sits between two token characters.
        const bool drop = prev == '\0' || next == '\0' || is_delimiter(prev) || is_delimiter(next);
        if (!drop)
            result.push_back(s[i]);
    }
    return result;
}

/// Compare two Iceberg type descriptors for the same field. A type is either a primitive
/// string ("long", "decimal(20, 0)", ...) or a nested object ("struct" with a fields array,
/// or "list" / "map" wrappers whose element/key/value members are themselves types).
/// Primitive type strings are whitespace-insensitive per the Iceberg spec, so recurse into
/// list/map members instead of comparing the wrapper object textually.
bool typesAreStructurallyIdentical(
    const Poco::Dynamic::Var & first_in, const Poco::Dynamic::Var & second_in, const std::unordered_map<String, String> & type_mapping)
{
    Poco::Dynamic::Var first = first_in;
    Poco::Dynamic::Var second = second_in;

    /// Apply configured type aliases (e.g. geography -> binary) to string types.
    /// Canonicalize spacing before the alias prefix match so leading/trailing whitespace does not
    /// defeat it: " geography(C,A)" must map to the same alias as "geography(C, A)". Otherwise the
    /// same schema-id serialized with one spelling that skips aliasing and another that maps to
    /// "binary" would compare unequal and be wrongly rejected.
    if (first.isString())
    {
        const String canon = canonicalizeTypeSpacing(first.toString());
        first = canon;
        for (const auto & [prefix, mapped] : type_mapping)
            if (canon.starts_with(prefix))
            {
                first = mapped;
                break;
            }
    }
    if (second.isString())
    {
        const String canon = canonicalizeTypeSpacing(second.toString());
        second = canon;
        for (const auto & [prefix, mapped] : type_mapping)
            if (canon.starts_with(prefix))
            {
                second = mapped;
                break;
            }
    }

    /// Primitive type strings: e.g. both "decimal(20,0)" and "decimal(20, 0)" denote the same
    /// type. Different writers emit different spacing, so ignore ASCII whitespace.
    if (first.isString() && second.isString())
        return canonicalizeTypeSpacing(first.toString()) == canonicalizeTypeSpacing(second.toString());

    const bool both_objects
        = first.type() == typeid(Poco::JSON::Object::Ptr) && second.type() == typeid(Poco::JSON::Object::Ptr);
    if (both_objects)
    {
        const auto & first_obj = first.extract<Poco::JSON::Object::Ptr>();
        const auto & second_obj = second.extract<Poco::JSON::Object::Ptr>();

        /// struct: compare nested field list recursively.
        if (first_obj->isArray(f_fields) || second_obj->isArray(f_fields))
            return schemasAreIdentical(*first_obj, *second_obj, type_mapping);

        /// list / map wrappers: same member set, with the nested type members (element / key /
        /// value) compared recursively so their primitive strings are whitespace-insensitive too,
        /// and the remaining scalar members (ids, required flags) compared textually.
        auto names_first = first_obj->getNames();
        auto names_second = second_obj->getNames();
        std::sort(names_first.begin(), names_first.end());
        std::sort(names_second.begin(), names_second.end());
        if (names_first != names_second)
            return false;
        for (const auto & name : names_first)
        {
            if (name == f_element || name == f_key || name == f_value)
            {
                if (!typesAreStructurallyIdentical(first_obj->get(name), second_obj->get(name), type_mapping))
                    return false;
            }
            else
            {
                Poco::JSON::Object wrapper_first;
                wrapper_first.set(name, first_obj->get(name));
                Poco::JSON::Object wrapper_second;
                wrapper_second.set(name, second_obj->get(name));
                if (!equals(wrapper_first, wrapper_second))
                    return false;
            }
        }
        return true;
    }

    /// Mismatched shapes (string vs object) or scalar values: compare textually.
    Poco::JSON::Object wrapper_first;
    wrapper_first.set(f_type, first);
    Poco::JSON::Object wrapper_second;
    wrapper_second.set(f_type, second);
    return equals(wrapper_first, wrapper_second);
}

bool schemaFieldsAreStructurallyIdentical(const Poco::JSON::Object & first, const Poco::JSON::Object & second, const std::unordered_map<String, String> & type_mapping)
{
    static constexpr const char * structural_keys[] = {f_id, f_name, f_required, f_type};
    for (const char * key : structural_keys)
    {
        const bool first_has = first.has(key);
        const bool second_has = second.has(key);
        if (first_has != second_has)
            return false;
        if (!first_has)
            continue;

        if (key == f_type)
        {
            if (!typesAreStructurallyIdentical(first.get(key), second.get(key), type_mapping))
                return false;
            continue;
        }

        Poco::JSON::Object wrapper_first;
        wrapper_first.set(key, first.get(key));
        Poco::JSON::Object wrapper_second;
        wrapper_second.set(key, second.get(key));
        if (!equals(wrapper_first, wrapper_second))
            return false;
    }
    return true;
}

bool schemasAreIdentical(const Poco::JSON::Object & first, const Poco::JSON::Object & second, const std::unordered_map<String, String> & type_mapping)
{
    if (!first.isArray(f_fields) || !second.isArray(f_fields))
        return false;
    const auto first_fields = first.getArray(f_fields);
    const auto second_fields = second.getArray(f_fields);
    if (first_fields->size() != second_fields->size())
        return false;
    for (UInt32 i = 0; i != first_fields->size(); ++i)
    {
        const auto first_field = first_fields->getObject(i);
        const auto second_field = second_fields->getObject(i);
        if (!first_field || !second_field || !schemaFieldsAreStructurallyIdentical(*first_field, *second_field, type_mapping))
            return false;
    }
    return true;
}

std::pair<size_t, size_t> parseDecimal(const String & type_name)
{
    DB::ReadBufferFromString buf(std::string_view(type_name.begin() + 8, type_name.end() - 1));
    size_t precision = 0;
    size_t scale = 0;
    readIntText(precision, buf);
    skipWhitespaceIfAny(buf);
    assertChar(',', buf);
    skipWhitespaceIfAny(buf);
    /// readIntText (not tryReadIntText) so a missing scale ("decimal(20,)") is rejected instead of
    /// silently read as 0. assertEOF then rejects trailing garbage (e.g. "decimal(20,0 0)", whose
    /// inner whitespace survives canonicalization), mirroring the fixed[N] handling.
    readIntText(scale, buf);
    assertEOF(buf);
    return {precision, scale};
}

}

namespace Iceberg
{

std::string IcebergSchemaProcessor::default_link{};

void IcebergSchemaProcessor::addIcebergTableSchema(Poco::JSON::Object::Ptr schema_ptr)
{
    std::lock_guard lock(mutex);

    Int32 schema_id = schema_ptr->getValue<Int32>(f_schema_id);

    /// Databricks UniForm writes a degenerate placeholder schema (e.g. {"schema-id":0,"fields":[]})
    /// into manifest files, while the real schema with the same schema-id lives in metadata.json.
    if (!schema_ptr->isArray(f_fields) || schema_ptr->getArray(f_fields)->size() == 0)
        return;

    current_schema_id = schema_id;
    if (iceberg_table_schemas_by_ids.contains(schema_id))
    {
        chassert(clickhouse_table_schemas_by_ids.contains(schema_id));
        std::unordered_map<String, String> type_mapping;
        if (allow_geo_parser)
        {
            type_mapping[f_geography] = f_binary;
            type_mapping[f_geometry] = f_binary;
        }
        /// A schema-id is immutable per the Iceberg spec: re-binding it to different fields is malformed metadata.
        if (!schemasAreIdentical(*iceberg_table_schemas_by_ids.at(schema_id), *schema_ptr, type_mapping))
            throw Exception(
                ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                "Iceberg schema with schema-id {} is bound to two different schemas across metadata versions",
                schema_id);
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
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Field with source id {} in schema version {} is unknown", source_id, schema_version);
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

DataTypePtr IcebergSchemaProcessor::getSimpleType(const String & type_name_arg, bool allow_geo_parser)
{
    /// Parameterized primitive type strings (decimal(P, S), fixed[N], geography(...)) can be
    /// serialized with different inner whitespace across metadata files. Canonicalize by removing
    /// ASCII whitespace so parsing accepts every spelling the whitespace-insensitive comparison does.
    const String type_name = canonicalizeTypeSpacing(type_name_arg);

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
        return std::make_shared<DataTypeDate32>();
    if (type_name == f_time)
        return std::make_shared<DataTypeInt64>();
    if (type_name == f_timestamp)
        return std::make_shared<DataTypeDateTime64>(6);
    if (type_name == f_timestamptz)
        return std::make_shared<DataTypeDateTime64>(6, "UTC");
    if (type_name == f_timestamp_ns)
        return std::make_shared<DataTypeDateTime64>(9);
    if (type_name == f_timestamptz_ns)
        return std::make_shared<DataTypeDateTime64>(9, "UTC");
    if (type_name == f_string || type_name == f_binary)
        return std::make_shared<DataTypeString>();

    if (type_name.starts_with(f_geometry) || type_name.starts_with(f_geography))
    {
        if (allow_geo_parser)
        {
            return DataTypeFactory::instance().get("Geometry");
        }
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Using geometry/geography types is not allowed without enabled allow_experimental_geo_types_in_iceberg flag");
    }
    if (type_name == f_uuid)
        return std::make_shared<DataTypeUUID>();

    if (type_name.starts_with("fixed[") && type_name.ends_with(']'))
    {
        ReadBufferFromString buf(std::string_view(type_name.begin() + 6, type_name.end() - 1));
        size_t n = 0;
        readIntText(n, buf);
        /// Reject trailing garbage such as embedded whitespace ("fixed[1 6]"): the canonicalized
        /// form of a valid spelling has no characters left after the size.
        assertEOF(buf);
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
                TSA_SUPPRESS_WARNING_FOR_WRITE(clickhouse_types_by_source_ids)
                [{schema_id, field->getValue<Int32>(f_id)}] = NameAndTypePair{current_full_name, element_types.back()};

                TSA_SUPPRESS_WARNING_FOR_WRITE(clickhouse_ids_by_source_names)
                [{schema_id, current_full_name}] = field->getValue<Int32>(f_id);
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
        auto data_type = getSimpleType(type_name, allow_geo_parser);
        return required || !data_type->canBeInsideNullable() ? data_type : makeNullable(data_type);
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
bool IcebergSchemaProcessor::allowPrimitiveTypeConversion(const String & old_type_arg, const String & new_type_arg)
{
    /// Match the whitespace-insensitive rules of the comparison and the parser: a whitespace-only
    /// difference in a parameterized type string denotes the identical type.
    const String old_type = canonicalizeTypeSpacing(old_type_arg);
    const String new_type = canonicalizeTypeSpacing(new_type_arg);

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
            if (field->isObject(f_type)
                && (field->getObject(f_type)->getValue<std::string>(f_type) == "struct"
                    || field->getObject(f_type)->getValue<std::string>(f_type) == "list"
                    || field->getObject(f_type)->getValue<std::string>(f_type) == "map"))
            {
                auto old_type = getFieldType(old_json, "type", required);
                auto transform = std::make_shared<EvolutionFunctionStruct>(DataTypes{type}, DataTypes{old_type}, old_json, field);
                old_node = &dag->addFunction(transform, std::vector<const Node *>{old_node}, name);

                outputs.push_back(old_node);
            }
            else
            {
                if (old_json->isObject(f_type) && !field->isObject(f_type))
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
                /// Parameterized primitive types (decimal, geography, ...) can be serialized with
                /// different spacing across metadata files, so compare ignoring ASCII whitespace:
                /// a whitespace-only difference is the same type and needs only a rename, not a cast.
                if (canonicalizeTypeSpacing(old_type) == canonicalizeTypeSpacing(new_type))
                {
                    if (old_json->getValue<String>(f_name) != name)
                    {
                        node = &dag->addAlias(*old_node, name);
                    }
                }
                else if (allowPrimitiveTypeConversion(old_type, new_type))
                {
                    node = &dag->addCast(*old_node, getFieldType(field, f_type, required), name, nullptr);
                }
                outputs.push_back(node);
            }
        }
        else
        {
            if (!type->isNullable() && !field->isObject(f_type))
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
            auto default_type_column = type->createColumnConstWithDefaultValue(0);
            const auto & constant = dag->addColumn(std::move(default_type_column), type, name);
            /// Materialize so the column leaves the transform as a full column, not a ColumnConst,
            /// like every other missing-column path (inplaceBlockConversions, IMergeTreeReader, ...).
            outputs.push_back(&dag->materializeNode(constant));
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

Poco::JSON::Object::Ptr IcebergSchemaProcessor::getIcebergTableSchemaById(Int32 id) const
{
    SharedLockGuard lock(mutex);

    auto it = iceberg_table_schemas_by_ids.find(id);
    if (it == iceberg_table_schemas_by_ids.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Schema with id {} is unknown", id);
    return it->second;
}

void IcebergSchemaProcessor::registerSnapshotWithSchemaId(Int64 snapshot_id, Int32 schema_id)
{
    std::lock_guard lock(mutex);
    auto it = schema_id_by_snapshot.find(snapshot_id);
    if (it != schema_id_by_snapshot.end())
    {
        Int32 old_id = it->second;
        if (old_id != schema_id)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Snapshot with id {} already registered with schema id {}, trying to register with new schema id {}",
                snapshot_id,
                old_id,
                schema_id);
        }
    }
    else
    {
        schema_id_by_snapshot[snapshot_id] = schema_id;
    }
}

Int32 IcebergSchemaProcessor::getSchemaIdForSnapshot(Int64 snapshot_id) const
{
    SharedLockGuard lock(mutex);
    return schema_id_by_snapshot.at(snapshot_id);
}


std::optional<Int32> IcebergSchemaProcessor::tryGetSchemaIdForSnapshot(Int64 snapshot_id) const
{
    SharedLockGuard lock(mutex);
    auto it = schema_id_by_snapshot.find(snapshot_id);
    if (it != schema_id_by_snapshot.end())
        return it->second;
    return std::nullopt;
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

std::unordered_map<String, Int64> IcebergSchemaProcessor::traverseSchema(Poco::JSON::Array::Ptr schema)
{
    std::unordered_map<String, Int64> result;
    for (UInt32 i = 0; i < schema->size(); ++i)
    {
        auto current_object = schema->getObject(i);
        auto field_id = current_object->getValue<Int32>(Iceberg::f_id);
        auto cur_name = current_object->getValue<String>(Iceberg::f_name);
        if (current_object->isObject(Iceberg::f_type))
            traverseComplexType(current_object->getObject(Iceberg::f_type), result, cur_name);
        result[cur_name] = field_id;
    }
    return result;
}

std::unordered_set<String> IcebergSchemaProcessor::collectIcebergStringPaths(Poco::JSON::Array::Ptr schema)
{
    std::unordered_set<String> result;
    for (UInt32 i = 0; i < schema->size(); ++i)
    {
        auto current_object = schema->getObject(i);
        if (current_object->has(Iceberg::f_type))
            collectStringPaths(current_object->get(Iceberg::f_type), result, current_object->getValue<String>(Iceberg::f_name));
    }
    return result;
}

ColumnMapperPtr IcebergSchemaProcessor::getColumnMapperById(Int32 id) const
{
    auto schema = getIcebergTableSchemaById(id);
    if (!schema)
        return nullptr;
    return createColumnMapper(schema);
}

ColumnMapperPtr createColumnMapper(Poco::JSON::Object::Ptr schema_object)
{
    auto column_mapper = std::make_shared<ColumnMapper>();
    auto fields = schema_object->getArray(Iceberg::f_fields);
    column_mapper->setStorageColumnEncoding(IcebergSchemaProcessor::traverseSchema(fields));
    column_mapper->setIcebergStringPaths(IcebergSchemaProcessor::collectIcebergStringPaths(fields));
    return column_mapper;
}

}
}
