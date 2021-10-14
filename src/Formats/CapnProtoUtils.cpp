#include <Formats/CapnProtoUtils.h>

#if USE_CAPNP

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/IDataType.h>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>
#include <capnp/schema.h>
#include <capnp/schema-parser.h>
#include <fcntl.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_CAPN_PROTO_SCHEMA;
    extern const int THERE_IS_NO_COLUMN;
    extern const int BAD_TYPE_OF_FIELD;
    extern const int CAPN_PROTO_BAD_CAST;
    extern const int FILE_DOESNT_EXIST;
    extern const int UNKNOWN_EXCEPTION;
    extern const int INCORRECT_DATA;
}

capnp::StructSchema CapnProtoSchemaParser::getMessageSchema(const FormatSchemaInfo & schema_info)
{
    capnp::ParsedSchema schema;
    try
    {
        int fd;
        KJ_SYSCALL(fd = open(schema_info.schemaDirectory().data(), O_RDONLY));
        auto schema_dir = kj::newDiskDirectory(kj::OsFileHandle(fd));
        schema = impl.parseFromDirectory(*schema_dir, kj::Path::parse(schema_info.schemaPath()), {});
    }
    catch (const kj::Exception & e)
    {
        /// That's not good to determine the type of error by its description, but
        /// this is the only way to do it here, because kj doesn't specify the type of error.
        String description = String(e.getDescription().cStr());
        if (description.starts_with("No such file or directory"))
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Cannot open CapnProto schema, file {} doesn't exists", schema_info.absoluteSchemaPath());

        if (description.starts_with("Parse error"))
            throw Exception(ErrorCodes::CANNOT_PARSE_CAPN_PROTO_SCHEMA, "Cannot parse CapnProto schema {}:{}", schema_info.schemaPath(), e.getLine());

        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Unknown exception while parsing CapnProro schema: {}, schema dir and file: {}, {}", description, schema_info.schemaDirectory(), schema_info.schemaPath());
    }

    auto message_maybe = schema.findNested(schema_info.messageName());
    auto * message_schema = kj::_::readMaybe(message_maybe);
    if (!message_schema)
        throw Exception(ErrorCodes::CANNOT_PARSE_CAPN_PROTO_SCHEMA, "CapnProto schema doesn't contain message with name {}", schema_info.messageName());
    return message_schema->asStruct();
}

bool compareEnumNames(const String & first, const String & second, FormatSettings::EnumComparingMode mode)
{
    if (mode == FormatSettings::EnumComparingMode::BY_NAMES_CASE_INSENSITIVE)
        return boost::algorithm::to_lower_copy(first) == boost::algorithm::to_lower_copy(second);
    return first == second;
}

static const std::map<capnp::schema::Type::Which, String> capnp_simple_type_names =
{
        {capnp::schema::Type::Which::BOOL, "Bool"},
        {capnp::schema::Type::Which::VOID, "Void"},
        {capnp::schema::Type::Which::INT8, "Int8"},
        {capnp::schema::Type::Which::INT16, "Int16"},
        {capnp::schema::Type::Which::INT32, "Int32"},
        {capnp::schema::Type::Which::INT64, "Int64"},
        {capnp::schema::Type::Which::UINT8, "UInt8"},
        {capnp::schema::Type::Which::UINT16, "UInt16"},
        {capnp::schema::Type::Which::UINT32, "UInt32"},
        {capnp::schema::Type::Which::UINT64, "UInt64"},
        {capnp::schema::Type::Which::FLOAT32, "Float32"},
        {capnp::schema::Type::Which::FLOAT64, "Float64"},
        {capnp::schema::Type::Which::TEXT, "Text"},
        {capnp::schema::Type::Which::DATA, "Data"},
        {capnp::schema::Type::Which::ENUM, "Enum"},
        {capnp::schema::Type::Which::INTERFACE, "Interface"},
        {capnp::schema::Type::Which::ANY_POINTER, "AnyPointer"},
};

static bool checkIfStructContainsUnnamedUnion(const capnp::StructSchema & struct_schema)
{
    return struct_schema.getFields().size() != struct_schema.getNonUnionFields().size();
}

static bool checkIfStructIsNamedUnion(const capnp::StructSchema & struct_schema)
{
    return struct_schema.getFields().size() == struct_schema.getUnionFields().size();
}

/// Get full name of type for better exception messages.
static String getCapnProtoFullTypeName(const capnp::Type & type)
{
    if (type.isStruct())
    {
        auto struct_schema = type.asStruct();

        auto non_union_fields = struct_schema.getNonUnionFields();
        std::vector<String> non_union_field_names;
        for (auto nested_field : non_union_fields)
            non_union_field_names.push_back(String(nested_field.getProto().getName()) + " " + getCapnProtoFullTypeName(nested_field.getType()));

        auto union_fields = struct_schema.getUnionFields();
        std::vector<String> union_field_names;
        for (auto nested_field : union_fields)
            union_field_names.push_back(String(nested_field.getProto().getName()) + " " + getCapnProtoFullTypeName(nested_field.getType()));

        String union_name = "Union(" + boost::algorithm::join(union_field_names, ", ") + ")";
        /// Check if the struct is a named union.
        if (non_union_field_names.empty())
            return union_name;

        String type_name = "Struct(" + boost::algorithm::join(non_union_field_names, ", ");
        /// Check if the struct contains unnamed union.
        if (!union_field_names.empty())
            type_name += "," + union_name;
        type_name += ")";
        return type_name;
    }

    if (type.isList())
        return "List(" + getCapnProtoFullTypeName(type.asList().getElementType()) + ")";

    if (!capnp_simple_type_names.contains(type.which()))
        throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "Unknown CapnProto type");

    return capnp_simple_type_names.at(type.which());
}

template <typename Type>
static bool checkEnums(const capnp::Type & capnp_type, const DataTypePtr column_type, FormatSettings::EnumComparingMode mode, UInt64 max_value, String & error_message)
{
    if (!capnp_type.isEnum())
        return false;

    auto enum_schema = capnp_type.asEnum();
    bool to_lower = mode == FormatSettings::EnumComparingMode::BY_NAMES_CASE_INSENSITIVE;
    const auto * enum_type = assert_cast<const DataTypeEnum<Type> *>(column_type.get());
    const auto & enum_values = dynamic_cast<const EnumValues<Type> &>(*enum_type);

    auto names = enum_values.getSetOfAllNames(to_lower);
    auto values = enum_values.getSetOfAllValues();

    std::unordered_set<String> capn_enum_names;
    std::unordered_set<Type> capn_enum_values;

    auto enumerants = enum_schema.getEnumerants();
    /// In CapnProto Enum fields are numbered sequentially starting from zero.
    if (mode == FormatSettings::EnumComparingMode::BY_VALUES && enumerants.size() > max_value)
    {
        error_message += "Enum from CapnProto schema contains values that is out of range for Clickhouse Enum";
        return false;
    }

    for (auto enumerant : enumerants)
    {
        String name = enumerant.getProto().getName();
        capn_enum_names.insert(to_lower ? boost::algorithm::to_lower_copy(name) : name);
        auto value = enumerant.getOrdinal();
        capn_enum_values.insert(Type(value));
    }

    if (mode == FormatSettings::EnumComparingMode::BY_NAMES || mode == FormatSettings::EnumComparingMode::BY_NAMES_CASE_INSENSITIVE)
    {
        auto result = names == capn_enum_names;
        if (!result)
            error_message += "The set of names in Enum from CapnProto schema is different from the set of names in ClickHouse Enum";
        return result;
    }

    auto result = values == capn_enum_values;
    if (!result)
        error_message += "The set of values in Enum from CapnProto schema is different from the set of values in ClickHouse Enum";
    return result;
}

static bool checkCapnProtoType(const capnp::Type & capnp_type, const DataTypePtr & data_type, FormatSettings::EnumComparingMode mode, String & error_message);

static bool checkNullableType(const capnp::Type & capnp_type, const DataTypePtr & data_type, FormatSettings::EnumComparingMode mode, String & error_message)
{
    if (!capnp_type.isStruct())
        return false;

    /// Check that struct is a named union of type VOID and one arbitrary type.
    auto struct_schema = capnp_type.asStruct();
    if (!checkIfStructIsNamedUnion(struct_schema))
        return false;

    auto union_fields = struct_schema.getUnionFields();
    if (union_fields.size() != 2)
        return false;

    auto first = union_fields[0];
    auto second = union_fields[1];

    auto nested_type = assert_cast<const DataTypeNullable *>(data_type.get())->getNestedType();
    if (first.getType().isVoid())
        return checkCapnProtoType(second.getType(), nested_type, mode, error_message);
    if (second.getType().isVoid())
        return checkCapnProtoType(first.getType(), nested_type, mode, error_message);
    return false;
}

static bool checkTupleType(const capnp::Type & capnp_type, const DataTypePtr & data_type, FormatSettings::EnumComparingMode mode, String & error_message)
{
    if (!capnp_type.isStruct())
        return false;
    auto struct_schema = capnp_type.asStruct();

    if (checkIfStructIsNamedUnion(struct_schema))
        return false;

    if (checkIfStructContainsUnnamedUnion(struct_schema))
    {
        error_message += "CapnProto struct contains unnamed union";
        return false;
    }

    const auto * tuple_data_type = assert_cast<const DataTypeTuple *>(data_type.get());
    auto nested_types = tuple_data_type->getElements();
    if (nested_types.size() != struct_schema.getFields().size())
    {
        error_message += "Tuple and Struct types have different sizes";
        return false;
    }

    if (!tuple_data_type->haveExplicitNames())
    {
        error_message += "Only named Tuple can be converted to CapnProto Struct";
        return false;
    }
    for (const auto & name : tuple_data_type->getElementNames())
    {
        KJ_IF_MAYBE(field, struct_schema.findFieldByName(name))
        {
            if (!checkCapnProtoType(field->getType(), nested_types[tuple_data_type->getPositionByName(name)], mode, error_message))
                return false;
        }
        else
        {
            error_message += "CapnProto struct doesn't contain a field with name " + name;
            return false;
        }
    }

    return true;
}

static bool checkArrayType(const capnp::Type & capnp_type, const DataTypePtr & data_type, FormatSettings::EnumComparingMode mode, String & error_message)
{
    if (!capnp_type.isList())
        return false;
    auto list_schema = capnp_type.asList();
    auto nested_type = assert_cast<const DataTypeArray *>(data_type.get())->getNestedType();
    return checkCapnProtoType(list_schema.getElementType(), nested_type, mode, error_message);
}

static bool checkCapnProtoType(const capnp::Type & capnp_type, const DataTypePtr & data_type, FormatSettings::EnumComparingMode mode, String & error_message)
{
    switch (data_type->getTypeId())
    {
        case TypeIndex::UInt8:
            return capnp_type.isBool() || capnp_type.isUInt8();
        case TypeIndex::Date: [[fallthrough]];
        case TypeIndex::UInt16:
            return capnp_type.isUInt16();
        case TypeIndex::DateTime: [[fallthrough]];
        case TypeIndex::UInt32:
            return capnp_type.isUInt32();
        case TypeIndex::UInt64:
            return capnp_type.isUInt64();
        case TypeIndex::Int8:
            return capnp_type.isInt8();
        case TypeIndex::Int16:
            return capnp_type.isInt16();
        case TypeIndex::Date32: [[fallthrough]];
        case TypeIndex::Int32:
            return capnp_type.isInt32();
        case TypeIndex::DateTime64: [[fallthrough]];
        case TypeIndex::Int64:
            return capnp_type.isInt64();
        case TypeIndex::Float32:
            return capnp_type.isFloat32();
        case TypeIndex::Float64:
            return capnp_type.isFloat64();
        case TypeIndex::Enum8:
            return checkEnums<Int8>(capnp_type, data_type, mode, INT8_MAX, error_message);
        case TypeIndex::Enum16:
            return checkEnums<Int16>(capnp_type, data_type, mode, INT16_MAX, error_message);
        case TypeIndex::Tuple:
            return checkTupleType(capnp_type, data_type, mode, error_message);
        case TypeIndex::Nullable:
        {
            auto result = checkNullableType(capnp_type, data_type, mode, error_message);
            if (!result)
                error_message += "Nullable can be represented only as a named union of type Void and nested type";
            return result;
        }
        case TypeIndex::Array:
            return checkArrayType(capnp_type, data_type, mode, error_message);
        case TypeIndex::LowCardinality:
            return checkCapnProtoType(capnp_type, assert_cast<const DataTypeLowCardinality *>(data_type.get())->getDictionaryType(), mode, error_message);
        case TypeIndex::FixedString: [[fallthrough]];
        case TypeIndex::String:
            return capnp_type.isText() || capnp_type.isData();
        default:
            return false;
    }
}

static std::pair<String, String> splitFieldName(const String & name)
{
    const auto * begin = name.data();
    const auto * end = name.data() + name.size();
    const auto * it = find_first_symbols<'_', '.'>(begin, end);
    String first = String(begin, it);
    String second = it == end ? "" : String(it + 1, end);
    return {first, second};
}

capnp::DynamicValue::Reader getReaderByColumnName(const capnp::DynamicStruct::Reader & struct_reader, const String & name)
{
    auto [field_name, nested_name] = splitFieldName(name);
    KJ_IF_MAYBE(field, struct_reader.getSchema().findFieldByName(field_name))
    {
        capnp::DynamicValue::Reader field_reader;
        try
        {
            field_reader = struct_reader.get(*field);
        }
        catch (const kj::Exception & e)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot extract field value from struct by provided schema, error: {} Perhaps the data was generated by another schema", String(e.getDescription().cStr()));
        }

        if (nested_name.empty())
            return field_reader;

        if (field_reader.getType() != capnp::DynamicValue::STRUCT)
            throw Exception(ErrorCodes::CAPN_PROTO_BAD_CAST, "Field {} is not a struct", field_name);

        return getReaderByColumnName(field_reader.as<capnp::DynamicStruct>(), nested_name);
    }

    throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "Capnproto struct doesn't contain field with name {}", field_name);
}

std::pair<capnp::DynamicStruct::Builder, capnp::StructSchema::Field> getStructBuilderAndFieldByColumnName(capnp::DynamicStruct::Builder struct_builder, const String & name)
{
    auto [field_name, nested_name] = splitFieldName(name);
    KJ_IF_MAYBE(field, struct_builder.getSchema().findFieldByName(field_name))
    {
        if (nested_name.empty())
            return {struct_builder, *field};

        auto field_builder = struct_builder.get(*field);
        if (field_builder.getType() != capnp::DynamicValue::STRUCT)
            throw Exception(ErrorCodes::CAPN_PROTO_BAD_CAST, "Field {} is not a struct", field_name);

        return getStructBuilderAndFieldByColumnName(field_builder.as<capnp::DynamicStruct>(), nested_name);
    }

    throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "Capnproto struct doesn't contain field with name {}", field_name);
}

static capnp::StructSchema::Field getFieldByName(const capnp::StructSchema & schema, const String & name)
{
    auto [field_name, nested_name] = splitFieldName(name);
    KJ_IF_MAYBE(field, schema.findFieldByName(field_name))
    {
        if (nested_name.empty())
            return *field;

        if (!field->getType().isStruct())
            throw Exception(ErrorCodes::CAPN_PROTO_BAD_CAST, "Field {} is not a struct", field_name);

        return getFieldByName(field->getType().asStruct(), nested_name);
    }

    throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "Capnproto schema doesn't contain field with name {}", field_name);
}

void checkCapnProtoSchemaStructure(const capnp::StructSchema & schema, const Block & header, FormatSettings::EnumComparingMode mode)
{
    /// Firstly check that struct doesn't contain unnamed union, because we don't support it.
    if (checkIfStructContainsUnnamedUnion(schema))
        throw Exception(ErrorCodes::CAPN_PROTO_BAD_CAST, "Schema contains unnamed union that is not supported");
    auto names_and_types = header.getNamesAndTypesList();
    String additional_error_message;
    for (auto & [name, type] : names_and_types)
    {
        auto field = getFieldByName(schema, name);
        if (!checkCapnProtoType(field.getType(), type, mode, additional_error_message))
        {
            auto e = Exception(
                ErrorCodes::CAPN_PROTO_BAD_CAST,
                "Cannot convert ClickHouse type {} to CapnProto type {}",
                type->getName(),
                getCapnProtoFullTypeName(field.getType()));
            if (!additional_error_message.empty())
                e.addMessage(additional_error_message);
            throw std::move(e);
        }
    }
}

}

#endif
