#include <Formats/CapnProtoUtils.h>

#if USE_CAPNP

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeMap.h>
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
    extern const int CAPN_PROTO_BAD_TYPE;
    extern const int BAD_ARGUMENTS;
}

std::pair<String, String> splitCapnProtoFieldName(const String & name)
{
    const auto * begin = name.data();
    const auto * end = name.data() + name.size();
    const auto * it = find_first_symbols<'_', '.'>(begin, end);
    String first = String(begin, it);
    String second = it == end ? "" : String(it + 1, end);
    return {first, second};
}

capnp::StructSchema CapnProtoSchemaParser::getMessageSchema(const FormatSchemaInfo & schema_info)
{
    capnp::ParsedSchema schema;
    try
    {
        int fd;
        KJ_SYSCALL(fd = open(schema_info.schemaDirectory().data(), O_RDONLY)); // NOLINT(bugprone-suspicious-semicolon)
        auto schema_dir = kj::newDiskDirectory(kj::OsFileHandle(fd));
        schema = impl.parseFromDirectory(*schema_dir, kj::Path::parse(schema_info.schemaPath()), {});
    }
    catch (const kj::Exception & e)
    {
        /// That's not good to determine the type of error by its description, but
        /// this is the only way to do it here, because kj doesn't specify the type of error.
        auto description = std::string_view(e.getDescription().cStr());
        if (description.find("No such file or directory") != String::npos || description.find("no such directory") != String::npos)
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Cannot open CapnProto schema, file {} doesn't exists", schema_info.absoluteSchemaPath());

        if (description.find("Parse error") != String::npos)
            throw Exception(ErrorCodes::CANNOT_PARSE_CAPN_PROTO_SCHEMA, "Cannot parse CapnProto schema {}:{}", schema_info.schemaPath(), e.getLine());

        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION,
                        "Unknown exception while parsing CapnProto schema: {}, schema dir and file: {}, {}",
                        description, schema_info.schemaDirectory(), schema_info.schemaPath());
    }

    auto message_maybe = schema.findNested(schema_info.messageName());
    auto * message_schema = kj::_::readMaybe(message_maybe);
    if (!message_schema)
        throw Exception(ErrorCodes::CANNOT_PARSE_CAPN_PROTO_SCHEMA,
                        "CapnProto schema doesn't contain message with name {}", schema_info.messageName());
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
    switch (type.which())
    {
        case capnp::schema::Type::Which::STRUCT:
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
                type_name += ", " + union_name;
            type_name += ")";
            return type_name;
        }
        case capnp::schema::Type::Which::LIST:
            return "List(" + getCapnProtoFullTypeName(type.asList().getElementType()) + ")";
        case capnp::schema::Type::Which::ENUM:
        {
            auto enum_schema = type.asEnum();
            String enum_name = "Enum(";
            auto enumerants = enum_schema.getEnumerants();
            for (unsigned i = 0; i != enumerants.size(); ++i)
            {
                enum_name += String(enumerants[i].getProto().getName()) + " = " + std::to_string(enumerants[i].getOrdinal());
                if (i + 1 != enumerants.size())
                    enum_name += ", ";
            }
            enum_name += ")";
            return enum_name;
        }
        default:
            auto it = capnp_simple_type_names.find(type.which());
            if (it == capnp_simple_type_names.end())
                throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "Unknown CapnProto type");
            return it->second;
    }
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

    auto enumerants = enum_schema.getEnumerants();
    if (mode == FormatSettings::EnumComparingMode::BY_VALUES)
    {
        /// In CapnProto Enum fields are numbered sequentially starting from zero.
        if (enumerants.size() > max_value)
        {
            error_message += "Enum from CapnProto schema contains values that is out of range for Clickhouse Enum";
            return false;
        }

        auto values = enum_values.getSetOfAllValues();
        std::unordered_set<Type> capn_enum_values;
        for (auto enumerant : enumerants)
            capn_enum_values.insert(Type(enumerant.getOrdinal()));
        auto result = values == capn_enum_values;
        if (!result)
            error_message += "The set of values in Enum from CapnProto schema is different from the set of values in ClickHouse Enum";
        return result;
    }

    auto names = enum_values.getSetOfAllNames(to_lower);
    std::unordered_set<String> capn_enum_names;

    for (auto enumerant : enumerants)
    {
        String name = enumerant.getProto().getName();
        capn_enum_names.insert(to_lower ? boost::algorithm::to_lower_copy(name) : name);
    }

    auto result = names == capn_enum_names;
    if (!result)
        error_message += "The set of names in Enum from CapnProto schema is different from the set of names in ClickHouse Enum";
    return result;
}

static bool checkCapnProtoType(const capnp::Type & capnp_type, const DataTypePtr & data_type, FormatSettings::EnumComparingMode mode, String & error_message, const String & column_name);

static bool checkNullableType(const capnp::Type & capnp_type, const DataTypePtr & data_type, FormatSettings::EnumComparingMode mode, String & error_message, const String & column_name)
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
        return checkCapnProtoType(second.getType(), nested_type, mode, error_message, column_name);
    if (second.getType().isVoid())
        return checkCapnProtoType(first.getType(), nested_type, mode, error_message, column_name);
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

    bool have_explicit_names = tuple_data_type->haveExplicitNames();
    const auto & nested_names = tuple_data_type->getElementNames();
    for (uint32_t i = 0; i != nested_names.size(); ++i)
    {
        if (have_explicit_names)
        {
            KJ_IF_MAYBE (field, struct_schema.findFieldByName(nested_names[i]))
            {
                if (!checkCapnProtoType(field->getType(), nested_types[tuple_data_type->getPositionByName(nested_names[i])], mode, error_message, nested_names[i]))
                    return false;
            }
            else
            {
                error_message += "CapnProto struct doesn't contain a field with name " + nested_names[i];
                return false;
            }
        }
        else if (!checkCapnProtoType(struct_schema.getFields()[i].getType(), nested_types[tuple_data_type->getPositionByName(nested_names[i])], mode, error_message, nested_names[i]))
            return false;
    }

    return true;
}

static bool checkArrayType(const capnp::Type & capnp_type, const DataTypePtr & data_type, FormatSettings::EnumComparingMode mode, String & error_message, const String & column_name)
{
    if (!capnp_type.isList())
        return false;
    auto list_schema = capnp_type.asList();
    auto nested_type = assert_cast<const DataTypeArray *>(data_type.get())->getNestedType();

    auto [field_name, nested_name] = splitCapnProtoFieldName(column_name);
    if (!nested_name.empty() && list_schema.getElementType().isStruct())
    {
        auto struct_schema = list_schema.getElementType().asStruct();
        KJ_IF_MAYBE(field, struct_schema.findFieldByName(nested_name))
            return checkCapnProtoType(field->getType(), nested_type, mode, error_message, nested_name);

        error_message += "Element type of List {} doesn't contain field with name " + nested_name;
        return false;
    }

    return checkCapnProtoType(list_schema.getElementType(), nested_type, mode, error_message, column_name);
}

static bool checkMapType(const capnp::Type & capnp_type, const DataTypePtr & data_type, FormatSettings::EnumComparingMode mode, String & error_message)
{
    /// We output/input Map type as follow CapnProto schema
    ///
    /// struct Map {
    ///     struct Entry {
    ///         key @0: Key;
    ///         value @1: Value;
    ///     }
    ///     entries @0 :List(Entry);
    /// }

    if (!capnp_type.isStruct())
        return false;
    auto struct_schema = capnp_type.asStruct();

    if (checkIfStructContainsUnnamedUnion(struct_schema))
    {
        error_message += "CapnProto struct contains unnamed union";
        return false;
    }

    if (struct_schema.getFields().size() != 1)
    {
        error_message += "CapnProto struct that represents Map type can contain only one field";
        return false;
    }

    const auto & field_type = struct_schema.getFields()[0].getType();
    if (!field_type.isList())
    {
        error_message += "Field of CapnProto struct that represents Map is not a list";
        return false;
    }

    auto list_element_type = field_type.asList().getElementType();
    if (!list_element_type.isStruct())
    {
        error_message += "Field of CapnProto struct that represents Map is not a list of structs";
        return false;
    }

    auto key_value_struct = list_element_type.asStruct();
    if (checkIfStructContainsUnnamedUnion(key_value_struct))
    {
        error_message += "CapnProto struct contains unnamed union";
        return false;
    }

    if (key_value_struct.getFields().size() != 2)
    {
        error_message += "Key-value structure for Map struct should have exactly 2 fields";
        return false;
    }

    const auto & map_type = assert_cast<const DataTypeMap &>(*data_type);
    DataTypes types = {map_type.getKeyType(), map_type.getValueType()};
    Names names = {"key", "value"};

    for (size_t i = 0; i != types.size(); ++i)
    {
        KJ_IF_MAYBE(field, key_value_struct.findFieldByName(names[i]))
        {
            if (!checkCapnProtoType(field->getType(), types[i], mode, error_message, names[i]))
                return false;
        }
        else
        {
            error_message += R"(Key-value structure for Map struct should have exactly 2 fields with names "key" and "value")";
            return false;
        }
    }

    return true;
}

static bool isCapnInteger(const capnp::Type & capnp_type)
{
    return capnp_type.isInt8() || capnp_type.isUInt8() || capnp_type.isInt16() || capnp_type.isUInt16() || capnp_type.isInt32()
        || capnp_type.isUInt32() || capnp_type.isInt64() || capnp_type.isUInt64();
}

static bool checkCapnProtoType(const capnp::Type & capnp_type, const DataTypePtr & data_type, FormatSettings::EnumComparingMode mode, String & error_message, const String & column_name)
{
    switch (data_type->getTypeId())
    {
        case TypeIndex::UInt8:
            return capnp_type.isBool() || isCapnInteger(capnp_type);
        case TypeIndex::Int8: [[fallthrough]];
        case TypeIndex::Int16: [[fallthrough]];
        case TypeIndex::UInt16: [[fallthrough]];
        case TypeIndex::Int32: [[fallthrough]];
        case TypeIndex::UInt32: [[fallthrough]];
        case TypeIndex::Int64: [[fallthrough]];
        case TypeIndex::UInt64:
            /// Allow integer conversions durin input/output.
            return isCapnInteger(capnp_type);
        case TypeIndex::Date:
            return capnp_type.isUInt16();
        case TypeIndex::DateTime: [[fallthrough]];
        case TypeIndex::IPv4:
            return capnp_type.isUInt32();
        case TypeIndex::Date32: [[fallthrough]];
        case TypeIndex::Decimal32:
            return capnp_type.isInt32() || capnp_type.isUInt32();
        case TypeIndex::DateTime64: [[fallthrough]];
        case TypeIndex::Decimal64:
            return capnp_type.isInt64() || capnp_type.isUInt64();
        case TypeIndex::Float32:[[fallthrough]];
        case TypeIndex::Float64:
            /// Allow converting between Float32 and isFloat64
            return capnp_type.isFloat32() || capnp_type.isFloat64();
        case TypeIndex::Enum8:
            return checkEnums<Int8>(capnp_type, data_type, mode, INT8_MAX, error_message);
        case TypeIndex::Enum16:
            return checkEnums<Int16>(capnp_type, data_type, mode, INT16_MAX, error_message);
        case TypeIndex::Int128: [[fallthrough]];
        case TypeIndex::UInt128: [[fallthrough]];
        case TypeIndex::Int256: [[fallthrough]];
        case TypeIndex::UInt256: [[fallthrough]];
        case TypeIndex::Decimal128: [[fallthrough]];
        case TypeIndex::Decimal256:
            return capnp_type.isData();
        case TypeIndex::Tuple:
            return checkTupleType(capnp_type, data_type, mode, error_message);
        case TypeIndex::Nullable:
        {
            auto result = checkNullableType(capnp_type, data_type, mode, error_message, column_name);
            if (!result)
                error_message += "Nullable can be represented only as a named union of type Void and nested type";
            return result;
        }
        case TypeIndex::Array:
            return checkArrayType(capnp_type, data_type, mode, error_message, column_name);
        case TypeIndex::LowCardinality:
            return checkCapnProtoType(capnp_type, assert_cast<const DataTypeLowCardinality *>(data_type.get())->getDictionaryType(), mode, error_message, column_name);
        case TypeIndex::FixedString: [[fallthrough]];
        case TypeIndex::IPv6: [[fallthrough]];
        case TypeIndex::String:
            return capnp_type.isText() || capnp_type.isData();
        case TypeIndex::Map:
            return checkMapType(capnp_type, data_type, mode, error_message);
        default:
            return false;
    }
}

capnp::DynamicValue::Reader getReaderByColumnName(const capnp::DynamicStruct::Reader & struct_reader, const String & name)
{
    auto [field_name, nested_name] = splitCapnProtoFieldName(name);
    KJ_IF_MAYBE(field, struct_reader.getSchema().findFieldByName(field_name))
    {
        capnp::DynamicValue::Reader field_reader;
        try
        {
            field_reader = struct_reader.get(*field);
        }
        catch (const kj::Exception & e)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA,
                            "Cannot extract field value from struct by provided schema, error: "
                            "{} Perhaps the data was generated by another schema", String(e.getDescription().cStr()));
        }

        if (nested_name.empty())
            return field_reader;

        /// Support reading Nested as List of Structs.
        if (field_reader.getType() == capnp::DynamicValue::LIST)
        {
            auto list_schema = field->getType().asList();
            if (!list_schema.getElementType().isStruct())
                throw Exception(ErrorCodes::CAPN_PROTO_BAD_CAST, "Element type of List {} is not a struct", field_name);

            auto struct_schema = list_schema.getElementType().asStruct();
            KJ_IF_MAYBE(nested_field, struct_schema.findFieldByName(nested_name))
                return field_reader;

            throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "Element type of List {} doesn't contain field with name \"{}\"", field_name, nested_name);
        }

        if (field_reader.getType() != capnp::DynamicValue::STRUCT)
            throw Exception(ErrorCodes::CAPN_PROTO_BAD_CAST, "Field {} is not a struct", field_name);

        return getReaderByColumnName(field_reader.as<capnp::DynamicStruct>(), nested_name);
    }

    throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "Capnproto struct doesn't contain field with name {}", field_name);
}

std::pair<capnp::DynamicStruct::Builder, capnp::StructSchema::Field> getStructBuilderAndFieldByColumnName(capnp::DynamicStruct::Builder struct_builder, const String & name)
{
    auto [field_name, nested_name] = splitCapnProtoFieldName(name);
    KJ_IF_MAYBE(field, struct_builder.getSchema().findFieldByName(field_name))
    {
        if (nested_name.empty())
            return {struct_builder, *field};

        auto field_builder = struct_builder.get(*field);

        /// Support reading Nested as List of Structs.
        if (field_builder.getType() == capnp::DynamicValue::LIST)
        {
            auto list_schema = field->getType().asList();
            if (!list_schema.getElementType().isStruct())
                throw Exception(ErrorCodes::CAPN_PROTO_BAD_CAST, "Element type of List {} is not a struct", field_name);

            auto struct_schema = list_schema.getElementType().asStruct();
            KJ_IF_MAYBE(nested_field, struct_schema.findFieldByName(nested_name))
                return {struct_builder, *field};

            throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "Element type of List {} doesn't contain field with name \"{}\"", field_name, nested_name);
        }

        if (field_builder.getType() != capnp::DynamicValue::STRUCT)
            throw Exception(ErrorCodes::CAPN_PROTO_BAD_CAST, "Field {} is not a struct", field_name);

        return getStructBuilderAndFieldByColumnName(field_builder.as<capnp::DynamicStruct>(), nested_name);
    }

    throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "Capnproto struct doesn't contain field with name {}", field_name);
}

static std::pair<capnp::StructSchema::Field, String> getFieldByName(const capnp::StructSchema & schema, const String & name)
{
    auto [field_name, nested_name] = splitCapnProtoFieldName(name);
    KJ_IF_MAYBE(field, schema.findFieldByName(field_name))
    {
        if (nested_name.empty())
            return {*field, name};

        /// Support reading Nested as List of Structs.
        if (field->getType().isList())
        {
            auto list_schema = field->getType().asList();
            if (!list_schema.getElementType().isStruct())
                throw Exception(ErrorCodes::CAPN_PROTO_BAD_CAST, "Element type of List {} is not a struct", field_name);

            auto struct_schema = list_schema.getElementType().asStruct();
            KJ_IF_MAYBE(nested_field, struct_schema.findFieldByName(nested_name))
                return {*field, name};

            throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "Element type of List {} doesn't contain field with name \"{}\"", field_name, nested_name);
        }

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
        auto [field, field_name] = getFieldByName(schema, name);
        if (!checkCapnProtoType(field.getType(), type, mode, additional_error_message, field_name))
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

template <typename ValueType>
static DataTypePtr getEnumDataTypeFromEnumerants(const capnp::EnumSchema::EnumerantList & enumerants)
{
    std::vector<std::pair<String, ValueType>> values;
    for (auto enumerant : enumerants)
        values.emplace_back(enumerant.getProto().getName(), ValueType(enumerant.getOrdinal()));
    return std::make_shared<DataTypeEnum<ValueType>>(std::move(values));
}

static DataTypePtr getEnumDataTypeFromEnumSchema(const capnp::EnumSchema & enum_schema)
{
    auto enumerants = enum_schema.getEnumerants();
    if (enumerants.size() < 128)
        return getEnumDataTypeFromEnumerants<Int8>(enumerants);
    if (enumerants.size() < 32768)
        return getEnumDataTypeFromEnumerants<Int16>(enumerants);

    throw Exception(ErrorCodes::CAPN_PROTO_BAD_TYPE, "ClickHouse supports only 8 and 16-bit Enums");
}

static DataTypePtr getDataTypeFromCapnProtoType(const capnp::Type & capnp_type, bool skip_unsupported_fields)
{
    switch (capnp_type.which())
    {
        case capnp::schema::Type::INT8:
            return std::make_shared<DataTypeInt8>();
        case capnp::schema::Type::INT16:
            return std::make_shared<DataTypeInt16>();
        case capnp::schema::Type::INT32:
            return std::make_shared<DataTypeInt32>();
        case capnp::schema::Type::INT64:
            return std::make_shared<DataTypeInt64>();
        case capnp::schema::Type::BOOL: [[fallthrough]];
        case capnp::schema::Type::UINT8:
            return std::make_shared<DataTypeUInt8>();
        case capnp::schema::Type::UINT16:
            return std::make_shared<DataTypeUInt16>();
        case capnp::schema::Type::UINT32:
            return std::make_shared<DataTypeUInt32>();
        case capnp::schema::Type::UINT64:
            return std::make_shared<DataTypeUInt64>();
        case capnp::schema::Type::FLOAT32:
            return std::make_shared<DataTypeFloat32>();
        case capnp::schema::Type::FLOAT64:
            return std::make_shared<DataTypeFloat64>();
        case capnp::schema::Type::DATA: [[fallthrough]];
        case capnp::schema::Type::TEXT:
            return std::make_shared<DataTypeString>();
        case capnp::schema::Type::ENUM:
            return getEnumDataTypeFromEnumSchema(capnp_type.asEnum());
        case capnp::schema::Type::LIST:
        {
            auto list_schema = capnp_type.asList();
            auto nested_type = getDataTypeFromCapnProtoType(list_schema.getElementType(), skip_unsupported_fields);
            if (!nested_type)
                return nullptr;
            return std::make_shared<DataTypeArray>(nested_type);
        }
        case capnp::schema::Type::STRUCT:
        {
            auto struct_schema = capnp_type.asStruct();


            if (struct_schema.getFields().size() == 0)
            {
                if (skip_unsupported_fields)
                    return nullptr;
                throw Exception(ErrorCodes::CAPN_PROTO_BAD_TYPE, "Empty messages are not supported");
            }

            /// Check if it can be Nullable.
            if (checkIfStructIsNamedUnion(struct_schema))
            {
                auto fields = struct_schema.getUnionFields();
                if (fields.size() != 2 || (!fields[0].getType().isVoid() && !fields[1].getType().isVoid()))
                {
                    if (skip_unsupported_fields)
                        return nullptr;
                    throw Exception(ErrorCodes::CAPN_PROTO_BAD_TYPE, "Unions are not supported");
                }
                auto value_type = fields[0].getType().isVoid() ? fields[1].getType() : fields[0].getType();
                if (value_type.isStruct() || value_type.isList())
                {
                    if (skip_unsupported_fields)
                        return nullptr;
                    throw Exception(ErrorCodes::CAPN_PROTO_BAD_TYPE, "Tuples and Lists cannot be inside Nullable");
                }

                auto nested_type = getDataTypeFromCapnProtoType(value_type, skip_unsupported_fields);
                if (!nested_type)
                    return nullptr;
                return std::make_shared<DataTypeNullable>(nested_type);
            }

            if (checkIfStructContainsUnnamedUnion(struct_schema))
                throw Exception(ErrorCodes::CAPN_PROTO_BAD_TYPE, "Unnamed union is not supported");

            /// Treat Struct as Tuple.
            DataTypes nested_types;
            Names nested_names;
            for (auto field : struct_schema.getNonUnionFields())
            {
                auto nested_type = getDataTypeFromCapnProtoType(field.getType(), skip_unsupported_fields);
                if (!nested_type)
                    continue;
                nested_names.push_back(field.getProto().getName());
                nested_types.push_back(nested_type);
            }
            if (nested_types.empty())
                return nullptr;
            return std::make_shared<DataTypeTuple>(std::move(nested_types), std::move(nested_names));
        }
        default:
        {
            if (skip_unsupported_fields)
                return nullptr;
            throw Exception(ErrorCodes::CAPN_PROTO_BAD_TYPE, "Unsupported CapnProtoType: {}", getCapnProtoFullTypeName(capnp_type));
        }
    }
}

NamesAndTypesList capnProtoSchemaToCHSchema(const capnp::StructSchema & schema, bool skip_unsupported_fields)
{
    if (checkIfStructContainsUnnamedUnion(schema))
        throw Exception(ErrorCodes::CAPN_PROTO_BAD_TYPE, "Unnamed union is not supported");

    NamesAndTypesList names_and_types;
    for (auto field : schema.getNonUnionFields())
    {
        auto name = field.getProto().getName();
        auto type = getDataTypeFromCapnProtoType(field.getType(), skip_unsupported_fields);
        if (type)
            names_and_types.emplace_back(name, type);
    }
    if (names_and_types.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Cannot convert CapnProto schema to ClickHouse table schema, all fields have unsupported types");

    return names_and_types;
}

}

#endif
