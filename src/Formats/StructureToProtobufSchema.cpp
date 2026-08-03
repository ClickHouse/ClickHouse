#include <Formats/StructureToProtobufSchema.h>
#include <Formats/StructureToFormatSchemaUtils.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeEnum.h>
#include <Common/StringUtils.h>

namespace DB
{

using namespace StructureToFormatSchemaUtils;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

const std::unordered_map<TypeIndex, String> protobuf_simple_type_names =
{
    {TypeIndex::Int8, "int32"},
    {TypeIndex::UInt8, "uint32"},
    {TypeIndex::Int16, "int32"},
    {TypeIndex::UInt16, "uint32"},
    {TypeIndex::Int32, "int32"},
    {TypeIndex::UInt32, "uint32"},
    {TypeIndex::Int64, "int64"},
    {TypeIndex::UInt64, "uint64"},
    {TypeIndex::Int128, "bytes"},
    {TypeIndex::UInt128, "bytes"},
    {TypeIndex::Int256, "bytes"},
    {TypeIndex::UInt256, "bytes"},
    {TypeIndex::Float32, "float"},
    {TypeIndex::Float64, "double"},
    {TypeIndex::Decimal32, "bytes"},
    {TypeIndex::Decimal64, "bytes"},
    {TypeIndex::Decimal128, "bytes"},
    {TypeIndex::Decimal256, "bytes"},
    {TypeIndex::String, "bytes"},
    {TypeIndex::FixedString, "bytes"},
    {TypeIndex::UUID, "bytes"},
    {TypeIndex::Date, "uint32"},
    {TypeIndex::Date32, "int32"},
    {TypeIndex::DateTime, "uint32"},
    {TypeIndex::DateTime64, "uint64"},
    {TypeIndex::IPv4, "uint32"},
    {TypeIndex::IPv6, "bytes"},
};

void writeProtobufHeader(WriteBuffer & buf)
{
    writeCString("syntax = \"proto3\";\n\n", buf);
}

void startEnum(WriteBuffer & buf, const String & enum_name, size_t indent)
{
    startNested(buf, enum_name, "enum", indent);
}

void startMessage(WriteBuffer & buf, const String & message_name, size_t indent)
{
    startNested(buf, message_name, "message", indent);
}

void writeFieldDefinition(WriteBuffer & buf, const String & type_name, const String & column_name, size_t & field_index, size_t indent)
{
    writeIndent(buf, indent);
    writeString(fmt::format("{} {} = {};\n", type_name, getSchemaFieldName(column_name), field_index++), buf);
}

String prepareAndGetProtobufTypeName(WriteBuffer & buf, const DataTypePtr & data_type, const String & column_name, size_t indent);

void writeProtobufField(WriteBuffer & buf, const DataTypePtr & data_type, const String & column_name, size_t & field_index, size_t indent)
{
    auto field_type_name = prepareAndGetProtobufTypeName(buf, data_type, column_name, indent);
    writeFieldDefinition(buf, field_type_name, column_name, field_index, indent);
}

String prepareArrayAndGetProtobufTypeName(WriteBuffer & buf, const DataTypePtr & data_type, const String & column_name, size_t indent)
{
    const auto & nested_type = assert_cast<const DataTypeArray &>(*data_type).getNestedType();
    /// Simple case when we can just use 'repeated <nested_type>'.
    if (!isArray(nested_type) && !isMap(nested_type))
    {
        auto nested_type_name = prepareAndGetProtobufTypeName(buf, nested_type, column_name, indent);
        return "repeated " + nested_type_name;
    }

    /// Protobuf doesn't support multidimensional repeated fields and repeated maps.
    /// When we have Array(Array(...)) or Array(Map(...)) we should place nested type into a nested Message with one field.
    String message_name = getSchemaMessageName(column_name);
    startMessage(buf, message_name, indent);
    size_t nested_field_index = 1;
    writeProtobufField(buf, nested_type, column_name, nested_field_index, indent + 1);
    endNested(buf, indent);
    return "repeated " + message_name;
}

String prepareTupleAndGetProtobufTypeName(WriteBuffer & buf, const DataTypePtr & data_type, const String & column_name, size_t indent)
{
    const auto & tuple_type = assert_cast<const DataTypeTuple &>(*data_type);
    auto nested_names_and_types = getCollectedTupleElements(tuple_type, false, "Protobuf");

    String message_name = getSchemaMessageName(column_name);
    startMessage(buf, message_name, indent);
    size_t nested_field_index = 1;
    for (const auto & [name, type] : nested_names_and_types)
        writeProtobufField(buf, type, name, nested_field_index, indent + 1);
    endNested(buf, indent);
    return message_name;
}

String prepareMapAndGetProtobufTypeName(WriteBuffer & buf, const DataTypePtr & data_type, const String & column_name, size_t indent)
{
    const auto & map_type = assert_cast<const DataTypeMap &>(*data_type);
    const auto & key_type = map_type.getKeyType();
    const auto & value_type = map_type.getValueType();
    auto it = protobuf_simple_type_names.find(key_type->getTypeId());
    if (it == protobuf_simple_type_names.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Type {} is not supported for conversion into Map key in Protobuf schema", data_type->getName());
    auto key_type_name = it->second;
    /// Protobuf map type doesn't support "bytes" type as a key. Change it to "string"
    if (key_type_name == "bytes")
        key_type_name = "string";

    /// Special cases when value type is Array or Map, because Protobuf
    /// doesn't support syntax "map<Key, repeated Value>" and "map<Key, map<..., ...>>"
    /// In this case we should place it into a nested Message with one field.
    String value_type_name;
    if (isArray(value_type) || isMap(value_type))
    {
        value_type_name = getSchemaMessageName(column_name) + "Value";
        startMessage(buf, value_type_name, indent);
        size_t nested_field_index = 1;
        writeProtobufField(buf, value_type, column_name + "Value", nested_field_index, indent + 1);
        endNested(buf, indent);
    }
    else
    {
        value_type_name = prepareAndGetProtobufTypeName(buf, value_type, column_name + "Value", indent);
    }

    return fmt::format("map<{}, {}>", key_type_name, value_type_name);
}

template <typename EnumType>
String prepareEnumAndGetProtobufTypeName(WriteBuffer & buf, const DataTypePtr & data_type, const String & column_name, size_t indent)
{
    const auto & enum_type = assert_cast<const DataTypeEnum<EnumType> &>(*data_type);
    String enum_name = getSchemaMessageName(column_name);
    startEnum(buf, enum_name, indent);
    const auto & names = enum_type.getAllRegisteredNames();
    for (size_t i = 0; i != names.size(); ++i)
    {
        writeIndent(buf, indent + 1);
        writeString(fmt::format("{} = {};\n", names[i], std::to_string(i)), buf);
    }
    endNested(buf, indent);
    return enum_name;
}

String prepareAndGetProtobufTypeName(WriteBuffer & buf, const DataTypePtr & data_type, const String & column_name, size_t indent)
{
    TypeIndex type_id = data_type->getTypeId();

    switch (data_type->getTypeId())
    {
        case TypeIndex::Nullable:
            return prepareAndGetProtobufTypeName(buf, assert_cast<const DataTypeNullable &>(*data_type).getNestedType(), column_name, indent);
        case TypeIndex::LowCardinality:
            return prepareAndGetProtobufTypeName(buf, assert_cast<const DataTypeLowCardinality &>(*data_type).getDictionaryType(), column_name, indent);
        case TypeIndex::Array:
            return prepareArrayAndGetProtobufTypeName(buf, data_type, column_name, indent);
        case TypeIndex::Tuple:
            return prepareTupleAndGetProtobufTypeName(buf, data_type, column_name, indent);
        case TypeIndex::Map:
            return prepareMapAndGetProtobufTypeName(buf, data_type, column_name, indent);
        case TypeIndex::Enum8:
            return prepareEnumAndGetProtobufTypeName<Int8>(buf, data_type, column_name, indent);
        case TypeIndex::Enum16:
            return prepareEnumAndGetProtobufTypeName<Int16>(buf, data_type, column_name, indent);
        default:
        {
            if (isBool(data_type))
                return "bool";

            auto it = protobuf_simple_type_names.find(type_id);
            if (it == protobuf_simple_type_names.end())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Type {} is not supported for conversion into Protobuf schema", data_type->getName());
            return it->second;
        }
    }
}

}

void StructureToProtobufSchema::writeSchema(WriteBuffer & buf, const String & message_name, const NamesAndTypesList & names_and_types_)
{
    auto names_and_types = collectNested(names_and_types_, false, "Protobuf");
    writeProtobufHeader(buf);
    startMessage(buf, getSchemaMessageName(message_name), 0);
    size_t field_index = 1;
    for (const auto & [column_name, data_type] : names_and_types)
        writeProtobufField(buf, data_type, column_name, field_index, 1);
    endNested(buf, 0);
}

}
