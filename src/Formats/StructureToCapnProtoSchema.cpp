#include <Formats/StructureToCapnProtoSchema.h>
#include <Formats/StructureToFormatSchemaUtils.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeEnum.h>
#include <Common/StringUtils.h>
#include <Common/randomSeed.h>
#include <pcg_random.hpp>


namespace DB
{

using namespace StructureToFormatSchemaUtils;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

const std::unordered_map<TypeIndex, String> capn_proto_simple_type_names =
{
    {TypeIndex::Int8, "Int8"},
    {TypeIndex::UInt8, "UInt8"},
    {TypeIndex::Int16, "Int16"},
    {TypeIndex::UInt16, "UInt16"},
    {TypeIndex::Int32, "Int32"},
    {TypeIndex::UInt32, "UInt32"},
    {TypeIndex::Int64, "Int64"},
    {TypeIndex::UInt64, "UInt64"},
    {TypeIndex::Int128, "Data"},
    {TypeIndex::UInt128, "Data"},
    {TypeIndex::Int256, "Data"},
    {TypeIndex::UInt256, "Data"},
    {TypeIndex::Float32, "Float32"},
    {TypeIndex::Float64, "Float64"},
    {TypeIndex::Decimal32, "Int32"},
    {TypeIndex::Decimal64, "Int64"},
    {TypeIndex::Decimal128, "Data"},
    {TypeIndex::Decimal256, "Data"},
    {TypeIndex::String, "Data"},
    {TypeIndex::FixedString, "Data"},
    {TypeIndex::UUID, "Data"},
    {TypeIndex::Date, "UInt16"},
    {TypeIndex::Date32, "Int32"},
    {TypeIndex::DateTime, "UInt32"},
    {TypeIndex::DateTime64, "Int64"},
    {TypeIndex::IPv4, "UInt32"},
    {TypeIndex::IPv6, "Data"},
};

void writeCapnProtoHeader(WriteBuffer & buf)
{
    pcg64 rng(randomSeed());
    size_t id = rng() | (1ull << 63); /// First bit should be 1
    writeString(fmt::format("@0x{};\n\n", getHexUIntLowercase(id)), buf);
}

void writeFieldDefinition(WriteBuffer & buf, const String & type_name, const String & column_name, size_t & field_index, size_t indent)
{
    writeIndent(buf, indent);
    writeString(fmt::format("{} @{} : {};\n", getSchemaFieldName(column_name), field_index++, type_name), buf);
}

void startEnum(WriteBuffer & buf, const String & enum_name, size_t indent)
{
    startNested(buf, enum_name, "enum", indent);
}

void startUnion(WriteBuffer & buf, size_t indent)
{
    startNested(buf, "", "union", indent);
}

void startStruct(WriteBuffer & buf, const String & struct_name, size_t indent)
{
    startNested(buf, struct_name, "struct", indent);
}

String prepareAndGetCapnProtoTypeName(WriteBuffer & buf, const DataTypePtr & data_type, const String & column_name, size_t indent);

void writeField(WriteBuffer & buf, const DataTypePtr & data_type, const String & column_name, size_t & field_index, size_t indent)
{
    auto field_type_name = prepareAndGetCapnProtoTypeName(buf, data_type, column_name, indent);
    writeFieldDefinition(buf, field_type_name, column_name, field_index, indent);
}

String prepareArrayAndGetCapnProtoTypeName(WriteBuffer & buf, const DataTypePtr & data_type, const String & column_name, size_t indent)
{
    const auto & nested_type = assert_cast<const DataTypeArray &>(*data_type).getNestedType();
    auto nested_type_name = prepareAndGetCapnProtoTypeName(buf, nested_type, column_name, indent);
    return "List(" + nested_type_name + ")";
}

String prepareNullableAndGetCapnProtoTypeName(WriteBuffer & buf, const DataTypePtr & data_type, const String & column_name, size_t indent)
{
    /// Nullable is represented as a struct with union with 2 fields:
    ///
    /// struct Nullable
    /// {
    ///     union
    ///     {
    ///         value @0 : Value;
    ///         null @1 : Void;
    ///     }
    /// }
    auto struct_name = getSchemaMessageName(column_name);
    startStruct(buf, struct_name, indent);
    auto nested_type_name = prepareAndGetCapnProtoTypeName(buf, assert_cast<const DataTypeNullable &>(*data_type).getNestedType(), column_name, indent);
    startUnion(buf, indent + 1);
    size_t field_index = 0;
    writeFieldDefinition(buf, nested_type_name, "value", field_index, indent + 2);
    writeFieldDefinition(buf, "Void", "null", field_index, indent + 2);
    endNested(buf, indent + 1);
    endNested(buf, indent);
    return struct_name;
}

String prepareTupleAndGetCapnProtoTypeName(WriteBuffer & buf, const DataTypePtr & data_type, const String & column_name, size_t indent)
{
    const auto & tuple_type = assert_cast<const DataTypeTuple &>(*data_type);
    auto nested_names_and_types = getCollectedTupleElements(tuple_type, false, "CapnProto");

    String struct_name = getSchemaMessageName(column_name);
    startStruct(buf, struct_name, indent);
    size_t nested_field_index = 0;
    for (const auto & [name, type] : nested_names_and_types)
        writeField(buf, type, name, nested_field_index, indent + 1);
    endNested(buf, indent);
    return struct_name;
}

String prepareMapAndGetCapnProtoTypeName(WriteBuffer & buf, const DataTypePtr & data_type, const String & column_name, size_t indent)
{
    /// We output/input Map type as follow CapnProto schema
    ///
    /// struct Map
    /// {
    ///     struct Entry
    ///     {
    ///         key @0: Key;
    ///         value @1: Value;
    ///     }
    ///     entries @0 :List(Entry);
    /// }
    const auto & map_type = assert_cast<const DataTypeMap &>(*data_type);
    const auto & key_type = map_type.getKeyType();
    const auto & value_type = map_type.getValueType();

    String struct_name = getSchemaMessageName(column_name);
    startStruct(buf, struct_name, indent);
    startStruct(buf, "Entry", indent + 1);
    auto key_type_name = prepareAndGetCapnProtoTypeName(buf, key_type, "key", indent + 2);
    auto value_type_name = prepareAndGetCapnProtoTypeName(buf, value_type, "value", indent + 2);
    size_t field_index = 0;
    writeFieldDefinition(buf, key_type_name, "key", field_index, indent + 2);
    writeFieldDefinition(buf, value_type_name, "value", field_index, indent + 2);
    endNested(buf, indent + 1);
    field_index = 0;
    writeFieldDefinition(buf, "List(Entry)", "entries", field_index, indent + 1);
    endNested(buf, indent);
    return struct_name;
}

template <typename EnumType>
String prepareEnumAndGetCapnProtoTypeName(WriteBuffer & buf, const DataTypePtr & data_type, const String & column_name, size_t indent)
{
    const auto & enum_type = assert_cast<const DataTypeEnum<EnumType> &>(*data_type);
    String enum_name = getSchemaMessageName(column_name);
    startEnum(buf, enum_name, indent);
    const auto & names = enum_type.getAllRegisteredNames();
    for (size_t i = 0; i != names.size(); ++i)
    {
        writeIndent(buf, indent + 1);
        writeString(fmt::format("{} @{};\n", names[i], std::to_string(i)), buf);
    }
    endNested(buf, indent);
    return enum_name;
}

String prepareAndGetCapnProtoTypeName(WriteBuffer & buf, const DataTypePtr & data_type, const String & column_name, size_t indent)
{
    TypeIndex type_id = data_type->getTypeId();

    switch (data_type->getTypeId())
    {
        case TypeIndex::Nullable:
            return prepareNullableAndGetCapnProtoTypeName(buf, data_type, column_name, indent);
        case TypeIndex::LowCardinality:
            return prepareAndGetCapnProtoTypeName(buf, assert_cast<const DataTypeLowCardinality &>(*data_type).getDictionaryType(), column_name, indent);
        case TypeIndex::Array:
            return prepareArrayAndGetCapnProtoTypeName(buf, data_type, column_name, indent);
        case TypeIndex::Tuple:
            return prepareTupleAndGetCapnProtoTypeName(buf, data_type, column_name, indent);
        case TypeIndex::Map:
            return prepareMapAndGetCapnProtoTypeName(buf, data_type, column_name, indent);
        case TypeIndex::Enum8:
            return prepareEnumAndGetCapnProtoTypeName<Int8>(buf, data_type, column_name, indent);
        case TypeIndex::Enum16:
            return prepareEnumAndGetCapnProtoTypeName<Int16>(buf, data_type, column_name, indent);
        default:
        {
            if (isBool(data_type))
                return "Bool";

            auto it = capn_proto_simple_type_names.find(type_id);
            if (it == capn_proto_simple_type_names.end())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "CapnProto type name is not found for type {}", data_type->getName());
            return it->second;
        }
    }
}

}

void StructureToCapnProtoSchema::writeSchema(WriteBuffer & buf, const String & message_name, const NamesAndTypesList & names_and_types_)
{
    auto names_and_types = collectNested(names_and_types_, true, "CapnProto");
    writeCapnProtoHeader(buf);
    startStruct(buf, getSchemaMessageName(message_name), 0);

    size_t field_index = 0;
    for (const auto & [column_name, data_type] : names_and_types)
        writeField(buf, data_type, column_name, field_index, 1);

    endNested(buf, 0);
}

}
