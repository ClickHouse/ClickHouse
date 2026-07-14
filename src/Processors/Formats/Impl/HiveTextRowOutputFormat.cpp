#include <Processors/Formats/Impl/HiveTextRowOutputFormat.h>
#include <Formats/FormatFactory.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/// Hive declares maps as MAP<primitive_type, data_type>: a map key cannot be a nested
/// (ARRAY/MAP/STRUCT) type, so no Hive schema could read such values back. ClickHouse allows
/// composite Map keys whose elements would serialize fine on their own, so reject them upfront.
/// The walk must descend through every wrapper whose serializeTextHive is a transparent
/// pass-through (Nullable), otherwise a composite-key Map hidden inside, e.g.,
/// Nullable(Tuple(Map(Array(UInt8), UInt8))) would slip past the check and still be written.
void assertMapKeysArePrimitive(const DataTypePtr & type)
{
    if (const auto * type_nullable = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        assertMapKeysArePrimitive(type_nullable->getNestedType());
    }
    else if (const auto * type_array = typeid_cast<const DataTypeArray *>(type.get()))
    {
        assertMapKeysArePrimitive(type_array->getNestedType());
    }
    else if (const auto * type_map = typeid_cast<const DataTypeMap *>(type.get()))
    {
        WhichDataType key_type(type_map->getKeyType());
        if (key_type.isArray() || key_type.isMap() || key_type.isTuple())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Type {} is not supported by the HiveText output format: Hive supports only primitive types as Map keys",
                type_map->getName());
        assertMapKeysArePrimitive(type_map->getValueType());
    }
    else if (const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        for (const auto & element : type_tuple->getElements())
            assertMapKeysArePrimitive(element);
    }
}

}


HiveTextRowOutputFormat::HiveTextRowOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_)
    : IRowOutputFormat(header_, out_), format_settings(format_settings_)
{
    for (const auto & column : *header_)
        assertMapKeysArePrimitive(column.type);
}

void HiveTextRowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    serialization.serializeTextHive(column, row_num, out, format_settings);
}

void HiveTextRowOutputFormat::writeFieldDelimiter()
{
    writeChar(format_settings.hive_text.fields_delimiter, out);
}

void HiveTextRowOutputFormat::writeRowEndDelimiter()
{
    writeChar(format_settings.hive_text.rows_delimiter, out);
}

void registerOutputFormatHiveText(FormatFactory & factory);
void registerOutputFormatHiveText(FormatFactory & factory)
{
    factory.registerOutputFormat("HiveText", [](
                   WriteBuffer & buf,
                   const Block & sample,
                   const FormatSettings & format_settings,
                   FormatFilterInfoPtr /*format_filter_info*/)
        {
            return std::make_shared<HiveTextRowOutputFormat>(buf, std::make_shared<const Block>(sample), format_settings);
        });
    factory.markOutputFormatSupportsParallelFormatting("HiveText");
}

}
