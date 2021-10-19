#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <Processors/Formats/Impl/BinaryRowInputFormat.h>
#include <Formats/FormatFactory.h>
#include <DataTypes/DataTypeFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

BinaryRowInputFormat::BinaryRowInputFormat(ReadBuffer & in_, Block header, Params params_, bool with_names_and_types, const FormatSettings & format_settings_)
    : RowInputFormatWithNamesAndTypes(std::move(header), in_, std::move(params_), with_names_and_types, with_names_and_types, format_settings_)
{
}

std::vector<String> BinaryRowInputFormat::readHeaderRow()
{
    std::vector<String> fields;
    String field;
    for (size_t i = 0; i < read_columns; ++i)
    {
        readStringBinary(field, *in);
        fields.push_back(field);
    }
    return fields;
}

std::vector<String> BinaryRowInputFormat::readNames()
{
    readVarUInt(read_columns, *in);
    return readHeaderRow();
}

std::vector<String> BinaryRowInputFormat::readTypes()
{
    auto types = readHeaderRow();
    for (const auto & type_name : types)
        read_data_types.push_back(DataTypeFactory::instance().get(type_name));
    return types;
}

bool BinaryRowInputFormat::readField(IColumn & column, const DataTypePtr & /*type*/, const SerializationPtr & serialization, bool /*is_last_file_column*/, const String & /*column_name*/)
{
    serialization->deserializeBinary(column, *in);
    return true;
}

void BinaryRowInputFormat::skipHeaderRow()
{
    String tmp;
    for (size_t i = 0; i < read_columns; ++i)
        readStringBinary(tmp, *in);
}

void BinaryRowInputFormat::skipNames()
{
    readVarUInt(read_columns, *in);
    skipHeaderRow();
}

void BinaryRowInputFormat::skipTypes()
{
    skipHeaderRow();
}

void BinaryRowInputFormat::skipField(size_t file_column)
{
    if (file_column >= read_data_types.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot skip field in Binary format, because it's type is unknown");
    Field field;
    read_data_types[file_column]->getDefaultSerialization()->deserializeBinary(field, *in);
}

void registerInputFormatRowBinary(FormatFactory & factory)
{
    factory.registerInputFormat("RowBinary", [](
        ReadBuffer & buf,
        const Block & sample,
        const IRowInputFormat::Params & params,
        const FormatSettings & settings)
    {
        return std::make_shared<BinaryRowInputFormat>(buf, sample, params, false, settings);
    });

    factory.registerInputFormat("RowBinaryWithNamesAndTypes", [](
        ReadBuffer & buf,
        const Block & sample,
        const IRowInputFormat::Params & params,
        const FormatSettings & settings)
    {
        return std::make_shared<BinaryRowInputFormat>(buf, sample, params, true, settings);
    });
}

}
