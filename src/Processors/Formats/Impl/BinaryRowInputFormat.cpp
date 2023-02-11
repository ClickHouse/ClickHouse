#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <Processors/Formats/Impl/BinaryRowInputFormat.h>
#include <Formats/FormatFactory.h>
#include <Formats/registerWithNamesAndTypes.h>
#include <DataTypes/DataTypeFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SKIP_UNKNOWN_FIELD;
}

BinaryRowInputFormat::BinaryRowInputFormat(ReadBuffer & in_, Block header, Params params_, bool with_names_, bool with_types_, const FormatSettings & format_settings_)
    : RowInputFormatWithNamesAndTypes(
        header,
        in_,
        params_,
        with_names_,
        with_types_,
        format_settings_,
        std::make_unique<BinaryFormatReader>(in_, format_settings_))
{
}


BinaryFormatReader::BinaryFormatReader(ReadBuffer & in_, const FormatSettings & format_settings_) : FormatWithNamesAndTypesReader(in_, format_settings_)
{
}

std::vector<String> BinaryFormatReader::readHeaderRow()
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

std::vector<String> BinaryFormatReader::readNames()
{
    readVarUInt(read_columns, *in);
    return readHeaderRow();
}

std::vector<String> BinaryFormatReader::readTypes()
{
    auto types = readHeaderRow();
    for (const auto & type_name : types)
        read_data_types.push_back(DataTypeFactory::instance().get(type_name));
    return types;
}

bool BinaryFormatReader::readField(IColumn & column, const DataTypePtr & /*type*/, const SerializationPtr & serialization, bool /*is_last_file_column*/, const String & /*column_name*/)
{
    serialization->deserializeBinary(column, *in);
    return true;
}

void BinaryFormatReader::skipHeaderRow()
{
    String tmp;
    for (size_t i = 0; i < read_columns; ++i)
        readStringBinary(tmp, *in);
}

void BinaryFormatReader::skipNames()
{
    readVarUInt(read_columns, *in);
    skipHeaderRow();
}

void BinaryFormatReader::skipTypes()
{
    if (read_columns == 0)
    {
        /// It's possible only when with_names = false and with_types = true
        readVarUInt(read_columns, *in);
    }

    skipHeaderRow();
}

void BinaryFormatReader::skipField(size_t file_column)
{
    if (file_column >= read_data_types.size())
        throw Exception(ErrorCodes::CANNOT_SKIP_UNKNOWN_FIELD, "Cannot skip unknown field in RowBinaryWithNames format, because it's type is unknown");
    Field field;
    read_data_types[file_column]->getDefaultSerialization()->deserializeBinary(field, *in);
}

BinaryWithNamesAndTypesSchemaReader::BinaryWithNamesAndTypesSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : FormatWithNamesAndTypesSchemaReader(in_, 0, true, true, &reader), reader(in_, format_settings_)
{
}

void registerInputFormatRowBinary(FormatFactory & factory)
{
    auto register_func = [&](const String & format_name, bool with_names, bool with_types)
    {
        factory.registerInputFormat(format_name, [with_names, with_types](
            ReadBuffer & buf,
            const Block & sample,
            const IRowInputFormat::Params & params,
            const FormatSettings & settings)
        {
            return std::make_shared<BinaryRowInputFormat>(buf, sample, params, with_names, with_types, settings);
        });
    };

    registerWithNamesAndTypes("RowBinary", register_func);
    factory.registerFileExtension("bin", "RowBinary");
}

void registerRowBinaryWithNamesAndTypesSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader("RowBinaryWithNamesAndTypes", [](ReadBuffer & buf, const FormatSettings & settings, ContextPtr)
    {
        return std::make_shared<BinaryWithNamesAndTypesSchemaReader>(buf, settings);
    });
}


}
