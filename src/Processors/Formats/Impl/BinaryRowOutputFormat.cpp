#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <Processors/Formats/Impl/BinaryRowOutputFormat.h>
#include <Formats/FormatFactory.h>
#include <Formats/registerWithNamesAndTypes.h>


namespace DB
{

BinaryRowOutputFormat::BinaryRowOutputFormat(WriteBuffer & out_, const Block & header, bool with_names_, bool with_types_, const FormatSettings & format_settings_)
    : IRowOutputFormat(header, out_), with_names(with_names_), with_types(with_types_), format_settings(format_settings_)
{
}

void BinaryRowOutputFormat::writePrefix()
{
    const auto & header = getPort(PortKind::Main).getHeader();
    size_t columns = header.columns();

    if (with_names || with_types)
    {
        writeVarUInt(columns, out);
    }

    if (with_names)
    {
        for (size_t i = 0; i < columns; ++i)
        {
            writeStringBinary(header.safeGetByPosition(i).name, out);
        }
    }

    if (with_types)
    {
        if (format_settings.binary.encode_types_in_binary_format)
        {
            for (size_t i = 0; i < columns; ++i)
                encodeDataType(header.safeGetByPosition(i).type, out);
        }
        else
        {
            for (size_t i = 0; i < columns; ++i)
                writeStringBinary(header.safeGetByPosition(i).type->getName(), out);
        }
    }
}

void BinaryRowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    serialization.serializeBinary(column, row_num, out, format_settings);
}


void registerOutputFormatRowBinary(FormatFactory & factory)
{
    auto register_func = [&](const String & format_name, bool with_names, bool with_types)
    {
        factory.registerOutputFormat(format_name, [with_names, with_types](
            WriteBuffer & buf,
            const Block & sample,
            const FormatSettings & format_settings)
        {
            return std::make_shared<BinaryRowOutputFormat>(buf, sample, with_names, with_types, format_settings);
        });
        factory.markOutputFormatSupportsParallelFormatting(format_name);
    };

    registerWithNamesAndTypes("RowBinary", register_func);
}

}
