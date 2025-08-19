#include <Processors/Formats/Impl/ValuesRowOutputFormat.h>

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteHelpers.h>
#include <Processors/Port.h>


namespace DB
{


ValuesRowOutputFormat::ValuesRowOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_)
    : IRowOutputFormat(header_, out_), format_settings(format_settings_)
{
}

void ValuesRowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    serialization.serializeTextQuoted(column, row_num, out, format_settings);
}

void ValuesRowOutputFormat::writeFieldDelimiter()
{
    writeChar(',', out);
}

void ValuesRowOutputFormat::writeRowStartDelimiter()
{
    writeChar('(', out);
}

void ValuesRowOutputFormat::writeRowEndDelimiter()
{
    writeChar(')', out);
}

void ValuesRowOutputFormat::writeRowBetweenDelimiter()
{
    writeCString(",", out);
}


void registerOutputFormatValues(FormatFactory & factory)
{
    factory.registerOutputFormat("Values", [](
        WriteBuffer & buf,
        const Block & sample,
        const FormatSettings & settings,
        FormatFilterInfoPtr /*format_filter_info*/)
    {
        return std::make_shared<ValuesRowOutputFormat>(buf, std::make_shared<const Block>(sample), settings);
    });

    factory.markOutputFormatSupportsParallelFormatting("Values");
}

}
