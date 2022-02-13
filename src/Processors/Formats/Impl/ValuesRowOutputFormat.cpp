#include <Processors/Formats/Impl/ValuesRowOutputFormat.h>
#include <Formats/FormatFactory.h>

#include <IO/WriteHelpers.h>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>


namespace DB
{


ValuesRowOutputFormat::ValuesRowOutputFormat(WriteBuffer & out_, const Block & header_, const RowOutputFormatParams & params_, const FormatSettings & format_settings_)
    : IRowOutputFormat(header_, out_, params_), format_settings(format_settings_)
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
        const RowOutputFormatParams & params,
        const FormatSettings & settings)
    {
        return std::make_shared<ValuesRowOutputFormat>(buf, sample, params, settings);
    });

    factory.markOutputFormatSupportsParallelFormatting("Values");
}

}
