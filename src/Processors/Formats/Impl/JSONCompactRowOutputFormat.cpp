#include <Processors/Formats/Impl/JSONCompactRowOutputFormat.h>
#include <Formats/FormatFactory.h>

#include <IO/WriteHelpers.h>


namespace DB
{

JSONCompactRowOutputFormat::JSONCompactRowOutputFormat(
    WriteBuffer & out_,
    const Block & header,
    const RowOutputFormatParams & params_,
    const FormatSettings & settings_,
    bool yield_strings_)
    : JSONRowOutputFormat(out_, header, params_, settings_, yield_strings_)
{
}


void JSONCompactRowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    if (yield_strings)
    {
        WriteBufferFromOwnString buf;

        serialization.serializeText(column, row_num, buf, settings);
        writeJSONString(buf.str(), *ostr, settings);
    }
    else
        serialization.serializeTextJSON(column, row_num, *ostr, settings);

    ++field_number;
}


void JSONCompactRowOutputFormat::writeFieldDelimiter()
{
    writeCString(", ", *ostr);
}

void JSONCompactRowOutputFormat::writeTotalsFieldDelimiter()
{
    writeCString(",", *ostr);
}


void JSONCompactRowOutputFormat::writeRowStartDelimiter()
{
    writeCString("\t\t[", *ostr);
}


void JSONCompactRowOutputFormat::writeRowEndDelimiter()
{
    writeChar(']', *ostr);
    field_number = 0;
    ++row_count;
}

void JSONCompactRowOutputFormat::writeBeforeTotals()
{
    writeCString(",\n", *ostr);
    writeChar('\n', *ostr);
    writeCString("\t\"totals\": [", *ostr);
}

void JSONCompactRowOutputFormat::writeAfterTotals()
{
    writeChar(']', *ostr);
}

void JSONCompactRowOutputFormat::writeExtremesElement(const char * title, const Columns & columns, size_t row_num)
{
    writeCString("\t\t\"", *ostr);
    writeCString(title, *ostr);
    writeCString("\": [", *ostr);

    size_t extremes_columns = columns.size();
    for (size_t i = 0; i < extremes_columns; ++i)
    {
        if (i != 0)
            writeTotalsFieldDelimiter();

        writeField(*columns[i], *serializations[i], row_num);
    }

    writeChar(']', *ostr);
}

void registerOutputFormatJSONCompact(FormatFactory & factory)
{
    factory.registerOutputFormat("JSONCompact", [](
        WriteBuffer & buf,
        const Block & sample,
        const RowOutputFormatParams & params,
        const FormatSettings & format_settings)
    {
        return std::make_shared<JSONCompactRowOutputFormat>(buf, sample, params, format_settings, false);
    });

    factory.markOutputFormatSupportsParallelFormatting("JSONCompact");

    factory.registerOutputFormat("JSONCompactStrings", [](
        WriteBuffer & buf,
        const Block & sample,
        const RowOutputFormatParams & params,
        const FormatSettings & format_settings)
    {
        return std::make_shared<JSONCompactRowOutputFormat>(buf, sample, params, format_settings, true);
    });

    factory.markOutputFormatSupportsParallelFormatting("JSONCompactStrings");
}

}
