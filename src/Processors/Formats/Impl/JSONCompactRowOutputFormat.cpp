#include <Processors/Formats/Impl/JSONCompactRowOutputFormat.h>
#include <Formats/FormatFactory.h>

#include <IO/WriteHelpers.h>


namespace DB
{

JSONCompactRowOutputFormat::JSONCompactRowOutputFormat(
    WriteBuffer & out_, const Block & header, FormatFactory::WriteCallback callback, const FormatSettings & settings_)
    : JSONRowOutputFormat(out_, header, callback, settings_)
{
}


void JSONCompactRowOutputFormat::writeField(const IColumn & column, const IDataType & type, size_t row_num)
{
    type.serializeAsTextJSON(column, row_num, *ostr, settings);
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

        writeField(*columns[i], *types[i], row_num);
    }

    writeChar(']', *ostr);
}

void registerOutputFormatProcessorJSONCompact(FormatFactory & factory)
{
    factory.registerOutputFormatProcessor("JSONCompact", [](
        WriteBuffer & buf,
        const Block & sample,
        FormatFactory::WriteCallback callback,
        const FormatSettings & format_settings)
    {
        return std::make_shared<JSONCompactRowOutputFormat>(buf, sample, callback, format_settings);
    });
}

}
