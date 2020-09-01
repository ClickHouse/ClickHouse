#include <Processors/Formats/Impl/JSONStringsRowOutputFormat.h>
#include <Formats/FormatFactory.h>

#include <IO/WriteHelpers.h>


namespace DB
{

JSONStringsRowOutputFormat::JSONStringsRowOutputFormat(
    WriteBuffer & out_, const Block & header, FormatFactory::WriteCallback callback, const FormatSettings & settings_)
    : JSONRowOutputFormat(out_, header, callback, settings_)
{
}


void JSONStringsRowOutputFormat::writeField(const IColumn & column, const IDataType & type, size_t row_num)
{
    WriteBufferFromOwnString buf;

    type.serializeAsText(column, row_num, buf, settings);
    writeJSONString(buf.str(), *ostr, settings);
    ++field_number;
}


void JSONStringsRowOutputFormat::writeFieldDelimiter()
{
    writeCString(", ", *ostr);
}

void JSONStringsRowOutputFormat::writeTotalsFieldDelimiter()
{
    writeCString(",", *ostr);
}


void JSONStringsRowOutputFormat::writeRowStartDelimiter()
{
    writeCString("\t\t[", *ostr);
}


void JSONStringsRowOutputFormat::writeRowEndDelimiter()
{
    writeChar(']', *ostr);
    field_number = 0;
    ++row_count;
}

void JSONStringsRowOutputFormat::writeBeforeTotals()
{
    writeCString(",\n", *ostr);
    writeChar('\n', *ostr);
    writeCString("\t\"totals\": [", *ostr);
}

void JSONStringsRowOutputFormat::writeAfterTotals()
{
    writeChar(']', *ostr);
}

void JSONStringsRowOutputFormat::writeExtremesElement(const char * title, const Columns & columns, size_t row_num)
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

void registerOutputFormatProcessorJSONStrings(FormatFactory & factory)
{
    factory.registerOutputFormatProcessor("JSONStrings", [](
        WriteBuffer & buf,
        const Block & sample,
        FormatFactory::WriteCallback callback,
        const FormatSettings & format_settings)
    {
        return std::make_shared<JSONStringsRowOutputFormat>(buf, sample, callback, format_settings);
    });
}

}
