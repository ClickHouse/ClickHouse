#include <Processors/Formats/Impl/JSONCompactRowOutputFormat.h>
#include <Formats/FormatFactory.h>
#include <Formats/JSONUtils.h>

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
    JSONUtils::writeFieldFromColumn(column, serialization, row_num, yield_strings, settings, *ostr);
    ++field_number;
}


void JSONCompactRowOutputFormat::writeFieldDelimiter()
{
    JSONUtils::writeFieldCompactDelimiter(*ostr);
}

void JSONCompactRowOutputFormat::writeRowStartDelimiter()
{
    JSONUtils::writeCompactArrayStart(*ostr, 2);
}


void JSONCompactRowOutputFormat::writeRowEndDelimiter()
{
    JSONUtils::writeCompactArrayEnd(*ostr);
    field_number = 0;
    ++row_count;
}

void JSONCompactRowOutputFormat::writeBeforeTotals()
{
    JSONUtils::writeFieldDelimiter(*ostr, 2);
    JSONUtils::writeCompactArrayStart(*ostr, 1, "totals");
}

void JSONCompactRowOutputFormat::writeTotals(const Columns & columns, size_t row_num)
{
    JSONUtils::writeCompactColumns(columns, serializations, row_num, yield_strings, settings, *ostr);
}

void JSONCompactRowOutputFormat::writeAfterTotals()
{
    JSONUtils::writeCompactArrayEnd(*ostr);
}

void JSONCompactRowOutputFormat::writeExtremesElement(const char * title, const Columns & columns, size_t row_num)
{
    JSONUtils::writeCompactArrayStart(*ostr, 2, title);
    JSONUtils::writeCompactColumns(columns, serializations, row_num, yield_strings, settings, *ostr);
    JSONUtils::writeCompactArrayEnd(*ostr);
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
