#include <IO/WriteHelpers.h>
#include <IO/WriteBufferValidUTF8.h>
#include <Processors/Formats/Impl/JSONEachRowRowOutputFormat.h>
#include <Formats/FormatFactory.h>
#include <Formats/JSONUtils.h>


namespace DB
{


JSONEachRowRowOutputFormat::JSONEachRowRowOutputFormat(
    WriteBuffer & out_,
    const Block & header_,
    const FormatSettings & settings_)
        : RowOutputFormatWithUTF8ValidationAdaptor(settings_.json.validate_utf8, header_, out_),
            settings(settings_)
{
    fields = JSONUtils::makeNamesValidJSONStrings(getPort(PortKind::Main).getHeader().getNames(), settings, settings.json.validate_utf8);
}


void JSONEachRowRowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    JSONUtils::writeFieldFromColumn(column, serialization, row_num, settings.json.serialize_as_strings, settings, *ostr, fields[field_number], 0, "");
    ++field_number;
}


void JSONEachRowRowOutputFormat::writeFieldDelimiter()
{
    writeChar(',', *ostr);
}


void JSONEachRowRowOutputFormat::writeRowStartDelimiter()
{
    writeChar('{', *ostr);
}


void JSONEachRowRowOutputFormat::writeRowEndDelimiter()
{
    if (settings.json.array_of_rows)
        writeChar('}', *ostr);
    else
        writeCString("}\n", *ostr);
    field_number = 0;
}


void JSONEachRowRowOutputFormat::writeRowBetweenDelimiter()
{
    if (settings.json.array_of_rows)
        writeCString(",\n", *ostr);
}


void JSONEachRowRowOutputFormat::writePrefix()
{
    if (settings.json.array_of_rows)
    {
        writeCString("[\n", *ostr);
    }
}


void JSONEachRowRowOutputFormat::writeSuffix()
{
    if (settings.json.array_of_rows)
        writeCString("\n]\n", *ostr);
}


void registerOutputFormatJSONEachRow(FormatFactory & factory)
{
    auto register_function = [&](const String & format, bool serialize_as_strings)
    {
        factory.registerOutputFormat(format, [serialize_as_strings](
            WriteBuffer & buf,
            const Block & sample,
            const FormatSettings & _format_settings)
        {
            FormatSettings settings = _format_settings;
            settings.json.serialize_as_strings = serialize_as_strings;
            return std::make_shared<JSONEachRowRowOutputFormat>(buf, sample, settings);
        });
        factory.markOutputFormatSupportsParallelFormatting(format);
    };

    register_function("JSONEachRow", false);
    register_function("JSONLines", false);
    register_function("NDJSON", false);
    register_function("JSONStringsEachRow", true);
}

}
