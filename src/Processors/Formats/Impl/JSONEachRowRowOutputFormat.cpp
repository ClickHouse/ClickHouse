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
    const FormatSettings & settings_,
    bool pretty_json_)
        : RowOutputFormatWithUTF8ValidationAdaptor(settings_.json.validate_utf8, header_, out_),
        pretty_json(pretty_json_),
        settings(settings_)
{
    fields = JSONUtils::makeNamesValidJSONStrings(getPort(PortKind::Main).getHeader().getNames(), settings, settings.json.validate_utf8);
}


void JSONEachRowRowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    JSONUtils::writeFieldFromColumn(column, serialization, row_num, settings.json.serialize_as_strings, settings, *ostr, fields[field_number], pretty_json ? 1 : 0, "", pretty_json);
    ++field_number;
}


void JSONEachRowRowOutputFormat::writeFieldDelimiter()
{
    writeChar(',', *ostr);
    if (pretty_json)
        writeChar('\n', *ostr);
}


void JSONEachRowRowOutputFormat::writeRowStartDelimiter()
{
    writeChar('{', *ostr);
    if (pretty_json)
        writeChar('\n', *ostr);
}


void JSONEachRowRowOutputFormat::writeRowEndDelimiter()
{
    if (pretty_json)
        writeChar('\n', *ostr);

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
    auto register_function = [&](const String & format, bool serialize_as_strings, bool pretty_json)
    {
        factory.registerOutputFormat(format, [serialize_as_strings, pretty_json](
            WriteBuffer & buf,
            const Block & sample,
            const FormatSettings & _format_settings)
        {
            FormatSettings settings = _format_settings;
            settings.json.serialize_as_strings = serialize_as_strings;
            return std::make_shared<JSONEachRowRowOutputFormat>(buf, sample, settings, pretty_json);
        });
        factory.markOutputFormatSupportsParallelFormatting(format);
    };

    register_function("JSONEachRow", false, false);
    register_function("PrettyJSONEachRow", false, true);
    register_function("JSONLines", false, false);
    register_function("PrettyJSONLines", false, true);
    register_function("NDJSON", false, false);
    register_function("PrettyNDJSON", false, true);
    register_function("JSONStringsEachRow", true, false);
}

}
