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
    const RowOutputFormatParams & params_,
    const FormatSettings & settings_)
        : RowOutputFormatWithUTF8ValidationAdaptor(settings_.json.validate_utf8, header_, out_, params_),
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
    // Why do we need this weird `if`?
    //
    // The reason is the formatRow function that is broken with respect to
    // row-between delimiters. It should not write them, but it does, and then
    // hacks around it by having a special formatRowNoNewline version, which, as
    // you guessed, removes the newline from the end of row. But the row-between
    // delimiter goes into a second row, so it turns out to be in the beginning
    // of the line, and the removal doesn't work. There is also a second bug --
    // the row-between delimiter in this format is written incorrectly. In fact,
    // it is not written at all, and the newline is written in a row-end
    // delimiter ("}\n" instead of the correct "}"). With these two bugs
    // combined, the test 01420_format_row works perfectly.
    //
    // A proper implementation of formatRow would use IRowOutputFormat directly,
    // and not write row-between delimiters, instead of using IOutputFormat
    // processor and its crutch row callback. This would require exposing
    // IRowOutputFormat, which we don't do now, but which can be generally useful
    // for other cases such as parallel formatting, that also require a control
    // flow different from the usual IOutputFormat.
    //
    // I just don't have time or energy to redo all of this, but I need to
    // support JSON array output here, which requires proper ",\n" row-between
    // delimiters. For compatibility, I preserve the bug in case of non-array
    // output.
    if (settings.json.array_of_rows)
    {
        writeChar('}', *ostr);
    }
    else
    {
        writeCString("}\n", *ostr);
    }
    field_number = 0;
}


void JSONEachRowRowOutputFormat::writeRowBetweenDelimiter()
{
    // We preserve an existing bug here for compatibility. See the comment above.
    if (settings.json.array_of_rows)
    {
        writeCString(",\n", *ostr);
    }
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
    {
        writeCString("\n]\n", *ostr);
    }
}


void registerOutputFormatJSONEachRow(FormatFactory & factory)
{
    auto register_function = [&](const String & format, bool serialize_as_strings)
    {
        factory.registerOutputFormat(format, [serialize_as_strings](
            WriteBuffer & buf,
            const Block & sample,
            const RowOutputFormatParams & params,
            const FormatSettings & _format_settings)
        {
            FormatSettings settings = _format_settings;
            settings.json.serialize_as_strings = serialize_as_strings;
            return std::make_shared<JSONEachRowRowOutputFormat>(buf, sample, params,
                settings);
        });
        factory.markOutputFormatSupportsParallelFormatting(format);
    };

    register_function("JSONEachRow", false);
    register_function("JSONLines", false);
    register_function("NDJSON", false);
    register_function("JSONStringsEachRow", true);
}

}
