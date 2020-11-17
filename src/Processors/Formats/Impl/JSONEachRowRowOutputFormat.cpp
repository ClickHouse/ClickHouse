#include <IO/WriteHelpers.h>
#include <IO/WriteBufferValidUTF8.h>
#include <Processors/Formats/Impl/JSONEachRowRowOutputFormat.h>
#include <Formats/FormatFactory.h>


namespace DB
{


JSONEachRowRowOutputFormat::JSONEachRowRowOutputFormat(
    WriteBuffer & out_,
    const Block & header_,
    const RowOutputFormatParams & params_,
    const FormatSettings & settings_)
        : IRowOutputFormat(header_, out_, params_),
            settings(settings_)
{
    const auto & sample = getPort(PortKind::Main).getHeader();
    size_t columns = sample.columns();
    fields.resize(columns);

    for (size_t i = 0; i < columns; ++i)
    {
        WriteBufferFromString buf(fields[i]);
        writeJSONString(sample.getByPosition(i).name, buf, settings);
    }
}


void JSONEachRowRowOutputFormat::writeField(const IColumn & column, const IDataType & type, size_t row_num)
{
    writeString(fields[field_number], out);
    writeChar(':', out);

    if (settings.json.serialize_as_strings)
    {
        WriteBufferFromOwnString buf;

        type.serializeAsText(column, row_num, buf, settings);
        writeJSONString(buf.str(), out, settings);
    }
    else
        type.serializeAsTextJSON(column, row_num, out, settings);

    ++field_number;
}


void JSONEachRowRowOutputFormat::writeFieldDelimiter()
{
    writeChar(',', out);
}


void JSONEachRowRowOutputFormat::writeRowStartDelimiter()
{
    writeChar('{', out);
}


void JSONEachRowRowOutputFormat::writeRowEndDelimiter()
{
    // Why this weird if?
    //
    // The reason is the formatRow function that is broken with respect to
    // row-between delimiters. It should not write them, but it does, and then
    // hacks around it by having a special formatRowNoNewline function
    // which, as you guessed, removes the newline from the end of row. But the
    // row-between delimiter goes into a second row, so it turns out to be in
    // the second line, and the removal doesn't work. But the row-between
    // delimiter in this format is also written incorrectly (not written at all,
    // in fact), so the test (01420_format_row) works! All good.
    //
    // A proper implementation of formatRow would use IRowOutputFormat directly,
    // and not write row-between delimiters, instead of using IOutputFormat
    // processor and its crutch row callback. We would also need to expose
    // IRowOutputFormat which we don't do now.
    //
    // I just don't have time or energy to redo all of this properly, but I need
    // to support JSON array output here. I don't want to copy the entire
    // JSONEachRow output code, so I preserve the bug for compatibility.
    if (settings.json.array_of_rows)
    {
        writeCString("}", out);
    }
    else
    {
        writeCString("}\n", out);
    }
    field_number = 0;
}


void JSONEachRowRowOutputFormat::writeRowBetweenDelimiter()
{
    // We preserve an existing bug here for compatibility. See the comment above.
    if (settings.json.array_of_rows)
    {
        writeCString(",\n", out);
    }
}


void JSONEachRowRowOutputFormat::writePrefix()
{
    if (settings.json.array_of_rows)
    {
        writeCString("[\n", out);
    }
}


void JSONEachRowRowOutputFormat::writeSuffix()
{
    if (settings.json.array_of_rows)
    {
        writeCString("\n]\n", out);
    }
}


void registerOutputFormatProcessorJSONEachRow(FormatFactory & factory)
{
    factory.registerOutputFormatProcessor("JSONEachRow", [](
        WriteBuffer & buf,
        const Block & sample,
        const RowOutputFormatParams & params,
        const FormatSettings & _format_settings)
    {
        FormatSettings settings = _format_settings;
        settings.json.serialize_as_strings = false;
        return std::make_shared<JSONEachRowRowOutputFormat>(buf, sample, params,
            settings);
    });

    factory.registerOutputFormatProcessor("JSONStringsEachRow", [](
        WriteBuffer & buf,
        const Block & sample,
        const RowOutputFormatParams & params,
        const FormatSettings & _format_settings)
    {
        FormatSettings settings = _format_settings;
        settings.json.serialize_as_strings = true;
        return std::make_shared<JSONEachRowRowOutputFormat>(buf, sample, params,
            settings);
    });

    factory.registerOutputFormatProcessor("JSONArray", [](
        WriteBuffer & buf,
        const Block & sample,
        const RowOutputFormatParams & params,
        const FormatSettings & _format_settings)
    {
        FormatSettings settings = _format_settings;
        settings.json.array_of_rows = true;
        return std::make_shared<JSONEachRowRowOutputFormat>(buf, sample, params,
            settings);
    });
}

}
