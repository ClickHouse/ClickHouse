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


void JSONEachRowRowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    writeString(fields[field_number], out);
    writeChar(':', out);

    if (settings.json.serialize_as_strings)
    {
        WriteBufferFromOwnString buf;

        serialization.serializeText(column, row_num, buf, settings);
        writeJSONString(buf.str(), out, settings);
    }
    else
        serialization.serializeTextJSON(column, row_num, out, settings);

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
    writeCString("}", out);
    field_number = 0;
}


void JSONEachRowRowOutputFormat::writeRowBetweenDelimiter()
{
    if (settings.json.array_of_rows)
        writeChar(',', out);

    writeChar('\n', out);
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
        writeCString("\n]", out);

    writeChar('\n', out);
}


void registerOutputFormatJSONEachRow(FormatFactory & factory)
{
    factory.registerOutputFormat("JSONEachRow", [](
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
    factory.markOutputFormatSupportsParallelFormatting("JSONEachRow");

    factory.registerOutputFormat("JSONLines", [](
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
    factory.markOutputFormatSupportsParallelFormatting("JSONLines");

    factory.registerOutputFormat("NDJSON", [](
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
    factory.markOutputFormatSupportsParallelFormatting("NDJSON");

    factory.registerOutputFormat("JSONStringsEachRow", [](
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
    factory.markOutputFormatSupportsParallelFormatting("JSONStringEachRow");
}

}
