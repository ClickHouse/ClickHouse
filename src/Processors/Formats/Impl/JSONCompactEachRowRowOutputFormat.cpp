#include <IO/WriteHelpers.h>
#include <IO/WriteBufferValidUTF8.h>
#include <Processors/Formats/Impl/JSONCompactEachRowRowOutputFormat.h>
#include <Formats/FormatFactory.h>
#include <Formats/registerWithNamesAndTypes.h>


namespace DB
{


JSONCompactEachRowRowOutputFormat::JSONCompactEachRowRowOutputFormat(WriteBuffer & out_,
        const Block & header_,
        const RowOutputFormatParams & params_,
        const FormatSettings & settings_,
        bool with_names_,
        bool with_types_,
        bool yield_strings_)
        : IRowOutputFormat(header_, out_, params_), settings(settings_), with_names(with_names_), with_types(with_types_), yield_strings(yield_strings_)
{
}


void JSONCompactEachRowRowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    if (yield_strings)
    {
        WriteBufferFromOwnString buf;

        serialization.serializeText(column, row_num, buf, settings);
        writeJSONString(buf.str(), out, settings);
    }
    else
        serialization.serializeTextJSON(column, row_num, out, settings);
}


void JSONCompactEachRowRowOutputFormat::writeFieldDelimiter()
{
    writeCString(", ", out);
}


void JSONCompactEachRowRowOutputFormat::writeRowStartDelimiter()
{
    writeChar('[', out);
}


void JSONCompactEachRowRowOutputFormat::writeRowEndDelimiter()
{
    writeCString("]\n", out);
}

void JSONCompactEachRowRowOutputFormat::writeTotals(const Columns & columns, size_t row_num)
{
    writeChar('\n', out);
    size_t num_columns = columns.size();
    writeRowStartDelimiter();
    for (size_t i = 0; i < num_columns; ++i)
    {
        if (i != 0)
            writeFieldDelimiter();

        writeField(*columns[i], *serializations[i], row_num);
    }
    writeRowEndDelimiter();
}

void JSONCompactEachRowRowOutputFormat::writeLine(const std::vector<String> & values)
{
    writeRowStartDelimiter();
    for (size_t i = 0; i < values.size(); ++i)
    {
        writeChar('\"', out);
        writeString(values[i], out);
        writeChar('\"', out);
        if (i != values.size() - 1)
            writeFieldDelimiter();
    }
    writeRowEndDelimiter();
}

void JSONCompactEachRowRowOutputFormat::doWritePrefix()
{
    const auto & header = getPort(PortKind::Main).getHeader();

    if (with_names)
        writeLine(header.getNames());

    if (with_types)
        writeLine(header.getDataTypeNames());
}

void JSONCompactEachRowRowOutputFormat::consumeTotals(DB::Chunk chunk)
{
    if (with_names)
        IRowOutputFormat::consumeTotals(std::move(chunk));
}

void registerOutputFormatJSONCompactEachRow(FormatFactory & factory)
{
    for (bool yield_strings : {false, true})
    {
        auto register_func = [&](const String & format_name, bool with_names, bool with_types)
        {
            factory.registerOutputFormat(format_name, [yield_strings, with_names, with_types](
                WriteBuffer & buf,
                const Block & sample,
                const RowOutputFormatParams & params,
                const FormatSettings & format_settings)
            {
                return std::make_shared<JSONCompactEachRowRowOutputFormat>(buf, sample, params, format_settings, with_names, with_types, yield_strings);
            });

            factory.markOutputFormatSupportsParallelFormatting(format_name);
        };

        registerWithNamesAndTypes(yield_strings ? "JSONCompactStringsEachRow" : "JSONCompactEachRow", register_func);
    }
}


}
