#include <Processors/Formats/Impl/TabSeparatedRowOutputFormat.h>
#include <Formats/FormatFactory.h>
#include <Formats/registerWithNamesAndTypes.h>
#include <IO/WriteHelpers.h>


namespace DB
{
TabSeparatedRowOutputFormat::TabSeparatedRowOutputFormat(
    WriteBuffer & out_,
    const Block & header_,
    bool with_names_,
    bool with_types_,
    bool is_raw_,
    const FormatSettings & format_settings_)
    : IRowOutputFormat(header_, out_), with_names(with_names_), with_types(with_types_), is_raw(is_raw_), format_settings(format_settings_)
{
}

void TabSeparatedRowOutputFormat::writeLine(const std::vector<String> & values)
{
    for (size_t i = 0; i < values.size(); ++i)
    {
        if (is_raw)
            writeString(values[i], out);
        else
            writeEscapedString(values[i], out);
        if (i + 1 != values.size())
            writeFieldDelimiter();
    }
    writeRowEndDelimiter();
}

void TabSeparatedRowOutputFormat::writePrefix()
{
    const auto & header = getPort(PortKind::Main).getHeader();

    if (with_names)
        writeLine(header.getNames());

    if (with_types)
        writeLine(header.getDataTypeNames());
}


void TabSeparatedRowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    if (is_raw)
        serialization.serializeTextRaw(column, row_num, out, format_settings);
    else
        serialization.serializeTextEscaped(column, row_num, out, format_settings);
}


void TabSeparatedRowOutputFormat::writeFieldDelimiter()
{
    writeChar('\t', out);
}


void TabSeparatedRowOutputFormat::writeRowEndDelimiter()
{
    if (format_settings.tsv.crlf_end_of_line)
        writeChar('\r', out);
    writeChar('\n', out);
}

void TabSeparatedRowOutputFormat::writeBeforeTotals()
{
    writeChar('\n', out);
}

void TabSeparatedRowOutputFormat::writeBeforeExtremes()
{
    writeChar('\n', out);
}

void registerOutputFormatTabSeparated(FormatFactory & factory)
{
    for (bool is_raw : {false, true})
    {
        auto register_func = [&](const String & format_name, bool with_names, bool with_types)
        {
            factory.registerOutputFormat(format_name, [is_raw, with_names, with_types](
                WriteBuffer & buf,
                const Block & sample,
                const FormatSettings & settings)
            {
                return std::make_shared<TabSeparatedRowOutputFormat>(buf, sample, with_names, with_types, is_raw, settings);
            });

            factory.markOutputFormatSupportsParallelFormatting(format_name);
        };

        registerWithNamesAndTypes(is_raw ? "TSVRaw" : "TSV", register_func);
        registerWithNamesAndTypes(is_raw ? "TabSeparatedRaw" : "TabSeparated", register_func);
        if (is_raw)
        {
            registerWithNamesAndTypes("LineAsString", register_func);
            registerWithNamesAndTypes("Raw", register_func);
        }
    }
}

}
