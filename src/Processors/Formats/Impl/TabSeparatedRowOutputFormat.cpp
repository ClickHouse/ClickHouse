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
    const RowOutputFormatParams & params_,
    const FormatSettings & format_settings_)
    : IRowOutputFormat(header_, out_, params_), with_names(with_names_), with_types(with_types_), is_raw(is_raw_), format_settings(format_settings_)
{
}

void TabSeparatedRowOutputFormat::writeLine(const std::vector<String> & values)
{
    for (size_t i = 0; i < values.size(); ++i)
    {
        writeEscapedString(values[i], out);
        if (i + 1 == values.size())
            writeRowEndDelimiter();
        else
            writeFieldDelimiter();
    }
}

void TabSeparatedRowOutputFormat::doWritePrefix()
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
        auto get_output_creator = [is_raw](bool with_names, bool with_types)
        {
            return [is_raw, with_names, with_types](
                WriteBuffer & buf,
                const Block & sample,
                const RowOutputFormatParams & params,
                const FormatSettings & settings)
            {
                return std::make_shared<TabSeparatedRowOutputFormat>(buf, sample, with_names, with_types, is_raw, params, settings);
            };
        };

        registerOutputFormatWithNamesAndTypes(factory, is_raw ? "TSVRaw" : "TSV", get_output_creator, true);
        registerOutputFormatWithNamesAndTypes(factory, is_raw ? "TabSeparatedRaw" : "TabSeparated", get_output_creator, true);
    }
}

}
