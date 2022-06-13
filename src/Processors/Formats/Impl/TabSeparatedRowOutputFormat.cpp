#include <Processors/Formats/Impl/TabSeparatedRowOutputFormat.h>
#include <Formats/FormatFactory.h>
#include <Formats/registerWithNamesAndTypes.h>
#include <IO/WriteHelpers.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/join.hpp>


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
        if (is_raw)
            writeString(values[i], out);
        else
            writeEscapedString(values[i], out);
        if (i + 1 == values.size())
            writeRowEndDelimiter();
        else
            writeFieldDelimiter();
    }
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

TabSeparatedSortedRowOutputFormat::TabSeparatedSortedRowOutputFormat(
    WriteBuffer & out_,
    const Block & header_,
    const RowOutputFormatParams & params_,
    const FormatSettings & format_settings_)
    : TabSeparatedRowOutputFormat(tmp_buf, header_, false, false, false, params_, format_settings_)
    , real_out(out_)
{}

static String sortLines(const String & str)
{
    Strings lines;
    boost::split(lines, str, boost::is_any_of("\n"));
    /// sort alphabetically, empty lines at the end
    std::sort(lines.begin(), lines.end(), [](const String & a, const String & b) { return a.empty() ? !b.empty() : a < b; });
    return boost::join(lines, "\n");
}

void TabSeparatedSortedRowOutputFormat::writeSuffix()
{
    tmp_buf.finalize();
    String sorted_lines = sortLines(tmp_buf.str());
    tmp_buf.restart();
    writeString(sorted_lines, real_out);
}

void TabSeparatedSortedRowOutputFormat::finalizeImpl()
{
    tmp_buf.finalize();
    writeString(tmp_buf.str(), real_out);
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
                const RowOutputFormatParams & params,
                const FormatSettings & settings)
            {
                return std::make_shared<TabSeparatedRowOutputFormat>(buf, sample, with_names, with_types, is_raw, params, settings);
            });

            factory.markOutputFormatSupportsParallelFormatting(format_name);
        };

        registerWithNamesAndTypes(is_raw ? "TSVRaw" : "TSV", register_func);
        registerWithNamesAndTypes(is_raw ? "TabSeparatedRaw" : "TabSeparated", register_func);
        if (is_raw)
            registerWithNamesAndTypes("LineAsString", register_func);
    }

    /// Used in tests
    factory.registerOutputFormat("TabSeparatedSorted", [](
        WriteBuffer & buf,
        const Block & sample,
        const RowOutputFormatParams & params,
        const FormatSettings & settings)
    {
        return std::make_shared<TabSeparatedSortedRowOutputFormat>(buf, sample, params, settings);
    });
}

}
