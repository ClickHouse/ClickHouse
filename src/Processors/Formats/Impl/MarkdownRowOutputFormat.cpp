#include <Processors/Formats/Impl/MarkdownRowOutputFormat.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/IDataType.h>

namespace DB
{

MarkdownRowOutputFormat::MarkdownRowOutputFormat(WriteBuffer & out_, const Block & header_, const RowOutputFormatParams & params_, const FormatSettings & format_settings_)
    : IRowOutputFormat(header_, out_, params_), format_settings(format_settings_) {}

void MarkdownRowOutputFormat::writePrefix()
{
    const auto & header = getPort(PortKind::Main).getHeader();
    size_t columns = header.columns();

    writeCString("| ", out);
    for (size_t i = 0; i < columns; ++i)
    {
        writeEscapedString(header.safeGetByPosition(i).name, out);
        writeCString(" | ", out);
    }
    writeCString("\n|", out);
    String left_alignment = ":-|";
    String right_alignment = "-:|";
    for (size_t i = 0; i < columns; ++i)
    {
        if (types[i]->shouldAlignRightInPrettyFormats())
            writeString(right_alignment, out);
        else
            writeString(left_alignment, out);
    }
    writeChar('\n', out);
}

void MarkdownRowOutputFormat::writeRowStartDelimiter()
{
    writeCString("| ", out);
}

void MarkdownRowOutputFormat::writeFieldDelimiter()
{
    writeCString(" | ", out);
}

void MarkdownRowOutputFormat::writeRowEndDelimiter()
{
    writeCString(" |\n", out);
}

void MarkdownRowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    serialization.serializeTextEscaped(column, row_num, out, format_settings);
}

void registerOutputFormatProcessorMarkdown(FormatFactory & factory)
{
    factory.registerOutputFormatProcessor("Markdown", [](
        WriteBuffer & buf,
        const Block & sample,
        const RowOutputFormatParams & params,
        const FormatSettings & settings)
    {
        return std::make_shared<MarkdownRowOutputFormat>(buf, sample, params, settings);
    });
}

}
