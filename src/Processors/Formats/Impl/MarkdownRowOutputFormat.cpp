#include <Processors/Formats/Impl/MarkdownRowOutputFormat.h>
#include <IO/WriteHelpers.h>

namespace DB
{

MarkdownRowOutputFormat::MarkdownRowOutputFormat(WriteBuffer & out_, const Block & header_, FormatFactory::WriteCallback callback, const FormatSettings & format_settings_)
    : IRowOutputFormat(header_, out_, callback), format_settings(format_settings_) {}

void MarkdownRowOutputFormat::writePrefix()
{
    const auto & header = getPort(PortKind::Main).getHeader();
    size_t columns = header.columns();

    writeChar('|', out);
    for (size_t i = 0; i < columns; ++i)
    {
        writeEscapedString(header.safeGetByPosition(i).name, out);
        writeChar('|', out);
    }
    writeCString("\n|", out);
    String format = ":-:|";
    for (size_t i = 0; i < columns; ++i)
    {
        writeString(format, out);
    }
    writeChar('\n', out);
}

void MarkdownRowOutputFormat::writeRowStartDelimiter()
{
    writeChar('|', out);
}

void MarkdownRowOutputFormat::writeFieldDelimiter()
{
    writeChar('|', out);
}

void MarkdownRowOutputFormat::writeRowEndDelimiter()
{
    writeCString("|\n", out);
}

void MarkdownRowOutputFormat::writeField(const IColumn & column, const IDataType & type, size_t row_num)
{
    type.serializeAsTextEscaped(column, row_num, out, format_settings);
}

void registerOutputFormatProcessorMarkdown(FormatFactory & factory)
{
    factory.registerOutputFormatProcessor("Markdown", [](
        WriteBuffer & buf,
        const Block & sample,
        FormatFactory::WriteCallback callback,
        const FormatSettings & settings)
    {
        return std::make_shared<MarkdownRowOutputFormat>(buf, sample, callback, settings);
    });
}

}
