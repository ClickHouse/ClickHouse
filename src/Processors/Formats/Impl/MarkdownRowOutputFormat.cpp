#include <Processors/Formats/Impl/MarkdownRowOutputFormat.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/IDataType.h>

namespace DB
{

MarkdownRowOutputFormat::MarkdownRowOutputFormat(WriteBuffer & out_, const Block & header_, FormatFactory::WriteCallback callback, const FormatSettings & format_settings_)
    : IRowOutputFormat(header_, out_, callback), format_settings(format_settings_) {}

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
    String central_alignment = ":-:|";
    String right_alignment = "-:|";
    for (size_t i = 0; i < columns; ++i)
    {
        if (isInteger(types[i]))
            writeString(right_alignment, out);
        else if (isString(types[i]))
            writeString(left_alignment, out);
        else
            writeString(central_alignment, out);
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
