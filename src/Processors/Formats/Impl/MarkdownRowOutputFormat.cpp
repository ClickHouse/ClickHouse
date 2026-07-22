#include <Processors/Formats/Impl/MarkdownRowOutputFormat.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatFactory.h>
#include <Processors/Port.h>

namespace DB
{

MarkdownRowOutputFormat::MarkdownRowOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_)
    : IRowOutputFormat(header_, out_), format_settings(format_settings_) {}

void MarkdownRowOutputFormat::writePrefix()
{
    const auto & header = getPort(PortKind::Main).getHeader();
    size_t columns = header.columns();

    writeCString("| ", out);
    for (size_t i = 0; i < columns; ++i)
    {
        writeEscapedString(header.safeGetByPosition(i).name, out);
        if (i == (columns - 1))
            writeCString(" |", out);
        else
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
    serialization.serializeTextMarkdown(column, row_num, out, format_settings);
}

void registerOutputFormatMarkdown(FormatFactory & factory);
void registerOutputFormatMarkdown(FormatFactory & factory)
{
    auto registerWithName = [&](const auto & name)
    {
        factory.registerOutputFormat(name, [](
            WriteBuffer & buf,
            const Block & sample,
            const FormatSettings & settings,
            FormatFilterInfoPtr /*format_filter_info*/)
        {
            return std::make_shared<MarkdownRowOutputFormat>(buf, std::make_shared<const Block>(sample), settings);
        });
    };

    registerWithName("Markdown");
    registerWithName("MD");

    factory.markOutputFormatSupportsParallelFormatting("Markdown");
    factory.registerFileExtension("md", "Markdown");

    factory.setDocumentation("MD", Documentation{
        .description = "An alias for the `Markdown` format. See the `Markdown` entry for the full documentation.",
        .related = {"Markdown"}});

    factory.setDocumentation("Markdown", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      | `MD`  |

## Description {#description}

You can export results using [Markdown](https://en.wikipedia.org/wiki/Markdown) format to generate output ready to be pasted into your `.md` files:

The markdown table will be generated automatically and can be used on markdown-enabled platforms, like Github. This format is used only for output.

## Example usage {#example-usage}

```sql
SELECT
    number,
    number * 2
FROM numbers(5)
FORMAT Markdown
```
```results
| number | multiply(number, 2) |
|-:|-:|
| 0 | 0 |
| 1 | 2 |
| 2 | 4 |
| 3 | 6 |
| 4 | 8 |
```

## Format settings {#format-settings}
)DOCS_MD"});
}

}
