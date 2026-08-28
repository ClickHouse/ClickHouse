#include <cmath>

#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Formats/Impl/VerticalRowOutputFormat.h>
#include <Formats/FormatFactory.h>
#include <Formats/PrettyFormatHelpers.h>
#include <Formats/EscapingRuleUtils.h>
#include <Formats/registerWithNamesAndTypes.h>
#include <Common/UTF8Helpers.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Processors/Port.h>


namespace DB
{

VerticalRowOutputFormat::VerticalRowOutputFormat(
    WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_)
    : IRowOutputFormat(std::move(header_), out_), format_settings(format_settings_)
{
    color = format_settings.pretty.color == 1 || (format_settings.pretty.color == 2 && format_settings.is_writing_to_terminal);

    const auto & sample = getPort(PortKind::Main).getHeader();
    size_t columns = sample.columns();

    using Widths = std::vector<size_t>;
    Widths name_widths(columns);
    size_t max_name_width = 0;

    names_and_paddings.resize(columns);
    is_number.resize(columns);
    is_json.resize(columns);

    for (size_t i = 0; i < columns; ++i)
    {
        /// Note that number of code points is just a rough approximation of visible string width.
        const String & name = sample.getByPosition(i).name;

        auto [name_cut, width] = truncateName(name,
          format_settings.pretty.max_column_name_width_cut_to,
          format_settings.pretty.max_column_name_width_min_chars_to_cut,
          format_settings.pretty.charset != FormatSettings::Pretty::Charset::UTF8);

        name_widths[i] = width;
        max_name_width = std::max(width, max_name_width);
        if (color)
            names_and_paddings[i] = "\033[1m" + name_cut + ":\033[0m ";
        else
            names_and_paddings[i] = name_cut + ": ";
    }

    for (size_t i = 0; i < columns; ++i)
    {
        size_t new_size = max_name_width - name_widths[i] + names_and_paddings[i].size();
        names_and_paddings[i].resize(new_size, ' ');
        const auto & type = removeNullable(recursiveRemoveLowCardinality(sample.getByPosition(i).type));
        is_number[i] = isNumber(type);
        is_json[i] = isObject(type);
    }
}


void VerticalRowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    if (row_number > format_settings.pretty.max_rows)
        return;

    writeString(names_and_paddings[field_number], out);
    writeValue(column, serialization, row_num);
    writeChar('\n', out);

    ++field_number;
}


void VerticalRowOutputFormat::writeValue(const IColumn & column, const ISerialization & serialization, const size_t row_num) const
{
    if (is_json[field_number])
    {
        constexpr size_t indent = 0;
        serialization.serializeTextJSONPretty(column, row_num, out, format_settings, indent);
    }
    /// Highlighting inspects the whole serialized value, so it has to be materialized first.
    /// This only happens in interactive (color) mode, where values are display-sized.
    else if (color
        && ((format_settings.pretty.highlight_digit_groups && is_number[field_number])
            || format_settings.pretty.highlight_trailing_spaces))
    {
        String serialized_value;
        {
            WriteBufferFromString buf(serialized_value);
            serialization.serializeText(column, row_num, buf, format_settings);
        }

        /// Make non-printable control characters visible instead of being silently swallowed.
        /// Trailing whitespace is highlighted in the same pass: it must be detected on the
        /// pre-replacement bytes, because the replacement turns trailing tabs and newlines into
        /// Control Pictures that `highlightTrailingSpaces` would not recognize.
        if (format_settings.pretty.vertical_display_control_characters)
            serialized_value = replaceControlCharactersWithPictures(std::move(serialized_value), format_settings.pretty.highlight_trailing_spaces);

        /// Highlight groups of thousands.
        if (format_settings.pretty.highlight_digit_groups && is_number[field_number])
            serialized_value = highlightDigitGroups(serialized_value);

        /// Highlight trailing spaces.
        if (format_settings.pretty.highlight_trailing_spaces && !format_settings.pretty.vertical_display_control_characters)
            serialized_value = highlightTrailingSpaces(serialized_value);

        out.write(serialized_value.data(), serialized_value.size());
    }
    else if (format_settings.pretty.vertical_display_control_characters)
    {
        /// Make non-printable control characters visible instead of being silently swallowed.
        /// Stream through a decorator so large values are not fully buffered in memory.
        WriteBufferReplacingControlCharacters buf(out);
        serialization.serializeText(column, row_num, buf, format_settings);
        buf.finalize();
    }
    else
    {
        /// No post-processing: stream directly, keeping the extra memory cost O(1).
        serialization.serializeText(column, row_num, out, format_settings);
    }

    /// Write a tip.
    if (is_number[field_number])
        writeReadableNumberTip(out, column, row_num, format_settings, color);
}


void VerticalRowOutputFormat::writeRowStartDelimiter()
{
    field_number = 0;
    ++row_number;

    if (row_number > format_settings.pretty.max_rows)
        return;

    writeCString("Row ", out);
    writeIntText(row_number, out);
    writeCString(":\n", out);

    size_t width = static_cast<size_t>(log10(row_number + 1)) + 1 + strlen("Row :");
    for (size_t i = 0; i < width; ++i)
        writeCString("─", out);
    writeChar('\n', out);
}


void VerticalRowOutputFormat::writeRowBetweenDelimiter()
{
    if (row_number > format_settings.pretty.max_rows)
        return;

    writeChar('\n', out);
}


void VerticalRowOutputFormat::writeSuffix()
{
    if (row_number > format_settings.pretty.max_rows)
    {
        writeCString("Showed first ", out);
        writeIntText(format_settings.pretty.max_rows, out);
        writeCString(".\n", out);
    }
}

void VerticalRowOutputFormat::writeBeforeTotals()
{
    writeCString("\n", out);
    writeCString("\n", out);
}

void VerticalRowOutputFormat::writeBeforeExtremes()
{
    if (!areTotalsWritten())
        writeCString("\n", out);

    writeCString("\n", out);
}

void VerticalRowOutputFormat::writeMinExtreme(const Columns & columns, size_t row_num)
{
    writeSpecialRow(columns, row_num, "Min");
}

void VerticalRowOutputFormat::writeMaxExtreme(const Columns & columns, size_t row_num)
{
    writeSpecialRow(columns, row_num, "Max");
}

void VerticalRowOutputFormat::writeTotals(const Columns & columns, size_t row_num)
{
    writeSpecialRow(columns, row_num, "Totals");
}

void VerticalRowOutputFormat::writeSpecialRow(const Columns & columns, size_t row_num, const char * title)
{
    row_number = 0;
    field_number = 0;

    size_t columns_size = columns.size();

    writeCString(title, out);
    writeCString(":\n", out);

    size_t width = strlen(title) + 1;
    for (size_t i = 0; i < width; ++i)
        writeCString("─", out);
    writeChar('\n', out);

    for (size_t i = 0; i < columns_size; ++i)
        writeField(*columns[i], *serializations[i], row_num);
}

void registerOutputFormatVertical(FormatFactory & factory);
void registerOutputFormatVertical(FormatFactory & factory)
{
    factory.registerOutputFormat("Vertical", [](
        WriteBuffer & buf,
        const Block & sample,
        const FormatSettings & settings,
        FormatFilterInfoPtr /*format_filter_info*/)
    {
        return std::make_shared<VerticalRowOutputFormat>(buf, std::make_shared<const Block>(sample), settings);
    });

    factory.markOutputFormatSupportsParallelFormatting("Vertical");

    /// Each field is labelled with its column name, written verbatim, so a name that is not valid UTF-8
    /// makes the output not valid UTF-8 either. The values are written through the plain
    /// `serializeText` kind, which writes the `Bool` representations verbatim (see
    /// `settingsLiteralsMayProduceRawBytes`). The text framings reject or base64-encode the output in
    /// these cases (see `checkIfOutputFormatMayProduceRawBytes`). `Vertical` does not write the data
    /// type names.
    factory.registerOutputFormatMayProduceRawBytesChecker(
        "Vertical",
        [](const FormatSettings & settings, const Block & header)
        {
            return headerNamesMayProduceRawBytes(header, /*with_names=*/ true, /*with_types=*/ false)
                || settingsLiteralsMayProduceRawBytes(settings, FormatSettings::EscapingRule::None);
        });

    factory.setDocumentation("Vertical", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

Prints each value on a separate line with the column name specified. This format is convenient for printing just one or a few rows if each row consists of a large number of columns.

Note that [`NULL`](/reference/syntax) is output as `ᴺᵁᴸᴸ` to make it easier to distinguish between the string value `NULL` and no value. JSON columns will be pretty printed, and `NULL` is output as `null`, because it is a valid JSON value and easily distinguishable from `"null"`.

## Example usage {#example-usage}

Example:

```sql
SELECT * FROM t_null FORMAT Vertical
```

```response
Row 1:
──────
x: 1
y: ᴺᵁᴸᴸ
```

By default, non-printable control characters (C0 controls `0x00`–`0x1F` and `DEL` `0x7F`) are displayed as the corresponding Unicode "Control Pictures" (`U+2400`–`U+2421`), so they stay visible instead of being silently swallowed by the terminal. For example, a tab is shown as `␉` and a line feed as `␊`:

```sql
SELECT 'string with \'quotes\' and \t with some special \n characters' AS test FORMAT Vertical
```

```response
Row 1:
──────
test: string with 'quotes' and ␉ with some special ␊ characters
```

To print control characters verbatim instead, disable [`output_format_vertical_display_control_characters`](/operations/settings/formats#output_format_vertical_display_control_characters):

```sql
SELECT 'string with \'quotes\' and \t with some special \n characters' AS test
FORMAT Vertical
SETTINGS output_format_vertical_display_control_characters = 0
```

```response
Row 1:
──────
test: string with 'quotes' and      with some special
 characters
```

This format is only appropriate for outputting a query result, but not for parsing (retrieving data to insert in a table).

## Format settings {#format-settings}
)DOCS_MD"});
}

}
