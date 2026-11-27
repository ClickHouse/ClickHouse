#include <cmath>

#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Formats/Impl/VerticalRowOutputFormat.h>
#include <Formats/FormatFactory.h>
#include <Formats/PrettyFormatHelpers.h>
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
    /// If we need highlighting.
    else if (color
        && ((format_settings.pretty.highlight_digit_groups && is_number[field_number])
            || format_settings.pretty.highlight_trailing_spaces))
    {
        String serialized_value;
        {
            WriteBufferFromString buf(serialized_value);
            serialization.serializeText(column, row_num, buf, format_settings);
        }

        /// Highlight groups of thousands.
        if (format_settings.pretty.highlight_digit_groups && is_number[field_number])
            serialized_value = highlightDigitGroups(serialized_value);

        /// Highlight trailing spaces.
        if (format_settings.pretty.highlight_trailing_spaces)
            serialized_value = highlightTrailingSpaces(serialized_value);

        out.write(serialized_value.data(), serialized_value.size());
    }
    else
    {
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
}

}
