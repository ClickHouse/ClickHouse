#include <Processors/Formats/Impl/PrettyBlockOutputFormat.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/UTF8Helpers.h>
#include <Common/PODArray.h>
#include <Common/formatReadable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>

#include <algorithm>


namespace DB
{

PrettyBlockOutputFormat::PrettyBlockOutputFormat(
    WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_, bool mono_block_, bool color_)
     : IOutputFormat(header_, out_), format_settings(format_settings_), serializations(header_.getSerializations()), color(color_), mono_block(mono_block_)
{
    /// Decide whether we should print a tip near the single number value in the result.
    if (header_.getColumns().size() == 1)
    {
        /// Check if it is a numeric type, possible wrapped by Nullable or LowCardinality.
        DataTypePtr type = removeNullable(recursiveRemoveLowCardinality(header_.getDataTypes().at(0)));
        if (isNumber(type))
            readable_number_tip = true;
    }
}


/// Evaluate the visible width of the values and column names.
/// Note that number of code points is just a rough approximation of visible string width.
void PrettyBlockOutputFormat::calculateWidths(
    const Block & header, const Chunk & chunk,
    WidthsPerColumn & widths, Widths & max_padded_widths, Widths & name_widths)
{
    size_t num_rows = std::min(chunk.getNumRows(), format_settings.pretty.max_rows);

    /// len(num_rows + total_rows) + len(". ")
    row_number_width = static_cast<size_t>(std::floor(std::log10(num_rows + total_rows))) + 3;

    size_t num_columns = chunk.getNumColumns();
    const auto & columns = chunk.getColumns();

    widths.resize(num_columns);
    max_padded_widths.resize_fill(num_columns);
    name_widths.resize(num_columns);

    /// Calculate widths of all values.
    String serialized_value;
    size_t prefix = 2; // Tab character adjustment
    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto & elem = header.getByPosition(i);
        const auto & column = columns[i];

        widths[i].resize(num_rows);

        for (size_t j = 0; j < num_rows; ++j)
        {
            {
                WriteBufferFromString out_serialize(serialized_value);
                auto serialization = elem.type->getDefaultSerialization();
                serialization->serializeText(*column, j, out_serialize, format_settings);
            }

            /// Avoid calculating width of too long strings by limiting the size in bytes.
            /// Note that it is just an estimation. 4 is the maximum size of Unicode code point in bytes in UTF-8.
            /// But it's possible that the string is long in bytes but very short in visible size.
            /// (e.g. non-printable characters, diacritics, combining characters)
            if (format_settings.pretty.max_value_width)
            {
                size_t max_byte_size = format_settings.pretty.max_value_width * 4;
                if (serialized_value.size() > max_byte_size)
                    serialized_value.resize(max_byte_size);
            }

            widths[i][j] = UTF8::computeWidth(reinterpret_cast<const UInt8 *>(serialized_value.data()), serialized_value.size(), prefix);
            max_padded_widths[i] = std::max<UInt64>(
                max_padded_widths[i],
                std::min<UInt64>({format_settings.pretty.max_column_pad_width, format_settings.pretty.max_value_width, widths[i][j]}));
        }

        /// And also calculate widths for names of columns.
        {
            // name string doesn't contain Tab, no need to pass `prefix`
            name_widths[i] = std::min<UInt64>(format_settings.pretty.max_column_pad_width,
                UTF8::computeWidth(reinterpret_cast<const UInt8 *>(elem.name.data()), elem.name.size()));
            max_padded_widths[i] = std::max<UInt64>(max_padded_widths[i], name_widths[i]);
        }
        prefix += max_padded_widths[i] + 3;
    }
}

namespace
{

/// Grid symbols are used for printing grid borders in a terminal.
/// Defaults values are UTF-8.
struct GridSymbols
{
    const char * bold_left_top_corner = "┏";
    const char * bold_right_top_corner = "┓";
    const char * left_bottom_corner = "└";
    const char * right_bottom_corner = "┘";
    const char * bold_left_separator = "┡";
    const char * left_separator = "├";
    const char * bold_right_separator = "┩";
    const char * right_separator = "┤";
    const char * bold_top_separator = "┳";
    const char * bold_middle_separator = "╇";
    const char * middle_separator = "┼";
    const char * bottom_separator = "┴";
    const char * bold_dash = "━";
    const char * dash = "─";
    const char * bold_bar = "┃";
    const char * bar = "│";
    const char * bold_right_separator_footer = "┫";
    const char * bold_left_separator_footer = "┣";
    const char * bold_middle_separator_footer = "╋";
    const char * bold_left_bottom_corner = "┗";
    const char * bold_right_bottom_corner = "┛";
    const char * bold_bottom_separator = "┻";
};

GridSymbols utf8_grid_symbols;

GridSymbols ascii_grid_symbols {
    "+",
    "+",
    "+",
    "+",
    "+",
    "+",
    "+",
    "+",
    "+",
    "+",
    "+",
    "+",
    "-",
    "-",
    "|",
    "|"
};

}

void PrettyBlockOutputFormat::write(Chunk chunk, PortKind port_kind)
{
    if (total_rows >= format_settings.pretty.max_rows)
    {
        total_rows += chunk.getNumRows();
        return;
    }
    if (mono_block)
    {
        if (port_kind == PortKind::Main)
        {
            if (mono_chunk)
                mono_chunk.append(chunk);
            else
                mono_chunk = std::move(chunk);
            return;
        }

        /// Should be written from writeSuffix()
        assert(!mono_chunk);
    }

    writeChunk(chunk, port_kind);
}

void PrettyBlockOutputFormat::writeChunk(const Chunk & chunk, PortKind port_kind)
{
    auto num_rows = chunk.getNumRows();
    auto num_columns = chunk.getNumColumns();
    const auto & columns = chunk.getColumns();
    const auto & header = getPort(port_kind).getHeader();

    size_t cut_to_width = format_settings.pretty.max_value_width;
    if (!format_settings.pretty.max_value_width_apply_for_single_value && num_rows == 1 && num_columns == 1 && total_rows == 0)
        cut_to_width = 0;

    WidthsPerColumn widths;
    Widths max_widths;
    Widths name_widths;
    calculateWidths(header, chunk, widths, max_widths, name_widths);

    const GridSymbols & grid_symbols
        = format_settings.pretty.charset == FormatSettings::Pretty::Charset::UTF8 ? utf8_grid_symbols : ascii_grid_symbols;

    /// Create separators
    WriteBufferFromOwnString top_separator;
    WriteBufferFromOwnString middle_names_separator;
    WriteBufferFromOwnString middle_values_separator;
    WriteBufferFromOwnString bottom_separator;
    WriteBufferFromOwnString footer_top_separator;
    WriteBufferFromOwnString footer_bottom_separator;

    top_separator << grid_symbols.bold_left_top_corner;
    middle_names_separator << grid_symbols.bold_left_separator;
    middle_values_separator << grid_symbols.left_separator;
    bottom_separator << grid_symbols.left_bottom_corner;
    footer_top_separator << grid_symbols.bold_left_separator_footer;
    footer_bottom_separator << grid_symbols.bold_left_bottom_corner;
    for (size_t i = 0; i < num_columns; ++i)
    {
        if (i != 0)
        {
            top_separator << grid_symbols.bold_top_separator;
            middle_names_separator << grid_symbols.bold_middle_separator;
            middle_values_separator << grid_symbols.middle_separator;
            bottom_separator << grid_symbols.bottom_separator;
            footer_top_separator << grid_symbols.bold_middle_separator_footer;
            footer_bottom_separator << grid_symbols.bold_bottom_separator;
        }

        for (size_t j = 0; j < max_widths[i] + 2; ++j)
        {
            top_separator << grid_symbols.bold_dash;
            middle_names_separator << grid_symbols.bold_dash;
            middle_values_separator << grid_symbols.dash;
            bottom_separator << grid_symbols.dash;
            footer_top_separator << grid_symbols.bold_dash;
            footer_bottom_separator << grid_symbols.bold_dash;
        }
    }
    top_separator << grid_symbols.bold_right_top_corner << "\n";
    middle_names_separator << grid_symbols.bold_right_separator << "\n";
    middle_values_separator << grid_symbols.right_separator << "\n";
    bottom_separator << grid_symbols.right_bottom_corner << "\n";
    footer_top_separator << grid_symbols.bold_right_separator_footer << "\n";
    footer_bottom_separator << grid_symbols.bold_right_bottom_corner << "\n";

    std::string top_separator_s = top_separator.str();
    std::string middle_names_separator_s = middle_names_separator.str();
    std::string middle_values_separator_s = middle_values_separator.str();
    std::string bottom_separator_s = bottom_separator.str();
    std::string footer_top_separator_s = footer_top_separator.str();
    std::string footer_bottom_separator_s = footer_bottom_separator.str();

    if (format_settings.pretty.output_format_pretty_row_numbers)
    {
        /// Write left blank
        writeString(String(row_number_width, ' '), out);
    }
    /// Output the block
    writeString(top_separator_s, out);

    if (format_settings.pretty.output_format_pretty_row_numbers)
    {
        /// Write left blank
        writeString(String(row_number_width, ' '), out);
    }

    /// Names
    auto write_names = [&]() -> void
    {
        writeCString(grid_symbols.bold_bar, out);
        writeCString(" ", out);
        for (size_t i = 0; i < num_columns; ++i)
        {
            if (i != 0)
            {
                writeCString(" ", out);
                writeCString(grid_symbols.bold_bar, out);
                writeCString(" ", out);
            }

            const auto & col = header.getByPosition(i);

            if (color)
                writeCString("\033[1m", out);

            if (col.type->shouldAlignRightInPrettyFormats())
            {
                for (size_t k = 0; k < max_widths[i] - name_widths[i]; ++k)
                    writeChar(' ', out);

                writeString(col.name, out);
            }
            else
            {
                writeString(col.name, out);

                for (size_t k = 0; k < max_widths[i] - name_widths[i]; ++k)
                    writeChar(' ', out);
            }

            if (color)
                writeCString("\033[0m", out);
        }
        writeCString(" ", out);
        writeCString(grid_symbols.bold_bar, out);
        writeCString("\n", out);
    };
    write_names();

    if (format_settings.pretty.output_format_pretty_row_numbers)
    {
        /// Write left blank
        writeString(String(row_number_width, ' '), out);
    }
    writeString(middle_names_separator_s, out);

    for (size_t i = 0; i < num_rows && total_rows + i < format_settings.pretty.max_rows; ++i)
    {
        if (i != 0)
        {
            if (format_settings.pretty.output_format_pretty_row_numbers)
            {
                /// Write left blank
                writeString(String(row_number_width, ' '), out);
            }
            writeString(middle_values_separator_s, out);
        }

        if (format_settings.pretty.output_format_pretty_row_numbers)
        {
            // Write row number;
            auto row_num_string = std::to_string(i + 1 + total_rows) + ". ";

            for (size_t j = 0; j < row_number_width - row_num_string.size(); ++j)
                writeChar(' ', out);
            if (color)
                writeCString("\033[90m", out);
            writeString(row_num_string, out);
            if (color)
                writeCString("\033[0m", out);
        }

        writeCString(grid_symbols.bar, out);

        for (size_t j = 0; j < num_columns; ++j)
        {
            if (j != 0)
                writeCString(grid_symbols.bar, out);
            const auto & type = *header.getByPosition(j).type;
            writeValueWithPadding(
                *columns[j],
                *serializations[j],
                i,
                widths[j].empty() ? max_widths[j] : widths[j][i],
                max_widths[j],
                cut_to_width,
                type.shouldAlignRightInPrettyFormats(),
                isNumber(type));
        }

        writeCString(grid_symbols.bar, out);
        writeReadableNumberTip(chunk);
        writeCString("\n", out);
    }

    if (format_settings.pretty.output_format_pretty_row_numbers)
    {
        /// Write left blank
        writeString(String(row_number_width, ' '), out);
    }

    /// output column names in the footer
    if ((num_rows >= format_settings.pretty.output_format_pretty_display_footer_column_names_min_rows) && format_settings.pretty.output_format_pretty_display_footer_column_names)
    {
        writeString(footer_top_separator_s, out);

        if (format_settings.pretty.output_format_pretty_row_numbers)
        {
            /// Write left blank
            writeString(String(row_number_width, ' '), out);
        }

        /// output header names
        write_names();

        if (format_settings.pretty.output_format_pretty_row_numbers)
        {
            /// Write left blank
            writeString(String(row_number_width, ' '), out);
        }

        writeString(footer_bottom_separator_s, out);
    }
    else
    {
        writeString(bottom_separator_s, out);
    }
    total_rows += num_rows;
}


static String highlightDigitGroups(String source)
{
    if (source.size() <= 4)
        return source;

    bool is_regular_number = true;
    size_t num_digits_before_decimal = 0;
    for (auto c : source)
    {
        if (c == '-' || c == ' ')
            continue;
        if (c == '.')
            break;
        if (c >= '0' && c <= '9')
        {
            ++num_digits_before_decimal;
        }
        else
        {
            is_regular_number = false;
            break;
        }
    }

    if (!is_regular_number || num_digits_before_decimal <= 4)
        return source;

    String result;
    size_t size = source.size();
    result.reserve(2 * size);

    bool before_decimal = true;
    size_t digit_num = 0;
    for (size_t i = 0; i < size; ++i)
    {
        auto c = source[i];
        if (before_decimal && c >= '0' && c <= '9')
        {
            ++digit_num;
            size_t offset = num_digits_before_decimal - digit_num;
            if (offset && offset % 3 == 0)
            {
                result += "\033[4m";
                result += c;
                result += "\033[0m";
            }
            else
            {
                result += c;
            }
        }
        else if (c == '.')
        {
            before_decimal = false;
            result += c;
        }
        else
        {
            result += c;
        }
    }

    return result;
}


void PrettyBlockOutputFormat::writeValueWithPadding(
    const IColumn & column, const ISerialization & serialization, size_t row_num,
    size_t value_width, size_t pad_to_width, size_t cut_to_width, bool align_right, bool is_number)
{
    String serialized_value = " ";
    {
        WriteBufferFromString out_serialize(serialized_value, AppendModeTag());
        serialization.serializeText(column, row_num, out_serialize, format_settings);
    }

    if (cut_to_width && value_width > cut_to_width)
    {
        serialized_value.resize(UTF8::computeBytesBeforeWidth(
            reinterpret_cast<const UInt8 *>(serialized_value.data()), serialized_value.size(), 0, 1 + format_settings.pretty.max_value_width));

        const char * ellipsis = format_settings.pretty.charset == FormatSettings::Pretty::Charset::UTF8 ? "⋯" : "~";
        if (color)
        {
            serialized_value += "\033[31;1m";
            serialized_value += ellipsis;
            serialized_value += "\033[0m";
        }
        else
            serialized_value += ellipsis;

        value_width = format_settings.pretty.max_value_width;
    }
    else
        serialized_value += ' ';

    auto write_padding = [&]()
    {
        if (pad_to_width > value_width)
            for (size_t k = 0; k < pad_to_width - value_width; ++k)
                writeChar(' ', out);
    };

    /// Highlight groups of thousands.
    if (color && is_number && format_settings.pretty.highlight_digit_groups)
        serialized_value = highlightDigitGroups(serialized_value);

    if (align_right)
    {
        write_padding();
        out.write(serialized_value.data(), serialized_value.size());
    }
    else
    {
        out.write(serialized_value.data(), serialized_value.size());
        write_padding();
    }
}


void PrettyBlockOutputFormat::consume(Chunk chunk)
{
    write(std::move(chunk), PortKind::Main);
}

void PrettyBlockOutputFormat::consumeTotals(Chunk chunk)
{
    total_rows = 0;
    writeCString("\nTotals:\n", out);
    write(std::move(chunk), PortKind::Totals);
}

void PrettyBlockOutputFormat::consumeExtremes(Chunk chunk)
{
    total_rows = 0;
    writeCString("\nExtremes:\n", out);
    write(std::move(chunk), PortKind::Extremes);
}


void PrettyBlockOutputFormat::writeMonoChunkIfNeeded()
{
    if (mono_chunk)
    {
        writeChunk(mono_chunk, PortKind::Main);
        mono_chunk.clear();
    }
}

void PrettyBlockOutputFormat::writeSuffix()
{
    writeMonoChunkIfNeeded();

    if (total_rows >= format_settings.pretty.max_rows)
    {
        writeCString("  Showed first ", out);
        writeIntText(format_settings.pretty.max_rows, out);
        writeCString(".\n", out);
    }
}

void PrettyBlockOutputFormat::writeReadableNumberTip(const Chunk & chunk)
{
    const auto & columns = chunk.getColumns();
    auto is_single_number = readable_number_tip && chunk.getNumRows() == 1 && chunk.getNumColumns() == 1;
    if (!is_single_number)
        return;

    if (columns[0]->isNullAt(0))
        return;

    auto value = columns[0]->getFloat64(0);
    auto threshold = format_settings.pretty.output_format_pretty_single_large_number_tip_threshold;

    if (threshold && isFinite(value) && abs(value) > threshold)
    {
        if (color)
            writeCString("\033[90m", out);
        writeCString(" -- ", out);
        formatReadableQuantity(value, out, 2);
        if (color)
            writeCString("\033[0m", out);
    }
}

void registerOutputFormatPretty(FormatFactory & factory)
{
    registerPrettyFormatWithNoEscapesAndMonoBlock<PrettyBlockOutputFormat>(factory, "Pretty");
}

}
