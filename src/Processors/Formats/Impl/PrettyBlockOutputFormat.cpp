#include <Processors/Formats/Impl/PrettyBlockOutputFormat.h>
#include <Processors/Formats/Impl/VerticalRowOutputFormat.h>
#include <Processors/Port.h>
#include <Formats/FormatFactory.h>
#include <Formats/PrettyFormatHelpers.h>
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
#include <cstddef>
#include <vector>

namespace DB
{

PrettyBlockOutputFormat::PrettyBlockOutputFormat(
    WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_, Style style_, bool mono_block_, bool color_)
     : IOutputFormat(header_, out_), format_settings(format_settings_), serializations(header_.getSerializations()), style(style_), mono_block(mono_block_), color(color_)
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

bool PrettyBlockOutputFormat::cutInTheMiddle(size_t row_num, size_t num_rows, size_t max_rows)
{
    return num_rows > max_rows
        && !(row_num < (max_rows + 1) / 2
            || row_num >= num_rows - max_rows / 2);
}

void PrettyBlockOutputFormat::findWidth(size_t & width, size_t & max_padded_width, String & serialized_value, bool split_by_lines, bool & out_has_newlines, size_t prefix)
{
    /// Avoid calculating width of too long strings by limiting their size in bytes.
    /// Note that it is just an estimation. 4 is the maximum size of Unicode code point in bytes in UTF-8.
    /// But it's possible that the string is long in bytes but very short in visible size.
    /// (e.g. non-printable characters, diacritics, combining characters)
    if (format_settings.pretty.max_value_width)
    {
        size_t max_byte_size = format_settings.pretty.max_value_width * 4;
        if (serialized_value.size() > max_byte_size)
            serialized_value.resize(max_byte_size);
    }

    size_t start_from_offset = 0;
    size_t next_offset = 0;
    while (start_from_offset < serialized_value.size())
    {
        if (split_by_lines)
        {
            const char * end = serialized_value.data() + serialized_value.size();
            const char * next_nl = find_first_symbols<'\n'>(serialized_value.data() + start_from_offset, end);
            if (next_nl < end)
                out_has_newlines = true;
            size_t fragment_end_offset = next_nl - serialized_value.data();
            next_offset = fragment_end_offset;
        }
        else
        {
            next_offset = serialized_value.size();
        }

        width = std::max(
            width,
            UTF8::computeWidth(reinterpret_cast<const UInt8 *>(serialized_value.data() + start_from_offset), next_offset - start_from_offset, prefix));

        max_padded_width = std::max<UInt64>(
            max_padded_width,
            std::min<UInt64>({format_settings.pretty.max_column_pad_width, format_settings.pretty.max_value_width, width}));

        start_from_offset = next_offset;
        if (start_from_offset < serialized_value.size())
            ++start_from_offset;
    }
}

/// Evaluate the visible width of the values and column names.
/// Note that number of code points is just a rough approximation of visible string width.
void PrettyBlockOutputFormat::calculateWidths(
    const Block & header, const Chunk & chunk, bool split_by_lines, bool & out_has_newlines,
    WidthsPerColumn & widths, Widths & max_padded_widths, Widths & name_widths, Strings & names,
    std::vector<Strings> & subcolumn_names, std::vector<WidthsPerColumn> & subcolumn_widths, std::vector<Widths> & max_subcolumn_widths, std::vector<Widths> & subcolumn_name_widths)
{
    size_t num_rows = chunk.getNumRows();
    size_t num_displayed_rows = std::min<size_t>(num_rows, format_settings.pretty.max_rows);

    /// len(num_rows + total_rows) + len(". ")
    row_number_width = static_cast<size_t>(std::floor(std::log10(num_rows + total_rows))) + 3;

    size_t num_columns = chunk.getNumColumns();
    const auto & columns = chunk.getColumns();

    widths.resize(num_columns);
    max_padded_widths.resize_fill(num_columns);
    subcolumn_widths.resize(num_columns);
    max_subcolumn_widths.resize(num_columns);
    name_widths.resize(num_columns);
    names.resize(num_columns);
    subcolumn_name_widths.resize(num_columns);
    subcolumn_names.resize(num_columns);

    /// Calculate the widths of all values.
    String serialized_value;
    size_t prefix = row_number_width + (style == Style::Space ? 1 : 2); // Tab character adjustment
    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto & elem = header.getByPosition(i);
        const auto & elem_names = elem.type->getSubcolumnNames();
        const auto num_subcolumns = elem_names.size();
        const auto & column = columns[i];

        widths[i].resize_fill(num_displayed_rows);

        if (num_subcolumns == 0 || elem.type->isNullable() || (elem.type->getTypeId() == TypeIndex::Array) || (elem.type->getTypeId() == TypeIndex::Variant))
        {
            for (size_t j = 0; j < num_rows; ++j)
            {
                if (cutInTheMiddle(j, num_rows, format_settings.pretty.max_rows))
                    continue;

                {
                    WriteBufferFromString out_serialize(serialized_value);
                    auto serialization = elem.type->getDefaultSerialization();
                    serialization->serializeText(*column, j, out_serialize, format_settings);
                }

                size_t row_num = j;
                if ((num_rows > format_settings.pretty.max_rows) && (row_num >= (num_rows - format_settings.pretty.max_rows / 2)))
                {
                    row_num -= (num_rows - format_settings.pretty.max_rows);
                }

                findWidth(widths[i][row_num], max_padded_widths[i], serialized_value, split_by_lines, out_has_newlines, prefix);
            }
        }
        else
        {
            subcolumn_widths[i].resize(num_subcolumns);
            max_subcolumn_widths[i].resize_fill(num_subcolumns);
            subcolumn_names[i].resize(num_subcolumns);
            subcolumn_name_widths[i].resize(num_subcolumns);

            for (size_t k = 0; k < num_subcolumns; ++k)
            {
                subcolumn_widths[i][k].resize_fill(num_displayed_rows);
                for (size_t j = 0; j < num_rows; ++j)
                {
                    if (cutInTheMiddle(j, num_rows, format_settings.pretty.max_rows))
                        continue;

                    {
                        auto serialization = elem.type->getDefaultSerialization();
                        WriteBufferFromString out_serialize_temp(serialized_value);
                        auto subcolumn_serialization = elem.type->getSubcolumnSerialization(elem_names[k], serialization);
                        const auto & subcolumn = elem.type->getSubcolumn(elem_names[k], column);
                        subcolumn_serialization->serializeText(*subcolumn, j, out_serialize_temp, format_settings);
                    }

                    size_t row_num = j;
                    if ((num_rows > format_settings.pretty.max_rows) && (row_num >= (num_rows - format_settings.pretty.max_rows / 2)))
                    {
                        row_num -= (num_rows - format_settings.pretty.max_rows);
                    }

                    findWidth(subcolumn_widths[i][k][row_num], max_subcolumn_widths[i][k], serialized_value, split_by_lines, out_has_newlines, prefix);
                }

                // Calculate the widths for the names of subcolumns.
                {
                    auto [name, width] = truncateName(elem_names[k],
                        format_settings.pretty.max_column_name_width_cut_to
                            ? std::max<UInt64>(max_subcolumn_widths[i][k], format_settings.pretty.max_column_name_width_cut_to)
                            : 0,
                        format_settings.pretty.max_column_name_width_min_chars_to_cut,
                        format_settings.pretty.charset != FormatSettings::Pretty::Charset::UTF8);

                    subcolumn_names[i][k] = std::move(name);
                    subcolumn_name_widths[i][k] = std::min<UInt64>(format_settings.pretty.max_column_pad_width, width);
                    max_subcolumn_widths[i][k] = std::max<UInt64>(max_subcolumn_widths[i][k], subcolumn_name_widths[i][k]);

                    max_padded_widths[i] += max_subcolumn_widths[i][k];

                    // Add padding
                    max_padded_widths[i] += (k ? 3 : 0);
                }
            }
        }

        // Also, calculate the widths for the names of columns.
        {
            auto [name, width] = truncateName(elem.name,
                format_settings.pretty.max_column_name_width_cut_to
                    ? std::max<UInt64>(max_padded_widths[i], format_settings.pretty.max_column_name_width_cut_to)
                    : 0,
                format_settings.pretty.max_column_name_width_min_chars_to_cut,
                format_settings.pretty.charset != FormatSettings::Pretty::Charset::UTF8);

            names[i] = std::move(name);
            name_widths[i] = std::min<UInt64>(format_settings.pretty.max_column_pad_width, width);

            // Add width if column name width is bigger than all subcolumns' max widths combined
            if (!elem.type->isNullable() && (elem.type->getTypeId() != TypeIndex::Array) && (elem.type->getTypeId() != TypeIndex::Variant) && num_subcolumns != 0 && name_widths[i] > max_padded_widths[i])
            {
                auto width_difference = name_widths[i] - max_padded_widths[i];
                auto width_to_add = width_difference/num_subcolumns;
                auto remainder = width_difference - width_to_add*num_subcolumns;
                for (size_t k = 0; k < num_subcolumns; ++k)
                {
                    max_subcolumn_widths[i][k] += width_to_add;
                    if (remainder)
                    {
                        max_subcolumn_widths[i][k]++;
                        --remainder;
                    }
                }
            }

            max_padded_widths[i] = std::max<UInt64>(max_padded_widths[i], name_widths[i]);
        }
        prefix += max_padded_widths[i] + 3;
    }
}

void PrettyBlockOutputFormat::write(Chunk chunk, PortKind port_kind)
{
    if (total_rows >= format_settings.pretty.max_rows)
    {
        total_rows += chunk.getNumRows();
        return;
    }
    if (mono_block || format_settings.pretty.squash_consecutive_ms)
    {
        if (port_kind == PortKind::Main)
        {
            if (format_settings.pretty.squash_consecutive_ms && !mono_block && !thread)
            {
                thread.emplace([this, thread_group = CurrentThread::getGroup()]
                {
                    ThreadGroupSwitcher switcher(thread_group, "PrettyWriter");

                    writingThread();
                });
            }

            if (mono_chunk)
                mono_chunk.append(chunk);
            else
                mono_chunk = std::move(chunk);
            mono_chunk_condvar.notify_one();
            return;
        }

        /// Should be written from writeSuffix()
        assert(!mono_chunk);
    }

    writeChunk(chunk, port_kind);
}

void PrettyBlockOutputFormat::writingThread()
{
    std::unique_lock lock(writing_mutex);
    Stopwatch watch(CLOCK_MONOTONIC_COARSE);
    while (!finish)
    {
        if (std::cv_status::timeout == mono_chunk_condvar.wait_for(lock, std::chrono::milliseconds(format_settings.pretty.squash_consecutive_ms))
            || watch.elapsedMilliseconds() > format_settings.pretty.squash_max_wait_ms)
        {
            writeMonoChunkIfNeeded();
            watch.restart();
        }
    }
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
    Strings names;

    std::vector<Strings> subcolumn_names;
    std::vector<WidthsPerColumn> subcolumn_widths;
    std::vector<Widths> max_subcolumn_widths;
    std::vector<Widths> subcolumn_name_widths;

    bool has_newlines = false;
    calculateWidths(header, chunk, format_settings.pretty.multiline_fields, has_newlines, widths, max_widths, name_widths, names,
                                 subcolumn_names, subcolumn_widths, max_subcolumn_widths, subcolumn_name_widths);

    size_t table_width = 0;
    for (size_t width : max_widths)
        table_width += width;

    /// Fallback to Vertical format if:
    /// enabled by the settings, this is the first chunk, the number of rows is small enough,
    /// either the table width is larger than the max_value_width or any of the values contain a newline.
    if (format_settings.pretty.fallback_to_vertical
        && displayed_rows == 0
        && num_rows <= format_settings.pretty.fallback_to_vertical_max_rows_per_chunk
        && num_columns >= format_settings.pretty.fallback_to_vertical_min_columns
        && (table_width >= format_settings.pretty.fallback_to_vertical_min_table_width || has_newlines))
    {
        use_vertical_format = true;
    }

    if (use_vertical_format)
    {
        if (!vertical_format_fallback)
        {
            vertical_format_fallback = std::make_unique<VerticalRowOutputFormat>(out, header, format_settings);
            vertical_format_fallback->writePrefixIfNeeded();
        }

        for (size_t i = 0; i < num_rows && displayed_rows < format_settings.pretty.max_rows; ++i)
        {
            if (i != 0)
                vertical_format_fallback->writeRowBetweenDelimiter();
            vertical_format_fallback->writeRow(columns, i);
            ++displayed_rows;
        }

        return;
    }

    /// Create separators

    String left_blank;
    if (format_settings.pretty.row_numbers)
        left_blank.assign(row_number_width, ' ');

    String header_begin;    /// ┏━━┳━━━┓
    String header_middle;   /// ┣━━╋━━━┫
    String header_end;      /// ┡━━╇━━━┩
    String rows_separator;  /// ├──┼───┤
    String rows_end;        /// └──┴───┘
    String footer_begin;    /// ┢━━╈━━━┪
    String footer_end;      /// ┗━━┻━━━┛

    bool unicode = format_settings.pretty.charset == FormatSettings::Pretty::Charset::UTF8;
    using GridPart = std::array<std::string_view, 4>;
    using Grid = std::array<GridPart, 8>;

    constexpr Grid utf8_grid
    {
        GridPart{"┏", "━", "┳", "┓"},
        GridPart{"┡", "━", "╇", "┩"},
        GridPart{"├", "─", "┼", "┤"},
        GridPart{"└", "─", "┴", "┘"},
        GridPart{"┢", "━", "╈", "┪"},
        GridPart{"┗", "━", "┻", "┛"},
        GridPart{"┌", "─", "┬", "┐"},
        GridPart{"┣", "━", "╋", "┫"},
    };

    constexpr Grid ascii_grid
    {
        GridPart{"+", "-", "+", "+"},
        GridPart{"+", "-", "+", "+"},
        GridPart{"+", "-", "+", "+"},
        GridPart{"+", "-", "+", "+"},
        GridPart{"+", "-", "+", "+"},
        GridPart{"+", "-", "+", "+"},
        GridPart{"+", "-", "+", "+"},
        GridPart{"+", "-", "+", "+"},
    };

    Grid grid = unicode ? utf8_grid : ascii_grid;

    std::string_view vertical_bold_bar   = unicode ? "┃" : "|";
    std::string_view vertical_bar        = unicode ? "│" : "|";
    std::string_view horizontal_bar      = unicode ? "─" : "-";

    if (style == Style::Full)
    {
        header_begin = left_blank;
        header_middle = left_blank;
        header_end = left_blank;
        rows_separator = left_blank;
        rows_end = left_blank;
        footer_begin = left_blank;
        footer_end = left_blank;

        WriteBufferFromString header_begin_out(header_begin, AppendModeTag{});
        WriteBufferFromString header_middle_out(header_middle, AppendModeTag{});
        WriteBufferFromString header_end_out(header_end, AppendModeTag{});
        WriteBufferFromString rows_separator_out(rows_separator, AppendModeTag{});
        WriteBufferFromString rows_end_out(rows_end, AppendModeTag{});
        WriteBufferFromString footer_begin_out(footer_begin, AppendModeTag{});
        WriteBufferFromString footer_end_out(footer_end, AppendModeTag{});

        header_begin_out    << grid[0][0];
        header_middle_out   << grid[7][0];
        header_end_out      << grid[1][0];
        rows_separator_out  << grid[2][0];
        rows_end_out        << grid[3][0];
        footer_begin_out    << grid[4][0];
        footer_end_out      << grid[5][0];

        for (size_t i = 0; i < num_columns; ++i)
        {
            if (i!=0)
            {
                header_begin_out    << grid[0][2];
                header_middle_out   << grid[7][2];
                header_end_out      << grid[1][2];
                rows_separator_out  << grid[2][2];
                rows_end_out        << grid[3][2];
                footer_begin_out    << grid[4][2];
                footer_end_out      << grid[5][2];
            }

            if (subcolumn_names[i].empty())
            {
                for (size_t k = 0; k < max_widths[i] + 2; ++k)
                {
                    header_begin_out    << grid[0][1];
                    header_middle_out   << grid[7][1];
                    header_end_out      << grid[1][1];
                    rows_separator_out  << grid[2][1];
                    rows_end_out        << grid[3][1];
                    footer_begin_out    << grid[4][1];
                    footer_end_out      << grid[5][1];
                }
            }
            else
            {
                for (size_t j = 0; j < subcolumn_names[i].size(); ++j)
                {
                    if (j!=0)
                    {
                        header_begin_out    << grid[0][1];
                        header_middle_out   << grid[0][2];
                        header_end_out      << grid[1][2];
                        rows_separator_out  << grid[2][2];
                        rows_end_out        << grid[3][2];
                        footer_begin_out    << grid[4][2];
                        footer_end_out      << grid[5][2];
                    }

                    for (size_t k = 0; k < max_subcolumn_widths[i][j] + 2; ++k)
                    {
                        header_begin_out    << grid[0][1];
                        header_middle_out   << grid[7][1];
                        header_end_out      << grid[1][1];
                        rows_separator_out  << grid[2][1];
                        rows_end_out        << grid[3][1];
                        footer_begin_out    << grid[4][1];
                        footer_end_out      << grid[5][1];
                    }
                }
            }
        }

        header_begin_out    << grid[0][3] << "\n";
        header_middle_out   << grid[7][3] << "\n";
        header_end_out      << grid[1][3] << "\n";
        rows_separator_out  << grid[2][3] << "\n";
        rows_end_out        << grid[3][3] << "\n";
        footer_begin_out    << grid[4][3] << "\n";
        footer_end_out      << grid[5][3] << "\n";
    }
    else if (style == Style::Compact)
    {
        rows_end = left_blank;
        WriteBufferFromString rows_end_out(rows_end, AppendModeTag{});
        rows_end_out << grid[3][0];
        for (size_t i = 0; i < num_columns; ++i)
        {
            if (subcolumn_names[i].empty())
            {
                if (i!=0)
                {
                    rows_end_out << grid[3][2];
                }

                for (size_t k = 0; k < max_widths[i] + 2; ++k)
                    rows_end_out << grid[3][1];
            }
            else
            {
                for (size_t j = 0; j < subcolumn_names[i].size(); ++j)
                {
                    if (i != 0 || j!=0)
                        rows_end_out << grid[3][2];

                    for (size_t k = 0; k < max_subcolumn_widths[i][j] + 2; ++k)
                        rows_end_out << grid[3][1];
                }
            }
        }
        rows_end_out << grid[3][3] << "\n";
    }
    else if (style == Style::Space)
    {
        header_end = "\n";
        footer_begin = "\n";
        footer_end = "\n";
    }

    ///    ─ ─ ─ ─
    String vertical_filler = left_blank;

    {
        size_t vertical_filler_size = 0;
        WriteBufferFromString vertical_filler_out(vertical_filler, AppendModeTag{});

        for (size_t i = 0; i < num_columns; ++i)
            vertical_filler_size += max_widths[i] + 3;

        if (style == Style::Space)
            vertical_filler_size -= 2;
        else
            vertical_filler_size += 1;

        for (size_t i = 0; i < vertical_filler_size; ++i)
            vertical_filler_out << (i % 2 ? " " : horizontal_bar);

        vertical_filler_out << "\n";
    }

    ///    ┃ name ┃
    ///    ┌─name─┐
    ///    └─name─┘
    ///      name
    auto write_names = [&](bool is_top) -> void
    {
        writeString(left_blank, out);

        if (style == Style::Full)
            out << vertical_bold_bar << " ";
        else if (style == Style::Compact)
            out << grid[is_top ? 6 : 3][0] << horizontal_bar;
        else if (style == Style::Space)
            out << " ";

        for (size_t i = 0; i < num_columns; ++i)
        {
            if (i != 0)
            {
                if (style == Style::Full)
                    out << " " << vertical_bold_bar << " ";
                else if (style == Style::Compact)
                    out << horizontal_bar << grid[is_top ? 6 : 3][2] << horizontal_bar;
                else if (style == Style::Space)
                    out << "   ";
            }

            const auto & col = header.getByPosition(i);

            auto write_value = [&]
            {
                if (color)
                    out << "\033[1m";
                writeString(names[i], out);
                if (color)
                    out << "\033[0m";
            };

            auto write_padding = [&]
            {
                for (size_t k = 0; k < max_widths[i] - name_widths[i]; ++k)
                {
                    if (style == Style::Compact)
                        out << horizontal_bar;
                    else
                        out << " ";
                }
            };

            if (col.type->shouldAlignRightInPrettyFormats())
            {
                write_padding();
                write_value();
            }
            else
            {
                write_value();
                write_padding();
            }
        }
        if (style == Style::Full)
            out << " " << vertical_bold_bar;
        else if (style == Style::Compact)
            out << horizontal_bar << grid[is_top ? 6 : 3][3];

        out << "\n";
    };

    auto write_subcolumn_names = [&]() -> void
    {
        writeString(left_blank, out);

        if (style == Style::Full)
            out << vertical_bold_bar << " ";
        else if (style == Style::Compact)
            out << grid[2][0] << horizontal_bar;
        else if (style == Style::Space)
            out << " ";

        for (size_t i = 0; i < num_columns; ++i)
        {
            if (i != 0)
            {
                if (style == Style::Full)
                    out << vertical_bold_bar << " ";
                else if (style == Style::Compact)
                    out << grid[2][2] << horizontal_bar;
                else if (style == Style::Space)
                    out << "  ";
            }

            auto write_padding = [&] (auto padding_width)
            {
                for (size_t k = 0; k < padding_width; ++k)
                {
                    if (style == Style::Compact)
                        out << horizontal_bar;
                    else
                        out << " ";
                }
            };

            const auto & elem = header.getByPosition(i);
            const auto & elem_names = elem.type->getSubcolumnNames();
            const auto num_subcolumns = elem.type->getSubcolumnNames().size();

            if (num_subcolumns == 0 || elem.type->isNullable() || (elem.type->getTypeId() == TypeIndex::Array) || (elem.type->getTypeId() == TypeIndex::Variant))
            {
                write_padding(max_widths[i]+1);
            }
            else
            {
                // calculate widths in the above calculateWidths function since this will affect the main column width
                for (size_t j = 0; j < num_subcolumns; ++j)
                {
                    if (j != 0)
                    {
                        if (style == Style::Full)
                            out << vertical_bold_bar << " ";
                        else if (style == Style::Compact)
                            out << grid[6][2] << horizontal_bar;
                        else if (style == Style::Space)
                            out << "  ";
                    }

                    auto write_value = [&]
                    {
                        if (color)
                            out << "\033[1m";
                        writeString(subcolumn_names[i][j], out);
                        if (color)
                            out << "\033[0m";
                    };

                    if (elem.type->shouldAlignRightInPrettyFormats())
                    {
                        write_padding(max_subcolumn_widths[i][j] - subcolumn_name_widths[i][j] + 1);
                        write_value();
                    }
                    else
                    {
                        write_value();
                        write_padding(max_subcolumn_widths[i][j] - subcolumn_name_widths[i][j] + 1);
                    }
                }
            }

        }
        if (style == Style::Full)
                out << vertical_bold_bar;
        else if (style == Style::Compact)
            out << grid[2][3];

        out << "\n";
    };

    writeString(header_begin, out);
    write_names(true);
    writeString(header_middle, out);
    write_subcolumn_names();
    writeString(header_end, out);

    bool vertical_filler_written = false;
    size_t displayed_row = 0;

    std::vector<std::vector<std::optional<String>>> serialized_values(num_columns);
    std::vector<std::vector<size_t>> offsets_inside_serialized_values(num_columns);
    std::vector<std::optional<String>> serialized_values_without_subcolumn(num_columns);
    std::vector<size_t> offsets_inside_serialized_values_n(num_columns);

    for (size_t i = 0; i < num_rows && displayed_rows < format_settings.pretty.max_rows; ++i)
    {
        if (cutInTheMiddle(i, num_rows, format_settings.pretty.max_rows))
        {
            if (!vertical_filler_written)
            {
                writeString(rows_separator, out);
                writeString(vertical_filler, out);
                vertical_filler_written = true;
            }
        }
        else
        {
            if (i != 0)
                writeString(rows_separator, out);

            /// A value can span multiple lines, and we want to iterate over them.
            for (size_t j = 0; j < num_columns; ++j)
            {
                const auto num_subcolumns = subcolumn_names[j].size();
                serialized_values[j].resize(num_subcolumns);
                offsets_inside_serialized_values[j].resize(num_subcolumns);

                serialized_values_without_subcolumn[j].reset();
                offsets_inside_serialized_values_n[j] = 0;

                for (size_t k = 0; k < num_subcolumns; ++k)
                {
                    serialized_values[j][k].reset();
                    offsets_inside_serialized_values[j][k] = 0;
                }
            }

            /// As long as there are lines in any of fields, output a line.
            bool first_line = true;
            while (true)
            {
                if (format_settings.pretty.row_numbers)
                {
                    if (first_line)
                    {
                        /// Write row number;
                        auto row_num_string = std::to_string(i + 1 + total_rows) + ". ";
                        for (size_t j = 0; j < row_number_width - row_num_string.size(); ++j)
                            writeChar(' ', out);

                        if (color)
                            out << "\033[90m";
                        writeString(row_num_string, out);
                        if (color)
                            out << "\033[0m";

                        first_line = false;
                    }
                    else
                        out << left_blank;
                }

                bool all_lines_printed = true;
                for (size_t j = 0; j < num_columns; ++j)
                {
                    const auto & type = *header.getByPosition(j).type;
                    const auto & elem_names = type.getSubcolumnNames();

                    if (elem_names.empty() || type.isNullable() || (type.getTypeId() == TypeIndex::Array) || (type.getTypeId() == TypeIndex::Variant))
                    {
                        if (style != Style::Space)
                            out << vertical_bar;
                        else if (j != 0)
                            out << " ";

                        writeValueWithPadding(
                                *columns[j],
                                *serializations[j],
                                i,
                                format_settings.pretty.multiline_fields, serialized_values_without_subcolumn[j], offsets_inside_serialized_values_n[j],
                                widths[j].empty() ? max_widths[j] : widths[j][displayed_row],
                                max_widths[j],
                                cut_to_width,
                                type.shouldAlignRightInPrettyFormats(),
                                isNumber(type));

                        if (offsets_inside_serialized_values_n[j] != serialized_values_without_subcolumn[j]->size())
                            all_lines_printed = false;
                    }
                    else
                    {
                        for (size_t k = 0; k < elem_names.size(); ++k)
                        {
                            if (style != Style::Space)
                                out << vertical_bar;
                            else if (k!=0 || j!= 0)
                                out << " ";

                            auto subcolumn_serialization = type.getSubcolumnSerialization(elem_names[k], serializations[j]);
                            const auto & subcolumn = type.getSubcolumn(elem_names[k], columns[j]);
                            const auto & subcolumn_type = type.tryGetSubcolumnType(elem_names[k]);

                            // if (subcolumn_type)
                                writeValueWithPadding(
                                    *subcolumn,
                                    *subcolumn_serialization,
                                    i,
                                    format_settings.pretty.multiline_fields, serialized_values[j][k], offsets_inside_serialized_values[j][k],
                                    subcolumn_widths[j][k].empty() ? max_subcolumn_widths[j][k] : subcolumn_widths[j][k][displayed_row],
                                    max_subcolumn_widths[j][k],
                                    cut_to_width,
                                    subcolumn_type->shouldAlignRightInPrettyFormats(),
                                    isNumber(*subcolumn_type));

                            if (offsets_inside_serialized_values[j][k] != serialized_values[j][k]->size())
                                all_lines_printed = false;
                        }
                    }
                }

                if (style != Style::Space)
                    out << vertical_bar;

                if (readable_number_tip)
                    writeReadableNumberTipIfSingleValue(out, chunk, format_settings, color);

                out << "\n";
                if (all_lines_printed)
                    break;
            }

            ++displayed_row;
            ++displayed_rows;
        }
    }

    /// output column names in the footer
    if ((num_rows >= format_settings.pretty.display_footer_column_names_min_rows) && format_settings.pretty.display_footer_column_names)
    {
        writeString(footer_begin, out);
        write_names(false);
        writeString(footer_end, out);
    }
    else
    {
        ///    └──────┘
        writeString(rows_end, out);
    }
    total_rows += num_rows;
}


void PrettyBlockOutputFormat::writeValueWithPadding(
    const IColumn & column, const ISerialization & serialization, size_t row_num,
    bool split_by_lines, std::optional<String> & serialized_value, size_t & start_from_offset,
    size_t value_width, size_t pad_to_width, size_t cut_to_width, bool align_right, bool is_number)
{
    if (!serialized_value)
    {
        serialized_value = String();
        start_from_offset = 0;
        WriteBufferFromString out_serialize(*serialized_value);
        serialization.serializeText(column, row_num, out_serialize, format_settings);
    }

    size_t prefix = row_number_width + (style == Style::Space ? 1 : 2);

    bool is_continuation = start_from_offset > 0 && start_from_offset < serialized_value->size();

    String serialized_fragment;
    if (start_from_offset == serialized_value->size())
    {
        /// Only padding, nothing remains.
        value_width = 0;
    }
    else if (split_by_lines)
    {
        const char * end = serialized_value->data() + serialized_value->size();
        const char * next_nl = find_first_symbols<'\n'>(serialized_value->data() + start_from_offset, end);
        size_t fragment_end_offset = next_nl - serialized_value->data();
        serialized_fragment = serialized_value->substr(start_from_offset, fragment_end_offset - start_from_offset);
        value_width = UTF8::computeWidth(reinterpret_cast<const UInt8 *>(serialized_fragment.data()), serialized_fragment.size(), prefix);
        start_from_offset = fragment_end_offset;
    }
    else
    {
        serialized_fragment = *serialized_value;
        start_from_offset = serialized_value->size();
    }

    /// Highlight groups of thousands.
    if (color && is_number && format_settings.pretty.highlight_digit_groups)
        serialized_fragment = highlightDigitGroups(serialized_fragment);

    /// Highlight trailing spaces.
    if (color && format_settings.pretty.highlight_trailing_spaces)
        serialized_fragment = highlightTrailingSpaces(serialized_fragment);

    const char * ellipsis = format_settings.pretty.charset == FormatSettings::Pretty::Charset::UTF8 ? "⋯" : "~";
    const char * line_feed = format_settings.pretty.charset == FormatSettings::Pretty::Charset::UTF8 ? "↴" : "\\";
    const char * line_continuation = format_settings.pretty.charset == FormatSettings::Pretty::Charset::UTF8 ? "↳" : ">";

    bool is_cut = false;
    if (cut_to_width && value_width > cut_to_width)
    {
        is_cut = true;
        serialized_fragment.resize(UTF8::computeBytesBeforeWidth(
            reinterpret_cast<const UInt8 *>(serialized_fragment.data()), serialized_fragment.size(), prefix, format_settings.pretty.max_value_width));

        if (color)
        {
            serialized_fragment += "\033[31;1m";
            serialized_fragment += ellipsis;
            serialized_fragment += "\033[0m";
        }
        else
            serialized_fragment += ellipsis;

        value_width = format_settings.pretty.max_value_width;
    }

    auto write_padding = [&]()
    {
        if (pad_to_width > value_width)
            for (size_t k = 0; k < pad_to_width - value_width; ++k)
                writeChar(' ', out);
    };

    if (is_continuation)
    {
        if (color)
            out << "\033[90m";
        writeCString(line_continuation, out);
        if (color)
            out << "\033[0m";
    }
    else
        out.write(' ');

    if (align_right)
    {
        write_padding();
        out.write(serialized_fragment.data(), serialized_fragment.size());
    }
    else
    {
        out.write(serialized_fragment.data(), serialized_fragment.size());
        write_padding();
    }

    if (start_from_offset != serialized_value->size())
    {
        if (color)
            out << "\033[90m";
        writeCString(line_feed, out);
        if (color)
            out << "\033[0m";
    }
    else if (!is_cut)
        out.write(' ');

    if (start_from_offset < serialized_value->size())
        ++start_from_offset;
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

void PrettyBlockOutputFormat::stopThread()
{
    if (thread)
    {
        finish = true;
        mono_chunk_condvar.notify_one();
    }
}

PrettyBlockOutputFormat::~PrettyBlockOutputFormat()
{
    if (thread)
    {
        stopThread();
        thread->join();
    }
}

void PrettyBlockOutputFormat::writeSuffix()
{
    stopThread();
    writeMonoChunkIfNeeded();
    writeSuffixImpl();
}

void PrettyBlockOutputFormat::writeSuffixImpl()
{
    if (total_rows >= format_settings.pretty.max_rows)
    {
        if (style == Style::Space)
            out << "\n";

        out << "Showed " << displayed_rows << " out of " << total_rows << " rows.\n";
    }
}

void registerOutputFormatPretty(FormatFactory & factory)
{
    /// Various combinations are available under their own names, e.g. PrettyCompactNoEscapesMonoBlock.
    for (auto style : {PrettyBlockOutputFormat::Style::Full, PrettyBlockOutputFormat::Style::Compact, PrettyBlockOutputFormat::Style::Space})
    {
        for (bool no_escapes : {false, true})
        {
            for (bool mono_block : {false, true})
            {
                String name = "Pretty";

                if (style == PrettyBlockOutputFormat::Style::Compact)
                    name += "Compact";
                else if (style == PrettyBlockOutputFormat::Style::Space)
                    name += "Space";

                if (no_escapes)
                    name += "NoEscapes";
                if (mono_block)
                    name += "MonoBlock";

                factory.registerOutputFormat(name, [style, no_escapes, mono_block](
                    WriteBuffer & buf,
                    const Block & sample,
                    const FormatSettings & format_settings)
                {
                    bool color = !no_escapes
                        && (format_settings.pretty.color == 1 || (format_settings.pretty.color == 2 && format_settings.is_writing_to_terminal));
                    return std::make_shared<PrettyBlockOutputFormat>(buf, sample, format_settings, style, mono_block, color);
                });
            }
        }
    }
}

}
