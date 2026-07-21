#include <Processors/Formats/Impl/PrettyBlockOutputFormat.h>
#include <Processors/Formats/Impl/VerticalRowOutputFormat.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Port.h>
#include <Formats/FormatFactory.h>
#include <Formats/PrettyFormatHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/CurrentThread.h>
#include <Common/UTF8Helpers.h>
#include <Common/PODArray.h>
#include <Common/formatReadable.h>
#include <Common/setThreadName.h>
#include <Common/TerminalSize.h>
#include <Common/ThreadPool.h>
#include <Common/ThreadGroupSwitcher.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>

#include <algorithm>


namespace DB
{

PrettyBlockOutputFormat::PrettyBlockOutputFormat(
    WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_, Style style_, bool mono_block_, bool color_, bool glue_chunks_)
     : IOutputFormat(header_, out_), format_settings(format_settings_), serializations(header_->getSerializations()), style(style_), mono_block(mono_block_), color(color_), glue_chunks(glue_chunks_)
{
    /// Decide whether we should print a tip near the single number value in the result.
    if (!header_->getColumns().empty())
    {
        /// Check if it is a numeric type, possible wrapped by Nullable or LowCardinality.
        DataTypePtr type = removeNullable(recursiveRemoveLowCardinality(header_->getDataTypes().back()));
        if (isNumber(type))
            readable_number_tip = true;
    }
    format_settings.pretty_format = true;
    format_settings.json = FormatSettings::JSON{};
    format_settings.json.pretty_print_indent_multiplier = 1;

    use_nbsp_for_padding = format_settings.pretty.use_nbsp_for_padding
        && format_settings.pretty.charset == FormatSettings::Pretty::Charset::UTF8;
}

namespace
{
    /// `U+00A0` survives tools that compress or trim runs of regular spaces.
    constexpr std::string_view nbsp_utf8{"\xC2\xA0"};
}

void PrettyBlockOutputFormat::writePaddingSpace()
{
    if (use_nbsp_for_padding)
        writeString(nbsp_utf8, out);
    else
        writeChar(' ', out);
}

void PrettyBlockOutputFormat::writePaddingSpaces(size_t count)
{
    for (size_t i = 0; i < count; ++i)
        writePaddingSpace();
}

bool PrettyBlockOutputFormat::cutInTheMiddle(size_t row_num, size_t num_rows, size_t max_rows)
{
    return num_rows > max_rows
        && !(row_num < (max_rows + 1) / 2
            || row_num >= num_rows - max_rows / 2);
}


/// Evaluate the visible width of the values and column names.
/// Note that number of code points is just a rough approximation of visible string width.
void PrettyBlockOutputFormat::calculateWidths(
    const Block & header, const Chunk & chunk, bool split_by_lines, bool & out_has_newlines,
    WidthsPerColumn & widths, Widths & max_padded_widths, Widths & name_widths, Strings & names)
{
    size_t num_rows = chunk.getNumRows();
    size_t num_displayed_rows = std::min<size_t>(num_rows, format_settings.pretty.max_rows);

    /// len(num_rows + total_rows) + len(". ")
    prev_row_number_width = row_number_width;
    row_number_width = num_rows + total_rows > 0
        ? static_cast<size_t>(std::floor(std::log10(num_rows + total_rows))) + 3
        : 3;

    size_t num_columns = chunk.getNumColumns();
    const auto & columns = chunk.getColumns();

    widths.resize(num_columns);
    max_padded_widths.resize_fill(num_columns);
    name_widths.resize(num_columns);
    names.resize(num_columns);

    /// Calculate the widths of all values.
    String serialized_value;
    size_t prefix = row_number_width + (style == Style::Space ? 1 : 2); // Tab character adjustment
    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto & elem = header.getByPosition(i);
        const auto & column = columns[i];

        widths[i].resize_fill(num_displayed_rows);

        size_t displayed_row = 0;
        for (size_t j = 0; j < num_rows; ++j)
        {
            if (cutInTheMiddle(j, num_rows, format_settings.pretty.max_rows))
                continue;

            {
                WriteBufferFromString out_serialize(serialized_value);
                auto serialization = elem.type->getDefaultSerialization();
                serialization->serializeText(*column, j, out_serialize, format_settings);
            }

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

                widths[i][displayed_row] = std::max(
                    widths[i][displayed_row],
                    UTF8::computeWidth(reinterpret_cast<const UInt8 *>(serialized_value.data() + start_from_offset), next_offset - start_from_offset, prefix));

                max_padded_widths[i] = std::max<UInt64>(
                    max_padded_widths[i],
                    std::min<UInt64>({format_settings.pretty.max_column_pad_width, format_settings.pretty.max_value_width, widths[i][displayed_row]}));

                start_from_offset = next_offset;
                if (start_from_offset < serialized_value.size())
                    ++start_from_offset;
            }

            ++displayed_row;
        }

        /// Also, calculate the widths for the names of columns.
        {
            auto [name, width] = truncateName(elem.name,
                format_settings.pretty.max_column_name_width_cut_to
                    ? std::max<UInt64>(max_padded_widths[i], format_settings.pretty.max_column_name_width_cut_to)
                    : 0,
                format_settings.pretty.max_column_name_width_min_chars_to_cut,
                format_settings.pretty.charset != FormatSettings::Pretty::Charset::UTF8);

            names[i] = std::move(name);
            name_widths[i] = std::min<UInt64>(format_settings.pretty.max_column_pad_width, width);
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
                    ThreadGroupSwitcher switcher(thread_group, ThreadName::PRETTY_WRITER);

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
        chassert(!mono_chunk);
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
    const auto & original_columns = chunk.getColumns();
    const auto & original_header = getPort(port_kind).getSharedHeader();
    size_t original_num_columns = original_columns.size();

    size_t cut_to_width = format_settings.pretty.max_value_width;
    if (!format_settings.pretty.max_value_width_apply_for_single_value && num_rows == 1 && original_num_columns == 1 && total_rows == 0)
        cut_to_width = 0;

    /// When `output_format_pretty_display_tuples_as_subcolumns` is enabled, every top-level `Tuple` column is
    /// expanded into a group of subcolumns (one per tuple element). The whole rendering machinery below then
    /// operates on the expanded columns, and the header gets a second row with the tuple element names.
    Columns expanded_columns;
    Serializations expanded_serializations;
    ColumnsWithTypeAndName expanded_header_columns;

    /// For each rendered column: the index of the group (top-level column) it belongs to.
    std::vector<size_t> column_to_group;
    /// Per group (top-level column): display name, first rendered column, number of rendered columns.
    Strings group_names;
    std::vector<size_t> group_first_column;
    std::vector<size_t> group_num_columns;

    bool has_subcolumns = false;
    if (format_settings.pretty.display_tuples_as_subcolumns)
    {
        for (size_t i = 0; i < original_num_columns; ++i)
        {
            if (typeid_cast<const DataTypeTuple *>(original_header->getByPosition(i).type.get()))
            {
                has_subcolumns = true;
                break;
            }
        }
    }

    if (has_subcolumns)
    {
        for (size_t i = 0; i < original_num_columns; ++i)
        {
            const auto & elem = original_header->getByPosition(i);
            const auto * tuple_type = typeid_cast<const DataTypeTuple *>(elem.type.get());

            size_t first = expanded_columns.size();
            if (tuple_type)
            {
                const auto & element_types = tuple_type->getElements();
                const auto & element_names = tuple_type->getElementNames();
                for (size_t k = 0; k < element_types.size(); ++k)
                {
                    expanded_columns.push_back(tuple_type->getSubcolumn(element_names[k], original_columns[i]));
                    expanded_serializations.push_back(tuple_type->getSubcolumnSerialization(element_names[k], serializations[i]));
                    expanded_header_columns.emplace_back(nullptr, element_types[k], element_names[k]);
                    column_to_group.push_back(group_names.size());
                }
                group_num_columns.push_back(element_types.size());
            }
            else
            {
                expanded_columns.push_back(original_columns[i]);
                expanded_serializations.push_back(serializations[i]);
                expanded_header_columns.emplace_back(nullptr, elem.type, elem.name);
                column_to_group.push_back(group_names.size());
                group_num_columns.push_back(1);
            }
            group_names.push_back(elem.name);
            group_first_column.push_back(first);
        }
    }

    Block expanded_header(expanded_header_columns);

    /// Views used by the table-rendering path (the vertical fallback below keeps using the originals).
    const Columns & columns = has_subcolumns ? expanded_columns : original_columns;
    const Serializations & render_serializations = has_subcolumns ? expanded_serializations : serializations;
    const Block & header = has_subcolumns ? expanded_header : *original_header;
    size_t num_columns = columns.size();

    WidthsPerColumn widths;
    Widths max_widths;
    Widths name_widths;
    Strings names;
    bool has_newlines = false;
    if (has_subcolumns)
    {
        Chunk expanded_chunk(Columns(expanded_columns), num_rows);
        calculateWidths(header, expanded_chunk, format_settings.pretty.multiline_fields, has_newlines, widths, max_widths, name_widths, names);
    }
    else
        calculateWidths(header, chunk, format_settings.pretty.multiline_fields, has_newlines, widths, max_widths, name_widths, names);

    /// A group (tuple) name must fit into the combined width of its subcolumns; if it does not, widen them.
    Strings group_display_names;
    Widths group_widths;      /// Paddable width of the group-name cell (sum of subcolumn widths + internal separators).
    Widths group_name_widths; /// Width of the (possibly truncated) group name itself.
    if (has_subcolumns)
    {
        size_t num_groups = group_names.size();
        group_display_names.resize(num_groups);
        group_widths.resize(num_groups);
        group_name_widths.resize(num_groups);
        bool ascii = format_settings.pretty.charset != FormatSettings::Pretty::Charset::UTF8;
        for (size_t g = 0; g < num_groups; ++g)
        {
            size_t first = group_first_column[g];
            size_t count = group_num_columns[g];

            if (count == 1)
            {
                group_display_names[g] = names[first];
                group_name_widths[g] = name_widths[first];
                group_widths[g] = max_widths[first];
                continue;
            }

            /// Each internal boundary between subcolumns occupies 3 more characters (`" │ "`).
            size_t combined = 3 * (count - 1);
            for (size_t k = 0; k < count; ++k)
                combined += max_widths[first + k];

            auto [name, width] = truncateName(
                group_names[g],
                format_settings.pretty.max_column_name_width_cut_to
                    ? std::max<UInt64>(combined, format_settings.pretty.max_column_name_width_cut_to)
                    : 0,
                format_settings.pretty.max_column_name_width_min_chars_to_cut,
                ascii);
            width = std::min<UInt64>(format_settings.pretty.max_column_pad_width, width);

            /// If the tuple name is wider than its subcolumns combined, distribute the extra width among them.
            if (width > combined)
            {
                size_t extra = width - combined;
                size_t per = extra / count;
                size_t remainder = extra - per * count;
                for (size_t k = 0; k < count; ++k)
                    max_widths[first + k] += per + (k < remainder ? 1 : 0);
                combined = width;
            }

            group_display_names[g] = std::move(name);
            group_name_widths[g] = width;
            group_widths[g] = combined;
        }
    }

    size_t table_width = 0;
    for (size_t width : max_widths)
        table_width += width;

    /// Fallback to Vertical format if:
    /// enabled by the settings, this is the first chunk, the number of rows is small enough,
    /// either the table width is larger than the max_value_width or any of the values contain a newline.
    if (format_settings.pretty.fallback_to_vertical
        && displayed_rows == 0
        && num_rows <= format_settings.pretty.fallback_to_vertical_max_rows_per_chunk
        && original_num_columns >= format_settings.pretty.fallback_to_vertical_min_columns
        && (table_width >= format_settings.pretty.fallback_to_vertical_min_table_width || has_newlines))
    {
        use_vertical_format = true;
    }

    if (use_vertical_format)
    {
        /// The Vertical fallback renders whole tuples (not expanded subcolumns), so it uses the original columns.
        if (!vertical_format_fallback)
        {
            vertical_format_fallback = std::make_unique<VerticalRowOutputFormat>(out, original_header, format_settings);
            vertical_format_fallback->writePrefixIfNeeded();
        }

        for (size_t i = 0; i < num_rows && displayed_rows < format_settings.pretty.max_rows; ++i)
        {
            if (i != 0)
                vertical_format_fallback->writeRowBetweenDelimiter();
            vertical_format_fallback->writeRow(original_columns, i);
            ++displayed_rows;
        }

        return;
    }

    /// Create separators

    String left_blank;
    if (format_settings.pretty.row_numbers)
    {
        if (use_nbsp_for_padding)
        {
            left_blank.reserve(row_number_width * nbsp_utf8.size());
            for (size_t i = 0; i < row_number_width; ++i)
                left_blank.append(nbsp_utf8);
        }
        else
            left_blank.assign(row_number_width, ' ');
    }

    String header_begin;    /// ┏━━┳━━━┓
    String header_middle;   /// ┣━━╋━━━┫ (only with subcolumns, between the two header name rows)
    String header_end;      /// ┡━━╇━━━┩
    String rows_separator;  /// ├──┼───┤
    String rows_end;        /// └──┴───┘
    String footer_begin;    /// ┢━━╈━━━┪
    String footer_middle;   /// ┣━━╋━━━┫ (only with subcolumns, between the two footer name rows)
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

    /// Whether the boundary *before* rendered column `i` (i > 0) starts a new top-level column group,
    /// as opposed to a boundary between subcolumns of the same tuple.
    auto is_group_start = [&](size_t i) -> bool
    {
        return !has_subcolumns || column_to_group[i] != column_to_group[i - 1];
    };

    /// Build a horizontal border line. `outer` provides the corners, fill and group-boundary junctions;
    /// `inner_junction` is the glyph placed at within-tuple (subcolumn) boundaries.
    auto make_border = [&](String & str, const GridPart & outer, std::string_view inner_junction)
    {
        str = left_blank;
        WriteBufferFromString buf(str, AppendModeTag{});
        buf << outer[0];
        for (size_t i = 0; i < num_columns; ++i)
        {
            if (i != 0)
                buf << (is_group_start(i) ? outer[2] : inner_junction);
            for (size_t j = 0; j < max_widths[i] + 2; ++j)
                buf << outer[1];
        }
        buf << outer[3] << "\n";
    };

    if (style == Style::Full)
    {
        make_border(header_begin, grid[0], grid[0][1]); /// continuous under tuple names
        make_border(header_end, grid[1], grid[1][2]);
        make_border(rows_separator, grid[2], grid[2][2]);
        make_border(rows_end, grid[3], grid[3][2]);
        make_border(footer_begin, grid[4], grid[4][2]);
        make_border(footer_end, grid[5], grid[5][1]); /// continuous under tuple names
        if (has_subcolumns)
        {
            make_border(header_middle, grid[7], grid[0][2]); /// ╋ at group boundaries, ┳ at subcolumn boundaries
            make_border(footer_middle, grid[7], grid[5][2]); /// ╋ at group boundaries, ┻ at subcolumn boundaries
        }
    }
    else if (style == Style::Compact)
    {
        make_border(rows_end, grid[3], grid[3][2]);
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
        {
            out << vertical_bold_bar;
            writePaddingSpace();
        }
        else if (style == Style::Compact)
            out << grid[is_top ? 6 : 3][0] << horizontal_bar;
        else if (style == Style::Space)
            writePaddingSpace();

        for (size_t i = 0; i < num_columns; ++i)
        {
            if (i != 0)
            {
                if (style == Style::Full)
                {
                    writePaddingSpace();
                    out << vertical_bold_bar;
                    writePaddingSpace();
                }
                else if (style == Style::Compact)
                    out << horizontal_bar << grid[is_top ? 6 : 3][2] << horizontal_bar;
                else if (style == Style::Space)
                    writePaddingSpaces(3);
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
                        writePaddingSpace();
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
        {
            writePaddingSpace();
            out << vertical_bold_bar;
        }
        else if (style == Style::Compact)
            out << horizontal_bar << grid[is_top ? 6 : 3][3];

        out << "\n";
    };

    /// The top-level names row when subcolumns are shown: one cell per top-level column (a tuple name spans
    /// the combined width of its subcolumns). Mirrors `write_names`, but iterates over groups.
    auto write_group_names = [&](bool is_top) -> void
    {
        writeString(left_blank, out);

        if (style == Style::Full)
        {
            out << vertical_bold_bar;
            writePaddingSpace();
        }
        else if (style == Style::Compact)
            out << grid[is_top ? 6 : 3][0] << horizontal_bar;
        else if (style == Style::Space)
            writePaddingSpace();

        for (size_t g = 0; g < group_names.size(); ++g)
        {
            if (g != 0)
            {
                if (style == Style::Full)
                {
                    writePaddingSpace();
                    out << vertical_bold_bar;
                    writePaddingSpace();
                }
                else if (style == Style::Compact)
                    out << horizontal_bar << grid[is_top ? 6 : 3][2] << horizontal_bar;
                else if (style == Style::Space)
                    writePaddingSpaces(3);
            }

            /// Tuple names are left-aligned; a single non-tuple column keeps its own alignment.
            bool align_right = group_num_columns[g] == 1
                && header.getByPosition(group_first_column[g]).type->shouldAlignRightInPrettyFormats();

            auto write_value = [&]
            {
                if (color)
                    out << "\033[1m";
                writeString(group_display_names[g], out);
                if (color)
                    out << "\033[0m";
            };

            auto write_padding = [&]
            {
                for (size_t k = 0; k < group_widths[g] - group_name_widths[g]; ++k)
                {
                    if (style == Style::Compact)
                        out << horizontal_bar;
                    else
                        writePaddingSpace();
                }
            };

            if (align_right)
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
        {
            writePaddingSpace();
            out << vertical_bold_bar;
        }
        else if (style == Style::Compact)
            out << horizontal_bar << grid[is_top ? 6 : 3][3];

        out << "\n";
    };

    /// The subcolumn names row: one cell per rendered column. Cells of single (non-tuple) columns are blank.
    /// `is_top` selects the within-tuple junction: `┬` (below the top-level names) for the header,
    /// `┴` (above the top-level names) for the footer.
    auto write_subcolumn_names = [&](bool is_top) -> void
    {
        writeString(left_blank, out);

        if (style == Style::Full)
        {
            out << vertical_bold_bar;
            writePaddingSpace();
        }
        else if (style == Style::Compact)
            out << grid[2][0] << horizontal_bar;
        else if (style == Style::Space)
            writePaddingSpace();

        for (size_t i = 0; i < num_columns; ++i)
        {
            if (i != 0)
            {
                if (style == Style::Full)
                {
                    writePaddingSpace();
                    out << vertical_bold_bar;
                    writePaddingSpace();
                }
                else if (style == Style::Compact)
                    out << horizontal_bar << (is_group_start(i) ? grid[2][2] : grid[is_top ? 6 : 3][2]) << horizontal_bar;
                else if (style == Style::Space)
                    writePaddingSpaces(3);
            }

            bool single = group_num_columns[column_to_group[i]] == 1;
            size_t name_width = single ? 0 : name_widths[i];
            bool align_right = !single && header.getByPosition(i).type->shouldAlignRightInPrettyFormats();

            auto write_value = [&]
            {
                if (single)
                    return;
                if (color)
                    out << "\033[1m";
                writeString(names[i], out);
                if (color)
                    out << "\033[0m";
            };

            auto write_padding = [&]
            {
                for (size_t k = 0; k < max_widths[i] - name_width; ++k)
                {
                    if (style == Style::Compact)
                        out << horizontal_bar;
                    else
                        writePaddingSpace();
                }
            };

            if (align_right)
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
        {
            writePaddingSpace();
            out << vertical_bold_bar;
        }
        else if (style == Style::Compact)
            out << horizontal_bar << grid[2][3];

        out << "\n";
    };

    auto write_header = [&]()
    {
        writeString(header_begin, out);
        if (has_subcolumns)
        {
            write_group_names(true);
            writeString(header_middle, out);
            write_subcolumn_names(true);
        }
        else
            write_names(true);
        writeString(header_end, out);
    };

    if (glue_chunks
        && !has_subcolumns
        && port_kind == PortKind::Main
        && (!format_settings.pretty.row_numbers || row_number_width == prev_row_number_width)
        && max_widths == prev_chunk_max_widths)
    {
        /// Move cursor up to overwrite the footer of the previous chunk:
        if (!rows_end.empty())
            writeCString("\033[1A\033[2K\033[G", out);
        if (had_footer)
        {
            size_t times = !footer_begin.empty() + !footer_end.empty() + rows_end.empty();
            for (size_t i = 0; i < times; ++i)
                writeCString("\033[1A\033[2K\033[G", out);
        }
        if (!rows_separator.empty())
            writeString(rows_separator, out);
    }
    else
    {
        write_header();
    }

    bool vertical_filler_written = false;
    size_t displayed_row = 0;

    std::vector<std::optional<String>> serialized_values(num_columns);
    std::vector<size_t> offsets_inside_serialized_values(num_columns);

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
                serialized_values[j].reset();
                offsets_inside_serialized_values[j] = 0;
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
                        writePaddingSpaces(row_number_width - row_num_string.size());

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
                    if (style != Style::Space)
                        out << vertical_bar;
                    else if (j != 0)
                        writePaddingSpace();

                    const auto & type = header.getByPosition(j).type;
                    writeValueWithPadding(
                        *columns[j],
                        *render_serializations[j],
                        i,
                        format_settings.pretty.multiline_fields, serialized_values[j], offsets_inside_serialized_values[j],
                        widths[j].empty() ? max_widths[j] : widths[j][displayed_row],
                        max_widths[j],
                        cut_to_width,
                        type->shouldAlignRightInPrettyFormats(),
                        isNumber(removeNullable(type)));

                    if (offsets_inside_serialized_values[j] != serialized_values[j]->size())
                        all_lines_printed = false;
                }

                if (style != Style::Space)
                    out << vertical_bar;

                if (readable_number_tip)
                {
                    size_t term_width = getTerminalWidth();
                    size_t visible_table_width = format_settings.pretty.row_numbers ? row_number_width : 0;

                    for (size_t w : max_widths)
                        visible_table_width += w;

                    if (style == Style::Space)
                        visible_table_width += (num_columns * 3) - 1;
                    else
                        visible_table_width += (num_columns * 3) + 1;

                    size_t remaining_width = 0;

                    // Unit tests or non-TTY
                    if (term_width == 0)
                        remaining_width = SIZE_MAX;
                    else if (term_width > visible_table_width)
                        remaining_width = term_width - visible_table_width;

                    if (remaining_width > 0)
                        writeReadableNumberTip(out, *columns.back(), i, format_settings, color, remaining_width);
                }

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
        if (has_subcolumns)
        {
            write_subcolumn_names(false);
            writeString(footer_middle, out);
            write_group_names(false);
        }
        else
            write_names(false);
        writeString(footer_end, out);
        had_footer = true;
    }
    else
    {
        ///    └──────┘
        writeString(rows_end, out);
        had_footer = false;
    }
    total_rows += num_rows;
    prev_chunk_max_widths = std::move(max_widths);
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
            writePaddingSpaces(pad_to_width - value_width);
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
        writePaddingSpace();

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
        writePaddingSpace();

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

void PrettyBlockOutputFormat::onRowsReadBeforeUpdate()
{
    total_rows = getRowsReadBefore();
}

void registerOutputFormatPretty(FormatFactory & factory);
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
                    const FormatSettings & format_settings,
                    FormatFilterInfoPtr /*format_filter_info*/)
                {
                    bool color = !no_escapes
                        && (format_settings.pretty.color == 1 || (format_settings.pretty.color == 2 && format_settings.is_writing_to_terminal));
                    bool glue_chunks = !no_escapes
                        && (format_settings.pretty.glue_chunks == 1 || (format_settings.pretty.glue_chunks == 2 && format_settings.is_writing_to_terminal));
                    return std::make_shared<PrettyBlockOutputFormat>(buf, std::make_shared<const Block>(sample), format_settings, style, mono_block, color, glue_chunks);
                });
            }
        }
    }

    factory.setDocumentation("Pretty", Documentation{
        .description = R"DOCS_MD(
import PrettyFormatSettings from '/snippets/common-pretty-format-settings.mdx';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description {#description}

The `Pretty` format outputs data as Unicode-art tables, 
using ANSI-escape sequences for displaying colors in the terminal.
A full grid of the table is drawn, and each row occupies two lines in the terminal.
Each result block is output as a separate table. 
This is necessary so that blocks can be output without buffering results (buffering would be necessary to pre-calculate the visible width of all the values).

[NULL](/reference/syntax) is output as `ᴺᵁᴸᴸ`.

## Example usage {#example-usage}

Example (shown for the [`PrettyCompact`](/reference/formats/Pretty/PrettyCompact) format):

```sql title="Query"
SELECT * FROM t_null
```

```response title="Response"
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

Rows are not escaped in any of the `Pretty` formats. The following example is shown for the [`PrettyCompact`](/reference/formats/Pretty/PrettyCompact) format:

```sql title="Query"
SELECT 'String with \'quotes\' and \t character' AS Escaping_test
```

```response title="Response"
┌─Escaping_test────────────────────────┐
│ String with 'quotes' and      character │
└──────────────────────────────────────┘
```

To avoid dumping too much data to the terminal, only the first `10,000` rows are printed. 
If the number of rows is greater than or equal to `10,000`, the message "Showed first 10 000" is printed.

<Note>
This format is only appropriate for outputting a query result, but not for parsing data.
</Note>

The Pretty format supports outputting total values (when using `WITH TOTALS`) and extremes (when 'extremes' is set to 1). 
In these cases, total values and extreme values are output after the main data, in separate tables. 
This is shown in the following example which uses the [`PrettyCompact`](/reference/formats/Pretty/PrettyCompact) format:

```sql title="Query"
SELECT EventDate, count() AS c 
FROM test.hits 
GROUP BY EventDate 
WITH TOTALS 
ORDER BY EventDate 
FORMAT PrettyCompact
```

```response title="Response"
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1406958 │
│ 2014-03-18 │ 1383658 │
│ 2014-03-19 │ 1405797 │
│ 2014-03-20 │ 1353623 │
│ 2014-03-21 │ 1245779 │
│ 2014-03-22 │ 1031592 │
│ 2014-03-23 │ 1046491 │
└────────────┴─────────┘

Totals:
┌──EventDate─┬───────c─┐
│ 1970-01-01 │ 8873898 │
└────────────┴─────────┘

Extremes:
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1031592 │
│ 2014-03-23 │ 1406958 │
└────────────┴─────────┘
```

## Format settings {#format-settings}

<PrettyFormatSettings/>
)DOCS_MD"});

    factory.setDocumentation("PrettyCompact", Documentation{
        .description = R"DOCS_MD(
import PrettyFormatSettings from '/snippets/common-pretty-format-settings.mdx';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description {#description}

Differs from the [`Pretty`](/reference/formats/Pretty/Pretty) format in that the table is displayed with a grid drawn between rows. 
Because of this the result is more compact.

<Note>
This format is used by default in the command-line client in interactive mode.
</Note>

## Example usage {#example-usage}

## Format settings {#format-settings}

<PrettyFormatSettings />
)DOCS_MD"});

    factory.setDocumentation("PrettyCompactMonoBlock", Documentation{
        .description = R"DOCS_MD(
import PrettyFormatSettings from '/snippets/common-pretty-format-settings.mdx';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description {#description}

Differs from the [`PrettyCompact`](/reference/formats/Pretty/PrettyCompact) format in that up to `10,000` rows are buffered, 
and then output as a single table, and not by [blocks](/resources/develop-contribute/introduction/architecture#block).

## Example usage {#example-usage}

## Format settings {#format-settings}

<PrettyFormatSettings/>
)DOCS_MD"});

    factory.setDocumentation("PrettyCompactNoEscapes", Documentation{
        .description = R"DOCS_MD(
import PrettyFormatSettings from '/snippets/common-pretty-format-settings.mdx';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description {#description}

Differs from the [`PrettyCompact`](/reference/formats/Pretty/PrettyCompact) format in that [ANSI-escape sequences](http://en.wikipedia.org/wiki/ANSI_escape_code) aren't used. 
This is necessary for displaying the format in a browser, as well as for using the 'watch' command-line utility.

## Example usage {#example-usage}

## Format settings {#format-settings}

<PrettyFormatSettings/>
)DOCS_MD"});

    factory.setDocumentation("PrettyCompactNoEscapesMonoBlock", Documentation{
        .description = R"DOCS_MD(
import PrettyFormatSettings from '/snippets/common-pretty-format-settings.mdx';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description {#description}

Differs from the [`PrettyCompactNoEscapes`](/reference/formats/Pretty/PrettyCompactNoEscapes) format in that up to `10,000` rows are buffered, 
and then output as a single table, and not by [blocks](/resources/develop-contribute/introduction/architecture#block).

## Example usage {#example-usage}

## Format settings {#format-settings}

<PrettyFormatSettings/>
)DOCS_MD"});

    factory.setDocumentation("PrettyMonoBlock", Documentation{
        .description = R"DOCS_MD(
import PrettyFormatSettings from '/snippets/common-pretty-format-settings.mdx';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description {#description}

Differs from the [`Pretty`](/reference/formats/Pretty/Pretty) format in that up to `10,000` rows are buffered,
and then output as a single table, and not by [blocks](/resources/develop-contribute/introduction/architecture#block).

## Example usage {#example-usage}

## Format settings {#format-settings}

<PrettyFormatSettings/>
)DOCS_MD"});

    factory.setDocumentation("PrettyNoEscapes", Documentation{
        .description = R"DOCS_MD(
import PrettyFormatSettings from '/snippets/common-pretty-format-settings.mdx';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description {#description}

Differs from [Pretty](/reference/formats/Pretty/Pretty) in that [ANSI-escape sequences](http://en.wikipedia.org/wiki/ANSI_escape_code) aren't used. 
This is necessary for displaying the format in a browser, as well as for using the 'watch' command-line utility.

## Example usage {#example-usage}

Example:

```bash
$ watch -n1 "clickhouse-client --query='SELECT event, value FROM system.events FORMAT PrettyCompactNoEscapes'"
```

<Note>
The [HTTP interface](/concepts/features/interfaces/http) can be used for displaying this format in the browser.
</Note>

## Format settings {#format-settings}

<PrettyFormatSettings/>
)DOCS_MD"});

    factory.setDocumentation("PrettyNoEscapesMonoBlock", Documentation{
        .description = R"DOCS_MD(
import PrettyFormatSettings from '/snippets/common-pretty-format-settings.mdx';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description {#description}

Differs from the [`PrettyNoEscapes`](/reference/formats/Pretty/PrettyNoEscapes) format in that up to `10,000` rows are buffered, 
and then output as a single table, and not by blocks.

## Example usage {#example-usage}

## Format settings {#format-settings}

<PrettyFormatSettings/>
)DOCS_MD"});

    factory.setDocumentation("PrettySpace", Documentation{
        .description = R"DOCS_MD(
import PrettyFormatSettings from '/snippets/common-pretty-format-settings.mdx';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description {#description}

Differs from the [`PrettyCompact`](/reference/formats/Pretty/PrettyCompact) format in that whitespace 
(space characters) is used for displaying the table instead of a grid.

## Example usage {#example-usage}

## Format settings {#format-settings}

<PrettyFormatSettings/>
)DOCS_MD"});

    factory.setDocumentation("PrettySpaceMonoBlock", Documentation{
        .description = R"DOCS_MD(
import PrettyFormatSettings from '/snippets/common-pretty-format-settings.mdx';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description {#description}

Differs from the [`PrettySpace`](/reference/formats/Pretty/PrettySpace) format in that up to `10,000` rows are buffered, 
and then output as a single table, and not by [blocks](/resources/develop-contribute/introduction/architecture#block).

## Example usage {#example-usage}

## Format settings {#format-settings}

<PrettyFormatSettings/>
)DOCS_MD"});

    factory.setDocumentation("PrettySpaceNoEscapes", Documentation{
        .description = R"DOCS_MD(
import PrettyFormatSettings from '/snippets/common-pretty-format-settings.mdx';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description {#description}

Differs from the [`PrettySpace`](/reference/formats/Pretty/PrettySpace) format in that [ANSI-escape sequences](http://en.wikipedia.org/wiki/ANSI_escape_code) are not used. 
This is necessary for displaying this format in a browser, as well as for using the 'watch' command-line utility.

## Example usage {#example-usage}

## Format settings {#format-settings}

<PrettyFormatSettings/>
)DOCS_MD"});

    factory.setDocumentation("PrettySpaceNoEscapesMonoBlock", Documentation{
        .description = R"DOCS_MD(
import PrettyFormatSettings from '/snippets/common-pretty-format-settings.mdx';

| Input | Output  | Alias |
|-------|---------|-------|
| ✗     | ✔       |       |

## Description {#description}

Differs from the [`PrettySpaceNoEscapes`](/reference/formats/Pretty/PrettySpaceNoEscapes) format in that up to `10,000` rows are buffered, 
and then output as a single table, and not by [blocks](/resources/develop-contribute/introduction/architecture#block).

## Example usage {#example-usage}

## Format settings {#format-settings}

<PrettyFormatSettings/>
)DOCS_MD"});
}

}
