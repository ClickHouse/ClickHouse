#include <Processors/Formats/Impl/PrettyBlockOutputFormat.h>
#include <Processors/Formats/Impl/VerticalRowOutputFormat.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Port.h>
#include <Formats/FormatFactory.h>
#include <Formats/PrettyFormatHelpers.h>
#include <Formats/EscapingRuleUtils.h>
#include <Formats/JSONUtils.h>
#include <Formats/registerWithNamesAndTypes.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/CurrentThread.h>
#include <Common/UTF8Helpers.h>
#include <Common/isValidUTF8.h>
#include <Common/PODArray.h>
#include <Common/formatReadable.h>
#include <Common/saturatedDuration.h>
#include <Common/setThreadName.h>
#include <Common/TerminalSize.h>
#include <Common/ThreadPool.h>
#include <Common/ThreadGroupSwitcher.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnTuple.h>
#include <Common/assert_cast.h>

#include <algorithm>


namespace DB
{

PrettyBlockOutputFormat::PrettyBlockOutputFormat(
    WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_, Style style_, bool mono_block_, bool color_, bool glue_chunks_)
     : IOutputFormat(header_, out_), format_settings(format_settings_), style(style_), mono_block(mono_block_), color(color_), glue_chunks(glue_chunks_)
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

    /// A named Tuple column that is displayed split into subcolumns
    /// (see the `output_format_pretty_named_tuples_as_subcolumns` setting).
    /// It spans the range [begin, end) of the flattened columns, and its name
    /// is displayed in a header line above the names of its elements.
    struct SubcolumnsGroup
    {
        String name;
        size_t name_width = 0;
        size_t depth = 0;
        size_t begin = 0;
        size_t end = 0;
    };

    /// The displayed view of a chunk: named Tuple columns are recursively replaced by their
    /// elements, and the hierarchy is kept aside to draw the nested header and footer.
    struct FlattenedColumns
    {
        Block header;
        Columns columns;

        /// In DFS pre-order: parents before children, ordered by `begin` within a depth.
        std::vector<SubcolumnsGroup> groups;

        /// Per flattened column: 0 for a top-level column, 1 for its Tuple elements, and so on.
        std::vector<size_t> depths;

        /// Per boundary between flattened columns (including both table edges): the topmost header
        /// level where the boundary appears. It is 0 for the table edges and the boundaries between
        /// top-level columns, and d + 1 for the boundaries between the elements of a Tuple at depth d.
        std::vector<size_t> junction_levels;

        size_t max_depth = 0;
    };

    void flattenColumn(const String & name, const DataTypePtr & type, const ColumnPtr & column, size_t depth, bool split_named_tuples, FlattenedColumns & flattened)
    {
        const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get());
        if (split_named_tuples && tuple_type && tuple_type->hasExplicitNames() && !tuple_type->getElements().empty() && !type->hasCustomName())
        {
            size_t group_index = flattened.groups.size();
            flattened.groups.push_back({.name = name, .depth = depth, .begin = flattened.columns.size()});

            const auto & element_types = tuple_type->getElements();
            const auto & element_names = tuple_type->getElementNames();
            const auto & tuple_column = assert_cast<const ColumnTuple &>(*column);
            for (size_t i = 0; i < element_types.size(); ++i)
                flattenColumn(element_names[i], element_types[i], tuple_column.getColumnPtr(i), depth + 1, split_named_tuples, flattened);

            flattened.groups[group_index].end = flattened.columns.size();
        }
        else
        {
            flattened.header.insert(ColumnWithTypeAndName(type, name));
            flattened.columns.push_back(column);
            flattened.depths.push_back(depth);
            flattened.max_depth = std::max(flattened.max_depth, depth);
        }
    }

    FlattenedColumns flattenNamedTuples(const Block & header, const Columns & columns, bool split_named_tuples)
    {
        FlattenedColumns flattened;
        for (size_t i = 0; i < header.columns(); ++i)
        {
            const auto & elem = header.getByPosition(i);
            flattenColumn(elem.name, elem.type, columns[i], 0, split_named_tuples, flattened);
        }

        /// A boundary appears at the level of the deepest group enclosing both of its sides,
        /// plus one - which is the number of groups it is strictly inside of.
        flattened.junction_levels.assign(flattened.columns.size() + 1, 0);
        for (const auto & group : flattened.groups)
            for (size_t i = group.begin + 1; i < group.end; ++i)
                ++flattened.junction_levels[i];

        return flattened;
    }

    /// Whether the subcolumn names written verbatim in the header (see `flattenColumn`) are not
    /// valid UTF-8. It mirrors the splitting rules of `flattenColumn` exactly: only a bare named
    /// `Tuple` is split, recursively, so the element names of a `Tuple` under an `Array`, a
    /// `Nullable`, or a custom type name are never written as subcolumn names.
    bool subcolumnNamesMayProduceRawBytes(const DataTypePtr & type)
    {
        const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get());
        if (!tuple_type || !tuple_type->hasExplicitNames() || tuple_type->getElements().empty() || type->hasCustomName())
            return false;

        for (const auto & element_name : tuple_type->getElementNames())
            if (!UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(element_name.data()), element_name.size()))
                return true;

        for (const auto & element_type : tuple_type->getElements())
            if (subcolumnNamesMayProduceRawBytes(element_type))
                return true;

        return false;
    }

    bool subcolumnNamesMayProduceRawBytes(const Block & header)
    {
        for (const auto & type : header.getDataTypes())
            if (subcolumnNamesMayProduceRawBytes(type))
                return true;
        return false;
    }
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
    const Block & header, const Chunk & chunk, bool split_by_lines, size_t value_width_limit,
    const Widths & min_widths, bool & out_has_newlines,
    WidthsPerColumn & widths, Widths & max_padded_widths, Widths & name_widths, Strings & names)
{
    /// A cell is never wider than `max_column_pad_width`, so when the values are not cut,
    /// it is the effective limit for the width calculation.
    size_t effective_value_width_limit = value_width_limit
        ? std::min<UInt64>(value_width_limit, format_settings.pretty.max_column_pad_width)
        : format_settings.pretty.max_column_pad_width;

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
    size_t prefix = firstCellPrefix(); // Tab character adjustment
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
            if (effective_value_width_limit)
            {
                size_t max_byte_size = effective_value_width_limit * 4;
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
                    std::min<UInt64>({effective_value_width_limit, widths[i][displayed_row]}));

                start_from_offset = next_offset;
                if (start_from_offset < serialized_value.size())
                    ++start_from_offset;
            }

            ++displayed_row;
        }

        if (!min_widths.empty())
            max_padded_widths[i] = std::max<UInt64>(max_padded_widths[i], min_widths[i]);

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
        if (std::cv_status::timeout == mono_chunk_condvar.wait_for(lock, saturatedMilliseconds(format_settings.pretty.squash_consecutive_ms))
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
    const auto & original_header = getPort(port_kind).getSharedHeader();

    /// Named Tuple columns can be displayed split into subcolumns, with extra header lines for the names of their elements.
    FlattenedColumns flattened = flattenNamedTuples(*original_header, chunk.getColumns(), format_settings.pretty.named_tuples_as_subcolumns);
    const Block & header = flattened.header;
    const Chunk displayed_chunk(std::move(flattened.columns), num_rows);
    const auto & columns = displayed_chunk.getColumns();
    const auto num_columns = displayed_chunk.getNumColumns();
    const Serializations serializations = header.getSerializations();

    size_t cut_to_width = format_settings.pretty.max_value_width;
    /// The single-value exemption is decided by the logical shape of the result (one row, one column),
    /// not by the flattened display shape: a lone named Tuple column split into subcolumns is still a single value.
    if (!format_settings.pretty.max_value_width_apply_for_single_value && num_rows == 1 && original_header->columns() == 1 && total_rows == 0)
        cut_to_width = 0;

    WidthsPerColumn widths;
    Widths max_widths;
    Widths name_widths;
    Strings names;
    bool has_newlines = false;

    Strings group_names;
    group_names.reserve(flattened.groups.size());
    for (const auto & group : flattened.groups)
        group_names.push_back(group.name);

    /// The name of a Tuple column is displayed above the combined span of its subcolumns and must
    /// fit into it; widen the subcolumns if it does not. The groups are processed in reverse
    /// order, so that a parent group sees the final widths of its children.
    /// Returns whether any column has been widened.
    auto fit_group_names = [&]() -> bool
    {
        bool widened = false;
        for (size_t group_index = flattened.groups.size(); group_index > 0; --group_index)
        {
            auto & group = flattened.groups[group_index - 1];

            size_t span_width = 3 * (group.end - group.begin) - 3;
            for (size_t i = group.begin; i != group.end; ++i)
                span_width += max_widths[i];

            auto [name, width] = truncateName(group_names[group_index - 1],
                format_settings.pretty.max_column_name_width_cut_to
                    ? std::max<UInt64>(span_width, format_settings.pretty.max_column_name_width_cut_to)
                    : 0,
                format_settings.pretty.max_column_name_width_min_chars_to_cut,
                format_settings.pretty.charset != FormatSettings::Pretty::Charset::UTF8);

            group.name = std::move(name);
            group.name_width = width;

            if (group.name_width > span_width)
            {
                size_t deficit = group.name_width - span_width;
                size_t num_subcolumns = group.end - group.begin;
                for (size_t i = group.begin; i != group.end; ++i)
                    max_widths[i] += deficit / num_subcolumns + (i - group.begin < deficit % num_subcolumns);
                widened = true;
            }
        }
        return widened;
    };

    /// The visible width of a value depends on the position where it starts - a tab advances to the
    /// next tab stop - so widening a column to fit the name of a Tuple invalidates the widths of the
    /// columns to its right. Calculate the widths again, with the widened columns as a lower bound,
    /// until they stop changing. Two passes are enough unless tabs shift the widths back and forth.
    static constexpr size_t max_width_calculation_passes = 4;
    size_t prev_row_number_width_before = prev_row_number_width;
    Widths min_widths;
    for (size_t pass = 0; pass < max_width_calculation_passes; ++pass)
    {
        prev_row_number_width = prev_row_number_width_before;
        has_newlines = false;
        calculateWidths(
            header, displayed_chunk, format_settings.pretty.multiline_fields, cut_to_width, min_widths,
            has_newlines, widths, max_widths, name_widths, names);

        if (!fit_group_names() || pass + 1 == max_width_calculation_passes)
            break;

        min_widths.assign(max_widths);
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
        && num_columns >= format_settings.pretty.fallback_to_vertical_min_columns
        && (table_width >= format_settings.pretty.fallback_to_vertical_min_table_width || has_newlines))
    {
        use_vertical_format = true;
    }

    if (use_vertical_format)
    {
        if (!vertical_format_fallback)
        {
            vertical_format_fallback = std::make_unique<VerticalRowOutputFormat>(out, original_header, format_settings);
            vertical_format_fallback->writePrefixIfNeeded();
        }

        for (size_t i = 0; i < num_rows && displayed_rows < format_settings.pretty.max_rows; ++i)
        {
            if (i != 0)
                vertical_format_fallback->writeRowBetweenDelimiter();
            vertical_format_fallback->writeRow(chunk.getColumns(), i);
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
    String header_end;      /// ┡━━╇━━━┩
    String rows_separator;  /// ├──┼───┤
    String rows_end;        /// └──┴───┘
    String footer_begin;    /// ┢━━╈━━━┪
    String footer_end;      /// ┗━━┻━━━┛

    bool unicode = format_settings.pretty.charset == FormatSettings::Pretty::Charset::UTF8;
    using GridPart = std::array<std::string_view, 4>;
    using Grid = std::array<GridPart, 7>;

    constexpr Grid utf8_grid
    {
        GridPart{"┏", "━", "┳", "┓"},
        GridPart{"┡", "━", "╇", "┩"},
        GridPart{"├", "─", "┼", "┤"},
        GridPart{"└", "─", "┴", "┘"},
        GridPart{"┢", "━", "╈", "┪"},
        GridPart{"┗", "━", "┻", "┛"},
        GridPart{"┌", "─", "┬", "┐"},
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
    };

    Grid grid = unicode ? utf8_grid : ascii_grid;

    std::string_view vertical_bold_bar   = unicode ? "┃" : "|";
    std::string_view vertical_bar        = unicode ? "│" : "|";
    std::string_view horizontal_bar      = unicode ? "─" : "-";

    /// Pieces of the lines that open and close the subcolumns of Tuple columns, e.g. ┃   ┣━━┳━━┫   ┃
    std::string_view bold_cross           = unicode ? "╋" : "+";
    std::string_view bold_left_connector  = unicode ? "┣" : "+";
    std::string_view bold_right_connector = unicode ? "┫" : "+";

    if (style == Style::Full)
    {
        header_begin = left_blank;
        header_end = left_blank;
        rows_separator = left_blank;
        rows_end = left_blank;
        footer_begin = left_blank;
        footer_end = left_blank;

        WriteBufferFromString header_begin_out(header_begin, AppendModeTag{});
        WriteBufferFromString header_end_out(header_end, AppendModeTag{});
        WriteBufferFromString rows_separator_out(rows_separator, AppendModeTag{});
        WriteBufferFromString rows_end_out(rows_end, AppendModeTag{});
        WriteBufferFromString footer_begin_out(footer_begin, AppendModeTag{});
        WriteBufferFromString footer_end_out(footer_end, AppendModeTag{});

        header_begin_out    << grid[0][0];
        header_end_out      << grid[1][0];
        rows_separator_out  << grid[2][0];
        rows_end_out        << grid[3][0];
        footer_begin_out    << grid[4][0];
        footer_end_out      << grid[5][0];

        for (size_t i = 0; i < num_columns; ++i)
        {
            if (i != 0)
            {
                /// The top and bottom borders have junctions only at the boundaries between
                /// the top-level columns; the boundaries between subcolumns appear deeper.
                header_begin_out    << (flattened.junction_levels[i] == 0 ? grid[0][2] : grid[0][1]);
                header_end_out      << grid[1][2];
                rows_separator_out  << grid[2][2];
                rows_end_out        << grid[3][2];
                footer_begin_out    << grid[4][2];
                footer_end_out      << (flattened.junction_levels[i] == 0 ? grid[5][2] : grid[5][1]);
            }

            for (size_t j = 0; j < max_widths[i] + 2; ++j)
            {
                header_begin_out    << grid[0][1];
                header_end_out      << grid[1][1];
                rows_separator_out  << grid[2][1];
                rows_end_out        << grid[3][1];
                footer_begin_out    << grid[4][1];
                footer_end_out      << grid[5][1];
            }
        }

        header_begin_out    << grid[0][3] << "\n";
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
            if (i != 0)
                rows_end_out << grid[3][2];
            for (size_t j = 0; j < max_widths[i] + 2; ++j)
                rows_end_out << grid[3][1];
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

    std::vector<std::vector<const SubcolumnsGroup *>> groups_by_level(flattened.max_depth + 1);
    for (const auto & group : flattened.groups)
        groups_by_level[group.depth].push_back(&group);

    ///    ┃ name ┃      Level 0 shows the names of the top-level columns. The deeper levels show
    ///    ┌─name─┐      the names of the Tuple elements at that depth of nesting, and the cells of
    ///    └─name─┘      the columns that do not reach that depth are left blank:
    ///      name
    ///                     ┃ x     ┃ t         ┃
    ///                     ┃       ┣━━━┳━━━━━━━┫
    ///                     ┃       ┃ a ┃ b     ┃
    auto write_names = [&](size_t level, bool is_top) -> void
    {
        writeString(left_blank, out);

        if (style == Style::Full)
        {
            out << vertical_bold_bar;
            writePaddingSpace();
        }
        else if (style == Style::Compact)
            out << (level == 0 ? grid[is_top ? 6 : 3][0] : grid[2][0]) << horizontal_bar;
        else if (style == Style::Space)
            writePaddingSpace();

        auto next_group = groups_by_level[level].begin();
        size_t column = 0;
        while (column < num_columns)
        {
            if (column != 0)
            {
                if (style == Style::Full)
                {
                    writePaddingSpace();
                    out << vertical_bold_bar;
                    writePaddingSpace();
                }
                else if (style == Style::Compact)
                {
                    out << horizontal_bar;
                    out << (flattened.junction_levels[column] == level ? grid[is_top ? 6 : 3][2] : grid[2][2]);
                    out << horizontal_bar;
                }
                else if (style == Style::Space)
                    writePaddingSpaces(3);
            }

            /// A cell is either a Tuple column that splits into subcolumns at the next level,
            /// a column displayed at exactly this level, or a blank continuation of a column
            /// that does not reach this level.
            const SubcolumnsGroup * group = nullptr;
            if (next_group != groups_by_level[level].end() && (*next_group)->begin == column)
            {
                group = *next_group;
                ++next_group;
            }

            size_t next_column = group ? group->end : column + 1;
            size_t cell_width = 3 * (next_column - column) - 3;
            for (size_t i = column; i != next_column; ++i)
                cell_width += max_widths[i];

            std::string_view name;
            size_t name_width = 0;
            bool align_right = false;
            if (group)
            {
                name = group->name;
                name_width = group->name_width;
            }
            else if (flattened.depths[column] == level)
            {
                name = names[column];
                name_width = name_widths[column];
                align_right = header.getByPosition(column).type->shouldAlignRightInPrettyFormats();
            }

            auto write_value = [&]
            {
                if (name.empty())
                    return;
                if (color)
                    out << "\033[1m";
                writeString(name, out);
                if (color)
                    out << "\033[0m";
            };

            auto write_padding = [&]
            {
                for (size_t k = name_width; k < cell_width; ++k)
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

            column = next_column;
        }
        if (style == Style::Full)
        {
            writePaddingSpace();
            out << vertical_bold_bar;
        }
        else if (style == Style::Compact)
            out << horizontal_bar << (level == 0 ? grid[is_top ? 6 : 3][3] : grid[2][3]);

        out << "\n";
    };

    ///    ┃       ┣━━━┳━━━┫       ┃
    /// The line that opens (in the header) or closes (in the footer) the subcolumn
    /// names of the given level in the Full style.
    auto write_subcolumns_separator = [&](size_t level, bool is_top) -> void
    {
        writeString(left_blank, out);

        for (size_t i = 0; i <= num_columns; ++i)
        {
            bool left = i != 0 && flattened.depths[i - 1] >= level;
            bool right = i != num_columns && flattened.depths[i] >= level;

            if (flattened.junction_levels[i] == level)
                out << (is_top ? grid[0][2] : grid[5][2]);  /// ┳ ┻ - where the subcolumns split off
            else if (flattened.junction_levels[i] > level)
                out << grid[0][1];                          /// ━ - inside a span that splits deeper
            else if (left && right)
                out << bold_cross;                          /// ╋ - a boundary from the level above continues through
            else if (left)
                out << bold_right_connector;                /// ┫
            else if (right)
                out << bold_left_connector;                 /// ┣
            else
                out << vertical_bold_bar;                   /// ┃

            if (i != num_columns)
            {
                if (right)
                {
                    for (size_t j = 0; j < max_widths[i] + 2; ++j)
                        out << grid[0][1];
                }
                else
                    writePaddingSpaces(max_widths[i] + 2);
            }
        }
        out << "\n";
    };

    if (glue_chunks
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
            /// The footer has an extra line with the subcolumn names per level
            /// (and, in the Full style, also a separator line per level).
            times += (style == Style::Full ? 2 : 1) * flattened.max_depth;
            for (size_t i = 0; i < times; ++i)
                writeCString("\033[1A\033[2K\033[G", out);
        }
        if (!rows_separator.empty())
            writeString(rows_separator, out);
    }
    else
    {
        writeString(header_begin, out);
        write_names(0, true);
        for (size_t level = 1; level <= flattened.max_depth; ++level)
        {
            if (style == Style::Full)
                write_subcolumns_separator(level, true);
            write_names(level, true);
        }
        writeString(header_end, out);
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
                size_t prefix = firstCellPrefix();
                for (size_t j = 0; j < num_columns; ++j)
                {
                    if (style != Style::Space)
                        out << vertical_bar;
                    else if (j != 0)
                        writePaddingSpace();

                    const auto & type = header.getByPosition(j).type;
                    writeValueWithPadding(
                        *columns[j],
                        *serializations[j],
                        i,
                        format_settings.pretty.multiline_fields, serialized_values[j], offsets_inside_serialized_values[j],
                        widths[j].empty() ? max_widths[j] : widths[j][displayed_row],
                        max_widths[j],
                        cut_to_width,
                        prefix,
                        type->shouldAlignRightInPrettyFormats(),
                        isNumber(removeNullable(type)));

                    prefix += max_widths[j] + 3;

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
        for (size_t level = flattened.max_depth; level > 0; --level)
        {
            write_names(level, false);
            if (style == Style::Full)
                write_subcolumns_separator(level, false);
        }
        write_names(0, false);
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


size_t PrettyBlockOutputFormat::firstCellPrefix() const
{
    return (format_settings.pretty.row_numbers ? row_number_width : 0) + (style == Style::Space ? 1 : 2);
}

void PrettyBlockOutputFormat::writeValueWithPadding(
    const IColumn & column, const ISerialization & serialization, size_t row_num,
    bool split_by_lines, std::optional<String> & serialized_value, size_t & start_from_offset,
    size_t value_width, size_t pad_to_width, size_t cut_to_width, size_t prefix, bool align_right, bool is_number)
{
    if (!serialized_value)
    {
        serialized_value = String();
        start_from_offset = 0;
        WriteBufferFromString out_serialize(*serialized_value);
        serialization.serializeText(column, row_num, out_serialize, format_settings);
    }

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

                /// The header (and, for many rows, the footer) column names are written verbatim, so a
                /// name that is not valid UTF-8 makes the output not valid UTF-8 either. The values are
                /// written through the plain `serializeText` kind, which writes the `Bool`
                /// representations verbatim (see `settingsLiteralsMayProduceRawBytes`). With
                /// `output_format_pretty_named_tuples_as_json` (on by default), that kind renders a
                /// named `Tuple` through `SerializationTuple::serializeTextJSONPretty` (the format sets
                /// `FormatSettings::pretty_format`), which synthesizes JSON object keys from the element
                /// names - verbatim, and `Pretty` installs no UTF-8 validating buffer at all (see
                /// `tupleElementNamesMayProduceRawBytesInJSON`). The check mirrors the constructor's
                /// reset of the JSON sub-settings to their defaults: the user's JSON settings (such as
                /// `output_format_json_named_tuples_as_objects = 0`) do not turn the element names off
                /// here, only `output_format_pretty_named_tuples_as_json` does.
                /// With `output_format_pretty_named_tuples_as_subcolumns` (on by default), the element
                /// names of the named `Tuple` columns are also written verbatim, as subcolumn names in
                /// the header - but only for the columns that `flattenColumn` actually splits, which
                /// is why that case uses `subcolumnNamesMayProduceRawBytes` (mirroring the same
                /// predicate) instead of the recursive scan of the JSON check: the element names of a
                /// `Tuple` under an `Array`, a `Nullable`, or a custom type name never reach the header.
                /// The text framings reject or base64-encode the output in these cases (see
                /// `checkIfOutputFormatMayProduceRawBytes`). `Pretty` does not write the data type names.
                factory.registerOutputFormatMayProduceRawBytesChecker(
                    name,
                    [](const FormatSettings & settings, const Block & header)
                    {
                        FormatSettings tuple_settings = settings;
                        tuple_settings.json = FormatSettings::JSON{};
                        return headerNamesMayProduceRawBytes(header, /*with_names=*/ true, /*with_types=*/ false)
                            || settingsLiteralsMayProduceRawBytes(settings, FormatSettings::EscapingRule::None)
                            || (settings.pretty.named_tuples_as_json
                                && JSONUtils::tupleElementNamesMayProduceRawBytesInJSON(header, tuple_settings, /*validate_utf8=*/ false))
                            || (settings.pretty.named_tuples_as_subcolumns && subcolumnNamesMayProduceRawBytes(header));
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
