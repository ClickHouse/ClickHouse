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
#include "Core/ColumnWithTypeAndName.h"
#include "base/types.h"
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>

#include <algorithm>
#include <vector>


namespace DB
{

PrettyBlockOutputFormat::PrettyBlockOutputFormat(
    WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_, Style style_, bool mono_block_, bool color_, bool glue_chunks_)
     : IOutputFormat(header_, out_), format_settings(format_settings_), serializations(header_.getSerializations()), style(style_), mono_block(mono_block_), color(color_), glue_chunks(glue_chunks_)
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

size_t PrettyBlockOutputFormat::returnNeededEnclosureLvl(
    DataTypePtr column_type, int32_t subcolumn_lvl, EnclosureLevelContainer & needed_enclosure_lvl,
    size_t column_ind, int64_t subcolumn_ind, int32_t & max_enc_lvl, int32_t cur_lvl)
{
    const auto & subcol_names = column_type->getSubcolumnNames();
    if (subcolumn_lvl == 0)
    {
        needed_enclosure_lvl.emplace_back(subcolumn_ind);
        return subcol_names.size();
    }
    if (subcol_names.empty())
    {
        if (subcolumn_lvl == -1)
        {
            needed_enclosure_lvl.emplace_back(subcolumn_ind);
            max_enc_lvl = std::max<Int32>(max_enc_lvl, cur_lvl);
        } else {
            if (subcolumn_ind == 0)
                needed_enclosure_lvl.emplace_back(INT64_MIN);
            else
                needed_enclosure_lvl.emplace_back(subcolumn_ind < 0 ? subcolumn_ind : -subcolumn_ind);
        }
        return 0;   
    }
    for (size_t i = 0; i < subcol_names.size(); ++i)
    {
        auto subcolumn_type = column_type->getSubcolumnType(subcol_names[i]);
        i += returnNeededEnclosureLvl(subcolumn_type, std::max<Int32>(-1, subcolumn_lvl - 1),
        needed_enclosure_lvl, column_ind, subcolumn_ind + i + 1, max_enc_lvl, cur_lvl + 1);
    }
    return subcol_names.size();
}


/// Evaluate the visible width of the values and column names.
/// Note that number of code points is just a rough approximation of visible string width.
void PrettyBlockOutputFormat::calculateWidths(
    const Block & header, const Chunk & chunk, bool split_by_lines, bool & out_has_newlines,
    WidthsPerColumn & widths, Widths & max_padded_widths, Widths & name_widths, Strings & names,
    StringsPerCol & subcolumn_names, WidthsPerSubcolumn & subcolumn_widths, WidthsPerColumn & max_subcolumn_widths,
    WidthsPerColumn & subcolumn_name_widths, LeavesPerColumn & column_leaves, std::vector<int32_t> & max_enc_lvl)
{
    size_t num_rows = chunk.getNumRows();
    size_t num_displayed_rows = std::min<size_t>(num_rows, format_settings.pretty.max_rows);

    /// len(num_rows + total_rows) + len(". ")
    prev_row_number_width = row_number_width;
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
    column_leaves.resize(num_columns);
    max_enc_lvl.resize(num_columns);

    /// Calculate the widths of all values.
    String serialized_value;
    size_t prefix = row_number_width + (style == Style::Space ? 1 : 2); // Tab character adjustment
    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto & elem = header.getByPosition(i);
        const auto & elem_names = elem.type->getSubcolumnNames();
        const auto num_subcolumns = elem_names.size();
        const auto & column = columns[i];
        EnclosureLevelContainer leaves;
        widths[i].resize_fill(num_displayed_rows);

        if (num_subcolumns == 0 || elem.type->isNullable() || (elem.type->getTypeId() == TypeIndex::Array) ||
        (elem.type->getTypeId() == TypeIndex::Variant) || !format_settings.pretty.display_tuple_as_subcolumns)
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
            max_enc_lvl[i] = 0;

            returnNeededEnclosureLvl(elem.type, -1, leaves, i, -1, max_enc_lvl[i], 0);

            column_leaves[i] = leaves;
            
            size_t width_leaves_sum = 0;


            for (auto & el : leaves)
            {
                if (el < 0 || el == INT64_MIN)
                    continue;
                subcolumn_widths[i][el].resize_fill(num_displayed_rows);
                for (size_t j = 0; j < num_rows; ++j)
                {
                    if (cutInTheMiddle(j, num_rows, format_settings.pretty.max_rows))
                        continue;

                    {
                        auto serialization = elem.type->getDefaultSerialization();
                        WriteBufferFromString out_serialize_temp(serialized_value);
                        auto subcolumn_serialization = elem.type->getSubcolumnSerialization(elem_names[el], serialization);
                        const auto & subcolumn = elem.type->getSubcolumn(elem_names[el], column);
                        subcolumn_serialization->serializeText(*subcolumn, j, out_serialize_temp, format_settings);
                    }

                    size_t row_num = j;
                    if ((num_rows > format_settings.pretty.max_rows) && (row_num >= (num_rows - format_settings.pretty.max_rows / 2)))
                    {
                        row_num -= (num_rows - format_settings.pretty.max_rows);
                    }

                    findWidth(subcolumn_widths[i][el][row_num], max_subcolumn_widths[i][el], serialized_value, split_by_lines, out_has_newlines, prefix);
                }

                // Calculate the widths for the names of subcolumns.
                {
                    auto [name, width] = truncateName(elem_names[el],
                        format_settings.pretty.max_column_name_width_cut_to
                            ? std::max<UInt64>(max_subcolumn_widths[i][el], format_settings.pretty.max_column_name_width_cut_to)
                            : 0,
                        format_settings.pretty.max_column_name_width_min_chars_to_cut,
                        format_settings.pretty.charset != FormatSettings::Pretty::Charset::UTF8);
                    subcolumn_names[i][el] = std::move(name);
                    subcolumn_name_widths[i][el] = std::min<UInt64>(format_settings.pretty.max_column_pad_width, width);
                    max_subcolumn_widths[i][el] = std::max<UInt64>(max_subcolumn_widths[i][el], subcolumn_name_widths[i][el]);

                    if (max_enc_lvl[i] == 1) {
                        max_padded_widths[i] += max_subcolumn_widths[i][el];
                    }
                    width_leaves_sum += max_subcolumn_widths[i][el];
                }
            }
            int32_t cur_lvl = max_enc_lvl[i];
            while (--cur_lvl > 0)
            {
                EnclosureLevelContainer needed_enclosure_lvl;
                returnNeededEnclosureLvl(elem.type, cur_lvl, needed_enclosure_lvl, i, -1, max_enc_lvl[i], 0);
                for (auto & el : needed_enclosure_lvl)
                {
                    if (el < 0 || el == INT64_MIN)
                        continue;
                    EnclosureLevelContainer next_level_under;
                    int32_t cur_max;
                    returnNeededEnclosureLvl(elem.type->getSubcolumnType(elem_names[el]), 1, next_level_under, i, -1, cur_max, 0);
                    size_t padding = 0;
                    for (auto & j : next_level_under)
                    {
                        if (j >= 0)
                        {
                            max_subcolumn_widths[i][el] += max_subcolumn_widths[i][el + j + 1];
                            padding += 1;
                        }
                        
                    }
                    if (padding)
                    {
                        max_subcolumn_widths[i][el] += (padding - 1) * 3;
                        width_leaves_sum += (padding - 1) * 3;
                    }
                    {
                        auto [name, width] = truncateName(elem_names[el],
                            format_settings.pretty.max_column_name_width_cut_to
                                ? std::max<UInt64>(max_subcolumn_widths[i][el], format_settings.pretty.max_column_name_width_cut_to)
                                : 0,
                            format_settings.pretty.max_column_name_width_min_chars_to_cut,
                            format_settings.pretty.charset != FormatSettings::Pretty::Charset::UTF8);
    
                        subcolumn_names[i][el] = std::move(name);
                        subcolumn_name_widths[i][el] = std::min<UInt64>(format_settings.pretty.max_column_pad_width, width);
                        max_subcolumn_widths[i][el] = std::max<UInt64>(max_subcolumn_widths[i][el], subcolumn_name_widths[i][el]);
                    }   
                    if (cur_lvl == 1) {
                        max_padded_widths[i] += max_subcolumn_widths[i][el];
                    }
                }
            }
            if (!elem.type->isNullable() && (elem.type->getTypeId() != TypeIndex::Array) && (elem.type->getTypeId() != TypeIndex::Variant)
            && max_padded_widths[i] > width_leaves_sum && max_enc_lvl[i] > 1)
            {
                auto width_difference = max_padded_widths[i] - width_leaves_sum;
                auto width_to_add = width_difference/leaves.size();
                auto remainder = width_difference - width_to_add*leaves.size();
                for (auto & el : leaves)
                {
                    if (el < 0 || el == INT64_MIN)
                        continue;
                    size_t k = el;
                    max_subcolumn_widths[i][k] += width_to_add;
                    if (remainder)
                    {
                        max_subcolumn_widths[i][k]++;
                        --remainder;
                    }
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
            
            EnclosureLevelContainer next_level_under;
            int32_t cur_max;
            returnNeededEnclosureLvl(elem.type, 1, next_level_under, i, -1, cur_max, 0);
            size_t padding = 0;
            for (auto & j : next_level_under)
            {
                if (j >= 0)
                {
                    padding += 1;
                }
                
            }
            if (padding)
            {
                max_padded_widths[i] += (padding - 1) * 3;
            }
            
            // Add width if column name width is bigger than all subcolumns' max widths combined
            if (!elem.type->isNullable() && (elem.type->getTypeId() != TypeIndex::Array) && (elem.type->getTypeId() != TypeIndex::Variant)
            && leaves.size() != 0 && name_widths[i] > max_padded_widths[i] && format_settings.pretty.display_tuple_as_subcolumns)
            {
                auto width_difference = name_widths[i] - max_padded_widths[i];
                auto width_to_add = width_difference/leaves.size();
                auto remainder = width_difference - width_to_add*leaves.size();
                for (size_t k = 0; k < leaves.size(); ++k)
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

    StringsPerCol subcol_names;
    WidthsPerSubcolumn subcol_widths;
    WidthsPerColumn subcol_max_widths;
    WidthsPerColumn subcol_name_widths;
    LeavesPerColumn leaves;
    std::vector<int32_t> max_enc_lvl;

    bool has_newlines = false;
    calculateWidths(header, chunk, format_settings.pretty.multiline_fields, has_newlines, widths, max_widths, name_widths, names,
        subcol_names, subcol_widths, subcol_max_widths, subcol_name_widths, leaves, max_enc_lvl);
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

    String header_begin;              /// ┏━━┳━━━┓
    String header_end;                /// ┡━━╇━━━┩
    String rows_separator;            /// ├──┼───┤
    String rows_end;                  /// └──┴───┘
    String footer_begin;              /// ┢━━╈━━━┪
    String footer_end;                /// ┗━━┻━━━┛

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

    std::string_view footer_begin_subcolumn_joiner = unicode ? "┷" : "-";
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

        header_begin_out             << grid[0][0];
        header_end_out               << grid[1][0];
        rows_separator_out           << grid[2][0];
        rows_end_out                 << grid[3][0];
        footer_begin_out             << grid[4][0];
        footer_end_out               << grid[5][0];
        for (size_t i = 0; i < num_columns; ++i)
        {   
            if (i != 0)
            {
                header_begin_out    << grid[0][2];
                header_end_out      << grid[1][2];
                rows_separator_out  << grid[2][2];
                rows_end_out        << grid[3][2];
                footer_begin_out    << grid[4][2];
                footer_end_out      << grid[5][2];
            }
            if (subcol_names[i].empty())
            {
                for (size_t j = 0; j < max_widths[i] + 2; ++j)
                {
                    header_begin_out             << grid[0][1];
                    header_end_out               << grid[1][1];
                    rows_separator_out           << grid[2][1];
                    rows_end_out                 << grid[3][1];
                    footer_begin_out             << grid[4][1];
                    footer_end_out               << grid[5][1];
                }
            }
            else
            {
                for (size_t j : leaves[i])
                {
                    for (size_t k = 0; k < subcol_max_widths[i][j] + 2; ++k)
                    {
                        header_begin_out             << grid[0][1];
                        header_end_out               << grid[1][1];
                        rows_separator_out           << grid[2][1];
                        rows_end_out                 << grid[3][1];
                        footer_begin_out             << grid[4][1];
                        footer_end_out               << grid[5][1];
                    }
                    if (j != subcol_names[i].size() - 1)
                    {
                        header_begin_out             << grid[0][1];
                        header_end_out               << grid[1][2];
                        rows_separator_out           << grid[2][2];
                        rows_end_out                 << grid[3][2]; 
                        footer_begin_out             << (((num_rows >= format_settings.pretty.display_footer_column_names_min_rows) && format_settings.pretty.display_footer_column_names) ?
                                                        grid[4][2] : footer_begin_subcolumn_joiner);
                        footer_end_out               << grid[5][1];
                    }
                }
            }
        }
        header_begin_out             << grid[0][3] << "\n";
        header_end_out               << grid[1][3] << "\n";
        rows_separator_out           << grid[2][3] << "\n";
        rows_end_out                 << grid[3][3] << "\n";
        footer_begin_out             << grid[4][3] << "\n";
        footer_end_out               << grid[5][3] << "\n";
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
            if (!subcol_names[i].empty())
            {
                for (size_t j : leaves[i])
                {
                    for (size_t k = 0; k < subcol_max_widths[i][j] + 2; ++k)
                        rows_end_out << grid[3][1];
                    if (j != subcol_names[i].size() - 1)
                        rows_end_out << grid[3][2];
                }
                    
            }
            else
            {
                for (size_t j = 0; j < max_widths[i] + 2; ++j)
                    rows_end_out << grid[3][1];
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

    auto write_next_separator = [&](int32_t cur_level, std::queue<size_t> & prev_cols_indexes, bool is_top) -> String
    {
        size_t string_offset = 0;
        String header_footer_separator = left_blank;
        WriteBufferFromString header_footer_separator_out(header_footer_separator, AppendModeTag{});
        bool was_empty = true;
        bool has_subcol = false;
        for (size_t i = 0; i < num_columns; ++i)
        {
            EnclosureLevelContainer needed_enclosure_lvl;
            const auto & elem = header.getByPosition(i);
            returnNeededEnclosureLvl(elem.type, cur_level, needed_enclosure_lvl, i, -1, max_enc_lvl[i], 0);
            if (subcol_names[i].empty())
            {
                if (i == 0)
                    header_footer_separator_out  << vertical_bold_bar;
                else
                {
                    header_footer_separator_out  << (was_empty ? vertical_bold_bar : grid[7][3]);
                }
                ++string_offset;
                was_empty = true;
                for (size_t j = 0; j < max_widths[i] + 2; ++j)
                {
                    header_footer_separator_out  << " ";
                    ++string_offset;
                }
            }
            else
            {
                if (!has_subcol)
                    has_subcol = true;
                
                for (size_t j = 0; j < needed_enclosure_lvl.size(); ++j)
                {
                    if (j == 0)
                    {
                        if (i == 0)
                            header_footer_separator_out  << (needed_enclosure_lvl[j] < 0 || needed_enclosure_lvl[j] == INT64_MIN ? vertical_bold_bar : grid[7][0]);
                        else if (needed_enclosure_lvl[j] < 0 || needed_enclosure_lvl[j] == INT64_MIN)
                            header_footer_separator_out  << (was_empty ? vertical_bold_bar : grid[7][3]);
                        else if (needed_enclosure_lvl[j] >= 0)
                            header_footer_separator_out  << (was_empty ? grid[7][0] : grid[7][2]);
                    }
                    else
                    {
                        if (needed_enclosure_lvl[j] < 0 || needed_enclosure_lvl[j] == INT64_MIN)
                        {
                            header_footer_separator_out  << (was_empty ? vertical_bold_bar : grid[7][3]);
                            if (!prev_cols_indexes.empty() && string_offset == prev_cols_indexes.front())
                                prev_cols_indexes.pop();
                        }
                        else
                        {
                            if (!prev_cols_indexes.empty() && prev_cols_indexes.front() == string_offset)
                            {
                                header_footer_separator_out  << (was_empty ? grid[7][0] : grid[7][2]);
                                prev_cols_indexes.pop();
                            }
                            else
                                header_footer_separator_out  << (is_top ? grid[0][2] : grid[5][2]);
                        }
                        prev_cols_indexes.push(string_offset);
                    }
                    ++string_offset;
                    if (needed_enclosure_lvl[j] < 0 || needed_enclosure_lvl[j] == INT64_MIN)
                    {
                        for (size_t k = 0; k < subcol_max_widths[i][(needed_enclosure_lvl[j] == INT64_MIN) ? 0 : needed_enclosure_lvl[j] * -1] + 2; ++k)
                        {
                            header_footer_separator_out  << " ";
                            ++string_offset;
                        }
                        was_empty = true;
                    }
                    else
                    {
                        for (size_t k = 0; k < subcol_max_widths[i][needed_enclosure_lvl[j]] + 2; ++k)
                        {
                            header_footer_separator_out  << grid[7][1];
                            ++string_offset;
                        }
                        was_empty = false;
                    }
                }
            }
        }
        header_footer_separator_out  << (was_empty ? vertical_bold_bar : grid[7][3]) << "\n";
        if (!has_subcol)
        {
            header_footer_separator.clear();
        }
        header_footer_separator_out.finalize();
        if (is_top)
        {
            writeString(header_footer_separator, out);
            return "";
        }
        else
        {
            return header_footer_separator;
        }
    };

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
    
    auto write_subcol_names = [&](bool is_top, int32_t cur_level, std::queue<size_t> & prev_cols_indexes) -> String
    {
        String string = left_blank;
        WriteBufferFromString string_out(string, AppendModeTag{});
        size_t string_offset = 0;
        if (style == Style::Full)
            string_out << vertical_bold_bar << " ";
        else if (style == Style::Compact)
        {
            string_out << grid[2][0] << horizontal_bar;
            string_offset += 2;
        }
        else if (style == Style::Space)
            string_out << " ";

        for (size_t i = 0; i < num_columns; ++i)
        {
            if (i != 0)
            {
                if (style == Style::Full)
                    string_out << " " << vertical_bold_bar << " ";
                else if (style == Style::Compact)
                {
                    string_out << horizontal_bar << grid[2][2] << horizontal_bar;
                    string_offset += 3;
                }
                else if (style == Style::Space)
                    string_out << "   ";
            }
            const auto & col = header.getByPosition(i);
            EnclosureLevelContainer needed_enclosure_lvl;
            returnNeededEnclosureLvl(col.type, cur_level, needed_enclosure_lvl, i, -1, max_enc_lvl[i], 0);
            if (!subcol_names[i].empty()) {
                for (size_t j = 0; j < needed_enclosure_lvl.size(); ++j)
                {
                    int64_t ind = needed_enclosure_lvl[j];
                    if (ind < 0 || ind == INT64_MIN)
                    {
                        for (size_t k = 0; k < subcol_max_widths[i][(ind == INT64_MIN) ? 0 : ind * -1]; ++k)
                        {
                            if (style == Style::Full)
                                string_out << " ";
                            else if (style == Style::Compact)
                            {
                                string_out << horizontal_bar;
                                ++string_offset;
                            }
                        }
                    }
                    else
                    {
                        auto write_value = [&]
                        {
                            if (color)
                                string_out << "\033[1m";
                            string_out << subcol_names[i][ind];
                            if (color)
                                string_out << "\033[0m";
                        };
        
                        auto write_padding = [&]
                        {
                            for (size_t k = 0; k < subcol_max_widths[i][ind] - subcol_name_widths[i][ind]; ++k)
                            {
                                if (style == Style::Compact)
                                {
                                    string_out << horizontal_bar;
                                    ++string_offset;
                                }
                                else
                                    string_out << " ";
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
                        string_offset += subcol_name_widths[i][ind];
                    }
                    if (j != needed_enclosure_lvl.size() - 1)
                    {
                        if (style == Style::Full)
                            string_out << " " << vertical_bold_bar << " ";
                        else if (style == Style::Compact)
                        {
                            if (!prev_cols_indexes.empty() && string_offset == prev_cols_indexes.front())
                            {
                                string_out << horizontal_bar << grid[2][2] << horizontal_bar;
                                prev_cols_indexes.pop();
                            }
                            else
                            {
                                string_out << horizontal_bar << (is_top ? grid[6][2] : grid[3][2]) << horizontal_bar;
                            }
                            prev_cols_indexes.push(string_offset);
                            string_offset += 3;
                        }
                        else if (style == Style::Space)
                            string_out << "   ";
                    }
                }
            }
            else
            {
                for (size_t k = 0; k < max_widths[i]; ++k)
                {
                    if (style == Style::Full)
                        string_out << " ";
                    else if (style == Style::Compact)
                    {
                        string_out << horizontal_bar;
                        ++string_offset;
                    }
                }
            }
        }
        if (style == Style::Full)
            string_out << " " << vertical_bold_bar;
        else if (style == Style::Compact)
            string_out << horizontal_bar << grid[2][3];

        string_out << "\n";
        string_out.finalize();
        if (!is_top && style == Style::Compact)
        {
            return string;
        }
        else
        {
            writeString(string, out);
            return "";
        }
    };
    writeString(header_begin, out);
    write_names(true);
    std::queue<size_t> prev_cols_indexes;
    std::queue<size_t> prev_cols_indexes_compact;
    int32_t mx_enc = *std::max_element(max_enc_lvl.begin(), max_enc_lvl.end());
    for (int32_t i = 1; i <= mx_enc; ++i) {
        if (style == Style::Full) 
            write_next_separator(i, prev_cols_indexes, true);
        write_subcol_names(true, i, prev_cols_indexes_compact);
    }
    writeString(header_end, out);

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
            for (size_t i = 0; i < times; ++i)
                writeCString("\033[1A\033[2K\033[G", out);
        }
        if (!rows_separator.empty())
            writeString(rows_separator, out);
    }
    else
    {
        writeString(header_begin, out);
        write_names(true);
        writeString(header_end, out);
    }

    bool vertical_filler_written = false;
    size_t displayed_row = 0;
    std::vector<std::optional<String>> serialized_values(num_columns);
    std::vector<size_t> offsets_inside_serialized_values(num_columns);
    std::vector<std::vector<std::optional<String>>> serialized_subcol_values(num_columns);
    std::vector<std::vector<size_t>> offsets_inside_serialized_subcol_values(num_columns);
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
                    if (style != Style::Space)
                        out << vertical_bar;
                    else if (j != 0)
                        out << " ";
                    auto sub_names = type.getSubcolumnNames();
                    if (!sub_names.empty() && format_settings.pretty.display_tuple_as_subcolumns)
                    {
                        serialized_subcol_values[j].resize(sub_names.size());
                        offsets_inside_serialized_subcol_values[j].resize(sub_names.size());
                        for (size_t k = 0; k < leaves[j].size(); ++k)
                        {
                            serialized_subcol_values[j][leaves[j][k]].reset();
                            offsets_inside_serialized_subcol_values[j][leaves[j][k]] = 0;
                            String serialized_value;
                            WriteBufferFromString out_serialize(serialized_value);
                            auto serialization = type.getDefaultSerialization();
                            auto subcolumn = type.getSubcolumn(sub_names[leaves[j][k]], columns[j]);
                            auto sub_serialization = type.getSubcolumnSerialization(sub_names[leaves[j][k]], serialization);
                            sub_serialization->serializeText(*subcolumn, i, out_serialize, format_settings);
                            writeValueWithPadding(
                                *subcolumn,
                                *sub_serialization,
                                i,
                                format_settings.pretty.multiline_fields, serialized_subcol_values[j][leaves[j][k]], offsets_inside_serialized_subcol_values[j][leaves[j][k]],
                                subcol_widths[j][leaves[j][k]].empty() ? subcol_max_widths[j][leaves[j][k]] : subcol_widths[j][leaves[j][k]][displayed_row],
                                subcol_max_widths[j][leaves[j][k]],
                                cut_to_width,
                                type.shouldAlignRightInPrettyFormats(),
                                isNumber(type));
                            if (serialized_subcol_values[j][leaves[j][k]]->size() != offsets_inside_serialized_subcol_values[j][leaves[j][k]])
                                all_lines_printed = false;
                            if (k != leaves[j].size() - 1)
                            {
                                if (style != Style::Space)
                                    out << vertical_bar;
                                else if (j != 0)
                                    out << " ";
                            }
                        }
                    }
                    else
                    {
                        writeValueWithPadding(
                            *columns[j],
                            *serializations[j],
                            i,
                            format_settings.pretty.multiline_fields, serialized_values[j], offsets_inside_serialized_values[j],
                            widths[j].empty() ? max_widths[j] : widths[j][displayed_row],
                            max_widths[j],
                            cut_to_width,
                            type.shouldAlignRightInPrettyFormats(),
                            isNumber(type));

                        if (offsets_inside_serialized_values[j] != serialized_values[j]->size())
                            all_lines_printed = false;
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
        prev_cols_indexes = {};
        std::vector<String> separators;
        if (style == Style::Full)
        {
            for (int32_t i = 1; i <= mx_enc; ++i)
            {
                separators.emplace_back(write_next_separator(i, prev_cols_indexes, false));
            }
        }
        if (style == Style::Compact)
        {
            for (int32_t i = 1; i <= mx_enc; ++i)
            {
                separators.emplace_back(write_subcol_names(false, i, prev_cols_indexes));
            }
        }
        for (int32_t i = mx_enc; i >= 1; --i) {
            if (style == Style::Compact)
            {
                writeString(separators[i - 1], out);
                continue;
            }
            write_subcol_names(false, i, prev_cols_indexes_compact);
            if (style == Style::Full)
                writeString(separators[i - 1], out);
        }
        write_names(false);
        writeString(footer_end, out);
        had_footer = true;
    }
    else
    {
        //    └──────┘
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
                    bool glue_chunks = !no_escapes
                        && (format_settings.pretty.glue_chunks == 1 || (format_settings.pretty.glue_chunks == 2 && format_settings.is_writing_to_terminal));
                    return std::make_shared<PrettyBlockOutputFormat>(buf, sample, format_settings, style, mono_block, color, glue_chunks);
                });
            }
        }
    }
}

}
