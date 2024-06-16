#include <Formats/FormatFactory.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Processors/Formats/Impl/PrettySpaceBlockOutputFormat.h>
#include <Common/PODArray.h>


namespace DB
{


void PrettySpaceBlockOutputFormat::writeChunk(const Chunk & chunk, PortKind port_kind)
{
    UInt64 max_rows = format_settings.pretty.max_rows;

    if (total_rows >= max_rows)
    {
        total_rows += chunk.getNumRows();
        return;
    }

    size_t num_rows = chunk.getNumRows();
    size_t num_columns = chunk.getNumColumns();
    const auto & header = getPort(port_kind).getHeader();
    const auto & columns = chunk.getColumns();

    size_t cut_to_width = format_settings.pretty.max_value_width;
    if (!format_settings.pretty.max_value_width_apply_for_single_value && num_rows == 1 && num_columns == 1 && total_rows == 0)
        cut_to_width = 0;

    WidthsPerColumn widths;
    Widths max_widths;
    Widths name_widths;
    BreakLinePosInTable break_line_pos_in_table;
    calculateWidths(header, chunk, widths, max_widths, name_widths, break_line_pos_in_table, 1);

    if (format_settings.pretty.output_format_pretty_row_numbers)
        writeString(String(row_number_width, ' '), out);
    /// Names
    for (size_t i = 0; i < num_columns; ++i)
    {
        if (i != 0)
            writeCString("   ", out);
        else
            writeChar(' ', out);

        const ColumnWithTypeAndName & col = header.getByPosition(i);

        if (col.type->shouldAlignRightInPrettyFormats())
        {
            for (ssize_t k = 0; k < std::max(0z, static_cast<ssize_t>(max_widths[i] - name_widths[i])); ++k)
                writeChar(' ', out);

            if (color)
                writeCString("\033[1m", out);
            writeString(col.name, out);
            if (color)
                writeCString("\033[0m", out);
        }
        else
        {
            if (color)
                writeCString("\033[1m", out);
            writeString(col.name, out);
            if (color)
                writeCString("\033[0m", out);

            for (ssize_t k = 0; k < std::max(0z, static_cast<ssize_t>(max_widths[i] - name_widths[i])); ++k)
                writeChar(' ', out);
        }
    }
    writeCString("\n\n", out);

    bool has_transferred_row = false;

    for (size_t row = 0; row < num_rows && total_rows + row < max_rows; ++row)
    {
        if (format_settings.pretty.output_format_pretty_row_numbers)
        {
            // Write row number;
            auto row_num_string = std::to_string(row + 1 + total_rows) + ". ";
            for (size_t i = 0; i < row_number_width - row_num_string.size(); ++i)
                writeChar(' ', out);
            if (color)
                writeCString("\033[90m", out);
            writeString(row_num_string, out);
            if (color)
                writeCString("\033[0m", out);

        }
        size_t prefix = format_settings.pretty.output_format_pretty_row_numbers ? row_number_width + 1 : 1;
        for (size_t column = 0; column < num_columns; ++column)
        {
            if (column != 0)
                writeCString(" ", out);

            const auto & type = *header.getByPosition(column).type;
            const auto & cur_width = widths[column].empty() ? max_widths[column] : widths[column][row];

            writeValueWithPadding(*columns[column], *serializations[column], row, cur_width, max_widths[column], cut_to_width,
                type.shouldAlignRightInPrettyFormats(), isNumber(type), break_line_pos_in_table[row][column], prefix, false);

            has_transferred_row |= !break_line_pos_in_table[row][column].empty();
            prefix += max_widths[column] + 3;
        }

        writeReadableNumberTip(chunk);
        writeChar('\n', out);

        if (has_transferred_row)
            writeTransferredRow(columns, header, widths, max_widths, break_line_pos_in_table[row], row, cut_to_width, true);
    }

    total_rows += num_rows;
}


void PrettySpaceBlockOutputFormat::writeSuffix()
{
    writeMonoChunkIfNeeded();

    if (total_rows >= format_settings.pretty.max_rows)
    {
        writeCString("\nShowed first ", out);
        writeIntText(format_settings.pretty.max_rows, out);
        writeCString(".\n", out);
    }
}


void registerOutputFormatPrettySpace(FormatFactory & factory)
{
    registerPrettyFormatWithNoEscapesAndMonoBlock<PrettySpaceBlockOutputFormat>(factory, "PrettySpace");
}

}
