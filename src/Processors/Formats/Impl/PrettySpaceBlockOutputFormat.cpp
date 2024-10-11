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
    calculateWidths(header, chunk, widths, max_widths, name_widths);

    if (format_settings.pretty.output_format_pretty_row_numbers)
        writeString(String(row_number_width, ' '), out);
    /// Names
    auto write_names = [&](const bool is_footer) -> void
    {
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
        if (!is_footer)
            writeCString("\n\n", out);
        else
            writeCString("\n", out);
    };
    write_names(false);

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
        for (size_t column = 0; column < num_columns; ++column)
        {
            if (column != 0)
                writeCString(" ", out);

            const auto & type = *header.getByPosition(column).type;
            auto & cur_width = widths[column].empty() ? max_widths[column] : widths[column][row];
            writeValueWithPadding(
                *columns[column], *serializations[column], row, cur_width, max_widths[column], cut_to_width, type.shouldAlignRightInPrettyFormats(), isNumber(type));
        }
        writeReadableNumberTip(chunk);
        writeChar('\n', out);
    }

    /// Write blank line between last row and footer
    if ((num_rows >= format_settings.pretty.output_format_pretty_display_footer_column_names_min_rows) && format_settings.pretty.output_format_pretty_display_footer_column_names)
        writeCString("\n", out);
    /// Write left blank
    if ((num_rows >= format_settings.pretty.output_format_pretty_display_footer_column_names_min_rows) && format_settings.pretty.output_format_pretty_row_numbers && format_settings.pretty.output_format_pretty_display_footer_column_names)
        writeString(String(row_number_width, ' '), out);
    /// Write footer
    if ((num_rows >= format_settings.pretty.output_format_pretty_display_footer_column_names_min_rows) && format_settings.pretty.output_format_pretty_display_footer_column_names)
        write_names(true);
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
