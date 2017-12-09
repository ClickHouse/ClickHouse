#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <DataStreams/PrettySpaceBlockOutputStream.h>


namespace DB
{


void PrettySpaceBlockOutputStream::write(const Block & block)
{
    if (total_rows >= max_rows)
    {
        total_rows += block.rows();
        return;
    }

    size_t rows = block.rows();
    size_t columns = block.columns();

    WidthsPerColumn widths;
    Widths max_widths;
    Widths name_widths;
    calculateWidths(block, widths, max_widths, name_widths);

    /// Do not align on too long values.
    if (terminal_width > 80)
        for (size_t i = 0; i < columns; ++i)
            if (max_widths[i] > terminal_width / 2)
                max_widths[i] = terminal_width / 2;

    /// Names
    for (size_t i = 0; i < columns; ++i)
    {
        if (i != 0)
            writeCString("   ", ostr);

        const ColumnWithTypeAndName & col = block.getByPosition(i);

        if (col.type->shouldAlignRightInPrettyFormats())
        {
            for (ssize_t k = 0; k < std::max(static_cast<ssize_t>(0), static_cast<ssize_t>(max_widths[i] - name_widths[i])); ++k)
                writeChar(' ', ostr);

            if (!no_escapes)
                writeCString("\033[1m", ostr);
            writeEscapedString(col.name, ostr);
            if (!no_escapes)
                writeCString("\033[0m", ostr);
        }
        else
        {
            if (!no_escapes)
                writeCString("\033[1m", ostr);
            writeEscapedString(col.name, ostr);
            if (!no_escapes)
                writeCString("\033[0m", ostr);

            for (ssize_t k = 0; k < std::max(static_cast<ssize_t>(0), static_cast<ssize_t>(max_widths[i] - name_widths[i])); ++k)
                writeChar(' ', ostr);
        }
    }
    writeCString("\n\n", ostr);

    for (size_t i = 0; i < rows && total_rows + i < max_rows; ++i)
    {
        for (size_t j = 0; j < columns; ++j)
        {
            if (j != 0)
                writeCString("   ", ostr);

            writeValueWithPadding(block.getByPosition(j), i, widths[j].empty() ? max_widths[j] : widths[j][i], max_widths[j]);
        }

        writeChar('\n', ostr);
    }

    total_rows += rows;
}


void PrettySpaceBlockOutputStream::writeSuffix()
{
    if (total_rows >= max_rows)
    {
        writeCString("\nShowed first ", ostr);
        writeIntText(max_rows, ostr);
        writeCString(".\n", ostr);
    }

    total_rows = 0;
    writeTotals();
    writeExtremes();
}

}
