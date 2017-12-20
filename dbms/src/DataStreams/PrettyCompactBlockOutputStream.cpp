#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <DataStreams/PrettyCompactBlockOutputStream.h>


namespace DB
{

namespace ErrorCodes
{

extern const int ILLEGAL_COLUMN;

}

void PrettyCompactBlockOutputStream::writeHeader(
    const Block & block,
    const Widths & max_widths,
    const Widths & name_widths)
{
    /// Names
    writeCString("┌─", ostr);
    for (size_t i = 0; i < max_widths.size(); ++i)
    {
        if (i != 0)
            writeCString("─┬─", ostr);

        const ColumnWithTypeAndName & col = block.getByPosition(i);

        if (col.type->shouldAlignRightInPrettyFormats())
        {
            for (size_t k = 0; k < max_widths[i] - name_widths[i]; ++k)
                writeCString("─", ostr);

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

            for (size_t k = 0; k < max_widths[i] - name_widths[i]; ++k)
                writeCString("─", ostr);
        }
    }
    writeCString("─┐\n", ostr);
}

void PrettyCompactBlockOutputStream::writeBottom(const Widths & max_widths)
{
    /// Create delimiters
    std::stringstream bottom_separator;

    bottom_separator << "└";
    for (size_t i = 0; i < max_widths.size(); ++i)
    {
        if (i != 0)
            bottom_separator << "┴";

        for (size_t j = 0; j < max_widths[i] + 2; ++j)
            bottom_separator << "─";
    }
    bottom_separator << "┘\n";

    writeString(bottom_separator.str(), ostr);
}

void PrettyCompactBlockOutputStream::writeRow(
    size_t row_num,
    const Block & block,
    const WidthsPerColumn & widths,
    const Widths & max_widths)
{
    size_t columns = max_widths.size();

    writeCString("│ ", ostr);

    for (size_t j = 0; j < columns; ++j)
    {
        if (j != 0)
            writeCString(" │ ", ostr);

        writeValueWithPadding(block.getByPosition(j), row_num, widths[j].empty() ? max_widths[j] : widths[j][row_num], max_widths[j]);
    }

    writeCString(" │\n", ostr);
}

void PrettyCompactBlockOutputStream::write(const Block & block)
{
    if (total_rows >= max_rows)
    {
        total_rows += block.rows();
        return;
    }

    size_t rows = block.rows();

    WidthsPerColumn widths;
    Widths max_widths;
    Widths name_widths;
    calculateWidths(block, widths, max_widths, name_widths);

    writeHeader(block, max_widths, name_widths);

    for (size_t i = 0; i < rows && total_rows + i < max_rows; ++i)
        writeRow(i, block, widths, max_widths);

    writeBottom(max_widths);

    total_rows += rows;
}

}
