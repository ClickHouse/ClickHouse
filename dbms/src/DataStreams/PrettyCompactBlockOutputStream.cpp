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
    const Widths_t & max_widths,
    const Widths_t & name_widths)
{
    /// Names
    writeCString("┌─", ostr);
    for (size_t i = 0; i < max_widths.size(); ++i)
    {
        if (i != 0)
            writeCString("─┬─", ostr);

        const ColumnWithTypeAndName & col = block.safeGetByPosition(i);

        if (col.type->isNumeric())
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

void PrettyCompactBlockOutputStream::writeBottom(const Widths_t & max_widths)
{
    /// Create delimiters
    std::stringstream bottom_separator;

    bottom_separator         << "└";
    for (size_t i = 0; i < max_widths.size(); ++i)
    {
        if (i != 0)
            bottom_separator         << "┴";

        for (size_t j = 0; j < max_widths[i] + 2; ++j)
            bottom_separator         << "─";
    }
    bottom_separator         << "┘\n";

    writeString(bottom_separator.str(), ostr);
}

void PrettyCompactBlockOutputStream::writeRow(
    size_t row_id,
    const Block & block,
    const Widths_t & max_widths,
    const Widths_t & name_widths)
{
    size_t columns = max_widths.size();

    writeCString("│ ", ostr);

    for (size_t j = 0; j < columns; ++j)
    {
        if (j != 0)
            writeCString(" │ ", ostr);

        const ColumnWithTypeAndName & col = block.safeGetByPosition(j);

        if (col.type->isNumeric())
        {
            size_t width = get<UInt64>((*block.safeGetByPosition(columns + j).column)[row_id]);
            for (size_t k = 0; k < max_widths[j] - width; ++k)
                writeChar(' ', ostr);

            col.type->serializeTextEscaped(*col.column.get(), row_id, ostr);
        }
        else
        {
            col.type->serializeTextEscaped(*col.column.get(), row_id, ostr);

            size_t width = get<UInt64>((*block.safeGetByPosition(columns + j).column)[row_id]);
            for (size_t k = 0; k < max_widths[j] - width; ++k)
                writeChar(' ', ostr);
        }
    }

    writeCString(" │\n", ostr);
}

void PrettyCompactBlockOutputStream::write(const Block & block_)
{
    if (total_rows >= max_rows)
    {
        total_rows += block_.rows();
        return;
    }

    /// We will insert columns here with the calculated values of visible lengths.
    Block block = block_;

    size_t rows = block.rows();

    Widths_t max_widths;
    Widths_t name_widths;
    calculateWidths(block, max_widths, name_widths);

    writeHeader(block, max_widths, name_widths);

    for (size_t i = 0; i < rows && total_rows + i < max_rows; ++i)
        writeRow(i, block, max_widths, name_widths);

    writeBottom(max_widths);

    total_rows += rows;
}

}
