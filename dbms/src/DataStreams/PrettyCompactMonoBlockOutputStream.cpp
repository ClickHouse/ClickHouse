#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <DataStreams/PrettyCompactMonoBlockOutputStream.h>


namespace DB
{

void PrettyCompactMonoBlockOutputStream::write(const Block & block)
{
    if (total_rows < max_rows)
        blocks.push_back(block);

    total_rows += block.rows();
}

void PrettyCompactMonoBlockOutputStream::writeSuffix()
{
    if (blocks.empty())
        return;

    Widths_t max_widths;
    Widths_t name_widths;

    for (size_t i = 0; i < blocks.size(); ++i)
        calculateWidths(blocks[i], max_widths, name_widths);

    writeHeader(blocks.front(), max_widths, name_widths);

    size_t row_count = 0;

    for (size_t block_id = 0; block_id < blocks.size() && row_count < max_rows; ++block_id)
    {
        const Block & block = blocks[block_id];
        size_t rows = block.rows();

        for (size_t i = 0; i < rows && row_count < max_rows; ++i)
        {
            writeRow(i, block, max_widths, name_widths);
            ++row_count;
        }
    }

    writeBottom(max_widths);

    if (total_rows >= max_rows)
    {
        writeCString("  Showed first ", ostr);
        writeIntText(max_rows, ostr);
        writeCString(".\n", ostr);
    }

    total_rows = 0;

    if (totals)
    {
        writeCString("\nTotals:\n", ostr);
        PrettyCompactBlockOutputStream::write(totals);
    }

    if (extremes)
    {
        writeCString("\nExtremes:\n", ostr);
        PrettyCompactBlockOutputStream::write(extremes);
    }
}

}
