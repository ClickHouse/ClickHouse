#include <Core/Block.h>
#include <Formats/BlockOutputStreamFromRowOutputStream.h>


namespace DB
{

BlockOutputStreamFromRowOutputStream::BlockOutputStreamFromRowOutputStream(RowOutputStreamPtr row_output_, const Block & header_)
    : row_output(row_output_), header(header_) {}


void BlockOutputStreamFromRowOutputStream::write(const Block & block)
{
    size_t rows = block.rows();
    for (size_t i = 0; i < rows; ++i)
    {
        if (!first_row)
            row_output->writeRowBetweenDelimiter();
        first_row = false;
        row_output->write(block, i);
    }
}


void BlockOutputStreamFromRowOutputStream::setRowsBeforeLimit(size_t rows_before_limit)
{
    row_output->setRowsBeforeLimit(rows_before_limit);
}

void BlockOutputStreamFromRowOutputStream::setTotals(const Block & totals)
{
    row_output->setTotals(totals);
}

void BlockOutputStreamFromRowOutputStream::setExtremes(const Block & extremes)
{
    row_output->setExtremes(extremes);
}

void BlockOutputStreamFromRowOutputStream::onProgress(const Progress & progress)
{
    row_output->onProgress(progress);
}

}
