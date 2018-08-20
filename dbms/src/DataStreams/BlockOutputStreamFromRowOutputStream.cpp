/* Some modifications Copyright (c) 2018 BlackBerry Limited

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */
#include <Core/Block.h>
#include <DataStreams/BlockOutputStreamFromRowOutputStream.h>


namespace DB
{

BlockOutputStreamFromRowOutputStream::BlockOutputStreamFromRowOutputStream(RowOutputStreamPtr row_output_)
    : row_output(row_output_), first_row(true) {}


void BlockOutputStreamFromRowOutputStream::write(const Block & block)
{
    size_t rows = block.rows();
    size_t columns = block.columns();

    for (size_t i = 0; i < rows; ++i)
    {
        if (!first_row)
            row_output->writeRowBetweenDelimiter();
        first_row = false;

        row_output->writeRowStartDelimiter();

        for (size_t j = 0; j < columns; ++j)
        {
            if (j != 0)
                row_output->writeFieldDelimiter();

            auto & col = block.getByPosition(j);
            row_output->writeField(col.name, *col.column, *col.type, i);
        }

        row_output->writeRowEndDelimiter();
    }
}

void BlockOutputStreamFromRowOutputStream::setSampleBlock(const Block & sample)
{
    row_output->setSampleBlock(sample);
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

void BlockOutputStreamFromRowOutputStream::onHeartbeat(const Heartbeat & heartbeat)
{
    row_output->onHeartbeat(heartbeat);
}


}
