#include <DB/DataStreams/BlockOutputStreamFromRowOutputStream.h>
#include <DB/DataStreams/JSONRowOutputStream.h>

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
			row_output->writeField((*block.getByPosition(j).column)[i]);
		}
		
		row_output->writeRowEndDelimiter();
	}
}


void BlockOutputStreamFromRowOutputStream::setRowsBeforeLimit(size_t rows_before_limit)
{
	if (JSONRowOutputStream * json_out = dynamic_cast<JSONRowOutputStream *>(&*row_output))
	{
		json_out->setRowsBeforeLimit(rows_before_limit);
	}
}

}
