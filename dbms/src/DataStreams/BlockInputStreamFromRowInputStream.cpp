#include <DB/Common/Exception.h>
#include <DB/DataStreams/BlockInputStreamFromRowInputStream.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
}


BlockInputStreamFromRowInputStream::BlockInputStreamFromRowInputStream(
	RowInputStreamPtr row_input_,
	const Block & sample_,
	size_t max_block_size_)
	: row_input(row_input_), sample(sample_), max_block_size(max_block_size_), total_rows(0)
{
}


Block BlockInputStreamFromRowInputStream::readImpl()
{
	Block res = sample.cloneEmpty();

	try
	{
		for (size_t rows = 0; rows < max_block_size; ++rows, ++total_rows)
		{
			if (total_rows == 0)
				row_input->readRowBetweenDelimiter();

			if (!row_input->read(res))
				break;
		}
	}
	catch (Exception & e)
	{
		e.addMessage("(at row " + toString(total_rows + 1) + ")");
		throw;
	}

	if (res.rowsInFirstColumn() == 0)
		res.clear();
	else
		res.optimizeNestedArraysOffsets();

	return res;
}

}
