#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/DataStreams/BlockInputStreamFromRowInputStream.h>
#include <DB/DataTypes/DataTypeNested.h>
#include <DB/Columns/ColumnArray.h>


namespace DB
{

using Poco::SharedPtr;


BlockInputStreamFromRowInputStream::BlockInputStreamFromRowInputStream(
	RowInputStreamPtr row_input_,
	const Block & sample_,
	size_t max_block_size_)
	: row_input(row_input_), sample(sample_), max_block_size(max_block_size_), total_rows(0)
{
}


Block BlockInputStreamFromRowInputStream::readImpl()
{
	Block res;

	try
	{
		for (size_t rows = 0; rows < max_block_size; ++rows, ++total_rows)
		{
			if (total_rows == 0)
				row_input->readRowBetweenDelimiter();

			Row row;
			bool has_row = row_input->read(row);

			if (!has_row)
				break;

			if (!res)
				res = sample.cloneEmpty();

			if (row.size() != sample.columns())
				throw Exception("Number of columns doesn't match", ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH);

			for (size_t i = 0; i < row.size(); ++i)
				res.getByPosition(i).column->insert(row[i]);
		}
	}
	catch (Exception & e)
	{
		e.addMessage("(at row " + toString(total_rows + 1) + ")");
		throw;
	}
	
	res.optimizeNestedArraysOffsets();
	
	return res;
}

}
