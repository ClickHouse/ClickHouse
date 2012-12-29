#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/DataStreams/BlockInputStreamFromRowInputStream.h>


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

			Row row = row_input->read();

			if (row.empty())
				return res;

			if (!res)
				res = sample.cloneEmpty();

			if (row.size() != sample.columns())
				throw Exception("Number of columns doesn't match", ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH);

			for (size_t i = 0; i < row.size(); ++i)
				res.getByPosition(i).column->insert(row[i]);
		}
	}
	catch (const DB::Exception & e)
	{
		throw DB::Exception(e.message() + " (at row " + Poco::NumberFormatter::format(total_rows + 1) + ")", e, e.code());
	}

	return res;
}

}
