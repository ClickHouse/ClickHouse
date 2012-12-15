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
	: row_input(row_input_), sample(sample_), columns(sample.columns()), max_block_size(max_block_size_),
	first_row(true), first_block(true), byte_counts(columns)
{
}


Block BlockInputStreamFromRowInputStream::readImpl()
{
	Block res;

	for (size_t rows = 0; rows < max_block_size; ++rows)
	{
		if (!first_row)
			row_input->readRowBetweenDelimiter();
		first_row = false;
		
		Row row = row_input->read();

		if (row.empty())
			return res;

		if (!res)
		{
			res = sample.cloneEmpty();

			if (!first_block)
			{
				/// Если прочитался уже хотя бы один полный блок, то будем резервировать память для max_block_size строк.
				for (size_t i = 0; i < columns; ++i)
					res.getByPosition(i).column->reserve(max_block_size, byte_counts[i]);
			}
		}

		if (row.size() != columns)
			throw Exception("Number of columns doesn't match", ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH);

		for (size_t i = 0; i < columns; ++i)
			res.getByPosition(i).column->insert(row[i]);
	}

	first_block = false;

	for (size_t i = 0; i < columns; ++i)
		byte_counts[i] = std::max(byte_counts[i], res.getByPosition(i).column->byteSize() + 32768);

	return res;
}

}
