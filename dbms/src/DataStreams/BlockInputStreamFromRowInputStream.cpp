#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/DataStreams/BlockInputStreamFromRowInputStream.h>


namespace DB
{

using Poco::SharedPtr;


BlockInputStreamFromRowInputStream::BlockInputStreamFromRowInputStream(
	IRowInputStream & row_input_,
	const Block & sample_,
	size_t max_block_size_)
	: row_input(row_input_), sample(sample_), max_block_size(max_block_size_)
{
}


void BlockInputStreamFromRowInputStream::initBlock(Block & res)
{
	for (size_t i = 0; i < sample.columns(); ++i)
	{
		const ColumnWithNameAndType & sample_elem = sample.getByPosition(i);
		ColumnWithNameAndType res_elem;

		res_elem.column = sample_elem.column->cloneEmpty();
		res_elem.type = sample_elem.type->clone();
		res_elem.name = sample_elem.name;

		res.insert(res_elem);
	}
}


Block BlockInputStreamFromRowInputStream::readImpl()
{
	Block res;

	for (size_t rows = 0; rows < max_block_size; ++rows)
	{
		Row row = row_input.read();

		if (row.empty())
			return res;

		if (!res)
			initBlock(res);

		if (row.size() != sample.columns())
			throw Exception("Number of columns doesn't match", ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH);

		for (size_t i = 0; i < row.size(); ++i)
			res.getByPosition(i).column->insert(row[i]);
	}

	return res;
}

}
