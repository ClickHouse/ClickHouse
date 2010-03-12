#include <DB/DataStreams/RowInputStreamFromBlockInputStream.h>

namespace DB
{

using Poco::SharedPtr;


RowInputStreamFromBlockInputStream::RowInputStreamFromBlockInputStream(IBlockInputStream & block_input_)
	: block_input(block_input_), pos(0), current_rows(0)
{
}


Row RowInputStreamFromBlockInputStream::read()
{
	if (pos >= current_rows)
	{
		current_block = block_input.read();
		current_rows = current_block.rows();
		pos = 0;
	}

	size_t columns = current_block.columns();
	Row row(columns);

	for (size_t i = 0; i < columns; ++i)
		row[i] = (*current_block.getByPosition(i).column)[pos];

	++pos;
	return row;
}

}
