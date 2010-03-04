#include <DB/DataStreams/RowInputStreamFromBlockInputStream.h>


namespace DB
{

using Poco::SharedPtr;


RowInputStreamFromBlockInputStream::RowInputStreamFromBlockInputStream(IBlockInputStream & block_input_)
	: block_input(block_input_), pos(0), current_rows(0)
{
}

class TestVisitor : public boost::static_visitor<UInt64>
{
public:
	template <typename T> UInt64 operator() (const T & x) const
	{
		return 0;
	}
};


Row RowInputStreamFromBlockInputStream::read()
{
/*	if (pos >= current_rows)
	{
		current_block = block_input.read();
		current_rows = current_block.rows();
		pos = 0;
	}

	ColumnVisitorNthElement visitor(pos);
	size_t columns = current_block.columns();
	Row row(columns);

	for (size_t i = 0; i < columns; ++i)
		row[i] = boost::apply_visitor(visitor, *current_block.getByPosition(i).column);

	return row;
*/

	Column column = UInt64Column(0);
	Field field = boost::apply_visitor(TestVisitor(), column);

	Row row;
	return row;
}

}
