#include <DB/Columns/ColumnsNumber.h>

#include <DB/DataStreams/FilterBlockInputStream.h>


namespace DB
{

FilterBlockInputStream::FilterBlockInputStream(SharedPtr<IBlockInputStream> input_, size_t filter_column_)
	: input(input_), filter_column(filter_column_)
{
}

Block FilterBlockInputStream::read()
{
	Block res = input->read();
	if (!res)
		return res;

	size_t columns = res.columns();
	IColumn::Filter & filter = dynamic_cast<ColumnUInt8 &>(*res.getByPosition(filter_column).column).getData();

	for (size_t i = 0; i < columns; ++i)
		if (i != filter_column)
			res.getByPosition(i).column->filter(filter);

	res.erase(filter_column);
	return res;
}

}
