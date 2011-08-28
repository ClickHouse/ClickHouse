#include <DB/Columns/ColumnsNumber.h>

#include <DB/DataStreams/FilterBlockInputStream.h>


namespace DB
{

	
FilterBlockInputStream::FilterBlockInputStream(BlockInputStreamPtr input_, ssize_t filter_column_)
	: input(input_), filter_column(filter_column_)
{
}


Block FilterBlockInputStream::read()
{
	/// Пока не встретится блок, после фильтрации которого что-нибудь останется, или поток не закончится.
	while (1)
	{
		Block res = input->read();
		if (!res)
			return res;

		/// Если кроме столбца с фильтром ничего нет.
		if (res.columns() <= 1)
			throw Exception("There is only filter column in block.", ErrorCodes::ONLY_FILTER_COLUMN_IN_BLOCK);

		if (filter_column < 0)
			filter_column = static_cast<ssize_t>(res.columns()) + filter_column;

		/// Любой столбец - не являющийся фильтром.
		IColumn & any_not_filter_column = *res.getByPosition(filter_column == 0 ? 1 : 0).column;

		size_t columns = res.columns();
		ColumnPtr column = res.getByPosition(filter_column).column;

		ColumnConstUInt8 * column_const = dynamic_cast<ColumnConstUInt8 *>(&*column);
		if (column_const)
		{
			return column_const->getData()
				? res
				: Block();
		}

		ColumnUInt8 * column_vec = dynamic_cast<ColumnUInt8 *>(&*column);
		if (!column_vec)
			throw Exception("Illegal type " + column->getName() + " of column for filter. Must be ColumnUInt8 or ColumnConstUInt8.", ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);

		IColumn::Filter & filter = column_vec->getData();

		for (size_t i = 0; i < columns; ++i)
		{
			if (i != static_cast<size_t>(filter_column))
			{
				IColumn & current_column = *res.getByPosition(i).column;
				current_column.filter(filter);
				if (current_column.empty())
					break;
			}
		}

		/// Если текущий блок полностью отфильтровался - перейдём к следующему.
		if (any_not_filter_column.empty())
			continue;

		/// Сам столбец с фильтром заменяем на столбец с константой 1, так как после фильтрации в нём ничего другого не останется.
		res.getByPosition(filter_column).column = new ColumnConstUInt8(any_not_filter_column.size(), 1);

		return res;
	}
}


}
