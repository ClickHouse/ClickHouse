#include <DB/Columns/ColumnsNumber.h>

#include <DB/DataStreams/FilterBlockInputStream.h>


namespace DB
{


FilterBlockInputStream::FilterBlockInputStream(BlockInputStreamPtr input_, ssize_t filter_column_)
	: filter_column(filter_column_)
{
	children.push_back(input_);
}

FilterBlockInputStream::FilterBlockInputStream(BlockInputStreamPtr input_, const String & filter_column_name_)
	: filter_column(-1), filter_column_name(filter_column_name_)
{
	children.push_back(input_);
}


Block FilterBlockInputStream::readImpl()
{
	Block res;

	/// Пока не встретится блок, после фильтрации которого что-нибудь останется, или поток не закончится.
	while (1)
	{
		res = children.back()->read();
		if (!res)
			return res;

		/// Найдём настоящую позицию столбца с фильтром в блоке.
		if (filter_column == -1)
			filter_column = res.getPositionByName(filter_column_name);

		size_t columns = res.columns();
		ColumnPtr column = res.getByPosition(filter_column).column;

		/** Если фильтр - константа (например, написано WHERE 1),
		  *  то либо вернём пустой блок, либо вернём блок без изменений.
		  */
		ColumnConstUInt8 * column_const = typeid_cast<ColumnConstUInt8 *>(&*column);
		if (column_const)
		{
			if (!column_const->getData())
				res.clear();

			return res;
		}

		ColumnUInt8 * column_vec = typeid_cast<ColumnUInt8 *>(&*column);
		if (!column_vec)
			throw Exception("Illegal type " + column->getName() + " of column for filter. Must be ColumnUInt8 or ColumnConstUInt8.", ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);

		IColumn::Filter & filter = column_vec->getData();

		/// Если кроме столбца с фильтром ничего нет.
		if (columns == 1)
		{
			/// То посчитаем в нём количество единичек.
			size_t filtered_rows = 0;
			for (size_t i = 0, size = filter.size(); i < size; ++i)
				if (filter[i])
					++filtered_rows;

			/// Если текущий блок полностью отфильтровался - перейдём к следующему.
			if (filtered_rows == 0)
				continue;

			/// Заменяем этот столбец на столбец с константой 1, нужного размера.
			res.getByPosition(filter_column).column = new ColumnConstUInt8(filtered_rows, 1);

			return res;
		}

		/// Общий случай - фильтруем остальные столбцы.
		for (size_t i = 0; i < columns; ++i)
		{
			if (i != static_cast<size_t>(filter_column))
			{
				ColumnWithNameAndType & current_column = res.getByPosition(i);
				current_column.column = current_column.column->filter(filter);
				if (current_column.column->empty())
					break;
			}
		}

		/// Любой столбец - не являющийся фильтром.
		IColumn & any_not_filter_column = *res.getByPosition(filter_column == 0 ? 1 : 0).column;

		/// Если текущий блок полностью отфильтровался - перейдём к следующему.
		if (any_not_filter_column.empty())
			continue;

		/// Сам столбец с фильтром заменяем на столбец с константой 1, так как после фильтрации в нём ничего другого не останется.
		res.getByPosition(filter_column).column = new ColumnConstUInt8(any_not_filter_column.size(), 1);

		return res;
	}
}


}
