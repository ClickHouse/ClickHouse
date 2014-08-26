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
		const ColumnConstUInt8 * column_const = typeid_cast<const ColumnConstUInt8 *>(&*column);
		if (column_const)
		{
			if (!column_const->getData())
				res.clear();

			return res;
		}

		const ColumnUInt8 * column_vec = typeid_cast<const ColumnUInt8 *>(&*column);
		if (!column_vec)
			throw Exception("Illegal type " + column->getName() + " of column for filter. Must be ColumnUInt8 or ColumnConstUInt8.", ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);

		const IColumn::Filter & filter = column_vec->getData();

		/** Выясним, сколько строк будет в результате.
		  * Для этого отфильтруем первый попавшийся неконстантный столбец
		  *  или же посчитаем количество выставленных байт в фильтре.
		  */
		size_t first_non_constant_column = 0;
		for (size_t i = 0; i < columns; ++i)
		{
			if (!res.getByPosition(i).column->isConst())
			{
				first_non_constant_column = i;

				if (first_non_constant_column != static_cast<size_t>(filter_column))
					break;
			}
		}

		size_t filtered_rows = 0;
		if (first_non_constant_column != static_cast<size_t>(filter_column))
		{
			ColumnWithNameAndType & current_column = res.getByPosition(first_non_constant_column);
			current_column.column = current_column.column->filter(filter);
			filtered_rows = current_column.column->size();
		}
		else
		{
			filtered_rows = countBytesInFilter(filter);
		}

		/// Если текущий блок полностью отфильтровался - перейдём к следующему.
		if (filtered_rows == 0)
			continue;

		/// Если через фильтр проходят все строчки.
		if (filtered_rows == filter.size())
		{
			/// Заменим столбец с фильтром на константу.
			res.getByPosition(filter_column).column = new ColumnConstUInt8(filtered_rows, 1);
			/// Остальные столбцы трогать не нужно.
			return res;
		}

		/// Фильтруем остальные столбцы.
		for (size_t i = 0; i < columns; ++i)
		{
			ColumnWithNameAndType & current_column = res.getByPosition(i);

			if (i == static_cast<size_t>(filter_column))
			{
				/// Сам столбец с фильтром заменяем на столбец с константой 1, так как после фильтрации в нём ничего другого не останется.
				current_column.column = new ColumnConstUInt8(filtered_rows, 1);
				continue;
			}

			if (i == first_non_constant_column)
				continue;

			if (current_column.column->isConst())
				current_column.column = current_column.column->cut(0, filtered_rows);
			else
				current_column.column = current_column.column->filter(filter);
		}

		return res;
	}
}


}
