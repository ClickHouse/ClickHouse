#include <DB/Columns/ColumnsNumber.h>
#include <DB/Columns/ColumnNullable.h>
#include <DB/Columns/ColumnsCommon.h>
#include <DB/Interpreters/ExpressionActions.h>

#include <DB/DataStreams/FilterBlockInputStream.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
}


FilterBlockInputStream::FilterBlockInputStream(BlockInputStreamPtr input_, ExpressionActionsPtr expression_, ssize_t filter_column_)
	: expression(expression_), filter_column(filter_column_)
{
	children.push_back(input_);
}

FilterBlockInputStream::FilterBlockInputStream(BlockInputStreamPtr input_, ExpressionActionsPtr expression_, const String & filter_column_name_)
	: expression(expression_), filter_column(-1), filter_column_name(filter_column_name_)
{
	children.push_back(input_);
}


String FilterBlockInputStream::getName() const { return "Filter"; }


String FilterBlockInputStream::getID() const
{
	std::stringstream res;
	res << "Filter(" << children.back()->getID() << ", " << expression->getID() << ", " << filter_column << ", " << filter_column_name << ")";
	return res.str();
}


const Block & FilterBlockInputStream::getTotals()
{
	if (IProfilingBlockInputStream * child = dynamic_cast<IProfilingBlockInputStream *>(&*children.back()))
	{
		totals = child->getTotals();
		expression->executeOnTotals(totals);
	}

	return totals;
}


Block FilterBlockInputStream::readImpl()
{
	Block res;

	if (is_first)
	{
		is_first = false;

		const Block & sample_block = expression->getSampleBlock();

		/// Найдём настоящую позицию столбца с фильтром в блоке.
		/** sample_block имеет структуру результата вычисления выражения.
		  * Но эта структура не обязательно совпадает с expression->execute(res) ниже,
		  *  потому что выражение может применяться к блоку, который также содержит дополнительные,
		  *  ненужные для данного выражения столбцы, но нужные позже, в следующих стадиях конвейера выполнения запроса.
		  * Таких столбцов в sample_block не будет.
		  * Поэтому, позиция столбца-фильтра в нём может быть другой.
		  */
		ssize_t filter_column_in_sample_block = filter_column;
		if (filter_column_in_sample_block == -1)
			filter_column_in_sample_block = sample_block.getPositionByName(filter_column_name);

		/// Проверим, не является ли столбец с фильтром константой, содержащей 0 или 1.
		ColumnPtr column = sample_block.getByPosition(filter_column_in_sample_block).column;

		if (column)
		{
			if (column->isNullable())
			{
				ColumnNullable & nullable_col = static_cast<ColumnNullable &>(*column);
				column = nullable_col.getNestedColumn();
			}

			const ColumnConstUInt8 * column_const = typeid_cast<const ColumnConstUInt8 *>(&*column);

			if (column_const)
			{
				if (column_const->getData())
					filter_always_true = true;
				else
					filter_always_false = true;
			}
		}

		if (filter_always_false)
			return res;
	}

	/// Пока не встретится блок, после фильтрации которого что-нибудь останется, или поток не закончится.
	while (1)
	{
		res = children.back()->read();
		if (!res)
			return res;

		expression->execute(res);

		if (filter_always_true)
			return res;

		/// Найдём настоящую позицию столбца с фильтром в блоке.
		if (filter_column == -1)
			filter_column = res.getPositionByName(filter_column_name);

		size_t columns = res.columns();
		ColumnPtr column = res.getByPosition(filter_column).column;
		bool is_nullable_column = column->isNullable();

		auto init_observed_column = [&column, &is_nullable_column]()
		{
			if (is_nullable_column)
			{
				ColumnNullable & nullable_col = static_cast<ColumnNullable &>(*column.get());
				return nullable_col.getNestedColumn().get();
			}
			else
				return column.get();
		};

		IColumn * observed_column = init_observed_column();

		const ColumnUInt8 * column_vec = typeid_cast<const ColumnUInt8 *>(observed_column);
		if (!column_vec)
		{
			/** Бывает, что на этапе анализа выражений (в sample_block) столбцы-константы ещё не вычислены,
			  *  а сейчас - вычислены. То есть, не все случаи покрываются кодом выше.
			  * Это происходит, если функция возвращает константу для неконстантного аргумента.
			  * Например, функция ignore.
			  */
			const ColumnConstUInt8 * column_const = typeid_cast<const ColumnConstUInt8 *>(observed_column);

			if (column_const)
			{
				if (column_const->getData())
				{
					filter_always_true = true;
				}
				else
				{
					filter_always_false = true;
					res.clear();
				}
				return res;
			}

			throw Exception("Illegal type " + column->getName() + " of column for filter. Must be ColumnUInt8 or ColumnConstUInt8.",
				ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);
		}

		if (is_nullable_column)
		{
			/// Exclude the entries of the filter column that actually are NULL values.

			/// Access the filter content.
			ColumnNullable & nullable_col = static_cast<ColumnNullable &>(*column);
			auto & nested_col = nullable_col.getNestedColumn();
			auto & actual_col = static_cast<ColumnUInt8 &>(*nested_col);
			auto & filter_col = actual_col.getData();

			/// Access the null values byte map content.
			ColumnPtr & null_map = nullable_col.getNullMapColumn();
			ColumnUInt8 & content = static_cast<ColumnUInt8 &>(*null_map);
			auto & data = content.getData();

			for (size_t i = 0; i < data.size(); ++i)
			{
				if (data[i] != 0)
					filter_col[i] = 0;
			}
		}

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
			ColumnWithTypeAndName & current_column = res.getByPosition(first_non_constant_column);
			current_column.column = current_column.column->filter(filter, -1);
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
			res.getByPosition(filter_column).column = std::make_shared<ColumnConstUInt8>(filtered_rows, 1);
			/// Остальные столбцы трогать не нужно.
			return res;
		}

		/// Фильтруем остальные столбцы.
		for (size_t i = 0; i < columns; ++i)
		{
			ColumnWithTypeAndName & current_column = res.getByPosition(i);

			if (i == static_cast<size_t>(filter_column))
			{
				/// Сам столбец с фильтром заменяем на столбец с константой 1, так как после фильтрации в нём ничего другого не останется.
				current_column.column = std::make_shared<ColumnConstUInt8>(filtered_rows, 1);
				continue;
			}

			if (i == first_non_constant_column)
				continue;

			if (current_column.column->isConst())
				current_column.column = current_column.column->cut(0, filtered_rows);
			else
				current_column.column = current_column.column->filter(filter, -1);
		}

		return res;
	}
}


}
