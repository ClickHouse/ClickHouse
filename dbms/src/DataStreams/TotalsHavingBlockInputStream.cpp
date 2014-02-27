#include <DB/DataStreams/TotalsHavingBlockInputStream.h>
#include <DB/Columns/ColumnAggregateFunction.h>
#include <DB/Columns/ColumnsNumber.h>

namespace DB
{

static void finalize(Block & block)
{
	for (size_t i = 0; i < block.columns(); ++i)
	{
		ColumnWithNameAndType & current = block.getByPosition(i);
		ColumnAggregateFunction * unfinalized_column = dynamic_cast<ColumnAggregateFunction *>(&*current.column);
		if (unfinalized_column)
		{
			current.type = unfinalized_column->getAggregateFunction()->getReturnType();
			current.column = unfinalized_column->convertToValues();
		}
	}
}

Block TotalsHavingBlockInputStream::readImpl()
{
	Block finalized;
	Block block;

	while (1)
	{
		block = children[0]->read();

		if (!block)
		{
			/** Если totals_mode==AFTER_HAVING_AUTO, нужно решить, добавлять ли в TOTALS агрегаты для строк,
			  *  не прошедших max_rows_to_group_by.
			  */
			if (overflow_aggregates && static_cast<float>(passed_keys) / total_keys >= auto_include_threshold)
				addToTotals(current_totals, overflow_aggregates, nullptr);
			finalize(current_totals);
			totals = current_totals;
			return finalized;
		}

		finalized = block;
		finalize(finalized);

		total_keys += finalized.rows() - (overflow_row ? 1 : 0);

		if (filter_column_name.empty() || totals_mode == TotalsMode::BEFORE_HAVING)
		{
			/** Включая особую нулевую строку, если overflow_row=true.
			  * Предполагается, что если totals_mode=AFTER_HAVING_EXCLUSIVE, нам эту строку не дадут.
			  */
			addToTotals(current_totals, block, nullptr);
		}

		if (!filter_column_name.empty())
		{
			expression->execute(finalized);

			size_t filter_column_pos = finalized.getPositionByName(filter_column_name);
			ColumnPtr filter_column_ptr = finalized.getByPosition(filter_column_pos).column;

			ColumnConstUInt8 * column_const = dynamic_cast<ColumnConstUInt8 *>(&*filter_column_ptr);
			if (column_const)
				filter_column_ptr = column_const->convertToFullColumn();

			ColumnUInt8 * filter_column = dynamic_cast<ColumnUInt8 *>(&*filter_column_ptr);
			if (!filter_column)
				throw Exception("Filter column must have type UInt8, found " +
					finalized.getByPosition(filter_column_pos).type->getName(),
					ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);

			IColumn::Filter & filter = filter_column->getData();

			if (totals_mode != TotalsMode::BEFORE_HAVING)
			{
				if (overflow_row)
				{
					filter[0] = totals_mode == TotalsMode::AFTER_HAVING_INCLUSIVE;
					addToTotals(current_totals, block, &filter);

					if (totals_mode == TotalsMode::AFTER_HAVING_AUTO)
						addToTotals(overflow_aggregates, block, nullptr, 1);
				}
				else
				{
					addToTotals(current_totals, block, &filter);
				}
			}

			if (overflow_row)
				filter[0] = 0;

			size_t columns = finalized.columns();

			for (size_t i = 0; i < columns; ++i)
			{
				ColumnWithNameAndType & current_column = finalized.getByPosition(i);
				current_column.column = current_column.column->filter(filter);
				if (current_column.column->empty())
				{
					finalized.clear();
					break;
				}
			}
		}
		else
		{
			if (overflow_row)
			{
				/// Придется выбросить одну строку из начала всех столбцов.
				size_t columns = finalized.columns();
				for (size_t i = 0; i < columns; ++i)
				{
					ColumnWithNameAndType & current_column = finalized.getByPosition(i);
					current_column.column = current_column.column->cut(1, current_column.column->size() - 1);
				}
			}
		}

		if (!finalized)
			continue;

		passed_keys += finalized.rows();
		return finalized;
	}
}

void TotalsHavingBlockInputStream::addToTotals(Block & totals, Block & block, const IColumn::Filter * filter,
												size_t rows)
{
	bool init = !totals;

	ArenaPtr arena;
	if (init)
		arena = new Arena;

	for (size_t i = 0; i < block.columns(); ++i)
	{
		ColumnWithNameAndType & current = block.getByPosition(i);
		ColumnAggregateFunction * column =
			dynamic_cast<ColumnAggregateFunction *>(&*current.column);

		if (!column)
		{
			if (init)
			{
				ColumnPtr new_column = current.type->createColumn();
				new_column->insertDefault();
				totals.insert(ColumnWithNameAndType(new_column, current.type, current.name));
			}
			continue;
		}

		ColumnAggregateFunction * target;
		IAggregateFunction * function;
		AggregateDataPtr data;

		if (init)
		{
			function = column->getAggregateFunction();
			target = new ColumnAggregateFunction(column->getAggregateFunction(), Arenas(1, arena));
			totals.insert(ColumnWithNameAndType(target, current.type, current.name));

			data = arena->alloc(function->sizeOfData());
			function->create(data);
			target->getData().push_back(data);
		}
		else
		{
			target = dynamic_cast<ColumnAggregateFunction *>(&*totals.getByPosition(i).column);
			if (!target)
				throw Exception("Unexpected type of column: " + totals.getByPosition(i).column->getName(),
					ErrorCodes::ILLEGAL_COLUMN);
			function = target->getAggregateFunction();
			data = target->getData()[0];
		}

		ColumnAggregateFunction::Container_t & vec = column->getData();
		size_t size = std::min(vec.size(), rows);

		if (filter)
		{
			for (size_t j = 0; j < size; ++j)
			{
				if ((*filter)[j])
					function->merge(data, vec[j]);
			}
		}
		else
		{
			for (size_t j = 0; j < size; ++j)
			{
				function->merge(data, vec[j]);
			}
		}
	}
}

}
