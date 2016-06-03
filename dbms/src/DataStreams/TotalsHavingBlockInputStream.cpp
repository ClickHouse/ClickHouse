#include <DB/DataStreams/TotalsHavingBlockInputStream.h>
#include <DB/Interpreters/ExpressionActions.h>
#include <DB/Interpreters/AggregateDescription.h>
#include <DB/Columns/ColumnAggregateFunction.h>
#include <DB/Columns/ColumnsNumber.h>

namespace DB
{

namespace ErrorCodes
{
	extern const int ILLEGAL_COLUMN;
	extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
}


TotalsHavingBlockInputStream::TotalsHavingBlockInputStream(
	BlockInputStreamPtr input_,
	bool overflow_row_, ExpressionActionsPtr expression_,
	const std::string & filter_column_, TotalsMode totals_mode_, double auto_include_threshold_)
	: overflow_row(overflow_row_),
	expression(expression_), filter_column_name(filter_column_), totals_mode(totals_mode_),
	auto_include_threshold(auto_include_threshold_)
{
	children.push_back(input_);
}


String TotalsHavingBlockInputStream::getID() const
{
	std::stringstream res;
	res << "TotalsHavingBlockInputStream(" << children.back()->getID()
		<< "," << filter_column_name << ")";
	return res.str();
}


static void finalize(Block & block)
{
	for (size_t i = 0; i < block.columns(); ++i)
	{
		ColumnWithTypeAndName & current = block.getByPosition(i);
		ColumnAggregateFunction * unfinalized_column = typeid_cast<ColumnAggregateFunction *>(&*current.column);
		if (unfinalized_column)
		{
			current.type = unfinalized_column->getAggregateFunction()->getReturnType();
			current.column = unfinalized_column->convertToValues();
		}
	}
}


const Block & TotalsHavingBlockInputStream::getTotals()
{
	if (!totals)
	{
		/** Если totals_mode == AFTER_HAVING_AUTO, нужно решить, добавлять ли в TOTALS агрегаты для строк,
		  *  не прошедших max_rows_to_group_by.
		  */
		if (overflow_aggregates)
		{
			if (totals_mode == TotalsMode::BEFORE_HAVING
				|| totals_mode == TotalsMode::AFTER_HAVING_INCLUSIVE
				|| (totals_mode == TotalsMode::AFTER_HAVING_AUTO
					&& static_cast<double>(passed_keys) / total_keys >= auto_include_threshold))
				addToTotals(current_totals, overflow_aggregates, nullptr);
		}

		finalize(current_totals);
		totals = current_totals;
	}

	if (totals && expression)
		expression->execute(totals);

	return totals;
}


Block TotalsHavingBlockInputStream::readImpl()
{
	Block finalized;
	Block block;

	while (1)
	{
		block = children[0]->read();

		/// Блок со значениями, не вошедшими в max_rows_to_group_by. Отложим его.
		if (overflow_row && block && block.info.is_overflows)
		{
			overflow_aggregates = block;
			continue;
		}

		if (!block)
			return finalized;

		finalized = block;
		finalize(finalized);

		total_keys += finalized.rows();

		if (filter_column_name.empty())
		{
			addToTotals(current_totals, block, nullptr);
		}
		else
		{
			/// Вычисляем выражение в HAVING.
			expression->execute(finalized);

			size_t filter_column_pos = finalized.getPositionByName(filter_column_name);
			ColumnPtr filter_column_ptr = finalized.getByPosition(filter_column_pos).column;

			ColumnConstUInt8 * column_const = typeid_cast<ColumnConstUInt8 *>(&*filter_column_ptr);
			if (column_const)
				filter_column_ptr = column_const->convertToFullColumn();

			ColumnUInt8 * filter_column = typeid_cast<ColumnUInt8 *>(&*filter_column_ptr);
			if (!filter_column)
				throw Exception("Filter column must have type UInt8, found " +
					finalized.getByPosition(filter_column_pos).type->getName(),
					ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);

			IColumn::Filter & filter = filter_column->getData();

			/// Прибавляем значения в totals (если это не было сделано ранее).
			if (totals_mode == TotalsMode::BEFORE_HAVING)
				addToTotals(current_totals, block, nullptr);
			else
				addToTotals(current_totals, block, &filter);

			/// Фильтруем блок по выражению в HAVING.
			size_t columns = finalized.columns();

			for (size_t i = 0; i < columns; ++i)
			{
				ColumnWithTypeAndName & current_column = finalized.getByPosition(i);
				current_column.column = current_column.column->filter(filter, -1);
				if (current_column.column->empty())
				{
					finalized.clear();
					break;
				}
			}
		}

		if (!finalized)
			continue;

		passed_keys += finalized.rows();
		return finalized;
	}
}

void TotalsHavingBlockInputStream::addToTotals(Block & totals, Block & block, const IColumn::Filter * filter)
{
	bool init = !totals;

	ArenaPtr arena;
	if (init)
		arena = new Arena;

	for (size_t i = 0; i < block.columns(); ++i)
	{
		const ColumnWithTypeAndName & current = block.getByPosition(i);
		const ColumnAggregateFunction * column = typeid_cast<const ColumnAggregateFunction *>(&*current.column);

		if (!column)
		{
			if (init)
			{
				ColumnPtr new_column = current.type->createColumn();
				new_column->insertDefault();
				totals.insert(ColumnWithTypeAndName(new_column, current.type, current.name));
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
			totals.insert(ColumnWithTypeAndName(target, current.type, current.name));

			data = arena->alloc(function->sizeOfData());
			function->create(data);
			target->getData().push_back(data);
		}
		else
		{
			target = typeid_cast<ColumnAggregateFunction *>(&*totals.getByPosition(i).column);
			if (!target)
				throw Exception("Unexpected type of column: " + totals.getByPosition(i).column->getName(),
					ErrorCodes::ILLEGAL_COLUMN);
			function = target->getAggregateFunction();
			data = target->getData()[0];
		}

		const ColumnAggregateFunction::Container_t & vec = column->getData();
		size_t size = vec.size();

		if (filter)
		{
			for (size_t j = 0; j < size; ++j)
				if ((*filter)[j])
					function->merge(data, vec[j]);
		}
		else
		{
			for (size_t j = 0; j < size; ++j)
				function->merge(data, vec[j]);
		}
	}
}

}
