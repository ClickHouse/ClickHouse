#pragma once

#include <DB/Interpreters/Aggregator.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Interpreters/ExpressionActions.h>


namespace DB
{

using Poco::SharedPtr;


/** Принимает блоки после группировки, с нефиализированными агрегатными функциями.
  * Вычисляет тотальные значения в соответствии с totals_mode.
  * Если нужно, вычисляет выражение из HAVING и фильтрует им строки. Отдает финализированные и отфильтрованные блоки.
  */
class TotalsHavingBlockInputStream : public IProfilingBlockInputStream
{
public:
	TotalsHavingBlockInputStream(BlockInputStreamPtr input_, const Names & keys_names_,
		const AggregateDescriptions & aggregates_, bool overflow_row_, ExpressionActionsPtr expression_,
		const std::string & filter_column_, TotalsMode totals_mode_, float auto_include_threshold_)
		: aggregator(new Aggregator(keys_names_, aggregates_, overflow_row_)), overflow_row(overflow_row_),
		expression(expression_), filter_column_name(filter_column_), totals_mode(totals_mode_),
		auto_include_threshold(auto_include_threshold_), passed_keys(0), total_keys(0)
	{
		children.push_back(input_);
	}

	String getName() const { return "TotalsHavingBlockInputStream"; }

	String getID() const
	{
		std::stringstream res;
		res << "TotalsHavingBlockInputStream(" << children.back()->getID() << ", " << aggregator->getID()
			<< "," << filter_column_name << ")";
		return res.str();
	}

	const Block & getTotals()
	{
		if (totals && expression)
			expression->execute(totals);

		return totals;
	}

protected:
	Block readImpl();

private:
	SharedPtr<Aggregator> aggregator;
	bool overflow_row;
	ExpressionActionsPtr expression;
	String filter_column_name;
	TotalsMode totals_mode;
	float auto_include_threshold;
	size_t passed_keys;
	size_t total_keys;

	Block current_totals;
	Block overflow_aggregates;

	void addToTotals(Block & totals, Block & block, const IColumn::Filter * filter, size_t rows);

	void addToTotals(Block & totals, Block & block, const IColumn::Filter * filter)
	{
		addToTotals(totals, block, filter, block.rows());
	}
};

}
