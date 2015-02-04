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
		const std::string & filter_column_, TotalsMode totals_mode_, double auto_include_threshold_)
		: overflow_row(overflow_row_),
		expression(expression_), filter_column_name(filter_column_), totals_mode(totals_mode_),
		auto_include_threshold(auto_include_threshold_)
	{
		children.push_back(input_);
	}

	String getName() const override { return "TotalsHavingBlockInputStream"; }

	String getID() const override
	{
		std::stringstream res;
		res << "TotalsHavingBlockInputStream(" << children.back()->getID()
			<< "," << filter_column_name << ")";
		return res.str();
	}

	const Block & getTotals() override;

protected:
	Block readImpl() override;

private:
	bool overflow_row;
	ExpressionActionsPtr expression;
	String filter_column_name;
	TotalsMode totals_mode;
	double auto_include_threshold;
	size_t passed_keys = 0;
	size_t total_keys = 0;

	/** Здесь находятся значения, не прошедшие max_rows_to_group_by.
	  * Они прибавляются или не прибавляются к current_totals в зависимости от totals_mode.
	  */
	Block overflow_aggregates;

	/// Здесь накапливаются тотальные значения. После окончания работы, они будут помещены в IProfilingBlockInputStream::totals.
	Block current_totals;

	/// Если filter == nullptr - прибавлять все строки. Иначе - только строки, проходящие фильтр (HAVING).
	void addToTotals(Block & totals, Block & block, const IColumn::Filter * filter);
};

}
