#pragma once

#include <DB/Interpreters/SplittingAggregator.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

using Poco::SharedPtr;


class SplittingAggregatingBlockInputStream : public IProfilingBlockInputStream
{
public:
	SplittingAggregatingBlockInputStream(
		BlockInputStreamPtr input_, const ColumnNumbers & keys_, AggregateDescriptions & aggregates_, size_t threads_,
		bool with_totals_, bool separate_totals_, bool final_, size_t max_rows_to_group_by_, Limits::OverflowMode group_by_overflow_mode_)
		: started(false), separate_totals(separate_totals_), final(final_),
		aggregator(new SplittingAggregator(keys_, aggregates_, threads_, with_totals_, max_rows_to_group_by_, group_by_overflow_mode_)),
		current_result(results.end())
	{
		children.push_back(input_);
	}

	/** keys берутся из GROUP BY части запроса
	  * Агрегатные функции ищутся везде в выражении.
	  * Столбцы, соответствующие keys и аргументам агрегатных функций, уже должны быть вычислены.
	  */
	SplittingAggregatingBlockInputStream(
		BlockInputStreamPtr input_, const Names & key_names, const AggregateDescriptions & aggregates, size_t threads_,
		bool with_totals_, bool separate_totals_, bool final_, size_t max_rows_to_group_by_, Limits::OverflowMode group_by_overflow_mode_)
		: started(false), separate_totals(separate_totals_), final(final_),
		aggregator(new SplittingAggregator(key_names, aggregates, threads_, with_totals_, max_rows_to_group_by_, group_by_overflow_mode_)),
		current_result(results.end())
	{
		children.push_back(input_);
	}

	String getName() const { return "SplittingAggregatingBlockInputStream"; }

	String getID() const
	{
		std::stringstream res;
		res << "SplittingAggregating(" << children.back()->getID() << ", " << aggregator->getID() << ")";
		return res.str();
	}

protected:
	Block readImpl()
	{
		if (!started)
		{
			started = true;
			
			ManyAggregatedDataVariants data;
			aggregator->execute(children.back(), data);

			if (isCancelled())
				return Block();

			aggregator->convertToBlocks(data, results, final);
			current_result = results.begin();
		}

		if (current_result == results.end())
			return Block();

		Block res = *current_result;
		++current_result;

		return res;
	}

	bool started;
	bool separate_totals;	/// TODO
	bool final;
	SharedPtr<SplittingAggregator> aggregator;
	Blocks results;
	Blocks::iterator current_result;
};

}
