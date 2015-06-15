#pragma once

#include <DB/Interpreters/Aggregator.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Доагрегирует поток блоков, в котором каждый блок уже агрегирован.
  * Агрегатные функции в блоках не должны быть финализированы, чтобы их состояния можно было объединить.
  */
class MergingAggregatedBlockInputStream : public IProfilingBlockInputStream
{
public:
	MergingAggregatedBlockInputStream(BlockInputStreamPtr input_, const Names & keys_names_,
		const AggregateDescriptions & aggregates_, bool overflow_row_, bool final_, size_t max_threads_)
		: aggregator(keys_names_, aggregates_, overflow_row_, 0, OverflowMode::THROW, nullptr, 0, 0),
		final(final_), max_threads(max_threads_)
	{
		children.push_back(input_);
	}

	String getName() const override { return "MergingAggregated"; }

	String getID() const override
	{
		std::stringstream res;
		res << "MergingAggregated(" << children.back()->getID() << ", " << aggregator.getID() << ")";
		return res.str();
	}

protected:
	Block readImpl() override;

private:
	Aggregator aggregator;
	bool final;
	size_t max_threads;

	bool executed = false;
	BlocksList blocks;
	BlocksList::iterator it;
};

}
