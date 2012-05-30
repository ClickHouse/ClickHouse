#pragma once

#include <DB/Interpreters/Aggregator.h>
#include <DB/Interpreters/Expression.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Доагрегирует поток блоков, в котором каждый блок уже агрегирован.
  * Агрегатные функции в блоках не должны быть финализированы, чтобы их состояния можно было объединить.
  * Сам тоже не финализирует агрегатные функции.
  */
class MergingAggregatedBlockInputStream : public IProfilingBlockInputStream
{
public:
	MergingAggregatedBlockInputStream(BlockInputStreamPtr input_, const ColumnNumbers & keys_, AggregateDescriptions & aggregates_)
		: input(input_), aggregator(new Aggregator(keys_, aggregates_)), has_been_read(false)
	{
		children.push_back(input);
	}

	/** keys берутся из GROUP BY части запроса
	  * Агрегатные функции ищутся везде в выражении.
	  */
	MergingAggregatedBlockInputStream(BlockInputStreamPtr input_, ExpressionPtr expression);

	Block readImpl();

	String getName() const { return "MergingAggregatedBlockInputStream"; }

	BlockInputStreamPtr clone() { return new MergingAggregatedBlockInputStream(*this); }

private:
	MergingAggregatedBlockInputStream(const MergingAggregatedBlockInputStream & src)
		: input(src.input), aggregator(src.aggregator), has_been_read(src.has_been_read) {}
	
	BlockInputStreamPtr input;
	SharedPtr<Aggregator> aggregator;
	bool has_been_read;
};

}
