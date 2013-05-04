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
		: aggregator(new Aggregator(keys_, aggregates_)), has_been_read(false)
	{
		children.push_back(input_);
	}

	/** keys берутся из GROUP BY части запроса
	  * Агрегатные функции ищутся везде в выражении.
	  */
	MergingAggregatedBlockInputStream(BlockInputStreamPtr input_, ExpressionPtr expression);

	String getName() const { return "MergingAggregatedBlockInputStream"; }

	String getID() const
	{
		std::stringstream res;
		res << "MergingAggregated(" << children.back()->getID() << ", " << aggregator->getID() << ")";
		return res.str();
	}

protected:
	Block readImpl();

private:
	SharedPtr<Aggregator> aggregator;
	bool has_been_read;
};

}
