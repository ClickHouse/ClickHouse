#pragma once

#include <DB/Core/ColumnNumbers.h>
#include <DB/DataStreams/IBlockInputStream.h>
#include <DB/AggregateFunctions/IAggregateFunction.h>


namespace DB
{


struct AggregateDescription
{
	AggregateFunctionPtr function;
	ColumnNumbers arguments;
};

typedef std::vector<AggregateDescription> AggregateDescriptions;

typedef std::map<Row, AggregateFunctions> AggregatedData;
	

/** Агрегирует поток блоков.
  */
class Aggregator
{
public:
	Aggregator(const ColumnNumbers & keys_, AggregateDescriptions & aggregates_) : keys(keys_), aggregates(aggregates_) {};

	AggregatedData execute(BlockInputStreamPtr stream);

	/// Получить пример блока, описывающего результат. Следует вызывать только после execute.
	Block getSampleBlock() { return sample; }

private:
	ColumnNumbers keys;
	AggregateDescriptions aggregates;

	Block sample;
};


}
