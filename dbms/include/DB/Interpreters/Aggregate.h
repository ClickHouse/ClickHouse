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
class Aggregate
{
public:
	Aggregate(const ColumnNumbers & keys_, AggregateDescriptions & aggregates_) : keys(keys_), aggregates(aggregates_) {};

	AggregatedData execute(BlockInputStreamPtr stream);

private:
	ColumnNumbers keys;
	AggregateDescriptions aggregates;
};


}
