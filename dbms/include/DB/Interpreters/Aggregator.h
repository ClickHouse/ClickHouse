#pragma once

#include <DB/Core/ColumnNumbers.h>
#include <DB/Core/Names.h>
#include <DB/DataStreams/IBlockInputStream.h>
#include <DB/AggregateFunctions/IAggregateFunction.h>


namespace DB
{


struct AggregateDescription
{
	AggregateFunctionPtr function;
	ColumnNumbers arguments;
	Names argument_names;	/// Используются, если arguments не заданы.
	String column_name;		/// Какое имя использовать для столбца со значениями агрегатной функции
};

typedef std::vector<AggregateDescription> AggregateDescriptions;

typedef std::map<Row, AggregateFunctions> AggregatedData;
	

/** Агрегирует поток блоков.
  */
class Aggregator
{
public:
	Aggregator(const ColumnNumbers & keys_, AggregateDescriptions & aggregates_) : keys(keys_), aggregates(aggregates_) {};
	Aggregator(const Names & key_names_, AggregateDescriptions & aggregates_) : key_names(key_names_), aggregates(aggregates_) {};

	AggregatedData execute(BlockInputStreamPtr stream);

	/// Получить пример блока, описывающего результат. Следует вызывать только после execute.
	Block getSampleBlock() { return sample; }

private:
	ColumnNumbers keys;
	Names key_names;
	AggregateDescriptions aggregates;

	Block sample;
};


}
