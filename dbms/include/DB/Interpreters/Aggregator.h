#pragma once

#include <map>
#include <tr1/unordered_map>

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


/// Для агрегации по md5.
struct UInt128
{
	UInt64 first;
	UInt64 second;

	bool operator== (const UInt128 rhs) const { return first == rhs.first && second == rhs.second; }
};

struct UInt128Hash
{
	size_t operator()(UInt128 x) const { return x.first ^ x.second; }
};


/// Разные структуры данных, которые могут использоваться для агрегации
typedef std::map<Row, AggregateFunctions> AggregatedData;
typedef AggregateFunctions AggregatedDataWithoutKey;
typedef std::tr1::unordered_map<UInt64, AggregateFunctions> AggregatedDataWithUInt64Key;
typedef std::tr1::unordered_map<UInt128, std::pair<Row, AggregateFunctions>, UInt128Hash> AggregatedDataHashed;


struct AggregatedDataVariants
{
	AggregatedData generic;
	AggregatedDataWithoutKey without_key;
	AggregatedDataWithUInt64Key key64;
	AggregatedDataHashed hashed;
};


/** Агрегирует поток блоков.
  */
class Aggregator
{
public:
	Aggregator(const ColumnNumbers & keys_, AggregateDescriptions & aggregates_) : keys(keys_), aggregates(aggregates_) {};
	Aggregator(const Names & key_names_, AggregateDescriptions & aggregates_) : key_names(key_names_), aggregates(aggregates_) {};

	void execute(BlockInputStreamPtr stream, AggregatedDataVariants & result);

	/// Получить пример блока, описывающего результат. Следует вызывать только после execute.
	Block getSampleBlock() { return sample; }

private:
	ColumnNumbers keys;
	Names key_names;
	AggregateDescriptions aggregates;

	Block sample;
};


}
