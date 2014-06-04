#pragma once

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/AggregateFunctions/INullaryAggregateFunction.h>


namespace DB
{

struct AggregateFunctionCountData
{
	UInt64 count;

	AggregateFunctionCountData() : count(0) {}
};


/// Просто считает, сколько раз её вызвали
class AggregateFunctionCount final : public INullaryAggregateFunction<AggregateFunctionCountData, AggregateFunctionCount>
{
public:
	String getName() const { return "count"; }

	DataTypePtr getReturnType() const
	{
		return new DataTypeUInt64;
	}


	void addZero(AggregateDataPtr place) const
	{
		++data(place).count;
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const
	{
		data(place).count += data(rhs).count;
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const
	{
		writeVarUInt(data(place).count, buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const
	{
		UInt64 tmp;
		readVarUInt(tmp, buf);
		data(place).count += tmp;
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const
	{
		static_cast<ColumnUInt64 &>(to).getData().push_back(data(place).count);
	}

	/// Для оптимизации
	void addDelta(AggregateDataPtr place, UInt64 x) const
	{
		data(place).count += x;
	}
};

}
