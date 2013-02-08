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
class AggregateFunctionCount : public INullaryAggregateFunction<AggregateFunctionCountData>
{
public:
	String getName() const { return "count"; }
	String getTypeID() const { return "count"; }

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

	Field getResult(ConstAggregateDataPtr place) const
	{
		return data(place).count;
	}

	/// Для оптимизации
	void addDelta(AggregateDataPtr place, UInt64 x) const
	{
		data(place).count += x;
	}
};

}
