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
	String getName() const override { return "count"; }

	DataTypePtr getReturnType() const override
	{
		return new DataTypeUInt64;
	}


	void addImpl(AggregateDataPtr place) const
	{
		++data(place).count;
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const override
	{
		data(place).count += data(rhs).count;
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
	{
		writeVarUInt(data(place).count, buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const override
	{
		UInt64 tmp;
		readVarUInt(tmp, buf);
		data(place).count += tmp;
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
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
