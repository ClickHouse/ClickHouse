#pragma once

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/AggregateFunctions/INullaryAggregateFunction.h>


namespace DB
{

/// Просто считает, сколько раз её вызвали
class AggregateFunctionCount : public INullaryAggregateFunction
{
private:
	UInt64 count;
	
public:
	AggregateFunctionCount() : count(0) {}

	String getName() const { return "count"; }
	String getTypeID() const { return "count"; }

	AggregateFunctionPlainPtr cloneEmpty() const
	{
		return new AggregateFunctionCount;
	}
	
	DataTypePtr getReturnType() const
	{
		return new DataTypeUInt64;
	}

	void addZero() { ++count; }

	void merge(const IAggregateFunction & rhs)
	{
		count += static_cast<const AggregateFunctionCount &>(rhs).count;
	}

	void serialize(WriteBuffer & buf) const
	{
		writeVarUInt(count, buf);
	}

	void deserializeMerge(ReadBuffer & buf)
	{
		UInt64 tmp;
		readVarUInt(tmp, buf);
		count += tmp;
	}

	Field getResult() const
	{
		return count;
	}

	/// Для оптимизации
	void addDelta(UInt64 x)
	{
		count += x;
	}
};

}
