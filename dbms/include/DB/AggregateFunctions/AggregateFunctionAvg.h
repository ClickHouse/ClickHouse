#pragma once

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>


namespace DB
{


/// Считает арифметическое среднее значение чисел. Параметром шаблона может быть UInt64, Int64 или Float64.
template <typename T>
class AggregateFunctionAvg : public IUnaryAggregateFunction
{
private:
	Float64 sum;
	UInt64 count;
	
public:
	AggregateFunctionAvg() : sum(0), count(0) {}

	String getName() const { return "avg"; }
	String getTypeID() const { return "avg_" + TypeName<T>::get(); }

	AggregateFunctionPlainPtr cloneEmpty() const
	{
		return new AggregateFunctionAvg<T>;
	}
	
	DataTypePtr getReturnType() const
	{
		return new DataTypeFloat64;
	}

	void setArgument(const DataTypePtr & argument)
	{
		if (!argument->isNumeric())
			throw Exception("Illegal type " + argument->getName() + " of argument for aggregate function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}

	void addOne(const Field & value)
	{
		sum += get<T>(value);
		++count;
	}

	void merge(const IAggregateFunction & rhs)
	{
		sum += static_cast<const AggregateFunctionAvg<T> &>(rhs).sum;
		count += static_cast<const AggregateFunctionAvg<T> &>(rhs).count;
	}

	void serialize(WriteBuffer & buf) const
	{
		writeFloatBinary(sum, buf);
		writeVarUInt(count, buf);
	}

	void deserializeMerge(ReadBuffer & buf)
	{
		Float64 tmp_sum = 0;
		UInt64 tmp_count = 0;
		readFloatBinary(tmp_sum, buf);
		readVarUInt(count, buf);
		sum += tmp_sum;
		count += tmp_count;
	}

	Field getResult() const
	{
		return sum / count;
	}
};

}
