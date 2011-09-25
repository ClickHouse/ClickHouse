#pragma once

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/DataTypes/DataTypesNumberVariable.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>


namespace DB
{

template <typename T> struct AggregateFunctionSumTraits;

template <> struct AggregateFunctionSumTraits<UInt64>
{
	static DataTypePtr getReturnType() { return new DataTypeVarUInt; }
	static void write(UInt64 x, WriteBuffer & buf) { writeVarUInt(x, buf); }
	static void read(UInt64 & x, ReadBuffer & buf) { readVarUInt(x, buf); }
};

template <> struct AggregateFunctionSumTraits<Int64>
{
	static DataTypePtr getReturnType() { return new DataTypeVarInt; }
	static void write(Int64 x, WriteBuffer & buf) { writeVarInt(x, buf); }
	static void read(Int64 & x, ReadBuffer & buf) { readVarInt(x, buf); }
};

template <> struct AggregateFunctionSumTraits<Float64>
{
	static DataTypePtr getReturnType() { return new DataTypeFloat64; }
	static void write(Float64 x, WriteBuffer & buf) { writeFloatBinary(x, buf); }
	static void read(Float64 & x, ReadBuffer & buf) { readFloatBinary(x, buf); }
};

	
/// Считает сумму чисел. Параметром шаблона может быть UInt64, Int64 или Float64.
template <typename T>
class AggregateFunctionSum : public IUnaryAggregateFunction
{
private:
	T sum;
	
public:
	AggregateFunctionSum() : sum(0) {}

	String getName() const { return "sum"; }
	String getTypeID() const { return "sum_" + TypeName<T>::get(); }

	AggregateFunctionPtr cloneEmpty() const
	{
		return new AggregateFunctionSum<T>;
	}
	
	DataTypePtr getReturnType() const
	{
		return AggregateFunctionSumTraits<T>::getReturnType();
	}

	void setArgument(const DataTypePtr & argument)
	{
		if (!argument->isNumeric())
			throw Exception("Illegal type " + argument->getName() + " of argument for aggregate function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}

	void addOne(const Field & value)
	{
		sum += boost::get<T>(value);
	}

	void merge(const IAggregateFunction & rhs)
	{
		sum += static_cast<const AggregateFunctionSum<T> &>(rhs).sum;
	}

	void serialize(WriteBuffer & buf) const
	{
		AggregateFunctionSumTraits<T>::write(sum, buf);
	}

	void deserializeMerge(ReadBuffer & buf)
	{
		T tmp;
		AggregateFunctionSumTraits<T>::read(tmp, buf);
		sum += tmp;
	}

	Field getResult() const
	{
		return sum;
	}
};

}
