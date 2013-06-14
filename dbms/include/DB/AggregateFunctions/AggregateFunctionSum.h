#pragma once

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>


namespace DB
{

template <typename T> struct AggregateFunctionSumTraits;

template <> struct AggregateFunctionSumTraits<UInt64>
{
	static DataTypePtr getReturnType() { return new DataTypeUInt64; }
	static void write(UInt64 x, WriteBuffer & buf) { writeVarUInt(x, buf); }
	static void read(UInt64 & x, ReadBuffer & buf) { readVarUInt(x, buf); }
};

template <> struct AggregateFunctionSumTraits<Int64>
{
	static DataTypePtr getReturnType() { return new DataTypeInt64; }
	static void write(Int64 x, WriteBuffer & buf) { writeVarInt(x, buf); }
	static void read(Int64 & x, ReadBuffer & buf) { readVarInt(x, buf); }
};

template <> struct AggregateFunctionSumTraits<Float64>
{
	static DataTypePtr getReturnType() { return new DataTypeFloat64; }
	static void write(Float64 x, WriteBuffer & buf) { writeFloatBinary(x, buf); }
	static void read(Float64 & x, ReadBuffer & buf) { readFloatBinary(x, buf); }
};


template <typename T>
struct AggregateFunctionSumData
{
	T sum;

	AggregateFunctionSumData() : sum(0) {}
};

	
/// Считает сумму чисел. Параметром шаблона может быть UInt64, Int64 или Float64.
template <typename T>
class AggregateFunctionSum : public IUnaryAggregateFunction<AggregateFunctionSumData<T> >
{
public:
	String getName() const { return "sum"; }
	String getTypeID() const { return "sum_" + TypeName<T>::get(); }

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


	void addOne(AggregateDataPtr place, const Field & value) const
	{
		this->data(place).sum += get<const T &>(value);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const
	{
		this->data(place).sum += this->data(rhs).sum;
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const
	{
		AggregateFunctionSumTraits<T>::write(this->data(place).sum, buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const
	{
		T tmp;
		AggregateFunctionSumTraits<T>::read(tmp, buf);
		this->data(place).sum += tmp;
	}

	Field getResult(ConstAggregateDataPtr place) const
	{
		return this->data(place).sum;
	}
};


/// Считает сумму чисел при выполнении условия. sumIf(x, cond) эквивалентно sum(cond ? x : 0).
template <typename T>
class AggregateFunctionSumIf : public IAggregateFunctionHelper<AggregateFunctionSumData<T> >
{
public:
	String getName() const { return "sumIf"; }
	String getTypeID() const { return "sumIf_" + TypeName<T>::get(); }

	DataTypePtr getReturnType() const
	{
		return AggregateFunctionSumTraits<T>::getReturnType();
	}

	void setArguments(const DataTypes & arguments)
	{
		if (!arguments[0]->isNumeric())
			throw Exception("Illegal type " + arguments[0]->getName() + " of first argument for aggregate function " + getName(),
							ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			if (!dynamic_cast<const DataTypeUInt8 *>(&*arguments[1]))
				throw Exception("Illegal type " + arguments[1]->getName() + " of second argument for aggregate function " + getName() + ". Must be UInt8.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}

	void add(AggregateDataPtr place, const Row & row) const
	{
		if (get<UInt64>(row[1]))
			this->data(place).sum += get<const T &>(row[0]);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const
	{
		this->data(place).sum += this->data(rhs).sum;
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const
	{
		AggregateFunctionSumTraits<T>::write(this->data(place).sum, buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const
	{
		T tmp;
		AggregateFunctionSumTraits<T>::read(tmp, buf);
		this->data(place).sum += tmp;
	}

	Field getResult(ConstAggregateDataPtr place) const
	{
		return this->data(place).sum;
	}
};

}
