#pragma once

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>


namespace DB
{

template <typename T>
struct AggregateFunctionSumData
{
	T sum;

	AggregateFunctionSumData() : sum(0) {}
};

	
/// Считает сумму чисел.
template <typename T>
class AggregateFunctionSum : public IUnaryAggregateFunction<AggregateFunctionSumData<typename NearestFieldType<T>::Type> >
{
public:
	String getName() const { return "sum"; }

	DataTypePtr getReturnType() const
	{
		return new typename DataTypeFromFieldType<typename NearestFieldType<T>::Type>::Type;
	}

	void setArgument(const DataTypePtr & argument)
	{
		if (!argument->isNumeric())
			throw Exception("Illegal type " + argument->getName() + " of argument for aggregate function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}


	void addOne(AggregateDataPtr place, const IColumn & column, size_t row_num) const
	{
		this->data(place).sum += static_cast<const ColumnVector<T> &>(column).getData()[row_num];
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const
	{
		this->data(place).sum += this->data(rhs).sum;
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const
	{
		writeBinary(this->data(place).sum, buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const
	{
		typename NearestFieldType<T>::Type tmp;
		readBinary(tmp, buf);
		this->data(place).sum += tmp;
	}

	Field getResult(ConstAggregateDataPtr place) const
	{
		return this->data(place).sum;
	}
};


/// Считает сумму чисел при выполнении условия. sumIf(x, cond) эквивалентно sum(cond ? x : 0).
template <typename T>
class AggregateFunctionSumIf : public IAggregateFunctionHelper<AggregateFunctionSumData<typename NearestFieldType<T>::Type> >
{
public:
	String getName() const { return "sumIf"; }

	DataTypePtr getReturnType() const
	{
		return new typename DataTypeFromFieldType<typename NearestFieldType<T>::Type>::Type;
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

	void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num) const
	{
		if (static_cast<const ColumnUInt8 &>(*columns[1]).getData()[row_num])
			this->data(place).sum += static_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num];
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const
	{
		this->data(place).sum += this->data(rhs).sum;
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const
	{
		writeBinary(this->data(place).sum, buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const
	{
		typename NearestFieldType<T>::Type tmp;
		readBinary(tmp, buf);
		this->data(place).sum += tmp;
	}

	Field getResult(ConstAggregateDataPtr place) const
	{
		return this->data(place).sum;
	}
};

}
