#pragma once

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>


namespace DB
{


template <typename T>
struct AggregateFunctionAvgData
{
	T sum;
	UInt64 count;

	AggregateFunctionAvgData() : sum(0), count(0) {}
};


/// Считает арифметическое среднее значение чисел.
template <typename T>
class AggregateFunctionAvg : public IUnaryAggregateFunction<AggregateFunctionAvgData<typename NearestFieldType<T>::Type> >
{
public:
	String getName() const { return "avg"; }
	String getTypeID() const { return "avg_" + TypeName<T>::get(); }

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


	void addOne(AggregateDataPtr place, const IColumn & column, size_t row_num) const
	{
		this->data(place).sum += static_cast<const ColumnVector<T> &>(column).getData()[row_num];
		++this->data(place).count;
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const
	{
		this->data(place).sum += this->data(rhs).sum;
		this->data(place).count += this->data(rhs).count;
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const
	{
		writeBinary(this->data(place).sum, buf);
		writeVarUInt(this->data(place).count, buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const
	{
		typename NearestFieldType<T>::Type tmp_sum = 0;
		UInt64 tmp_count = 0;
		readBinary(tmp_sum, buf);
		readVarUInt(tmp_count, buf);
		this->data(place).sum += tmp_sum;
		this->data(place).count += tmp_count;
	}

	Field getResult(ConstAggregateDataPtr place) const
	{
		return static_cast<Float64>(this->data(place).sum) / this->data(place).count;
	}
};


/** Принимает два аргумента - значение и условие.
  * Вычисляет среднее значений при выполнении условия.
  * avgIf(x, cond) эквивалентно sum(cond ? x : 0) / sum(cond).
  */
template <typename T>
class AggregateFunctionAvgIf : public IAggregateFunctionHelper<AggregateFunctionAvgData<typename NearestFieldType<T>::Type> >
{
public:
	String getName() const { return "avgIf"; }
	String getTypeID() const { return "avgIf_" + TypeName<T>::get(); }

	DataTypePtr getReturnType() const
	{
		return new DataTypeFloat64;
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
		{
			this->data(place).sum += static_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num];
			++this->data(place).count;
		}
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const
	{
		this->data(place).sum += this->data(rhs).sum;
		this->data(place).count += this->data(rhs).count;
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const
	{
		writeBinary(this->data(place).sum, buf);
		writeVarUInt(this->data(place).count, buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const
	{
		typename NearestFieldType<T>::Type tmp_sum = 0;
		UInt64 tmp_count = 0;
		readBinary(tmp_sum, buf);
		readVarUInt(tmp_count, buf);
		this->data(place).sum += tmp_sum;
		this->data(place).count += tmp_count;
	}

	Field getResult(ConstAggregateDataPtr place) const
	{
		return static_cast<Float64>(this->data(place).sum) / this->data(place).count;
	}
};

}
