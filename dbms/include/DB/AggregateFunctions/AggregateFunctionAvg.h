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
class AggregateFunctionAvg final : public IUnaryAggregateFunction<AggregateFunctionAvgData<typename NearestFieldType<T>::Type>, AggregateFunctionAvg<T> >
{
public:
	String getName() const { return "avg"; }

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

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const
	{
		static_cast<ColumnFloat64 &>(to).getData().push_back(
			static_cast<Float64>(this->data(place).sum) / this->data(place).count);
	}
};


}
