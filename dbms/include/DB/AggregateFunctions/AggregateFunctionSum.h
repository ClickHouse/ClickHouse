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
class AggregateFunctionSum final : public IUnaryAggregateFunction<AggregateFunctionSumData<typename NearestFieldType<T>::Type>, AggregateFunctionSum<T> >
{
public:
	String getName() const override { return "sum"; }

	DataTypePtr getReturnType() const override
	{
		return new typename DataTypeFromFieldType<typename NearestFieldType<T>::Type>::Type;
	}

	void setArgument(const DataTypePtr & argument)
	{
		if (!argument->behavesAsNumber())
			throw Exception("Illegal type " + argument->getName() + " of argument for aggregate function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}


	void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num) const
	{
		this->data(place).sum += static_cast<const ColumnVector<T> &>(column).getData()[row_num];
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const override
	{
		this->data(place).sum += this->data(rhs).sum;
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
	{
		writeBinary(this->data(place).sum, buf);
	}

	void deserialize(AggregateDataPtr place, ReadBuffer & buf) const override
	{
		readBinary(this->data(place).sum, buf);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		static_cast<ColumnVector<typename NearestFieldType<T>::Type> &>(to).getData().push_back(this->data(place).sum);
	}
};


}
