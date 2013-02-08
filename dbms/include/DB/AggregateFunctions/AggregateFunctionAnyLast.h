#pragma once

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>


namespace DB
{


struct AggregateFunctionAnyLastData
{
	Field value;
};


/// Берёт последнее попавшееся значение
class AggregateFunctionAnyLast : public IUnaryAggregateFunction<AggregateFunctionAnyLastData>
{
private:
	DataTypePtr type;
	
public:
	String getName() const { return "anyLast"; }
	String getTypeID() const { return "anyLast"; }

	DataTypePtr getReturnType() const
	{
		return type;
	}

	void setArgument(const DataTypePtr & argument)
	{
		type = argument;
	}


	void addOne(AggregateDataPtr place, const Field & value_) const
	{
		data(place).value = value_;
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const
	{
		data(place).value = data(rhs).value;
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const
	{
		type->serializeBinary(data(place).value, buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const
	{
		type->deserializeBinary(data(place).value, buf);
	}

	Field getResult(ConstAggregateDataPtr place) const
	{
		return data(place).value;
	}
};

}
