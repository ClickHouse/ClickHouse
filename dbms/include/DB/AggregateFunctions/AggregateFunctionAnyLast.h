#pragma once

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>


namespace DB
{

/// Берёт последнее попавшееся значение
class AggregateFunctionAnyLast : public IUnaryAggregateFunction
{
private:
	Field value;
	DataTypePtr type;
	
public:
	AggregateFunctionAnyLast() {}

	String getName() const { return "anyLast"; }
	String getTypeID() const { return "anyLast"; }

	AggregateFunctionPtr cloneEmpty() const
	{
		AggregateFunctionAnyLast * res = new AggregateFunctionAnyLast;
		res->type = type;
		return res;
	}
	
	DataTypePtr getReturnType() const
	{
		return type;
	}

	void setArgument(const DataTypePtr & argument)
	{
		type = argument;
	}

	void addOne(const Field & value_)
	{
		value = value_;
	}

	void merge(const IAggregateFunction & rhs)
	{
		value = dynamic_cast<const AggregateFunctionAnyLast &>(rhs).value;
	}

	void serialize(WriteBuffer & buf) const
	{
		type->serializeBinary(value, buf);
	}

	void deserializeMerge(ReadBuffer & buf)
	{
		type->deserializeBinary(value, buf);
	}

	Field getResult() const
	{
		return value;
	}
};

}
