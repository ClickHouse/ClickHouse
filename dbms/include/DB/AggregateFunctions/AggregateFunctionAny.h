#pragma once

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>


namespace DB
{

/// Берёт первое попавшееся значение
class AggregateFunctionAny : public IUnaryAggregateFunction
{
private:
	bool got;
	Field value;
	DataTypePtr type;
	
public:
	AggregateFunctionAny() : got(false) {}

	String getName() const { return "any"; }
	String getTypeID() const { return "any"; }

	AggregateFunctionPtr cloneEmpty() const
	{
		AggregateFunctionAny * res = new AggregateFunctionAny;
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
		if (got)
			return;
		got = true;
		value = value_;
	}

	void merge(const IAggregateFunction & rhs)
	{
		if (!got)
			value = dynamic_cast<const AggregateFunctionAny &>(rhs).value;
	}

	void serialize(WriteBuffer & buf) const
	{
		type->serializeBinary(value, buf);
	}

	void deserializeMerge(ReadBuffer & buf)
	{
		if (!got)
			type->deserializeBinary(value, buf);
	}

	Field getResult() const
	{
		return value;
	}
};

}
