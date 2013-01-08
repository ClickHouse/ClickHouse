#pragma once

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>


namespace DB
{


struct AggregateFunctionMinTraits
{
	static bool better(const Field & lhs, const Field & rhs) { return lhs < rhs; }
	static String name() { return "min"; }
};

struct AggregateFunctionMaxTraits
{
	static bool better(const Field & lhs, const Field & rhs) { return lhs > rhs; }
	static String name() { return "max"; }
};


/// Берёт минимальное (или максимальное) значение. Если таких много - то первое попавшееся из них.
template <typename Traits>
class AggregateFunctionsMinMax : public IUnaryAggregateFunction
{
private:
	bool got;
	Field value;
	DataTypePtr type;
	
public:
	AggregateFunctionsMinMax() : got(false) {}

	String getName() const { return Traits::name(); }
	String getTypeID() const { return Traits::name(); }

	AggregateFunctionPlainPtr cloneEmpty() const
	{
		AggregateFunctionsMinMax * res = new AggregateFunctionsMinMax;
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
		{
			if (Traits::better(value_, value))
				value = value_;
		}
		else
		{
			got = true;
			value = value_;
		}
	}

	void merge(const IAggregateFunction & rhs)
	{
		if (got)
		{
			Field value_ = static_cast<const AggregateFunctionsMinMax &>(rhs).value;
			if (Traits::better(value_, value))
				value = value_;
		}
		else
			value = static_cast<const AggregateFunctionsMinMax &>(rhs).value;
	}

	void serialize(WriteBuffer & buf) const
	{
		type->serializeBinary(value, buf);
	}

	void deserializeMerge(ReadBuffer & buf)
	{
		if (got)
		{
			Field value_;
			type->deserializeBinary(value_, buf);
			if (Traits::better(value_, value))
				value = value_;
		}
		else
			type->deserializeBinary(value, buf);
	}

	Field getResult() const
	{
		return value;
	}
};


typedef AggregateFunctionsMinMax<AggregateFunctionMinTraits> AggregateFunctionMin;
typedef AggregateFunctionsMinMax<AggregateFunctionMaxTraits> AggregateFunctionMax;

}
