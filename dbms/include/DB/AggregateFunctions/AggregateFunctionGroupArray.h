#pragma once

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/DataTypes/DataTypeArray.h>

#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>

#define AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE 0xFFFFFF


namespace DB
{

/// Складывает все значения в массив.
class AggregateFunctionGroupArray : public IUnaryAggregateFunction
{
private:
	Array value;
	DataTypePtr type;
	
public:
	String getName() const { return "groupArray"; }
	String getTypeID() const { return "groupArray"; }

	AggregateFunctionPlainPtr cloneEmpty() const
	{
		AggregateFunctionGroupArray * res = new AggregateFunctionGroupArray;
		res->type = type;
		return res;
	}
	
	DataTypePtr getReturnType() const
	{
		return new DataTypeArray(type);
	}

	void setArgument(const DataTypePtr & argument)
	{
		type = argument;
	}

	void addOne(const Field & value_)
	{
		value.push_back(value_);
	}

	void merge(const IAggregateFunction & rhs)
	{
 		const Array & rhs_value = static_cast<const AggregateFunctionGroupArray &>(rhs).value;
		value.insert(value.begin(), rhs_value.begin(), rhs_value.end());
	}

	void serialize(WriteBuffer & buf) const
	{
		size_t size = value.size();
		writeVarUInt(size, buf);
		for (size_t i = 0; i < size; ++i)
			type->serializeBinary(value[i], buf);
	}

	void deserializeMerge(ReadBuffer & buf)
	{
		size_t size = 0;
		readVarUInt(size, buf);

		if (size > AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE)
			throw Exception("Too large array size", ErrorCodes::TOO_LARGE_ARRAY_SIZE);

		value.resize(size);
		for (size_t i = 0; i < size; ++i)
			type->deserializeBinary(value[i], buf);
	}

	Field getResult() const
	{
		return value;
	}
};


#undef AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE

}
