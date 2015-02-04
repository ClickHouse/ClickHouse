#pragma once

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/DataTypes/DataTypeArray.h>

#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>

#define AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE 0xFFFFFF


namespace DB
{

struct AggregateFunctionGroupArrayData
{
	Array value;	/// TODO Добавить MemoryTracker
};


/// Складывает все значения в массив. Реализовано неэффективно.
class AggregateFunctionGroupArray final : public IUnaryAggregateFunction<AggregateFunctionGroupArrayData, AggregateFunctionGroupArray>
{
private:
	DataTypePtr type;

public:
	String getName() const { return "groupArray"; }

	DataTypePtr getReturnType() const
	{
		return new DataTypeArray(type);
	}

	void setArgument(const DataTypePtr & argument)
	{
		type = argument;
	}


	void addOne(AggregateDataPtr place, const IColumn & column, size_t row_num) const
	{
		data(place).value.push_back(Array::value_type());
		column.get(row_num, data(place).value.back());
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const
	{
		data(place).value.insert(data(place).value.end(), data(rhs).value.begin(), data(rhs).value.end());
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const
	{
		const Array & value = data(place).value;
		size_t size = value.size();
		writeVarUInt(size, buf);
		for (size_t i = 0; i < size; ++i)
			type->serializeBinary(value[i], buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const
	{
		size_t size = 0;
		readVarUInt(size, buf);

		if (size > AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE)
			throw Exception("Too large array size", ErrorCodes::TOO_LARGE_ARRAY_SIZE);

		Array & value = data(place).value;

		size_t old_size = value.size();
		value.resize(old_size + size);
		for (size_t i = 0; i < size; ++i)
			type->deserializeBinary(value[old_size + i], buf);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const
	{
		to.insert(data(place).value);
	}
};


#undef AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE

}
