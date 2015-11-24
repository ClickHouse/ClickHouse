#pragma once

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/Columns/ColumnVector.h>
#include <DB/Columns/ColumnArray.h>

#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>

#define AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE 0xFFFFFF


namespace DB
{


/// Частный случай - реализация для числовых типов.
template <typename T>
struct AggregateFunctionGroupArrayDataNumeric
{
	/// Сразу будет выделена память на несколько элементов так, чтобы состояние занимало 64 байта.
	static constexpr size_t bytes_in_arena = 64 - sizeof(PODArray<T>);

	using Array = PODArray<T, bytes_in_arena / sizeof(T), AllocatorWithStackMemory<Allocator<false>, bytes_in_arena>>;
	Array value;
};


/// Общий случай (неэффективно). NOTE Можно ещё реализовать частный случай для строк.
struct AggregateFunctionGroupArrayDataGeneric
{
	Array value;	/// TODO Добавить MemoryTracker
};


template <typename T>
class AggregateFunctionGroupArrayNumeric final
	: public IUnaryAggregateFunction<AggregateFunctionGroupArrayDataNumeric<T>, AggregateFunctionGroupArrayNumeric<T>>
{
public:
	String getName() const override { return "groupArray"; }

	DataTypePtr getReturnType() const override
	{
		return new DataTypeArray(new typename DataTypeFromFieldType<T>::Type);
	}

	void setArgument(const DataTypePtr & argument)
	{
	}

	void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num) const
	{
		this->data(place).value.push_back(static_cast<const ColumnVector<T> &>(column).getData()[row_num]);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const override
	{
		this->data(place).value.insert(this->data(rhs).value.begin(), this->data(rhs).value.end());
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
	{
		const auto & value = this->data(place).value;
		size_t size = value.size();
		writeVarUInt(size, buf);
		buf.write(reinterpret_cast<const char *>(&value[0]), size * sizeof(value[0]));
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const override
	{
		size_t size = 0;
		readVarUInt(size, buf);

		if (size > AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE)
			throw Exception("Too large array size", ErrorCodes::TOO_LARGE_ARRAY_SIZE);

		auto & value = this->data(place).value;

		size_t old_size = value.size();
		value.resize(old_size + size);
		buf.read(reinterpret_cast<char *>(&value[old_size]), size * sizeof(value[0]));
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		const auto & value = this->data(place).value;
		size_t size = value.size();

		ColumnArray & arr_to = static_cast<ColumnArray &>(to);
		ColumnArray::Offsets_t & offsets_to = arr_to.getOffsets();

		offsets_to.push_back((offsets_to.size() == 0 ? 0 : offsets_to.back()) + size);

		typename ColumnVector<T>::Container_t & data_to = static_cast<ColumnVector<T> &>(arr_to.getData()).getData();
		data_to.insert(this->data(place).value.begin(), this->data(place).value.end());
	}
};


/// Складывает все значения в массив, общий случай. Реализовано неэффективно.
class AggregateFunctionGroupArrayGeneric final
	: public IUnaryAggregateFunction<AggregateFunctionGroupArrayDataGeneric, AggregateFunctionGroupArrayGeneric>
{
private:
	DataTypePtr type;

public:
	String getName() const override { return "groupArray"; }

	DataTypePtr getReturnType() const override
	{
		return new DataTypeArray(type);
	}

	void setArgument(const DataTypePtr & argument)
	{
		type = argument;
	}


	void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num) const
	{
		data(place).value.push_back(Array::value_type());
		column.get(row_num, data(place).value.back());
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const override
	{
		data(place).value.insert(data(place).value.end(), data(rhs).value.begin(), data(rhs).value.end());
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
	{
		const Array & value = data(place).value;
		size_t size = value.size();
		writeVarUInt(size, buf);
		for (size_t i = 0; i < size; ++i)
			type->serializeBinary(value[i], buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const override
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

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		to.insert(data(place).value);
	}
};


#undef AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE

}
