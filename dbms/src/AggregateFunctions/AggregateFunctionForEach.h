#pragma once

#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeArray.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int PARAMETER_OUT_OF_BOUND;
}


struct AggregateFunctionForEachData
{
	size_t dynamic_array_size;
	char* array_of_aggregate_datas;
public:
	AggregateFunctionForEachData(): dynamic_array_size(0), array_of_aggregate_datas(nullptr) {}
};



class AggregateFunctionForEach final : public IAggregateFunction
{
private:
	AggregateFunctionPtr nested_func_owner;
	IAggregateFunction * nested_func;


	AggregateFunctionForEachData* ensureAggregateData(AggregateDataPtr place, size_t newSize, Arena * arena) const
	{
		AggregateFunctionForEachData* data = reinterpret_cast<AggregateFunctionForEachData*>(place);
		///Ensure we have aggreate states for array_size elements, allocate from arena if needed
		if (data->dynamic_array_size < newSize)
		{
			size_t new_memory_amount = newSize *nested_func->sizeOfData();

			arena->allocContinue(
					new_memory_amount,
					const_cast<const char* &>(data->array_of_aggregate_datas));

			for(size_t i = data->dynamic_array_size; i < newSize; ++i)
			{
				char* aggregateData = data->array_of_aggregate_datas + i * nested_func->sizeOfData();
				nested_func->create(aggregateData);
			}
			data->dynamic_array_size = newSize;
		}
		return data;
	}

public:
	AggregateFunctionForEach(AggregateFunctionPtr nested_) : nested_func_owner(nested_), nested_func(nested_func_owner.get())
		{}

	String getName() const override
	{
		return nested_func->getName() + "ForEach";
	}

	DataTypePtr getReturnType() const override
	{
		return std::make_shared<DataTypeArray>(nested_func->getReturnType());
	}

	void create(AggregateDataPtr place) const override
	{
		new (place) AggregateFunctionForEachData();
	}

	void destroy(AggregateDataPtr place) const noexcept override
	{
		AggregateFunctionForEachData* data = reinterpret_cast<AggregateFunctionForEachData*>(place);
		char* begin = data->array_of_aggregate_datas;
		for(size_t i = 0; i < data->dynamic_array_size; ++i)
		{
			nested_func->destroy(begin);
			begin += nested_func->sizeOfData();
		}
	}

	bool hasTrivialDestructor() const override
	{
		return nested_func->hasTrivialDestructor();
	}

	size_t sizeOfData() const override
	{
		//nested_func states will be allocated in the Arena in the add metho.
		return sizeof(AggregateFunctionForEachData);
	}

	size_t alignOfData() const override
	{
		return nested_func->alignOfData();
	}


	void setArguments(const DataTypes & arguments) override
	{
		size_t num_arguments = arguments.size();

		if (1 != num_arguments)
			throw Exception("Aggregate functions of the xxxForEach group require exactly one argument of array type", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		DataTypes nested_argument;
		if (const DataTypeArray * array = typeid_cast<const DataTypeArray *>(&*arguments[0]))
			nested_argument.push_back(array->getNestedType());
		else
			throw Exception("Illegal type " + arguments[0]->getName() + " of first argument for aggregate function " + getName() + ". Must be array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		nested_func->setArguments(nested_argument);
	}

	void setParameters(const Array & params) override
	{
		if (0 != params.size())
			throw Exception("Aggregate functions of the xxxForEach group accept zero parameters.", ErrorCodes::PARAMETER_OUT_OF_BOUND);
	}

	void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override
	{
		const ColumnArray & first_array_column = static_cast<const ColumnArray &>(*columns[0]);
		const IColumn::Offsets_t & offsets = first_array_column.getOffsets();
		const IColumn* array_data = &first_array_column.getData();
		size_t begin = row_num == 0 ? 0 : offsets[row_num - 1];
		size_t end = offsets[row_num];

		size_t array_size = end - begin;

		if (array_size <= 0)
			return;

		AggregateFunctionForEachData* data = ensureAggregateData(place, array_size, arena);

		for(size_t i = begin; i < end; ++i)
		{
			char* aggregateData = data->array_of_aggregate_datas + (i - begin) * nested_func->sizeOfData();
			nested_func->add(aggregateData, static_cast<const IColumn**>(&array_data), i, arena);
		}
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
	{
		const AggregateFunctionForEachData* rh_data = reinterpret_cast<const AggregateFunctionForEachData*>(rhs);
		AggregateFunctionForEachData* data = ensureAggregateData(place, rh_data->dynamic_array_size, arena);

		for(size_t i = 0; i < data->dynamic_array_size && i < rh_data->dynamic_array_size; ++i)
			nested_func->merge(
					data->array_of_aggregate_datas + i * nested_func->sizeOfData(),
					rh_data->array_of_aggregate_datas + i * nested_func->sizeOfData(), arena);
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
	{
		const AggregateFunctionForEachData* data = reinterpret_cast<const AggregateFunctionForEachData*>(place);
		buf.write(reinterpret_cast<const char*>(&(data->dynamic_array_size)), sizeof(data->dynamic_array_size));
		for(size_t i = 0; i < data->dynamic_array_size; ++i)
			nested_func->serialize(data->array_of_aggregate_datas + i * nested_func->sizeOfData(), buf);
	}

	void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena * arena) const override
	{
		AggregateFunctionForEachData* data = reinterpret_cast<AggregateFunctionForEachData*>(place);
		buf.read(reinterpret_cast<char*>(&data->dynamic_array_size), sizeof(data->dynamic_array_size));
		data->array_of_aggregate_datas = nullptr;
		ensureAggregateData(place, data->dynamic_array_size, arena);
		for(size_t i = 0; i < data->dynamic_array_size; ++i)
			nested_func->deserialize(data->array_of_aggregate_datas + i * nested_func->sizeOfData(), buf, arena);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		//Insert results of nested functions into temporary columns
		const AggregateFunctionForEachData* data = reinterpret_cast<const AggregateFunctionForEachData*>(place);
		Columns temporaryColumns;
		size_t temporaryColumnSize = 0;
		for(size_t i = 0; i < data->dynamic_array_size; ++i)
		{
			ColumnPtr temporaryColumn = nested_func->getReturnType()->createColumn();
			temporaryColumns.push_back(temporaryColumn);

			nested_func->insertResultInto(data->array_of_aggregate_datas + i * nested_func->sizeOfData(), *temporaryColumn);

			if (temporaryColumnSize == 0)
				temporaryColumnSize = temporaryColumn->size();
			else
				if (temporaryColumnSize != temporaryColumn->size())
					throw Exception("AggregateFunctionEach::insertResultInto: Column sizes don't match", ErrorCodes::PARAMETER_OUT_OF_BOUND);
		}

		// Convert array_size columns of temporaryColumnSize each into one array column of temporaryColumnSize
		ColumnArray & arr_to = static_cast<ColumnArray &>(to);
		ColumnArray::Offsets_t & offsets_to = arr_to.getOffsets();
		ColumnPtr arr_data = arr_to.getDataPtr();

		for(size_t row = 0; row < temporaryColumnSize; ++row)
		{
			offsets_to.push_back((offsets_to.size() == 0 ? 0 : offsets_to.back()) + data->dynamic_array_size);
			for(size_t i = 0; i < data->dynamic_array_size; ++i)
				arr_data->insertFrom(*temporaryColumns[i], row);
		}
	}

	bool allocatesMemoryInArena() const override
	{
		return true;
	}

	static void addFree(const IAggregateFunction * that, AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena)
	{
		static_cast<const AggregateFunctionForEach &>(*that).add(place, columns, row_num, arena);
	}

	IAggregateFunction::AddFunc getAddressOfAddFunction() const override final { return &addFree; }
};



}
