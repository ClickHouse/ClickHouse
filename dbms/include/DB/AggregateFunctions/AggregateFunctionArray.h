#pragma once

#include <DB/Columns/ColumnArray.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/AggregateFunctions/IAggregateFunction.h>


namespace DB
{


/** Не агрегатная функция, а адаптер агрегатных функций,
  *  который любую агрегатную функцию agg(x) делает агрегатной функцией вида aggArray(x).
  * Адаптированная агрегатная функция вычисляет вложенную агрегатную функцию для каждого элемента массива.
  */
class AggregateFunctionArray : public IAggregateFunction
{
private:
	AggregateFunctionPtr nested_func_owner;
	IAggregateFunction * nested_func;
	int num_agruments;

public:
	AggregateFunctionArray(AggregateFunctionPtr nested_) : nested_func_owner(nested_), nested_func(nested_func_owner.get()) {}

	String getName() const
	{
		return nested_func->getName() + "Array";
	}

	DataTypePtr getReturnType() const
	{
		return nested_func->getReturnType();
	}

	void setArguments(const DataTypes & arguments)
	{
		num_agruments = arguments.size();

		DataTypes nested_arguments;
		for (int i = 0; i < num_agruments; ++i)
		{
			if (const DataTypeArray * array = dynamic_cast<const DataTypeArray *>(&*arguments[i]))
				nested_arguments.push_back(array->getNestedType());
			else
				throw Exception("Illegal type " + arguments[i]->getName() + " of argument #" + toString(i + 1) + " for aggregate function " + getName() + ". Must be array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
		}
		nested_func->setArguments(nested_arguments);
	}

	void setParameters(const Array & params)
	{
		nested_func->setParameters(params);
	}

	void create(AggregateDataPtr place) const
	{
		nested_func->create(place);
	}

	void destroy(AggregateDataPtr place) const
	{
		nested_func->destroy(place);
	}

	bool hasTrivialDestructor() const
	{
		return nested_func->hasTrivialDestructor();
	}

	size_t sizeOfData() const
	{
		return nested_func->sizeOfData();
	}

	size_t alignOfData() const
	{
		return nested_func->alignOfData();
	}

	void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num) const
	{
		const IColumn ** nested = new const IColumn*[num_agruments];
		std::vector<ColumnPtr> column_ptrs;
		for (int i = 0; i < num_agruments; ++i)
		{
			ColumnPtr single_value_column = dynamic_cast<const ColumnArray &>(*columns[i]).cut(row_num, 1);
			column_ptrs.push_back(single_value_column);
			nested[i] = dynamic_cast<const ColumnArray &>(*single_value_column).getDataPtr().get();
		}
		for (int i = 0; i < num_agruments; ++i)
			if (nested[i]->size() != nested[0]->size())
				throw Exception("All arrays must be of the same size. Aggregate function " + getName(), ErrorCodes::BAD_ARGUMENTS);
		for (size_t i = 0; i < nested[0]->size(); ++i)
			nested_func->add(place, nested, i);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const
	{
		nested_func->merge(place, rhs);
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const
	{
		nested_func->serialize(place, buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const
	{
		nested_func->deserializeMerge(place, buf);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const
	{
		nested_func->insertResultInto(place, to);
	}
};

}
