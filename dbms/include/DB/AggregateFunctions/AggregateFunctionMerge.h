#pragma once

#include <DB/Columns/ColumnArray.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeAggregateFunction.h>
#include <DB/AggregateFunctions/IAggregateFunction.h>
#include <DB/IO/ReadBufferFromString.h>


namespace DB
{


/** Не агрегатная функция, а адаптер агрегатных функций,
  * Агрегатные функции с суффиксом Merge принимают в качестве аргумента DataTypeAggregateFunction (состояние агрегатной функции),
  * и объединяют их при агрегации.
  */

class AggregateFunctionMerge : public IAggregateFunction
{
private:
	AggregateFunctionPtr nested_func_owner;
	IAggregateFunction * nested_func;

public:
	AggregateFunctionMerge(AggregateFunctionPtr nested_) : nested_func_owner(nested_), nested_func(nested_func_owner.get()) {}

	String getName() const
	{
		return nested_func->getName() + "Merge";
	}

	DataTypePtr getReturnType() const
	{
		return nested_func->getReturnType();
	}

	void setArguments(const DataTypes & arguments)
	{
//		size_t num_agruments = arguments.size();

		if (arguments.size() != 1)
			throw Exception("Passed " + toString(arguments.size()) + " arguments to unary aggregate function " + this->getName(),
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
		const DataTypeAggregateFunction * data_type = dynamic_cast<const DataTypeAggregateFunction *>(&*arguments[0]);
		if (!data_type || data_type->getFunctionName() != nested_func->getName())
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument for aggregate function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
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
		Field field;
		columns[0]->get(row_num, field);
		ReadBufferFromString read_buffer(field.safeGet<String>());
		nested_func->deserializeMerge(place, read_buffer);
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
