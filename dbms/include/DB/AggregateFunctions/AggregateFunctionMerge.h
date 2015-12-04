#pragma once

#include <DB/DataTypes/DataTypeAggregateFunction.h>
#include <DB/AggregateFunctions/IAggregateFunction.h>
#include <DB/Columns/ColumnAggregateFunction.h>


namespace DB
{


/** Не агрегатная функция, а адаптер агрегатных функций,
  * Агрегатные функции с суффиксом Merge принимают в качестве аргумента DataTypeAggregateFunction
  *  (состояние агрегатной функции, полученное ранее с помощью применения агрегатной функции с суффиксом State)
  *  и объединяют их при агрегации.
  */

class AggregateFunctionMerge final : public IAggregateFunction
{
private:
	AggregateFunctionPtr nested_func_owner;
	IAggregateFunction * nested_func;

public:
	AggregateFunctionMerge(AggregateFunctionPtr nested_) : nested_func_owner(nested_), nested_func(nested_func_owner.get()) {}

	String getName() const override
	{
		return nested_func->getName() + "Merge";
	}

	DataTypePtr getReturnType() const override
	{
		return nested_func->getReturnType();
	}

	void setArguments(const DataTypes & arguments) override
	{
		if (arguments.size() != 1)
			throw Exception("Passed " + toString(arguments.size()) + " arguments to unary aggregate function " + this->getName(),
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		const DataTypeAggregateFunction * data_type = typeid_cast<const DataTypeAggregateFunction *>(&*arguments[0]);

		if (!data_type || data_type->getFunctionName() != nested_func->getName())
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument for aggregate function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		nested_func->setArguments(data_type->getArgumentsDataTypes());
	}

	void setParameters(const Array & params) override
	{
		nested_func->setParameters(params);
	}

	void create(AggregateDataPtr place) const override
	{
		nested_func->create(place);
	}

	void destroy(AggregateDataPtr place) const noexcept override
	{
		nested_func->destroy(place);
	}

	bool hasTrivialDestructor() const override
	{
		return nested_func->hasTrivialDestructor();
	}

	size_t sizeOfData() const override
	{
		return nested_func->sizeOfData();
	}

	size_t alignOfData() const override
	{
		return nested_func->alignOfData();
	}

	void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num) const override
	{
		nested_func->merge(place, static_cast<const ColumnAggregateFunction &>(*columns[0]).getData()[row_num]);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const override
	{
		nested_func->merge(place, rhs);
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
	{
		nested_func->serialize(place, buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const override
	{
		nested_func->deserializeMerge(place, buf);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		nested_func->insertResultInto(place, to);
	}

	static void addFree(const IAggregateFunction * that, AggregateDataPtr place, const IColumn ** columns, size_t row_num)
	{
		return static_cast<const AggregateFunctionMerge &>(*that).add(place, columns, row_num);
	}

	IAggregateFunction::AddFunc getAddressOfAddFunction() const override final { return &addFree; }
};

}
