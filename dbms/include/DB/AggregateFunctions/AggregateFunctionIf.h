#pragma once

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/AggregateFunctions/IAggregateFunction.h>


namespace DB
{


/** Не агрегатная функция, а адаптер агрегатных функций,
  *  который любую агрегатную функцию agg(x) делает агрегатной функцией вида aggIf(x, cond).
  * Адаптированная агрегатная функция принимает два аргумента - значение и условие,
  *  и вычисляет вложенную агрегатную функцию для значений при выполненном условии.
  * Например, avgIf(x, cond) вычисляет среднее x при условии cond.
  */
class AggregateFunctionIf final : public IAggregateFunction
{
private:
	AggregateFunctionPtr nested_func_owner;
	IAggregateFunction * nested_func;
	int num_agruments;
public:
	AggregateFunctionIf(AggregateFunctionPtr nested_) : nested_func_owner(nested_), nested_func(nested_func_owner.get()) {}

	String getName() const override
	{
		return nested_func->getName() + "If";
	}

	DataTypePtr getReturnType() const override
	{
		return nested_func->getReturnType();
	}

	void setArguments(const DataTypes & arguments) override
	{
		num_agruments = arguments.size();

		if (!typeid_cast<const DataTypeUInt8 *>(&*arguments[num_agruments - 1]))
			throw Exception("Illegal type " + arguments[num_agruments - 1]->getName() + " of second argument for aggregate function " + getName() + ". Must be UInt8.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		DataTypes nested_arguments;
		for (int i = 0; i < num_agruments - 1; i ++)
			nested_arguments.push_back(arguments[i]);
		nested_func->setArguments(nested_arguments);
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
		if (static_cast<const ColumnUInt8 &>(*columns[num_agruments - 1]).getData()[row_num])
			nested_func->add(place, columns, row_num);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const override
	{
		nested_func->merge(place, rhs);
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
	{
		nested_func->serialize(place, buf);
	}

	void deserialize(AggregateDataPtr place, ReadBuffer & buf) const override
	{
		nested_func->deserialize(place, buf);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		nested_func->insertResultInto(place, to);
	}

	static void addFree(const IAggregateFunction * that, AggregateDataPtr place, const IColumn ** columns, size_t row_num)
	{
		return static_cast<const AggregateFunctionIf &>(*that).add(place, columns, row_num);
	}

	IAggregateFunction::AddFunc getAddressOfAddFunction() const override final { return &addFree; }
};

}
