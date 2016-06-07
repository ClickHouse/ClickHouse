#pragma once

#include <DB/DataTypes/DataTypeAggregateFunction.h>
#include <DB/AggregateFunctions/IAggregateFunction.h>
#include <DB/Columns/ColumnAggregateFunction.h>


namespace DB
{


/** Не агрегатная функция, а адаптер агрегатных функций,
  * Агрегатные функции с суффиксом State отличаются от соответствующих тем, что их состояния не финализируются.
  * Возвращаемый тип - DataTypeAggregateFunction.
  */

class AggregateFunctionState final : public IAggregateFunction
{
private:
	AggregateFunctionPtr nested_func_owner;
	IAggregateFunction * nested_func;
	DataTypes arguments;
	Array params;

public:
	AggregateFunctionState(AggregateFunctionPtr nested_) : nested_func_owner(nested_), nested_func(nested_func_owner.get()) {}

	String getName() const override
	{
		return nested_func->getName() + "State";
	}

	DataTypePtr getReturnType() const override
	{
		return new DataTypeAggregateFunction(nested_func_owner, arguments, params);
	}

	void setArguments(const DataTypes & arguments_) override
	{
		arguments = arguments_;
		nested_func->setArguments(arguments);
	}

	void setParameters(const Array & params_) override
	{
		params = params_;
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
		static_cast<ColumnAggregateFunction &>(to).getData().push_back(const_cast<AggregateDataPtr>(place));
	}

	/// Аггрегатная функция или состояние аггрегатной функции.
	bool isState() const override { return true; }

	AggregateFunctionPtr getNestedFunction() const { return nested_func_owner; }

	static void addFree(const IAggregateFunction * that, AggregateDataPtr place, const IColumn ** columns, size_t row_num)
	{
		return static_cast<const AggregateFunctionState &>(*that).add(place, columns, row_num);
	}

	IAggregateFunction::AddFunc getAddressOfAddFunction() const override final { return &addFree; }
};

}
