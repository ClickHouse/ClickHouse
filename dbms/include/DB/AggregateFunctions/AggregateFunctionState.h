#pragma once

#include <DB/Columns/ColumnArray.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeAggregateFunction.h>
#include <DB/AggregateFunctions/IAggregateFunction.h>


namespace DB
{


/** Не агрегатная функция, а адаптер агрегатных функций,
  * Агрегатные функции с суффиксом State отличаются от соответствующих тем, что их состояния не финализируются.
  * Возвращаемый тип - DataTypeAggregateFunction. Функция insertResultInto не используется (реализация будет кидать исключение).
  * Aggregator/SplittingAggregator будет проверять, что вычисляется агрегатная функция -State, и не будет вызывать insertResultInto.
  */

class AggregateFunctionState : public IAggregateFunction
{
private:
	AggregateFunctionPtr nested_func_owner;
	IAggregateFunction * nested_func;
	DataTypes arguments;
	Array params;

public:
	AggregateFunctionState(AggregateFunctionPtr nested_) : nested_func_owner(nested_), nested_func(nested_func_owner.get()) {}

	String getName() const
	{
		return nested_func->getName() + "State";
	}

	DataTypePtr getReturnType() const
	{
		return new DataTypeAggregateFunction(nested_func_owner, arguments, params);
	}

	void setArguments(const DataTypes & arguments_)
	{
		arguments = arguments_;
		nested_func->setArguments(arguments);
	}

	void setParameters(const Array & params_)
	{
		params = params_;
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
		nested_func->add(place, columns, row_num);
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
		throw Exception("Aggregate function " + getName() + " doesn't support insertResultInto method", ErrorCodes::NOT_IMPLEMENTED);
	}

	/// Для аггрегатных функции типа state никогда не нужно вызывать insertResultInto
	bool canBeFinal() const { return false; }

};

}
