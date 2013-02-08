#pragma once

#include <DB/AggregateFunctions/IAggregateFunction.h>


namespace DB
{

/** Интерфейс для агрегатных функций, принимающих одно значение. Это почти все агрегатные функции.
  */
template <typename T>
class IUnaryAggregateFunction : public IAggregateFunctionHelper<T>
{
public:
	void setArguments(const DataTypes & arguments)
	{
		if (arguments.size() != 1)
			throw Exception("Passed " + Poco::NumberFormatter::format(arguments.size()) + " arguments to unary aggregate function " + this->getName(),
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
		setArgument(arguments[0]);
	}

	virtual void setArgument(const DataTypePtr & argument) = 0;

	/// Добавить значение.
	void add(AggregateDataPtr place, const Row & row) const
	{
		addOne(place, row[0]);
	}

	virtual void addOne(AggregateDataPtr place, const Field & value) const = 0;
};

}
