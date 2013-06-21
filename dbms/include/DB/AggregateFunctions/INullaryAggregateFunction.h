#pragma once

#include <DB/AggregateFunctions/IAggregateFunction.h>


namespace DB
{

/** Интерфейс для ноль-арных агрегатных функций. Это, например, агрегатная функция count.
  */
template <typename T>
class INullaryAggregateFunction : public IAggregateFunctionHelper<T>
{
public:
	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	void setArguments(const DataTypes & arguments)
	{
		if (arguments.size() != 0)
			throw Exception("Passed " + toString(arguments.size()) + " arguments to nullary aggregate function " + this->getName(),
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
	}

	/// Добавить значение.
	void add(AggregateDataPtr place, const Row & row) const
	{
		addZero(place);
	}

	virtual void addZero(AggregateDataPtr place) const = 0;
};

}
