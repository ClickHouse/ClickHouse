#pragma once

#include <DB/AggregateFunctions/IAggregateFunction.h>


namespace DB
{

/** Интерфейс для ноль-арных агрегатных функций. Это, например, агрегатная функция count.
  */
class INullaryAggregateFunction : public IAggregateFunction
{
public:
	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	void setArguments(const DataTypes & arguments)
	{
		if (arguments.size() != 0)
			throw Exception("Passed " + Poco::NumberFormatter::format(arguments.size()) + " arguments to nullary aggregate function " + getName(),
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
	}

	/// Добавить значение.
	void add(const Row & row)
	{
		addZero();
	}

	virtual void addZero() = 0;
};

}
