#pragma once

#include <DB/AggregateFunctions/IAggregateFunction.h>


namespace DB
{

/** Интерфейс для ноль-арных агрегатных функций. Это, например, агрегатная функция count.
  */
template <typename T, typename Derived>
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
	void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num) const
	{
		static_cast<const Derived &>(*this).addZero(place);
	}

	/** Реализуйте это в классе-наследнике:
	  * void addZero(AggregateDataPtr place) const;
	  */
};

}
