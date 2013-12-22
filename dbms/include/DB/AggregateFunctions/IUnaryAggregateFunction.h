#pragma once

#include <DB/AggregateFunctions/IAggregateFunction.h>


namespace DB
{

/** Интерфейс для агрегатных функций, принимающих одно значение. Это почти все агрегатные функции.
  */
template <typename T, typename Derived>
class IUnaryAggregateFunction : public IAggregateFunctionHelper<T>
{
public:
	void setArguments(const DataTypes & arguments)
	{
		if (arguments.size() != 1)
			throw Exception("Passed " + toString(arguments.size()) + " arguments to unary aggregate function " + this->getName(),
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
		setArgument(arguments[0]);
	}

	virtual void setArgument(const DataTypePtr & argument) = 0;

	/// Добавить значение.
	void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num) const
	{
		static_cast<const Derived &>(*this).addOne(place, *columns[0], row_num);
	}

	/** Реализуйте это в классе-наследнике:
	  * void addOne(AggregateDataPtr place, const IColumn & column, size_t row_num) const;
	  */
};

}
