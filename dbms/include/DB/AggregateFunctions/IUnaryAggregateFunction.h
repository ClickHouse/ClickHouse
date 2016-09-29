#pragma once

#include <DB/IO/WriteHelpers.h>
#include <DB/AggregateFunctions/IAggregateFunction.h>


namespace DB
{

/** Интерфейс для агрегатных функций, принимающих одно значение. Это почти все агрегатные функции.
  */
template <typename T, typename Derived>
class IUnaryAggregateFunction : public IAggregateFunctionHelper<T>
{
private:
	Derived & getDerived() { return static_cast<Derived &>(*this); }
	const Derived & getDerived() const { return static_cast<const Derived &>(*this); }

public:
	void setArguments(const DataTypes & arguments) override final
	{
		if (arguments.size() != 1)
			throw Exception("Passed " + toString(arguments.size()) + " arguments to unary aggregate function " + this->getName(),
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		getDerived().setArgument(arguments[0]);
	}

	/// Добавить значение.
	void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override final
	{
		getDerived().addImpl(place, *columns[0], row_num, arena);
	}

	static void addFree(const IAggregateFunction * that, AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena)
	{
		static_cast<const Derived &>(*that).addImpl(place, *columns[0], row_num, arena);
	}

	IAggregateFunction::AddFunc getAddressOfAddFunction() const override { return &addFree; }

	/** Реализуйте это в классе-наследнике:
	  * void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num) const;
	  * void setArgument(const DataTypePtr & argument);
	  */
};

}
