#pragma once

#include <DB/IO/WriteHelpers.h>
#include <DB/AggregateFunctions/IAggregateFunction.h>


namespace DB
{

/** Интерфейс для ноль-арных агрегатных функций. Это, например, агрегатная функция count.
  */
template <typename T, typename Derived>
class INullaryAggregateFunction : public IAggregateFunctionHelper<T>
{
private:
	Derived & getDerived() { return static_cast<Derived &>(*this); }
	const Derived & getDerived() const { return static_cast<const Derived &>(*this); }

public:
	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	void setArguments(const DataTypes & arguments) override final
	{
		if (arguments.size() != 0)
			throw Exception("Passed " + toString(arguments.size()) + " arguments to nullary aggregate function " + this->getName(),
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
	}

	/// Добавить значение.
	void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override final
	{
		getDerived().addImpl(place);
	}

	static void addFree(const IAggregateFunction * that, AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *)
	{
		return static_cast<const Derived &>(*that).addImpl(place);
	}

	IAggregateFunction::AddFunc getAddressOfAddFunction() const override final { return &addFree; }

	/** Реализуйте это в классе-наследнике:
	  * void addImpl(AggregateDataPtr place) const;
	  */
};

}
