#pragma once

#include <DB/AggregateFunctions/IAggregateFunction.h>
#include <DB/DataTypes/IDataType.h>
#include <boost/iterator/transform_iterator.hpp>

namespace DB
{

/** Позволяет создать агрегатную функцию по её имени.
  */
class AggregateFunctionFactory final
{
private:
	/// Не std::function, так как меньше indirection и размер объекта.
	using Creator = AggregateFunctionPtr(*)(const std::string & name, const DataTypes & argument_types);

public:
	AggregateFunctionFactory();
	AggregateFunctionPtr get(const String & name, const DataTypes & argument_types, int recursion_level = 0) const;
	AggregateFunctionPtr tryGet(const String & name, const DataTypes & argument_types) const;
	bool isAggregateFunctionName(const String & name, int recursion_level = 0) const;

	/// Зарегистрировать агрегатную функцию заданную по одному или нескольким названиям.
	void registerFunction(const std::vector<std::string> & names, Creator creator);

	AggregateFunctionFactory(const AggregateFunctionFactory &) = delete;
	AggregateFunctionFactory & operator=(const AggregateFunctionFactory &) = delete;

private:
	struct Descriptor
	{
		Creator creator;
		bool is_alias;
	};

	using AggregateFunctions = std::unordered_map<std::string, Descriptor>;

public:
	struct Details
	{
		std::string name;
		bool is_alias;
	};

private:
	/// Вспомогательная функция для реализации итератора (см. ниже).
	static Details getDetails(const AggregateFunctions::value_type & entry);

public:
	/** Итератор над агрегатными функциями. Возвращает объект типа Details.
	  * Этот итератор нужен для таблицы system.functions.
	  */
	using const_iterator = boost::transform_iterator<decltype(&AggregateFunctionFactory::getDetails),
		typename AggregateFunctions::const_iterator>;

public:
	const_iterator begin() const;
	const_iterator end() const;

private:
	AggregateFunctions aggregate_functions;
};

}
