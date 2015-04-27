#pragma once

#include <Poco/RegularExpression.h>

#include <DB/AggregateFunctions/IAggregateFunction.h>


namespace DB
{


/** Позволяет создать агрегатную функцию по её имени.
  */
class AggregateFunctionFactory
{
public:
	using FunctionNames = std::vector<std::string>;

public:
	AggregateFunctionFactory();
	AggregateFunctionPtr get(const String & name, const DataTypes & argument_types, int recursion_level = 0) const;
	AggregateFunctionPtr tryGet(const String & name, const DataTypes & argument_types) const;
	const FunctionNames & getFunctionNames() const;
	bool isAggregateFunctionName(const String & name, int recursion_level = 0) const;
};


}
