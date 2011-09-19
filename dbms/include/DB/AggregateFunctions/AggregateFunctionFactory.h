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
	AggregateFunctionFactory();
	AggregateFunctionPtr get(const String & name) const;
	AggregateFunctionPtr tryGet(const String & name) const;

private:
	typedef std::map<String, AggregateFunctionPtr> NonParametricAggregateFunctions;
	NonParametricAggregateFunctions non_parametric_aggregate_functions;
};

using Poco::SharedPtr;

typedef SharedPtr<AggregateFunctionFactory> AggregateFunctionFactoryPtr;


}
