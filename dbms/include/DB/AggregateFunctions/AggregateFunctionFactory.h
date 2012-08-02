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
	AggregateFunctionPtr get(const String & name, const DataTypes & argument_types) const;
	AggregateFunctionPtr tryGet(const String & name, const DataTypes & argument_types) const;
	AggregateFunctionPtr getByTypeID(const String & type_id) const;
};


}
