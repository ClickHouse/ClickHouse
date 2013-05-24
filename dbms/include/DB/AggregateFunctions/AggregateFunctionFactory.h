#pragma once

#include <Poco/RegularExpression.h>

#include <DB/AggregateFunctions/IAggregateFunction.h>


namespace DB
{


/** Позволяет создать агрегатную функцию по её имени.
  * 
  * Чтобы создать большое количество экземпляров агрегатных функций
  *  для агрегации и последующей вставки в ColumnAggregateFunction,
  *  создайте один объект - "прототип", и затем используйте метод cloneEmpty.
  */
class AggregateFunctionFactory
{
public:
	AggregateFunctionFactory();
	AggregateFunctionPtr get(const String & name, const DataTypes & argument_types) const;
	AggregateFunctionPtr tryGet(const String & name, const DataTypes & argument_types) const;
	AggregateFunctionPtr getByTypeID(const String & type_id) const;
	bool isAggregateFunctionName(const String & name) const;
};


}
