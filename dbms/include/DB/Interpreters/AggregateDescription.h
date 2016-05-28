#pragma once

#include <DB/Core/ColumnNumbers.h>
#include <DB/Core/Names.h>
#include <DB/AggregateFunctions/IAggregateFunction.h>


namespace DB
{

struct AggregateDescription
{
	AggregateFunctionPtr function;
	Array parameters;		/// Параметры (параметрической) агрегатной функции.
	ColumnNumbers arguments;
	Names argument_names;	/// Используются, если arguments не заданы.
	String column_name;		/// Какое имя использовать для столбца со значениями агрегатной функции
};

using AggregateDescriptions = std::vector<AggregateDescription>;

}
