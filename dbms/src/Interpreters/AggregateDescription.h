#pragma once

#include <Core/ColumnNumbers.h>
#include <Core/Names.h>
#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{

struct AggregateDescription
{
    AggregateFunctionPtr function;
    Array parameters;        /// Параметры (параметрической) агрегатной функции.
    ColumnNumbers arguments;
    Names argument_names;    /// Используются, если arguments не заданы.
    String column_name;        /// Какое имя использовать для столбца со значениями агрегатной функции
};

using AggregateDescriptions = std::vector<AggregateDescription>;

}
