#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/ColumnNumbers.h>
#include <Core/Names.h>
#include <Core/Types.h>

namespace DB
{

namespace JSONBuilder { class JSONMap; }

struct AggregateDescription
{
    AggregateFunctionPtr function;
    Array parameters;        /// Parameters of the (parametric) aggregate function.
    Names argument_names;
    String column_name;      /// What name to use for a column with aggregate function values

    std::optional<Names> by_columns; /// If set, then it is names of columns to partition by.
                                     /// If set and empty, then it is a global aggregate function.

    void explain(WriteBuffer & out, size_t indent) const; /// Get description for EXPLAIN query.
    void explain(JSONBuilder::JSONMap & map) const;
};

using AggregateDescriptions = std::vector<AggregateDescription>;
}
