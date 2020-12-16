#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <DataTypes/IDataType.h>
#include <Core/ColumnNumbers.h>
#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class ASTFunction;

struct AggregateDescription
{
    AggregateFunctionPtr function;
    Array parameters;        /// Parameters of the (parametric) aggregate function.
    ColumnNumbers arguments;
    Names argument_names;    /// used if no `arguments` are specified.
    String column_name;      /// What name to use for a column with aggregate function values

    void explain(WriteBuffer & out, size_t indent) const; /// Get description for EXPLAIN query.
};

using AggregateDescriptions = std::vector<AggregateDescription>;


struct WindowFunctionDescription
{
    std::string window_name;
    std::string column_name;
    const ASTFunction * function_node;
    AggregateFunctionPtr aggregate_function;
    Array function_parameters;
    DataTypes argument_types;
    Names argument_names;

    std::string dump() const;
};

struct WindowDescription
{
    std::string window_name;
    // Always ASC for now.
    SortDescription partition_by;
    SortDescription order_by;
    SortDescription full_sort_description;
    // No frame info as of yet.

    std::string dump() const;
};

using WindowFunctionDescriptions = std::vector<WindowFunctionDescription>;

using WindowDescriptions = std::unordered_map<std::string, WindowDescription>;

}
