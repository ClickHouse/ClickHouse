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
    std::string column_name;
    const ASTFunction * function_node;
    AggregateFunctionPtr aggregate_function;
    Array function_parameters;
    DataTypes argument_types;
    Names argument_names;

    std::string dump() const;
};

struct WindowFrame
{
    enum class FrameType { Rows, Groups, Range };
    enum class OffsetType { Unbounded, Current, Offset };

    // This flag signifies that the frame properties were not set explicitly by
    // user, but the fields of this structure still have to contain proper values
    // for the default frame of ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
    bool is_default = true;

    FrameType type = FrameType::Rows;

    /*
     * We don't need these yet.
     * OffsetType begin_offset = Unbounded;

     * OffsetType end_offset = Current;
     */


    bool operator == (const WindowFrame & other) const
    {
        // We don't compare is_default because it's not a real property of the
        // frame, and only influences how we display it.
        return other.type == type;
    }
};

struct WindowDescription
{
    std::string window_name;

    // We don't care about the particular order of keys for PARTITION BY, only
    // that they are sorted. For now we always require ASC, but we could be more
    // flexible and match any direction, or even different order of columns.
    SortDescription partition_by;

    SortDescription order_by;

    // To calculate the window function, we sort input data first by PARTITION BY,
    // then by ORDER BY. This field holds this combined sort order.
    SortDescription full_sort_description;

    WindowFrame frame;

    // The window functions that are calculated for this window.
    std::vector<WindowFunctionDescription> window_functions;

    std::string dump() const;
};

using WindowFunctionDescriptions = std::vector<WindowFunctionDescription>;

using WindowDescriptions = std::unordered_map<std::string, WindowDescription>;

}
