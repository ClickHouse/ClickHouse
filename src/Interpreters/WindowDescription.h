#pragma once

#include <Core/Field.h>
#include <Parsers/IAST_fwd.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/SortDescription.h>
#include <DataTypes/IDataType.h>
#include <Core/Names.h>
#include <Core/Types.h>

namespace DB
{

class ASTFunction;

struct WindowFunctionDescription
{
    std::string column_name;
    const ASTFunction * function_node = nullptr;
    AggregateFunctionPtr aggregate_function;
    Array function_parameters;
    DataTypes argument_types;
    Names argument_names;

    std::string dump() const;
};

struct WindowFrame
{
    enum class FrameType { Rows, Groups, Range };
    enum class BoundaryType { Unbounded, Current, Offset };

    // This flag signifies that the frame properties were not set explicitly by
    // user, but the fields of this structure still have to contain proper values
    // for the default frame of RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
    bool is_default = true;

    FrameType type = FrameType::Range;

    // UNBOUNDED FOLLOWING for the frame end is forbidden by the standard, but for
    // uniformity the begin_preceding still has to be set to true for UNBOUNDED
    // frame start.
    // Offset might be both preceding and following, controlled by begin_preceding,
    // but the offset value must be positive.
    BoundaryType begin_type = BoundaryType::Unbounded;
    Field begin_offset = 0;
    bool begin_preceding = true;

    // Here as well, Unbounded can only be UNBOUNDED FOLLOWING, and end_preceding
    // must be false.
    BoundaryType end_type = BoundaryType::Current;
    Field end_offset = 0;
    bool end_preceding = false;


    // Throws BAD_ARGUMENTS exception if the frame definition is incorrect, e.g.
    // the frame start comes later than the frame end.
    void checkValid() const;

    std::string toString() const;
    void toString(WriteBuffer & buf) const;

    bool operator == (const WindowFrame & other) const
    {
        // We don't compare is_default because it's not a real property of the
        // frame, and only influences how we display it.
        return other.type == type
            && other.begin_type == begin_type
            && other.begin_offset == begin_offset
            && other.begin_preceding == begin_preceding
            && other.end_type == end_type
            && other.end_offset == end_offset
            && other.end_preceding == end_preceding
            ;
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

    void checkValid() const;
};

using WindowFunctionDescriptions = std::vector<WindowFunctionDescription>;

using WindowDescriptions = std::unordered_map<std::string, WindowDescription>;

}
