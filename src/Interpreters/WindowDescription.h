#pragma once

#include <Common/FieldVisitors.h>
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
    enum class BoundaryType { Unbounded, Current, Offset };

    // This flag signifies that the frame properties were not set explicitly by
    // user, but the fields of this structure still have to contain proper values
    // for the default frame of RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
    bool is_default = true;

    FrameType type = FrameType::Range;

    // UNBOUNDED FOLLOWING for the frame end doesn't make much sense, so
    // Unbounded here means UNBOUNDED PRECEDING.
    // Offset might be both preceding and following, preceding is negative
    // (be careful, this is not symmetric w/the frame end unlike in the grammar,
    // so a positive literal in PRECEDING will give a negative number here).
    BoundaryType begin_type = BoundaryType::Unbounded;
    // This should have been a Field but I'm getting some crazy linker errors.
    int64_t begin_offset = 0;

    // Here as well, Unbounded is UNBOUNDED FOLLOWING, and positive Offset is
    // following.
    BoundaryType end_type = BoundaryType::Current;
    int64_t end_offset = 0;


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
            && other.end_type == end_type
            && other.end_offset == end_offset
            ;
    }

    static std::string toString(FrameType type)
    {
        switch (type)
        {
            case FrameType::Rows:
                return "ROWS";
            case FrameType::Groups:
                return "GROUPS";
            case FrameType::Range:
                return "RANGE";
        }

        // Somehow GCC 10 doesn't understand that the above switch is exhaustive.
        assert(false);
        return "<unknown frame>";
    }

    static std::string toString(BoundaryType type)
    {
        switch (type)
        {
            case BoundaryType::Unbounded:
                return "UNBOUNDED";
            case BoundaryType::Offset:
                return "OFFSET";
            case BoundaryType::Current:
                return "CURRENT ROW";
        }

        // Somehow GCC 10 doesn't understand that the above switch is exhaustive.
        assert(false);
        return "<unknown frame boundary>";
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
