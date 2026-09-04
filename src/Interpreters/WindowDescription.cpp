#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/Field.h>
#include <IO/Operators.h>
#include <Interpreters/WindowDescription.h>
#include <Parsers/ASTFunction.h>
#include <Common/FieldVisitorToString.h>
#include <Common/FieldAccurateComparison.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

std::string WindowFunctionDescription::dump() const
{
    WriteBufferFromOwnString ss;

    ss << "window function '" << column_name << "\n";
    if (function_node)
        ss << "function node " << function_node->dumpTree() << "\n";
    ss << "aggregate function '" << aggregate_function->getName() << "'\n";
    if (!function_parameters.empty())
    {
        ss << "parameters " << toString(function_parameters) << "\n";
    }

    return ss.str();
}

std::string WindowDescription::dump() const
{
    WriteBufferFromOwnString ss;

    ss << "window '" << window_name << "'\n";
    ss << "partition_by " << dumpSortDescription(partition_by) << "\n";
    ss << "order_by " << dumpSortDescription(order_by) << "\n";
    ss << "full_sort_description " << dumpSortDescription(full_sort_description) << "\n";

    return ss.str();
}

std::string WindowFrame::toString() const
{
    WriteBufferFromOwnString buf;
    toString(buf);
    return buf.str();
}

void WindowFrame::toString(WriteBuffer & buf) const
{
    if (type == FrameType::SESSION)
    {
        // A SESSION frame is disjoint (one shared frame per session), so the
        // BEGIN/END boundaries are meaningless. Print the threshold instead.
        buf << type << " " << applyVisitor(FieldVisitorToString(), session_window_threshold);
        return;
    }

    buf << type << " BETWEEN ";
    if (begin_type == BoundaryType::Current)
    {
        buf << "CURRENT ROW";
    }
    else if (begin_type == BoundaryType::Unbounded)
    {
        buf << "UNBOUNDED";
        buf << " "
            << (begin_preceding ? "PRECEDING" : "FOLLOWING");
    }
    else
    {
        buf << applyVisitor(FieldVisitorToString(), begin_offset);
        buf << " "
            << (begin_preceding ? "PRECEDING" : "FOLLOWING");
    }
    buf << " AND ";
    if (end_type == BoundaryType::Current)
    {
        buf << "CURRENT ROW";
    }
    else if (end_type == BoundaryType::Unbounded)
    {
        buf << "UNBOUNDED";
        buf << " "
            << (end_preceding ? "PRECEDING" : "FOLLOWING");
    }
    else
    {
        buf << applyVisitor(FieldVisitorToString(), end_offset);
        buf << " "
            << (end_preceding ? "PRECEDING" : "FOLLOWING");
    }
}

void WindowFrame::checkValid() const
{
    // A SESSION frame is bounded by its threshold rather than by boundary offsets. NaN is tested
    // separately because it compares as greater than every value, and negated "less" is used for
    // the rest because there is no "greater" visitor.
    if (type == FrameType::SESSION
        && (session_window_threshold.isNaN() || session_window_threshold.isInf()
            || !accurateLess(Field(0), session_window_threshold)))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Window frame SESSION threshold must be a positive finite number, '{}' given",
            applyVisitor(FieldVisitorToString(), session_window_threshold));
    }

    // Check the validity of offsets.
    if (begin_type == BoundaryType::Offset
        && !((begin_offset.getType() == Field::Types::UInt64
                || begin_offset.getType() == Field::Types::Int64)
            && begin_offset.safeGet<Int64>() >= 0
            && begin_offset.safeGet<Int64>() < INT_MAX))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Frame start offset for '{}' frame must be a nonnegative 32-bit integer, '{}' of type '{}' given",
            type,
            applyVisitor(FieldVisitorToString(), begin_offset),
            begin_offset.getType());
    }

    if (end_type == BoundaryType::Offset
        && !((end_offset.getType() == Field::Types::UInt64
                || end_offset.getType() == Field::Types::Int64)
            && end_offset.safeGet<Int64>() >= 0
            && end_offset.safeGet<Int64>() < INT_MAX))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Frame end offset for '{}' frame must be a nonnegative 32-bit integer, '{}' of type '{}' given",
            type,
            applyVisitor(FieldVisitorToString(), end_offset),
            end_offset.getType());
    }

    // Check relative positioning of offsets.
    // UNBOUNDED PRECEDING end and UNBOUNDED FOLLOWING start should have been
    // forbidden at the parsing level.
    chassert(!(begin_type == BoundaryType::Unbounded && !begin_preceding));
    chassert(!(end_type == BoundaryType::Unbounded && end_preceding));

    if (begin_type == BoundaryType::Unbounded
        || end_type == BoundaryType::Unbounded)
    {
        return;
    }

    if (begin_type == BoundaryType::Current
        && end_type == BoundaryType::Offset
        && !end_preceding)
    {
        return;
    }

    if (end_type == BoundaryType::Current
        && begin_type == BoundaryType::Offset
        && begin_preceding)
    {
        return;
    }

    if (end_type == BoundaryType::Current
        && begin_type == BoundaryType::Current)
    {
        // BETWEEN CURRENT ROW AND CURRENT ROW makes some sense for RANGE or
        // GROUP frames, and is technically valid for ROWS frame.
        return;
    }

    if (end_type == BoundaryType::Offset
        && begin_type == BoundaryType::Offset)
    {
        // Frame start offset must be less or equal that the frame end offset.
        bool begin_less_equal_end = false;
        if (begin_preceding && end_preceding)
        {
            /// we can't compare Fields using operator<= if fields have different types
            begin_less_equal_end = accurateLessOrEqual(end_offset, begin_offset);
        }
        else if (begin_preceding && !end_preceding)
        {
            begin_less_equal_end = true;
        }
        else if (!begin_preceding && end_preceding)
        {
            begin_less_equal_end = false;
        }
        else /* if (!begin_preceding && !end_preceding) */
        {
            begin_less_equal_end = accurateLessOrEqual(begin_offset, end_offset);
        }

        if (!begin_less_equal_end)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Frame start offset {} {} does not precede the frame end offset {} {}",
                begin_offset, begin_preceding ? "PRECEDING" : "FOLLOWING",
                end_offset, end_preceding ? "PRECEDING" : "FOLLOWING");
        }
        return;
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "Window frame '{}' is invalid",
        toString());
}

void WindowDescription::checkValid() const
{
    frame.checkValid();

    // Both frames compare each row against a key value, so they need a single ORDER BY column.
    const bool is_session = frame.type == WindowFrame::FrameType::SESSION;
    if ((is_session || (frame.type == WindowFrame::FrameType::RANGE
        && (frame.begin_type == WindowFrame::BoundaryType::Offset
            || frame.end_type == WindowFrame::BoundaryType::Offset)))
        && order_by.size() != 1)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The {} window frame requires exactly one ORDER BY column, {} given",
           is_session ? "SESSION" : "RANGE OFFSET", order_by.size());
    }
}

}
