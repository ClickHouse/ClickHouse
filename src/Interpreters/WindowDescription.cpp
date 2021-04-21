#include <Interpreters/WindowDescription.h>

#include <IO/Operators.h>
#include <Parsers/ASTFunction.h>

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
    buf << toString(type) << " BETWEEN ";
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
        buf << abs(begin_offset);
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
        buf << abs(end_offset);
        buf << " "
            << (end_preceding ? "PRECEDING" : "FOLLOWING");
    }
}

void WindowFrame::checkValid() const
{
    // UNBOUNDED PRECEDING end and UNBOUNDED FOLLOWING start should have been
    // forbidden at the parsing level.
    assert(!(begin_type == BoundaryType::Unbounded && !begin_preceding));
    assert(!(end_type == BoundaryType::Unbounded && end_preceding));

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
        // Frame starting with following rows can't have preceding rows.
        if (!(end_preceding && !begin_preceding))
        {
            // Frame start offset must be less or equal that the frame end offset.
            const bool begin_before_end
                = begin_offset * (begin_preceding ? -1 : 1)
                    <= end_offset * (end_preceding ? -1 : 1);

            if (!begin_before_end)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Frame start offset {} {} does not precede the frame end offset {} {}",
                    begin_offset, begin_preceding ? "PRECEDING" : "FOLLOWING",
                    end_offset, end_preceding ? "PRECEDING" : "FOLLOWING");
            }
            return;
        }
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "Window frame '{}' is invalid",
        toString());
}

void WindowDescription::checkValid() const
{
    frame.checkValid();

    // RANGE OFFSET requires exactly one ORDER BY column.
    if (frame.type == WindowFrame::FrameType::Range
        && (frame.begin_type == WindowFrame::BoundaryType::Offset
            || frame.end_type == WindowFrame::BoundaryType::Offset)
        && order_by.size() != 1)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The RANGE OFFSET window frame requires exactly one ORDER BY column, {} given",
           order_by.size());
    }
}

}
