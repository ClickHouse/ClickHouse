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
        buf << "UNBOUNDED PRECEDING";
    }
    else
    {
        buf << abs(begin_offset);
        buf << " "
            << (begin_offset > 0 ? "FOLLOWING" : "PRECEDING");
    }
    buf << " AND ";
    if (end_type == BoundaryType::Current)
    {
        buf << "CURRENT ROW";
    }
    else if (end_type == BoundaryType::Unbounded)
    {
        buf << "UNBOUNDED PRECEDING";
    }
    else
    {
        buf << abs(end_offset);
        buf << " "
            << (end_offset > 0 ? "FOLLOWING" : "PRECEDING");
    }
}

void WindowFrame::checkValid() const
{
    if (begin_type == BoundaryType::Unbounded
        || end_type == BoundaryType::Unbounded)
    {
        return;
    }

    if (begin_type == BoundaryType::Current
        && end_type == BoundaryType::Offset
        && end_offset > 0)
    {
        return;
    }

    if (end_type == BoundaryType::Current
        && begin_type == BoundaryType::Offset
        && begin_offset < 0)
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
        if (type == FrameType::Rows)
        {
            if (end_offset >= begin_offset)
            {
                return;
            }
        }

        // For RANGE and GROUPS, we must check that end follows begin if sorted
        // according to ORDER BY (we don't support them yet).
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "Window frame '{}' is invalid",
        toString());
}

}
