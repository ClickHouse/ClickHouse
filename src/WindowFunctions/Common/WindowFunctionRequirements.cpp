#include <WindowFunctions/Common/WindowFunctionRequirements.h>

namespace DB
{

WindowFunctionRequirements unionOf(const WindowFunctionRequirements & lhs, const WindowFunctionRequirements & rhs)
{
    return WindowFunctionRequirements{
        .frame = lhs.frame || rhs.frame,
        .peers = lhs.peers || rhs.peers,
        .partition = lhs.partition || rhs.partition,
        .order_by = lhs.order_by || rhs.order_by,
    };
}

}
