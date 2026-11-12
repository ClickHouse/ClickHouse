#include <WindowFunctions/Common/WindowFunctionRequirements.h>

namespace DB
{

WindowFunctionRequirements unionOf(const WindowFunctionRequirements & lhs, const WindowFunctionRequirements & rhs)
{
    return WindowFunctionRequirements{
        .frame_start = lhs.frame_start || rhs.frame_start,
        .frame_end = lhs.frame_end || rhs.frame_end,
        .peers_start = lhs.peers_start || rhs.peers_start,
        .peers_end = lhs.peers_end || rhs.peers_end,
        .partition_end = lhs.partition_end || rhs.partition_end,
        .order_by = lhs.order_by || rhs.order_by,
    };
}

}
