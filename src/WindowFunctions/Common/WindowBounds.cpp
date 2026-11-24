#include <WindowFunctions/Common/WindowBounds.h>

namespace DB
{

void clearBounds(WindowBounds & bounds)
{
    bounds.rows = 0;
    bounds.row.clear();
    bounds.frame_start.clear();
    bounds.frame_end.clear();
    bounds.peers_start.clear();
    bounds.peers_end.clear();
    bounds.peer_group_index.clear();
    bounds.partition_end.clear();
}

Int64 findNextFrameChange(const WindowBounds & bounds, Int64 from, bool account_start, bool account_end)
{
    for (Int64 to = from + 1; to < bounds.rows; ++to)
    {
        if (account_start && bounds.frame_start[to] != bounds.frame_start[from])
            return to;

        if (account_end && bounds.frame_end[to] != bounds.frame_end[from])
            return to;
    }

    return bounds.rows;
}

}
