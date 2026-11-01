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

}
