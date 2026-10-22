#include <WindowFunctions/Common/WindowBounds.h>

namespace DB
{

namespace
{

Int64 valueAt(const PaddedPODArray<Int64> & values, size_t i)
{
    return values.empty() ? -1 : values[i];
}

}

WindowRowBounds WindowBounds::at(size_t i) const
{
    return WindowRowBounds{
        .row = valueAt(row, i),
        .frame_start = valueAt(frame_start, i),
        .frame_end = valueAt(frame_end, i),
        .peers_start = valueAt(peers_start, i),
        .peers_end = valueAt(peers_end, i),
        .peer_group_index = valueAt(peer_group_index, i),
        .partition_end = valueAt(partition_end, i),
    };
}

void WindowBounds::clear()
{
    rows = 0;
    row.clear();
    frame_start.clear();
    frame_end.clear();
    peers_start.clear();
    peers_end.clear();
    peer_group_index.clear();
    partition_end.clear();
}

}
