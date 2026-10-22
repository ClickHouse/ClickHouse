#pragma once

#include <Common/PODArray.h>
#include <base/types.h>

namespace DB
{

struct WindowRowBounds
{
    Int64 row = -1;
    Int64 frame_start = -1;
    Int64 frame_end = -1;
    Int64 peers_start = -1;
    Int64 peers_end = -1;
    Int64 peer_group_index = -1;
    Int64 partition_end = -1;
};

struct WindowBounds
{
    size_t rows = 0;
    PaddedPODArray<Int64> row;
    PaddedPODArray<Int64> frame_start;
    PaddedPODArray<Int64> frame_end;
    PaddedPODArray<Int64> peers_start;
    PaddedPODArray<Int64> peers_end;
    PaddedPODArray<Int64> peer_group_index;
    PaddedPODArray<Int64> partition_end;

    WindowRowBounds at(size_t i) const;
    void clear();
};

}
