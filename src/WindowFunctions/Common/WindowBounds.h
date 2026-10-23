#pragma once

#include <Common/PODArray.h>
#include <base/types.h>

namespace DB
{

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
};

void clearBounds(WindowBounds & bounds);

}
