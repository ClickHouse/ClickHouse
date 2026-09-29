#pragma once

#include <rmm/cuda_stream_view.hpp>

namespace DB::GPU
{

struct StreamRegistry
{
    rmm::cuda_stream_view compute;
    rmm::cuda_stream_view upload;
    rmm::cuda_stream_view decompression;

    static const StreamRegistry & get();
};

}
