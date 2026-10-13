#pragma once

#include <rmm/cuda_stream_view.hpp>

namespace DB::GPU
{

/// The streams of the `GPUDevice`, which creates them at the start of the server and destroys them at shutdown.
struct StreamRegistry
{
    rmm::cuda_stream_view compute;
    rmm::cuda_stream_view upload;
    rmm::cuda_stream_view decompression;

    /// Of the `GPUDevice` of the global `Context`.
    __host__ static const StreamRegistry & get();
};

}
