#pragma once

#include <cuda_runtime_api.h>

namespace DB::GPU
{

struct StreamRegistry
{
    cudaStream_t compute = nullptr;
    cudaStream_t upload = nullptr;
    cudaStream_t decompression = nullptr;

    static const StreamRegistry & get();
};

}
