#pragma once

#include <Core/Cuda/Types.h>
#include <Common/Cuda/cudaMurmurHash64.cuh>

template <unsigned int seed = 1>
struct CudaStringMurmurHash64
{
    using result_type = DB::UInt64;

    result_type operator()(const char * s, DB::UInt32 len) const
    {
        return cudaMurmurHash64(s, len, seed);
    }
};
