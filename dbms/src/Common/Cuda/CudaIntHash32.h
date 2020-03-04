#pragma once 

#include <Core/Cuda/Types.h>

template <DB::UInt64 salt>
inline __device__ DB::UInt32 cudaIntHash32(DB::UInt64 key)
{
    key ^= salt;

    key = (~key) + (key << 18);
    key = key ^ ((key >> 31) | (key << 33));
    key = key * 21;
    key = key ^ ((key >> 11) | (key << 53));
    key = key + (key << 6);
    key = key ^ ((key >> 22) | (key << 42));

    return key;
}

template <typename T, DB::UInt64 salt = 0>
struct CudaIntHash32
{
    __device__ DB::UInt32 operator() (const T & key) const
    {
        return cudaIntHash32<salt>(key);
    }
};

