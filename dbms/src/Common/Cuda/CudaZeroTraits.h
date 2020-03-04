#pragma once

/// These functions can be overloaded for custom types.
namespace CudaZeroTraits
{

template <typename T>
__device__ __host__ bool check(const T x) { return x == 0; }

template <typename T>
__device__ __host__ void set(T & x) { x = 0; }

/// Returns 'sample' of zero object
template <typename T>
__device__ __host__ T    zero() { return (T)0; }

};