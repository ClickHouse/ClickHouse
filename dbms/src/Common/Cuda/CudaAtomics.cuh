#pragma once

#include <cuda.h>
#include <cuda_runtime.h>

#include <Core/Cuda/Types.h>

/// This simply taken from cuda programming guide
#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ < 600 
inline __device__ double atomicAdd(double* address, double val) 
{ 
    unsigned long long int* address_as_ull = (unsigned long long int*)address; 
    unsigned long long int old = *address_as_ull, assumed; 
    do { 
        assumed = old; 
        old = atomicCAS(address_as_ull, assumed, 
            __double_as_longlong(val + __longlong_as_double(assumed))); 
        // Note: uses integer comparison to avoid hang in case of NaN (since NaN != NaN) 
    } while (assumed != old); 

    return __longlong_as_double(old); 
} 
#endif
#endif

namespace cuda_details 
{

template<typename T>
inline __device__ T    atomicCAS(T* address, T compare, T val)
{
    static_assert(sizeof(T) == 0, "CUDA atomicCAS not supported type");
}

template<>
inline __device__ DB::UInt32  atomicCAS<DB::UInt32>(DB::UInt32* address, DB::UInt32 compare, DB::UInt32 val)
{
    return ::atomicCAS((unsigned int*)address, (unsigned int)compare, (unsigned int)val);
}

template<>
inline __device__ DB::UInt64  atomicCAS<DB::UInt64>(DB::UInt64* address, DB::UInt64 compare, DB::UInt64 val)
{
    return ::atomicCAS((unsigned long long int*)address, (unsigned long long int)compare, (unsigned long long int)val);
}

template<typename T>
inline __device__ T    atomicAdd(T* address, T val)
{
    static_assert(sizeof(T) == 0, "CUDA atomicAdd not supported type");
}

/// Taken from nvidia forums.
/// WARNING it does not properly handle overflows
template<>
inline __device__ DB::UInt16  atomicAdd<DB::UInt16>(DB::UInt16* address, DB::UInt16 val)
{
    unsigned int *base_address = (unsigned int *)((size_t)address & ~2);
    unsigned int long_val = ((size_t)address & 2) ? ((unsigned int)val << 16) : val;

    unsigned int long_old = ::atomicAdd(base_address, long_val);

    return ((size_t)address & 2) ? (unsigned short)(long_old >> 16) : (unsigned short)(long_old & 0xffff);
}

template<>
inline __device__ DB::UInt32  atomicAdd<DB::UInt32>(DB::UInt32* address, DB::UInt32 val)
{
    return ::atomicAdd((unsigned int*)address, (unsigned int)val);
}

template<>
inline __device__ DB::UInt64  atomicAdd<DB::UInt64>(DB::UInt64* address, DB::UInt64 val)
{
    return ::atomicAdd((unsigned long long int*)address, (unsigned long long int)val);
}

template<>
inline __device__ double  atomicAdd<double>(double* address, double val)
{
    return ::atomicAdd(address, val);
}

template<typename T>
inline __device__ T    atomicSub(T* address, T val)
{
    static_assert(sizeof(T) == 0, "CUDA atomicSub not supported type");
}

/// Taken from nvidia forums.
/// WARNING it does not properly handle overflows
template<>
inline __device__ DB::UInt16  atomicSub<DB::UInt16>(DB::UInt16* address, DB::UInt16 val)
{
    unsigned int *base_address = (unsigned int *)((size_t)address & ~2);
    unsigned int long_val = ((size_t)address & 2) ? ((unsigned int)val << 16) : val;

    unsigned int long_old = ::atomicSub(base_address, long_val);

    return ((size_t)address & 2) ? (unsigned short)(long_old >> 16) : (unsigned short)(long_old & 0xffff);
}

template<>
inline __device__ DB::UInt32  atomicSub<DB::UInt32>(DB::UInt32* address, DB::UInt32 val)
{
    return ::atomicSub((unsigned int*)address, (unsigned int)val);
}

template<typename T>
inline __device__ T    atomicMax(T* address, T val)
{
    static_assert(sizeof(T) == 0, "CUDA atomicMax not supported type");
}

/// taken from nvidia forum
template<>
inline __device__ DB::UInt8   atomicMax<DB::UInt8>(DB::UInt8* address, DB::UInt8 val)
{
    unsigned int *base_address = (unsigned int *)((size_t)address & ~3);
    unsigned int selectors[] = {0x3214, 0x3240, 0x3410, 0x4210};
    unsigned int sel = selectors[(size_t)address & 3];
    unsigned int old, assumed, min_, new_;

    old = *base_address;

    do {
        assumed = old;
        min_ = max(val, (DB::UInt8)__byte_perm(old, 0, ((size_t)address & 3) | 0x4440));
        new_ = __byte_perm(old, min_, sel);
        if (new_ == old)
            break;
        old = atomicCAS(base_address, assumed, new_);
    } while (assumed != old);

    return (DB::UInt8)__byte_perm(old, 0, ((size_t)address & 3) | 0x4440);
}

}
