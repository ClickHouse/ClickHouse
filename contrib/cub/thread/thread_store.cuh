/******************************************************************************
 * Copyright (c) 2011, Duane Merrill.  All rights reserved.
 * Copyright (c) 2011-2017, NVIDIA CORPORATION.  All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the NVIDIA CORPORATION nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL NVIDIA CORPORATION BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 ******************************************************************************/

/**
 * \file
 * Thread utilities for writing memory using PTX cache modifiers.
 */

#pragma once

#include <cuda.h>

#include "../util_ptx.cuh"
#include "../util_type.cuh"
#include "../util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {

/**
 * \addtogroup UtilIo
 * @{
 */


//-----------------------------------------------------------------------------
// Tags and constants
//-----------------------------------------------------------------------------

/**
 * \brief Enumeration of cache modifiers for memory store operations.
 */
enum CacheStoreModifier
{
    STORE_DEFAULT,              ///< Default (no modifier)
    STORE_WB,                   ///< Cache write-back all coherent levels
    STORE_CG,                   ///< Cache at global level
    STORE_CS,                   ///< Cache streaming (likely to be accessed once)
    STORE_WT,                   ///< Cache write-through (to system memory)
    STORE_VOLATILE,             ///< Volatile shared (any memory space)
};


/**
 * \name Thread I/O (cache modified)
 * @{
 */

/**
 * \brief Thread utility for writing memory using cub::CacheStoreModifier cache modifiers.  Can be used to store any data type.
 *
 * \par Example
 * \code
 * #include <cub/cub.cuh>   // or equivalently <cub/thread/thread_store.cuh>
 *
 * // 32-bit store using cache-global modifier:
 * int *d_out;
 * int val;
 * cub::ThreadStore<cub::STORE_CG>(d_out + threadIdx.x, val);
 *
 * // 16-bit store using default modifier
 * short *d_out;
 * short val;
 * cub::ThreadStore<cub::STORE_DEFAULT>(d_out + threadIdx.x, val);
 *
 * // 256-bit store using write-through modifier
 * double4 *d_out;
 * double4 val;
 * cub::ThreadStore<cub::STORE_WT>(d_out + threadIdx.x, val);
 *
 * // 96-bit store using cache-streaming cache modifier
 * struct TestFoo { bool a; short b; };
 * TestFoo *d_struct;
 * TestFoo val;
 * cub::ThreadStore<cub::STORE_CS>(d_out + threadIdx.x, val);
 * \endcode
 *
 * \tparam MODIFIER             <b>[inferred]</b> CacheStoreModifier enumeration
 * \tparam InputIteratorT       <b>[inferred]</b> Output iterator type \iterator
 * \tparam T                    <b>[inferred]</b> Data type of output value
 */
template <
    CacheStoreModifier  MODIFIER,
    typename            OutputIteratorT,
    typename            T>
__device__ __forceinline__ void ThreadStore(OutputIteratorT itr, T val);


//@}  end member group


#ifndef DOXYGEN_SHOULD_SKIP_THIS    // Do not document


/// Helper structure for templated store iteration (inductive case)
template <int COUNT, int MAX>
struct IterateThreadStore
{
    template <CacheStoreModifier MODIFIER, typename T>
    static __device__ __forceinline__ void Store(T *ptr, T *vals)
    {
        ThreadStore<MODIFIER>(ptr + COUNT, vals[COUNT]);
        IterateThreadStore<COUNT + 1, MAX>::template Store<MODIFIER>(ptr, vals);
    }

    template <typename OutputIteratorT, typename T>
    static __device__ __forceinline__ void Dereference(OutputIteratorT ptr, T *vals)
    {
        ptr[COUNT] = vals[COUNT];
        IterateThreadStore<COUNT + 1, MAX>::Dereference(ptr, vals);
    }

};

/// Helper structure for templated store iteration (termination case)
template <int MAX>
struct IterateThreadStore<MAX, MAX>
{
    template <CacheStoreModifier MODIFIER, typename T>
    static __device__ __forceinline__ void Store(T * /*ptr*/, T * /*vals*/) {}

    template <typename OutputIteratorT, typename T>
    static __device__ __forceinline__ void Dereference(OutputIteratorT /*ptr*/, T * /*vals*/) {}
};


/**
 * Define a uint4 (16B) ThreadStore specialization for the given Cache load modifier
 */
#define _CUB_STORE_16(cub_modifier, ptx_modifier)                                            \
    template<>                                                                              \
    __device__ __forceinline__ void ThreadStore<cub_modifier, uint4*, uint4>(uint4* ptr, uint4 val)                         \
    {                                                                                       \
        asm volatile ("st."#ptx_modifier".v4.u32 [%0], {%1, %2, %3, %4};" : :               \
            _CUB_ASM_PTR_(ptr),                                                             \
            "r"(val.x),                                                                     \
            "r"(val.y),                                                                     \
            "r"(val.z),                                                                     \
            "r"(val.w));                                                                    \
    }                                                                                       \
    template<>                                                                              \
    __device__ __forceinline__ void ThreadStore<cub_modifier, ulonglong2*, ulonglong2>(ulonglong2* ptr, ulonglong2 val)     \
    {                                                                                       \
        asm volatile ("st."#ptx_modifier".v2.u64 [%0], {%1, %2};" : :                       \
            _CUB_ASM_PTR_(ptr),                                                             \
            "l"(val.x),                                                                     \
            "l"(val.y));                                                                    \
    }


/**
 * Define a uint2 (8B) ThreadStore specialization for the given Cache load modifier
 */
#define _CUB_STORE_8(cub_modifier, ptx_modifier)                                             \
    template<>                                                                              \
    __device__ __forceinline__ void ThreadStore<cub_modifier, ushort4*, ushort4>(ushort4* ptr, ushort4 val)                 \
    {                                                                                       \
        asm volatile ("st."#ptx_modifier".v4.u16 [%0], {%1, %2, %3, %4};" : :               \
            _CUB_ASM_PTR_(ptr),                                                             \
            "h"(val.x),                                                                     \
            "h"(val.y),                                                                     \
            "h"(val.z),                                                                     \
            "h"(val.w));                                                                    \
    }                                                                                       \
    template<>                                                                              \
    __device__ __forceinline__ void ThreadStore<cub_modifier, uint2*, uint2>(uint2* ptr, uint2 val)                         \
    {                                                                                       \
        asm volatile ("st."#ptx_modifier".v2.u32 [%0], {%1, %2};" : :                       \
            _CUB_ASM_PTR_(ptr),                                                             \
            "r"(val.x),                                                                     \
            "r"(val.y));                                                                    \
    }                                                                                       \
    template<>                                                                              \
    __device__ __forceinline__ void ThreadStore<cub_modifier, unsigned long long*, unsigned long long>(unsigned long long* ptr, unsigned long long val)     \
    {                                                                                       \
        asm volatile ("st."#ptx_modifier".u64 [%0], %1;" : :                                \
            _CUB_ASM_PTR_(ptr),                                                             \
            "l"(val));                                                                      \
    }

/**
 * Define a unsigned int (4B) ThreadStore specialization for the given Cache load modifier
 */
#define _CUB_STORE_4(cub_modifier, ptx_modifier)                                             \
    template<>                                                                              \
    __device__ __forceinline__ void ThreadStore<cub_modifier, unsigned int*, unsigned int>(unsigned int* ptr, unsigned int val)                             \
    {                                                                                       \
        asm volatile ("st."#ptx_modifier".u32 [%0], %1;" : :                                \
            _CUB_ASM_PTR_(ptr),                                                             \
            "r"(val));                                                                      \
    }


/**
 * Define a unsigned short (2B) ThreadStore specialization for the given Cache load modifier
 */
#define _CUB_STORE_2(cub_modifier, ptx_modifier)                                             \
    template<>                                                                              \
    __device__ __forceinline__ void ThreadStore<cub_modifier, unsigned short*, unsigned short>(unsigned short* ptr, unsigned short val)                     \
    {                                                                                       \
        asm volatile ("st."#ptx_modifier".u16 [%0], %1;" : :                                \
            _CUB_ASM_PTR_(ptr),                                                             \
            "h"(val));                                                                      \
    }


/**
 * Define a unsigned char (1B) ThreadStore specialization for the given Cache load modifier
 */
#define _CUB_STORE_1(cub_modifier, ptx_modifier)                                             \
    template<>                                                                              \
    __device__ __forceinline__ void ThreadStore<cub_modifier, unsigned char*, unsigned char>(unsigned char* ptr, unsigned char val)                         \
    {                                                                                       \
        asm volatile (                                                                      \
        "{"                                                                                 \
        "   .reg .u8 datum;"                                                                \
        "   cvt.u8.u16 datum, %1;"                                                          \
        "   st."#ptx_modifier".u8 [%0], datum;"                                             \
        "}" : :                                                                             \
            _CUB_ASM_PTR_(ptr),                                                             \
            "h"((unsigned short) val));                                                               \
    }

/**
 * Define powers-of-two ThreadStore specializations for the given Cache load modifier
 */
#define _CUB_STORE_ALL(cub_modifier, ptx_modifier)                                           \
    _CUB_STORE_16(cub_modifier, ptx_modifier)                                                \
    _CUB_STORE_8(cub_modifier, ptx_modifier)                                                 \
    _CUB_STORE_4(cub_modifier, ptx_modifier)                                                 \
    _CUB_STORE_2(cub_modifier, ptx_modifier)                                                 \
    _CUB_STORE_1(cub_modifier, ptx_modifier)                                                 \


/**
 * Define ThreadStore specializations for the various Cache load modifiers
 */
#if CUB_PTX_ARCH >= 200
    _CUB_STORE_ALL(STORE_WB, wb)
    _CUB_STORE_ALL(STORE_CG, cg)
    _CUB_STORE_ALL(STORE_CS, cs)
    _CUB_STORE_ALL(STORE_WT, wt)
#else
    _CUB_STORE_ALL(STORE_WB, global)
    _CUB_STORE_ALL(STORE_CG, global)
    _CUB_STORE_ALL(STORE_CS, global)
    _CUB_STORE_ALL(STORE_WT, volatile.global)
#endif


// Macro cleanup
#undef _CUB_STORE_ALL
#undef _CUB_STORE_1
#undef _CUB_STORE_2
#undef _CUB_STORE_4
#undef _CUB_STORE_8
#undef _CUB_STORE_16


/**
 * ThreadStore definition for STORE_DEFAULT modifier on iterator types
 */
template <typename OutputIteratorT, typename T>
__device__ __forceinline__ void ThreadStore(
    OutputIteratorT             itr,
    T                           val,
    Int2Type<STORE_DEFAULT>     /*modifier*/,
    Int2Type<false>             /*is_pointer*/)
{
    *itr = val;
}


/**
 * ThreadStore definition for STORE_DEFAULT modifier on pointer types
 */
template <typename T>
__device__ __forceinline__ void ThreadStore(
    T                           *ptr,
    T                           val,
    Int2Type<STORE_DEFAULT>     /*modifier*/,
    Int2Type<true>              /*is_pointer*/)
{
    *ptr = val;
}


/**
 * ThreadStore definition for STORE_VOLATILE modifier on primitive pointer types
 */
template <typename T>
__device__ __forceinline__ void ThreadStoreVolatilePtr(
    T                           *ptr,
    T                           val,
    Int2Type<true>              /*is_primitive*/)
{
    *reinterpret_cast<volatile T*>(ptr) = val;
}


/**
 * ThreadStore definition for STORE_VOLATILE modifier on non-primitive pointer types
 */
template <typename T>
__device__ __forceinline__ void ThreadStoreVolatilePtr(
    T                           *ptr,
    T                           val,
    Int2Type<false>             /*is_primitive*/)
{
    // Create a temporary using shuffle-words, then store using volatile-words
    typedef typename UnitWord<T>::VolatileWord  VolatileWord;  
    typedef typename UnitWord<T>::ShuffleWord   ShuffleWord;

    const int VOLATILE_MULTIPLE = sizeof(T) / sizeof(VolatileWord);
    const int SHUFFLE_MULTIPLE  = sizeof(T) / sizeof(ShuffleWord);
    
    VolatileWord words[VOLATILE_MULTIPLE];

    #pragma unroll
    for (int i = 0; i < SHUFFLE_MULTIPLE; ++i)
        reinterpret_cast<ShuffleWord*>(words)[i] = reinterpret_cast<ShuffleWord*>(&val)[i];

    IterateThreadStore<0, VOLATILE_MULTIPLE>::template Dereference(
        reinterpret_cast<volatile VolatileWord*>(ptr),
        words);
}


/**
 * ThreadStore definition for STORE_VOLATILE modifier on pointer types
 */
template <typename T>
__device__ __forceinline__ void ThreadStore(
    T                           *ptr,
    T                           val,
    Int2Type<STORE_VOLATILE>    /*modifier*/,
    Int2Type<true>              /*is_pointer*/)
{
    ThreadStoreVolatilePtr(ptr, val, Int2Type<Traits<T>::PRIMITIVE>());
}


/**
 * ThreadStore definition for generic modifiers on pointer types
 */
template <typename T, int MODIFIER>
__device__ __forceinline__ void ThreadStore(
    T                           *ptr,
    T                           val,
    Int2Type<MODIFIER>          /*modifier*/,
    Int2Type<true>              /*is_pointer*/)
{
    // Create a temporary using shuffle-words, then store using device-words
    typedef typename UnitWord<T>::DeviceWord    DeviceWord;  
    typedef typename UnitWord<T>::ShuffleWord   ShuffleWord;

    const int DEVICE_MULTIPLE   = sizeof(T) / sizeof(DeviceWord);
    const int SHUFFLE_MULTIPLE  = sizeof(T) / sizeof(ShuffleWord);
    
    DeviceWord words[DEVICE_MULTIPLE];

    #pragma unroll
    for (int i = 0; i < SHUFFLE_MULTIPLE; ++i)
        reinterpret_cast<ShuffleWord*>(words)[i] = reinterpret_cast<ShuffleWord*>(&val)[i];

    IterateThreadStore<0, DEVICE_MULTIPLE>::template Store<CacheStoreModifier(MODIFIER)>(
        reinterpret_cast<DeviceWord*>(ptr),
        words);
}


/**
 * ThreadStore definition for generic modifiers
 */
template <CacheStoreModifier MODIFIER, typename OutputIteratorT, typename T>
__device__ __forceinline__ void ThreadStore(OutputIteratorT itr, T val)
{
    ThreadStore(
        itr,
        val,
        Int2Type<MODIFIER>(),
        Int2Type<IsPointer<OutputIteratorT>::VALUE>());
}



#endif // DOXYGEN_SHOULD_SKIP_THIS


/** @} */       // end group UtilIo


}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)
