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
 * Static architectural properties by SM version.
 */

#pragma once

#include "util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {

#ifndef DOXYGEN_SHOULD_SKIP_THIS    // Do not document

#if (__CUDACC_VER_MAJOR__ >= 9) && !defined(CUB_USE_COOPERATIVE_GROUPS)
    #define CUB_USE_COOPERATIVE_GROUPS
#endif

/// CUB_PTX_ARCH reflects the PTX version targeted by the active compiler pass (or zero during the host pass).
#ifndef CUB_PTX_ARCH
    #ifndef __CUDA_ARCH__
        #define CUB_PTX_ARCH 0
    #else
        #define CUB_PTX_ARCH __CUDA_ARCH__
    #endif
#endif


/// Whether or not the source targeted by the active compiler pass is allowed to  invoke device kernels or methods from the CUDA runtime API.
#ifndef CUB_RUNTIME_FUNCTION
    #if !defined(__CUDA_ARCH__) || (__CUDA_ARCH__>= 350 && defined(__CUDACC_RDC__))
        #define CUB_RUNTIME_ENABLED
        #define CUB_RUNTIME_FUNCTION __host__ __device__
    #else
        #define CUB_RUNTIME_FUNCTION __host__
    #endif
#endif


/// Number of threads per warp
#ifndef CUB_LOG_WARP_THREADS
    #define CUB_LOG_WARP_THREADS(arch)                      \
        (5)
    #define CUB_WARP_THREADS(arch)                          \
        (1 << CUB_LOG_WARP_THREADS(arch))

    #define CUB_PTX_WARP_THREADS        CUB_WARP_THREADS(CUB_PTX_ARCH)
    #define CUB_PTX_LOG_WARP_THREADS    CUB_LOG_WARP_THREADS(CUB_PTX_ARCH)
#endif


/// Number of smem banks
#ifndef CUB_LOG_SMEM_BANKS
    #define CUB_LOG_SMEM_BANKS(arch)                        \
        ((arch >= 200) ?                                    \
            (5) :                                           \
            (4))
    #define CUB_SMEM_BANKS(arch)                            \
        (1 << CUB_LOG_SMEM_BANKS(arch))

    #define CUB_PTX_LOG_SMEM_BANKS      CUB_LOG_SMEM_BANKS(CUB_PTX_ARCH)
    #define CUB_PTX_SMEM_BANKS          CUB_SMEM_BANKS(CUB_PTX_ARCH)
#endif


/// Oversubscription factor
#ifndef CUB_SUBSCRIPTION_FACTOR
    #define CUB_SUBSCRIPTION_FACTOR(arch)                   \
        ((arch >= 300) ?                                    \
            (5) :                                           \
            ((arch >= 200) ?                                \
                (3) :                                       \
                (10)))
    #define CUB_PTX_SUBSCRIPTION_FACTOR             CUB_SUBSCRIPTION_FACTOR(CUB_PTX_ARCH)
#endif


/// Prefer padding overhead vs X-way conflicts greater than this threshold
#ifndef CUB_PREFER_CONFLICT_OVER_PADDING
    #define CUB_PREFER_CONFLICT_OVER_PADDING(arch)          \
        ((arch >= 300) ?                                    \
            (1) :                                           \
            (4))
    #define CUB_PTX_PREFER_CONFLICT_OVER_PADDING    CUB_PREFER_CONFLICT_OVER_PADDING(CUB_PTX_ARCH)
#endif


/// Scale down the number of warps to keep same amount of "tile" storage as the nominal configuration for 4B data.  Minimum of two warps.
#ifndef CUB_BLOCK_THREADS
    #define CUB_BLOCK_THREADS(NOMINAL_4B_BLOCK_THREADS, T, PTX_ARCH)                        \
        (CUB_MIN(                                                                           \
            NOMINAL_4B_BLOCK_THREADS * 2,                                                   \
            CUB_WARP_THREADS(PTX_ARCH) * CUB_MAX(                                           \
                (NOMINAL_4B_BLOCK_THREADS / CUB_WARP_THREADS(PTX_ARCH)) * 3 / 4,            \
                (NOMINAL_4B_BLOCK_THREADS / CUB_WARP_THREADS(PTX_ARCH)) * 4 / sizeof(T))))
#endif

/// Scale up/down number of items per thread to keep the same amount of "tile" storage as the nominal configuration for 4B data.  Minimum 1 item per thread
#ifndef CUB_ITEMS_PER_THREAD
    #define CUB_ITEMS_PER_THREAD(NOMINAL_4B_ITEMS_PER_THREAD, NOMINAL_4B_BLOCK_THREADS, T, PTX_ARCH)    \
	    (CUB_MIN(                                                                                       \
	        NOMINAL_4B_ITEMS_PER_THREAD * 2,                                                            \
	        CUB_MAX(                                                                                    \
	            1,                                                                                      \
	            (NOMINAL_4B_ITEMS_PER_THREAD * NOMINAL_4B_BLOCK_THREADS * 4 / sizeof(T)) / CUB_BLOCK_THREADS(NOMINAL_4B_BLOCK_THREADS, T, PTX_ARCH))))
#endif

/// Define both nominal threads-per-block and items-per-thread
#ifndef CUB_NOMINAL_CONFIG
    #define CUB_NOMINAL_CONFIG(NOMINAL_4B_BLOCK_THREADS, NOMINAL_4B_ITEMS_PER_THREAD, T)    \
        CUB_BLOCK_THREADS(NOMINAL_4B_BLOCK_THREADS, T, 200),                                \
        CUB_ITEMS_PER_THREAD(NOMINAL_4B_ITEMS_PER_THREAD, NOMINAL_4B_BLOCK_THREADS, T, 200)
#endif



#endif  // Do not document

}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)
