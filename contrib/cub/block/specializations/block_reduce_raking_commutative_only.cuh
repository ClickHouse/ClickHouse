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
 * cub::BlockReduceRakingCommutativeOnly provides raking-based methods of parallel reduction across a CUDA thread block.  Does not support non-commutative reduction operators.
 */

#pragma once

#include "block_reduce_raking.cuh"
#include "../../warp/warp_reduce.cuh"
#include "../../thread/thread_reduce.cuh"
#include "../../util_ptx.cuh"
#include "../../util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {


/**
 * \brief BlockReduceRakingCommutativeOnly provides raking-based methods of parallel reduction across a CUDA thread block.  Does not support non-commutative reduction operators.  Does not support block sizes that are not a multiple of the warp size.
 */
template <
    typename    T,              ///< Data type being reduced
    int         BLOCK_DIM_X,    ///< The thread block length in threads along the X dimension
    int         BLOCK_DIM_Y,    ///< The thread block length in threads along the Y dimension
    int         BLOCK_DIM_Z,    ///< The thread block length in threads along the Z dimension
    int         PTX_ARCH>       ///< The PTX compute capability for which to to specialize this collective
struct BlockReduceRakingCommutativeOnly
{
    /// Constants
    enum
    {
        /// The thread block size in threads
        BLOCK_THREADS = BLOCK_DIM_X * BLOCK_DIM_Y * BLOCK_DIM_Z,
    };

    // The fall-back implementation to use when BLOCK_THREADS is not a multiple of the warp size or not all threads have valid values
    typedef BlockReduceRaking<T, BLOCK_DIM_X, BLOCK_DIM_Y, BLOCK_DIM_Z, PTX_ARCH> FallBack;

    /// Constants
    enum
    {
        /// Number of warp threads
        WARP_THREADS = CUB_WARP_THREADS(PTX_ARCH),

        /// Whether or not to use fall-back
        USE_FALLBACK = ((BLOCK_THREADS % WARP_THREADS != 0) || (BLOCK_THREADS <= WARP_THREADS)),

        /// Number of raking threads
        RAKING_THREADS = WARP_THREADS,

        /// Number of threads actually sharing items with the raking threads
        SHARING_THREADS = CUB_MAX(1, BLOCK_THREADS - RAKING_THREADS),

        /// Number of raking elements per warp synchronous raking thread
        SEGMENT_LENGTH = SHARING_THREADS / WARP_THREADS,
    };

    ///  WarpReduce utility type
    typedef WarpReduce<T, RAKING_THREADS, PTX_ARCH> WarpReduce;

    /// Layout type for padded thread block raking grid
    typedef BlockRakingLayout<T, SHARING_THREADS, PTX_ARCH> BlockRakingLayout;

    /// Shared memory storage layout type
    union _TempStorage
    {
        struct
        {
            typename WarpReduce::TempStorage        warp_storage;        ///< Storage for warp-synchronous reduction
            typename BlockRakingLayout::TempStorage raking_grid;         ///< Padded thread block raking grid
        };
        typename FallBack::TempStorage              fallback_storage;    ///< Fall-back storage for non-commutative block scan
    };


    /// Alias wrapper allowing storage to be unioned
    struct TempStorage : Uninitialized<_TempStorage> {};


    // Thread fields
    _TempStorage &temp_storage;
    unsigned int linear_tid;


    /// Constructor
    __device__ __forceinline__ BlockReduceRakingCommutativeOnly(
        TempStorage &temp_storage)
    :
        temp_storage(temp_storage.Alias()),
        linear_tid(RowMajorTid(BLOCK_DIM_X, BLOCK_DIM_Y, BLOCK_DIM_Z))
    {}


    /// Computes a thread block-wide reduction using addition (+) as the reduction operator. The first num_valid threads each contribute one reduction partial.  The return value is only valid for thread<sub>0</sub>.
    template <bool FULL_TILE>
    __device__ __forceinline__ T Sum(
        T                   partial,            ///< [in] Calling thread's input partial reductions
        int                 num_valid)          ///< [in] Number of valid elements (may be less than BLOCK_THREADS)
    {
        if (USE_FALLBACK || !FULL_TILE)
        {
            return FallBack(temp_storage.fallback_storage).template Sum<FULL_TILE>(partial, num_valid);
        }
        else
        {
            // Place partial into shared memory grid
            if (linear_tid >= RAKING_THREADS)
                *BlockRakingLayout::PlacementPtr(temp_storage.raking_grid, linear_tid - RAKING_THREADS) = partial;

            CTA_SYNC();

            // Reduce parallelism to one warp
            if (linear_tid < RAKING_THREADS)
            {
                // Raking reduction in grid
                T *raking_segment = BlockRakingLayout::RakingPtr(temp_storage.raking_grid, linear_tid);
                partial = internal::ThreadReduce<SEGMENT_LENGTH>(raking_segment, cub::Sum(), partial);

                // Warpscan
                partial = WarpReduce(temp_storage.warp_storage).Sum(partial);
            }
        }

        return partial;
    }


    /// Computes a thread block-wide reduction using the specified reduction operator. The first num_valid threads each contribute one reduction partial.  The return value is only valid for thread<sub>0</sub>.
    template <
        bool                FULL_TILE,
        typename            ReductionOp>
    __device__ __forceinline__ T Reduce(
        T                   partial,            ///< [in] Calling thread's input partial reductions
        int                 num_valid,          ///< [in] Number of valid elements (may be less than BLOCK_THREADS)
        ReductionOp         reduction_op)       ///< [in] Binary reduction operator
    {
        if (USE_FALLBACK || !FULL_TILE)
        {
            return FallBack(temp_storage.fallback_storage).template Reduce<FULL_TILE>(partial, num_valid, reduction_op);
        }
        else
        {
            // Place partial into shared memory grid
            if (linear_tid >= RAKING_THREADS)
                *BlockRakingLayout::PlacementPtr(temp_storage.raking_grid, linear_tid - RAKING_THREADS) = partial;

            CTA_SYNC();

            // Reduce parallelism to one warp
            if (linear_tid < RAKING_THREADS)
            {
                // Raking reduction in grid
                T *raking_segment = BlockRakingLayout::RakingPtr(temp_storage.raking_grid, linear_tid);
                partial = internal::ThreadReduce<SEGMENT_LENGTH>(raking_segment, reduction_op, partial);

                // Warpscan
                partial = WarpReduce(temp_storage.warp_storage).Reduce(partial, reduction_op);
            }
        }

        return partial;
    }

};

}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)

