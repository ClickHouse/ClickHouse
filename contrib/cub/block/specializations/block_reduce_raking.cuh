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
 * cub::BlockReduceRaking provides raking-based methods of parallel reduction across a CUDA thread block.  Supports non-commutative reduction operators.
 */

#pragma once

#include "../../block/block_raking_layout.cuh"
#include "../../warp/warp_reduce.cuh"
#include "../../thread/thread_reduce.cuh"
#include "../../util_ptx.cuh"
#include "../../util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {


/**
 * \brief BlockReduceRaking provides raking-based methods of parallel reduction across a CUDA thread block.  Supports non-commutative reduction operators.
 *
 * Supports non-commutative binary reduction operators.  Unlike commutative
 * reduction operators (e.g., addition), the application of a non-commutative
 * reduction operator (e.g, string concatenation) across a sequence of inputs must
 * honor the relative ordering of items and partial reductions when applying the
 * reduction operator.
 *
 * Compared to the implementation of BlockReduceRaking (which does not support
 * non-commutative operators), this implementation requires a few extra
 * rounds of inter-thread communication.
 */
template <
    typename    T,              ///< Data type being reduced
    int         BLOCK_DIM_X,    ///< The thread block length in threads along the X dimension
    int         BLOCK_DIM_Y,    ///< The thread block length in threads along the Y dimension
    int         BLOCK_DIM_Z,    ///< The thread block length in threads along the Z dimension
    int         PTX_ARCH>       ///< The PTX compute capability for which to to specialize this collective
struct BlockReduceRaking
{
    /// Constants
    enum
    {
        /// The thread block size in threads
        BLOCK_THREADS = BLOCK_DIM_X * BLOCK_DIM_Y * BLOCK_DIM_Z,
    };

    /// Layout type for padded thread block raking grid
    typedef BlockRakingLayout<T, BLOCK_THREADS, PTX_ARCH> BlockRakingLayout;

    ///  WarpReduce utility type
    typedef typename WarpReduce<T, BlockRakingLayout::RAKING_THREADS, PTX_ARCH>::InternalWarpReduce WarpReduce;

    /// Constants
    enum
    {
        /// Number of raking threads
        RAKING_THREADS = BlockRakingLayout::RAKING_THREADS,

        /// Number of raking elements per warp synchronous raking thread
        SEGMENT_LENGTH = BlockRakingLayout::SEGMENT_LENGTH,

        /// Cooperative work can be entirely warp synchronous
        WARP_SYNCHRONOUS = (RAKING_THREADS == BLOCK_THREADS),

        /// Whether or not warp-synchronous reduction should be unguarded (i.e., the warp-reduction elements is a power of two
        WARP_SYNCHRONOUS_UNGUARDED = PowerOfTwo<RAKING_THREADS>::VALUE,

        /// Whether or not accesses into smem are unguarded
        RAKING_UNGUARDED = BlockRakingLayout::UNGUARDED,

    };


    /// Shared memory storage layout type
    union _TempStorage
    {
        typename WarpReduce::TempStorage            warp_storage;        ///< Storage for warp-synchronous reduction
        typename BlockRakingLayout::TempStorage     raking_grid;         ///< Padded thread block raking grid
    };


    /// Alias wrapper allowing storage to be unioned
    struct TempStorage : Uninitialized<_TempStorage> {};


    // Thread fields
    _TempStorage &temp_storage;
    unsigned int linear_tid;


    /// Constructor
    __device__ __forceinline__ BlockReduceRaking(
        TempStorage &temp_storage)
    :
        temp_storage(temp_storage.Alias()),
        linear_tid(RowMajorTid(BLOCK_DIM_X, BLOCK_DIM_Y, BLOCK_DIM_Z))
    {}


    template <bool IS_FULL_TILE, typename ReductionOp, int ITERATION>
    __device__ __forceinline__ T RakingReduction(
        ReductionOp                 reduction_op,       ///< [in] Binary scan operator
        T                           *raking_segment,
        T                           partial,            ///< [in] <b>[<em>lane</em><sub>0</sub> only]</b> Warp-wide aggregate reduction of input items
        int                         num_valid,          ///< [in] Number of valid elements (may be less than BLOCK_THREADS)
        Int2Type<ITERATION>         /*iteration*/)
    {
        // Update partial if addend is in range
        if ((IS_FULL_TILE && RAKING_UNGUARDED) || ((linear_tid * SEGMENT_LENGTH) + ITERATION < num_valid))
        {
            T addend = raking_segment[ITERATION];
            partial = reduction_op(partial, addend);
        }
        return RakingReduction<IS_FULL_TILE>(reduction_op, raking_segment, partial, num_valid, Int2Type<ITERATION + 1>());
    }

    template <bool IS_FULL_TILE, typename ReductionOp>
    __device__ __forceinline__ T RakingReduction(
        ReductionOp                 /*reduction_op*/,   ///< [in] Binary scan operator
        T                           * /*raking_segment*/,
        T                           partial,            ///< [in] <b>[<em>lane</em><sub>0</sub> only]</b> Warp-wide aggregate reduction of input items
        int                         /*num_valid*/,      ///< [in] Number of valid elements (may be less than BLOCK_THREADS)
        Int2Type<SEGMENT_LENGTH>    /*iteration*/)
    {
        return partial;
    }



    /// Computes a thread block-wide reduction using the specified reduction operator. The first num_valid threads each contribute one reduction partial.  The return value is only valid for thread<sub>0</sub>.
    template <
        bool                IS_FULL_TILE,
        typename            ReductionOp>
    __device__ __forceinline__ T Reduce(
        T                   partial,            ///< [in] Calling thread's input partial reductions
        int                 num_valid,          ///< [in] Number of valid elements (may be less than BLOCK_THREADS)
        ReductionOp         reduction_op)       ///< [in] Binary reduction operator
    {
        if (WARP_SYNCHRONOUS)
        {
            // Short-circuit directly to warp synchronous reduction (unguarded if active threads is a power-of-two)
            partial = WarpReduce(temp_storage.warp_storage).template Reduce<IS_FULL_TILE, SEGMENT_LENGTH>(
                partial,
                num_valid,
                reduction_op);
        }
        else
        {
            // Place partial into shared memory grid.
            *BlockRakingLayout::PlacementPtr(temp_storage.raking_grid, linear_tid) = partial;

            CTA_SYNC();

            // Reduce parallelism to one warp
            if (linear_tid < RAKING_THREADS)
            {
                // Raking reduction in grid
                T *raking_segment = BlockRakingLayout::RakingPtr(temp_storage.raking_grid, linear_tid);
                partial = raking_segment[0];

                partial = RakingReduction<IS_FULL_TILE>(reduction_op, raking_segment, partial, num_valid, Int2Type<1>());

                partial = WarpReduce(temp_storage.warp_storage).template Reduce<IS_FULL_TILE && RAKING_UNGUARDED, SEGMENT_LENGTH>(
                    partial,
                    num_valid,
                    reduction_op);

            }
        }

        return partial;
    }


    /// Computes a thread block-wide reduction using addition (+) as the reduction operator. The first num_valid threads each contribute one reduction partial.  The return value is only valid for thread<sub>0</sub>.
    template <bool IS_FULL_TILE>
    __device__ __forceinline__ T Sum(
        T                   partial,            ///< [in] Calling thread's input partial reductions
        int                 num_valid)          ///< [in] Number of valid elements (may be less than BLOCK_THREADS)
    {
        cub::Sum reduction_op;

        return Reduce<IS_FULL_TILE>(partial, num_valid, reduction_op);
    }



};

}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)

