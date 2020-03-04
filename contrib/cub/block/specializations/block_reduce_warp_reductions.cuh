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
 * cub::BlockReduceWarpReductions provides variants of warp-reduction-based parallel reduction across a CUDA thread block.  Supports non-commutative reduction operators.
 */

#pragma once

#include "../../warp/warp_reduce.cuh"
#include "../../util_ptx.cuh"
#include "../../util_arch.cuh"
#include "../../util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {


/**
 * \brief BlockReduceWarpReductions provides variants of warp-reduction-based parallel reduction across a CUDA thread block.  Supports non-commutative reduction operators.
 */
template <
    typename    T,              ///< Data type being reduced
    int         BLOCK_DIM_X,    ///< The thread block length in threads along the X dimension
    int         BLOCK_DIM_Y,    ///< The thread block length in threads along the Y dimension
    int         BLOCK_DIM_Z,    ///< The thread block length in threads along the Z dimension
    int         PTX_ARCH>       ///< The PTX compute capability for which to to specialize this collective
struct BlockReduceWarpReductions
{
    /// Constants
    enum
    {
        /// The thread block size in threads
        BLOCK_THREADS = BLOCK_DIM_X * BLOCK_DIM_Y * BLOCK_DIM_Z,

        /// Number of warp threads
        WARP_THREADS = CUB_WARP_THREADS(PTX_ARCH),

        /// Number of active warps
        WARPS = (BLOCK_THREADS + WARP_THREADS - 1) / WARP_THREADS,

        /// The logical warp size for warp reductions
        LOGICAL_WARP_SIZE = CUB_MIN(BLOCK_THREADS, WARP_THREADS),

        /// Whether or not the logical warp size evenly divides the thread block size
        EVEN_WARP_MULTIPLE = (BLOCK_THREADS % LOGICAL_WARP_SIZE == 0)
    };


    ///  WarpReduce utility type
    typedef typename WarpReduce<T, LOGICAL_WARP_SIZE, PTX_ARCH>::InternalWarpReduce WarpReduce;


    /// Shared memory storage layout type
    struct _TempStorage
    {
        typename WarpReduce::TempStorage    warp_reduce[WARPS];                ///< Buffer for warp-synchronous scan
        T                                   warp_aggregates[WARPS];     ///< Shared totals from each warp-synchronous scan
        T                                   block_prefix;               ///< Shared prefix for the entire thread block
    };

    /// Alias wrapper allowing storage to be unioned
    struct TempStorage : Uninitialized<_TempStorage> {};


    // Thread fields
    _TempStorage &temp_storage;
    unsigned int linear_tid;
    unsigned int warp_id;
    unsigned int lane_id;


    /// Constructor
    __device__ __forceinline__ BlockReduceWarpReductions(
        TempStorage &temp_storage)
    :
        temp_storage(temp_storage.Alias()),
        linear_tid(RowMajorTid(BLOCK_DIM_X, BLOCK_DIM_Y, BLOCK_DIM_Z)),
        warp_id((WARPS == 1) ? 0 : linear_tid / WARP_THREADS),
        lane_id(LaneId())
    {}


    template <bool FULL_TILE, typename ReductionOp, int SUCCESSOR_WARP>
    __device__ __forceinline__ T ApplyWarpAggregates(
        ReductionOp                 reduction_op,       ///< [in] Binary scan operator
        T                           warp_aggregate,     ///< [in] <b>[<em>lane</em><sub>0</sub> only]</b> Warp-wide aggregate reduction of input items
        int                         num_valid,          ///< [in] Number of valid elements (may be less than BLOCK_THREADS)
        Int2Type<SUCCESSOR_WARP>    /*successor_warp*/)
    {
        if (FULL_TILE || (SUCCESSOR_WARP * LOGICAL_WARP_SIZE < num_valid))
        {
            T addend = temp_storage.warp_aggregates[SUCCESSOR_WARP];
            warp_aggregate = reduction_op(warp_aggregate, addend);
        }
        return ApplyWarpAggregates<FULL_TILE>(reduction_op, warp_aggregate, num_valid, Int2Type<SUCCESSOR_WARP + 1>());
    }

    template <bool FULL_TILE, typename ReductionOp>
    __device__ __forceinline__ T ApplyWarpAggregates(
        ReductionOp         /*reduction_op*/,   ///< [in] Binary scan operator
        T                   warp_aggregate,     ///< [in] <b>[<em>lane</em><sub>0</sub> only]</b> Warp-wide aggregate reduction of input items
        int                 /*num_valid*/,      ///< [in] Number of valid elements (may be less than BLOCK_THREADS)
        Int2Type<WARPS>     /*successor_warp*/)
    {
        return warp_aggregate;
    }


    /// Returns block-wide aggregate in <em>thread</em><sub>0</sub>.
    template <
        bool                FULL_TILE,
        typename            ReductionOp>
    __device__ __forceinline__ T ApplyWarpAggregates(
        ReductionOp         reduction_op,       ///< [in] Binary scan operator
        T                   warp_aggregate,     ///< [in] <b>[<em>lane</em><sub>0</sub> only]</b> Warp-wide aggregate reduction of input items
        int                 num_valid)          ///< [in] Number of valid elements (may be less than BLOCK_THREADS)
    {
        // Share lane aggregates
        if (lane_id == 0)
        {
            temp_storage.warp_aggregates[warp_id] = warp_aggregate;
        }

        CTA_SYNC();

        // Update total aggregate in warp 0, lane 0
        if (linear_tid == 0)
        {
            warp_aggregate = ApplyWarpAggregates<FULL_TILE>(reduction_op, warp_aggregate, num_valid, Int2Type<1>());
        }

        return warp_aggregate;
    }


    /// Computes a thread block-wide reduction using addition (+) as the reduction operator. The first num_valid threads each contribute one reduction partial.  The return value is only valid for thread<sub>0</sub>.
    template <bool FULL_TILE>
    __device__ __forceinline__ T Sum(
        T                   input,          ///< [in] Calling thread's input partial reductions
        int                 num_valid)      ///< [in] Number of valid elements (may be less than BLOCK_THREADS)
    {
        cub::Sum        reduction_op;
        unsigned int    warp_offset = warp_id * LOGICAL_WARP_SIZE;
        unsigned int    warp_num_valid = (FULL_TILE && EVEN_WARP_MULTIPLE) ?
                            LOGICAL_WARP_SIZE :
                            (warp_offset < num_valid) ?
                                num_valid - warp_offset :
                                0;

        // Warp reduction in every warp
        T warp_aggregate = WarpReduce(temp_storage.warp_reduce[warp_id]).template Reduce<(FULL_TILE && EVEN_WARP_MULTIPLE), 1>(
            input,
            warp_num_valid,
            cub::Sum());

        // Update outputs and block_aggregate with warp-wide aggregates from lane-0s
        return ApplyWarpAggregates<FULL_TILE>(reduction_op, warp_aggregate, num_valid);
    }


    /// Computes a thread block-wide reduction using the specified reduction operator. The first num_valid threads each contribute one reduction partial.  The return value is only valid for thread<sub>0</sub>.
    template <
        bool                FULL_TILE,
        typename            ReductionOp>
    __device__ __forceinline__ T Reduce(
        T                   input,              ///< [in] Calling thread's input partial reductions
        int                 num_valid,          ///< [in] Number of valid elements (may be less than BLOCK_THREADS)
        ReductionOp         reduction_op)       ///< [in] Binary reduction operator
    {
        unsigned int    warp_offset = warp_id * LOGICAL_WARP_SIZE;
        unsigned int    warp_num_valid = (FULL_TILE && EVEN_WARP_MULTIPLE) ?
                            LOGICAL_WARP_SIZE :
                            (warp_offset < static_cast<unsigned int>(num_valid)) ?
                                num_valid - warp_offset :
                                0;

        // Warp reduction in every warp
        T warp_aggregate = WarpReduce(temp_storage.warp_reduce[warp_id]).template Reduce<(FULL_TILE && EVEN_WARP_MULTIPLE), 1>(
            input,
            warp_num_valid,
            reduction_op);

        // Update outputs and block_aggregate with warp-wide aggregates from lane-0s
        return ApplyWarpAggregates<FULL_TILE>(reduction_op, warp_aggregate, num_valid);
    }

};


}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)

