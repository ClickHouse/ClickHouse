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
 * cub::WarpReduceSmem provides smem-based variants of parallel reduction of items partitioned across a CUDA thread warp.
 */

#pragma once

#include "../../thread/thread_operators.cuh"
#include "../../thread/thread_load.cuh"
#include "../../thread/thread_store.cuh"
#include "../../util_type.cuh"
#include "../../util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {

/**
 * \brief WarpReduceSmem provides smem-based variants of parallel reduction of items partitioned across a CUDA thread warp.
 */
template <
    typename    T,                      ///< Data type being reduced
    int         LOGICAL_WARP_THREADS,   ///< Number of threads per logical warp
    int         PTX_ARCH>               ///< The PTX compute capability for which to to specialize this collective
struct WarpReduceSmem
{
    /******************************************************************************
     * Constants and type definitions
     ******************************************************************************/

    enum
    {
        /// Whether the logical warp size and the PTX warp size coincide
        IS_ARCH_WARP = (LOGICAL_WARP_THREADS == CUB_WARP_THREADS(PTX_ARCH)),

        /// Whether the logical warp size is a power-of-two
        IS_POW_OF_TWO = PowerOfTwo<LOGICAL_WARP_THREADS>::VALUE,

        /// The number of warp scan steps
        STEPS = Log2<LOGICAL_WARP_THREADS>::VALUE,

        /// The number of threads in half a warp
        HALF_WARP_THREADS = 1 << (STEPS - 1),

        /// The number of shared memory elements per warp
        WARP_SMEM_ELEMENTS =  LOGICAL_WARP_THREADS + HALF_WARP_THREADS,

        /// FlagT status (when not using ballot)
        UNSET   = 0x0,  // Is initially unset
        SET     = 0x1,  // Is initially set
        SEEN    = 0x2,  // Has seen another head flag from a successor peer
    };

    /// Shared memory flag type
    typedef unsigned char SmemFlag;

    /// Shared memory storage layout type (1.5 warps-worth of elements for each warp)
    struct _TempStorage
    {
        T           reduce[WARP_SMEM_ELEMENTS];
        SmemFlag    flags[WARP_SMEM_ELEMENTS];
    };

    // Alias wrapper allowing storage to be unioned
    struct TempStorage : Uninitialized<_TempStorage> {};


    /******************************************************************************
     * Thread fields
     ******************************************************************************/

    _TempStorage    &temp_storage;
    unsigned int    lane_id;
    unsigned int    member_mask;


    /******************************************************************************
     * Construction
     ******************************************************************************/

    /// Constructor
    __device__ __forceinline__ WarpReduceSmem(
        TempStorage     &temp_storage)
    :
        temp_storage(temp_storage.Alias()),

        lane_id(IS_ARCH_WARP ?
            LaneId() :
            LaneId() % LOGICAL_WARP_THREADS),

        member_mask((0xffffffff >> (32 - LOGICAL_WARP_THREADS)) << ((IS_ARCH_WARP || !IS_POW_OF_TWO ) ?
            0 : // arch-width and non-power-of-two subwarps cannot be tiled with the arch-warp
            ((LaneId() / LOGICAL_WARP_THREADS) * LOGICAL_WARP_THREADS)))
    {}

    /******************************************************************************
     * Utility methods
     ******************************************************************************/

    //---------------------------------------------------------------------
    // Regular reduction
    //---------------------------------------------------------------------

    /**
     * Reduction step
     */
    template <
        bool                ALL_LANES_VALID,        ///< Whether all lanes in each warp are contributing a valid fold of items
        int                 FOLDED_ITEMS_PER_LANE,  ///< Number of items folded into each lane
        typename            ReductionOp,
        int                 STEP>
    __device__ __forceinline__ T ReduceStep(
        T                   input,                  ///< [in] Calling thread's input
        int                 folded_items_per_warp,  ///< [in] Total number of valid items folded into each logical warp
        ReductionOp         reduction_op,           ///< [in] Reduction operator
        Int2Type<STEP>      /*step*/)
    {
        const int OFFSET = 1 << STEP;

        // Share input through buffer
        ThreadStore<STORE_VOLATILE>(&temp_storage.reduce[lane_id], input);

        WARP_SYNC(member_mask);

        // Update input if peer_addend is in range
        if ((ALL_LANES_VALID && IS_POW_OF_TWO) || ((lane_id + OFFSET) * FOLDED_ITEMS_PER_LANE < folded_items_per_warp))
        {
            T peer_addend = ThreadLoad<LOAD_VOLATILE>(&temp_storage.reduce[lane_id + OFFSET]);
            input = reduction_op(input, peer_addend);
        }

        WARP_SYNC(member_mask);

        return ReduceStep<ALL_LANES_VALID, FOLDED_ITEMS_PER_LANE>(input, folded_items_per_warp, reduction_op, Int2Type<STEP + 1>());
    }


    /**
     * Reduction step (terminate)
     */
    template <
        bool                ALL_LANES_VALID,            ///< Whether all lanes in each warp are contributing a valid fold of items
        int                 FOLDED_ITEMS_PER_LANE,      ///< Number of items folded into each lane
        typename            ReductionOp>
    __device__ __forceinline__ T ReduceStep(
        T                   input,                      ///< [in] Calling thread's input
        int                 /*folded_items_per_warp*/,  ///< [in] Total number of valid items folded into each logical warp
        ReductionOp         /*reduction_op*/,           ///< [in] Reduction operator
        Int2Type<STEPS>     /*step*/)
    {
        return input;
    }


    //---------------------------------------------------------------------
    // Segmented reduction
    //---------------------------------------------------------------------


    /**
     * Ballot-based segmented reduce
     */
    template <
        bool            HEAD_SEGMENTED,     ///< Whether flags indicate a segment-head or a segment-tail
        typename        FlagT,
        typename        ReductionOp>
    __device__ __forceinline__ T SegmentedReduce(
        T               input,                  ///< [in] Calling thread's input
        FlagT           flag,                   ///< [in] Whether or not the current lane is a segment head/tail
        ReductionOp     reduction_op,           ///< [in] Reduction operator
        Int2Type<true>  /*has_ballot*/)         ///< [in] Marker type for whether the target arch has ballot functionality
    {
        // Get the start flags for each thread in the warp.
        int warp_flags = WARP_BALLOT(flag, member_mask);

        if (!HEAD_SEGMENTED)
            warp_flags <<= 1;

        // Keep bits above the current thread.
        warp_flags &= LaneMaskGt();

        // Accommodate packing of multiple logical warps in a single physical warp
        if (!IS_ARCH_WARP)
        {
            warp_flags >>= (LaneId() / LOGICAL_WARP_THREADS) * LOGICAL_WARP_THREADS;
        }

        // Find next flag
        int next_flag = __clz(__brev(warp_flags));

        // Clip the next segment at the warp boundary if necessary
        if (LOGICAL_WARP_THREADS != 32)
            next_flag = CUB_MIN(next_flag, LOGICAL_WARP_THREADS);

        #pragma unroll
        for (int STEP = 0; STEP < STEPS; STEP++)
        {
            const int OFFSET = 1 << STEP;

            // Share input into buffer
            ThreadStore<STORE_VOLATILE>(&temp_storage.reduce[lane_id], input);

            WARP_SYNC(member_mask);

            // Update input if peer_addend is in range
            if (OFFSET + lane_id < next_flag)
            {
                T peer_addend = ThreadLoad<LOAD_VOLATILE>(&temp_storage.reduce[lane_id + OFFSET]);
                input = reduction_op(input, peer_addend);
            }

            WARP_SYNC(member_mask);
        }

        return input;
    }


    /**
     * Smem-based segmented reduce
     */
    template <
        bool            HEAD_SEGMENTED,     ///< Whether flags indicate a segment-head or a segment-tail
        typename        FlagT,
        typename        ReductionOp>
    __device__ __forceinline__ T SegmentedReduce(
        T               input,                  ///< [in] Calling thread's input
        FlagT           flag,                   ///< [in] Whether or not the current lane is a segment head/tail
        ReductionOp     reduction_op,           ///< [in] Reduction operator
        Int2Type<false> /*has_ballot*/)         ///< [in] Marker type for whether the target arch has ballot functionality
    {
        enum
        {
            UNSET   = 0x0,  // Is initially unset
            SET     = 0x1,  // Is initially set
            SEEN    = 0x2,  // Has seen another head flag from a successor peer
        };

        // Alias flags onto shared data storage
        volatile SmemFlag *flag_storage = temp_storage.flags;

        SmemFlag flag_status = (flag) ? SET : UNSET;

        for (int STEP = 0; STEP < STEPS; STEP++)
        {
            const int OFFSET = 1 << STEP;

            // Share input through buffer
            ThreadStore<STORE_VOLATILE>(&temp_storage.reduce[lane_id], input);

            WARP_SYNC(member_mask);

            // Get peer from buffer
            T peer_addend = ThreadLoad<LOAD_VOLATILE>(&temp_storage.reduce[lane_id + OFFSET]);

            WARP_SYNC(member_mask);

            // Share flag through buffer
            flag_storage[lane_id] = flag_status;

            // Get peer flag from buffer
            SmemFlag peer_flag_status = flag_storage[lane_id + OFFSET];

            // Update input if peer was in range
            if (lane_id < LOGICAL_WARP_THREADS - OFFSET)
            {
                if (HEAD_SEGMENTED)
                {
                    // Head-segmented
                    if ((flag_status & SEEN) == 0)
                    {
                        // Has not seen a more distant head flag
                        if (peer_flag_status & SET)
                        {
                            // Has now seen a head flag
                            flag_status |= SEEN;
                        }
                        else
                        {
                            // Peer is not a head flag: grab its count
                            input = reduction_op(input, peer_addend);
                        }

                        // Update seen status to include that of peer
                        flag_status |= (peer_flag_status & SEEN);
                    }
                }
                else
                {
                    // Tail-segmented.  Simply propagate flag status
                    if (!flag_status)
                    {
                        input = reduction_op(input, peer_addend);
                        flag_status |= peer_flag_status;
                    }

                }
            }
        }

        return input;
    }


    /******************************************************************************
     * Interface
     ******************************************************************************/

    /**
     * Reduction
     */
    template <
        bool                ALL_LANES_VALID,        ///< Whether all lanes in each warp are contributing a valid fold of items
        int                 FOLDED_ITEMS_PER_LANE,  ///< Number of items folded into each lane
        typename            ReductionOp>
    __device__ __forceinline__ T Reduce(
        T                   input,                  ///< [in] Calling thread's input
        int                 folded_items_per_warp,  ///< [in] Total number of valid items folded into each logical warp
        ReductionOp         reduction_op)           ///< [in] Reduction operator
    {
        return ReduceStep<ALL_LANES_VALID, FOLDED_ITEMS_PER_LANE>(input, folded_items_per_warp, reduction_op, Int2Type<0>());
    }


    /**
     * Segmented reduction
     */
    template <
        bool            HEAD_SEGMENTED,     ///< Whether flags indicate a segment-head or a segment-tail
        typename        FlagT,
        typename        ReductionOp>
    __device__ __forceinline__ T SegmentedReduce(
        T               input,              ///< [in] Calling thread's input
        FlagT            flag,               ///< [in] Whether or not the current lane is a segment head/tail
        ReductionOp     reduction_op)       ///< [in] Reduction operator
    {
        return SegmentedReduce<HEAD_SEGMENTED>(input, flag, reduction_op, Int2Type<(PTX_ARCH >= 200)>());
    }


};


}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)
