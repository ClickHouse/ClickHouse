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
 * cub::BlockScanWarpscans provides warpscan-based variants of parallel prefix scan across a CUDA thread block.
 */

#pragma once

#include "../../util_arch.cuh"
#include "../../util_ptx.cuh"
#include "../../warp/warp_scan.cuh"
#include "../../util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {

/**
 * \brief BlockScanWarpScans provides warpscan-based variants of parallel prefix scan across a CUDA thread block.
 */
template <
    typename    T,
    int         BLOCK_DIM_X,    ///< The thread block length in threads along the X dimension
    int         BLOCK_DIM_Y,    ///< The thread block length in threads along the Y dimension
    int         BLOCK_DIM_Z,    ///< The thread block length in threads along the Z dimension
    int         PTX_ARCH>       ///< The PTX compute capability for which to to specialize this collective
struct BlockScanWarpScans
{
    //---------------------------------------------------------------------
    // Types and constants
    //---------------------------------------------------------------------

    /// Constants
    enum
    {
        /// Number of warp threads
        WARP_THREADS = CUB_WARP_THREADS(PTX_ARCH),

        /// The thread block size in threads
        BLOCK_THREADS = BLOCK_DIM_X * BLOCK_DIM_Y * BLOCK_DIM_Z,

        /// Number of active warps
        WARPS = (BLOCK_THREADS + WARP_THREADS - 1) / WARP_THREADS,
    };

    ///  WarpScan utility type
    typedef WarpScan<T, WARP_THREADS, PTX_ARCH> WarpScanT;

    ///  WarpScan utility type
    typedef WarpScan<T, WARPS, PTX_ARCH> WarpAggregateScan;

    /// Shared memory storage layout type

    struct __align__(32) _TempStorage
    {
        T                               warp_aggregates[WARPS];
        typename WarpScanT::TempStorage warp_scan[WARPS];           ///< Buffer for warp-synchronous scans
        T                               block_prefix;               ///< Shared prefix for the entire thread block
    };


    /// Alias wrapper allowing storage to be unioned
    struct TempStorage : Uninitialized<_TempStorage> {};


    //---------------------------------------------------------------------
    // Per-thread fields
    //---------------------------------------------------------------------

    // Thread fields
    _TempStorage    &temp_storage;
    unsigned int    linear_tid;
    unsigned int    warp_id;
    unsigned int    lane_id;


    //---------------------------------------------------------------------
    // Constructors
    //---------------------------------------------------------------------

    /// Constructor
    __device__ __forceinline__ BlockScanWarpScans(
        TempStorage &temp_storage)
    :
        temp_storage(temp_storage.Alias()),
        linear_tid(RowMajorTid(BLOCK_DIM_X, BLOCK_DIM_Y, BLOCK_DIM_Z)),
        warp_id((WARPS == 1) ? 0 : linear_tid / WARP_THREADS),
        lane_id(LaneId())
    {}


    //---------------------------------------------------------------------
    // Utility methods
    //---------------------------------------------------------------------

    template <typename ScanOp, int WARP>
    __device__ __forceinline__ void ApplyWarpAggregates(
        T               &warp_prefix,           ///< [out] The calling thread's partial reduction
        ScanOp          scan_op,            ///< [in] Binary scan operator
        T               &block_aggregate,   ///< [out] Threadblock-wide aggregate reduction of input items
        Int2Type<WARP>  /*addend_warp*/)
    {
        if (warp_id == WARP)
            warp_prefix = block_aggregate;

        T addend = temp_storage.warp_aggregates[WARP];
        block_aggregate = scan_op(block_aggregate, addend);

        ApplyWarpAggregates(warp_prefix, scan_op, block_aggregate, Int2Type<WARP + 1>());
    }

    template <typename ScanOp>
    __device__ __forceinline__ void ApplyWarpAggregates(
        T               &/*warp_prefix*/,       ///< [out] The calling thread's partial reduction
        ScanOp          /*scan_op*/,            ///< [in] Binary scan operator
        T               &/*block_aggregate*/,   ///< [out] Threadblock-wide aggregate reduction of input items
        Int2Type<WARPS> /*addend_warp*/)
    {}


    /// Use the warp-wide aggregates to compute the calling warp's prefix.  Also returns block-wide aggregate in all threads.
    template <typename ScanOp>
    __device__ __forceinline__ T ComputeWarpPrefix(
        ScanOp          scan_op,            ///< [in] Binary scan operator
        T               warp_aggregate,     ///< [in] <b>[<em>lane</em><sub>WARP_THREADS - 1</sub> only]</b> Warp-wide aggregate reduction of input items
        T               &block_aggregate)   ///< [out] Threadblock-wide aggregate reduction of input items
    {
        // Last lane in each warp shares its warp-aggregate
        if (lane_id == WARP_THREADS - 1)
            temp_storage.warp_aggregates[warp_id] = warp_aggregate;

        CTA_SYNC();

        // Accumulate block aggregates and save the one that is our warp's prefix
        T warp_prefix;
        block_aggregate = temp_storage.warp_aggregates[0];

        // Use template unrolling (since the PTX backend can't handle unrolling it for SM1x)
        ApplyWarpAggregates(warp_prefix, scan_op, block_aggregate, Int2Type<1>());
/*
        #pragma unroll
        for (int WARP = 1; WARP < WARPS; ++WARP)
        {
            if (warp_id == WARP)
                warp_prefix = block_aggregate;

            T addend = temp_storage.warp_aggregates[WARP];
            block_aggregate = scan_op(block_aggregate, addend);
        }
*/

        return warp_prefix;
    }


    /// Use the warp-wide aggregates and initial-value to compute the calling warp's prefix.  Also returns block-wide aggregate in all threads.
    template <typename ScanOp>
    __device__ __forceinline__ T ComputeWarpPrefix(
        ScanOp          scan_op,            ///< [in] Binary scan operator
        T               warp_aggregate,     ///< [in] <b>[<em>lane</em><sub>WARP_THREADS - 1</sub> only]</b> Warp-wide aggregate reduction of input items
        T               &block_aggregate,   ///< [out] Threadblock-wide aggregate reduction of input items
        const T         &initial_value)     ///< [in] Initial value to seed the exclusive scan
    {
        T warp_prefix = ComputeWarpPrefix(scan_op, warp_aggregate, block_aggregate);

        warp_prefix = scan_op(initial_value, warp_prefix);

        if (warp_id == 0)
            warp_prefix = initial_value;

        return warp_prefix;
    }

    //---------------------------------------------------------------------
    // Exclusive scans
    //---------------------------------------------------------------------

    /// Computes an exclusive thread block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes one input element.  With no initial value, the output computed for <em>thread</em><sub>0</sub> is undefined.
    template <typename ScanOp>
    __device__ __forceinline__ void ExclusiveScan(
        T               input,                          ///< [in] Calling thread's input item
        T               &exclusive_output,              ///< [out] Calling thread's output item (may be aliased to \p input)
        ScanOp          scan_op)                        ///< [in] Binary scan operator
    {
        // Compute block-wide exclusive scan.  The exclusive output from tid0 is invalid.
        T block_aggregate;
        ExclusiveScan(input, exclusive_output, scan_op, block_aggregate);
    }


    /// Computes an exclusive thread block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes one input element.
    template <typename ScanOp>
    __device__ __forceinline__ void ExclusiveScan(
        T               input,              ///< [in] Calling thread's input items
        T               &exclusive_output,  ///< [out] Calling thread's output items (may be aliased to \p input)
        const T         &initial_value,     ///< [in] Initial value to seed the exclusive scan
        ScanOp          scan_op)            ///< [in] Binary scan operator
    {
        T block_aggregate;
        ExclusiveScan(input, exclusive_output, initial_value, scan_op, block_aggregate);
    }


    /// Computes an exclusive thread block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes one input element.  Also provides every thread with the block-wide \p block_aggregate of all inputs.  With no initial value, the output computed for <em>thread</em><sub>0</sub> is undefined.
    template <typename ScanOp>
    __device__ __forceinline__ void ExclusiveScan(
        T               input,              ///< [in] Calling thread's input item
        T               &exclusive_output,  ///< [out] Calling thread's output item (may be aliased to \p input)
        ScanOp          scan_op,            ///< [in] Binary scan operator
        T               &block_aggregate)   ///< [out] Threadblock-wide aggregate reduction of input items
    {
        // Compute warp scan in each warp.  The exclusive output from each lane0 is invalid.
        T inclusive_output;
        WarpScanT(temp_storage.warp_scan[warp_id]).Scan(input, inclusive_output, exclusive_output, scan_op);

        // Compute the warp-wide prefix and block-wide aggregate for each warp.  Warp prefix for warp0 is invalid.
        T warp_prefix = ComputeWarpPrefix(scan_op, inclusive_output, block_aggregate);

        // Apply warp prefix to our lane's partial
        if (warp_id != 0)
        {
            exclusive_output = scan_op(warp_prefix, exclusive_output);
            if (lane_id == 0)
                exclusive_output = warp_prefix;
        }
    }


    /// Computes an exclusive thread block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes one input element.  Also provides every thread with the block-wide \p block_aggregate of all inputs.
    template <typename ScanOp>
    __device__ __forceinline__ void ExclusiveScan(
        T               input,              ///< [in] Calling thread's input items
        T               &exclusive_output,  ///< [out] Calling thread's output items (may be aliased to \p input)
        const T         &initial_value,     ///< [in] Initial value to seed the exclusive scan
        ScanOp          scan_op,            ///< [in] Binary scan operator
        T               &block_aggregate)   ///< [out] Threadblock-wide aggregate reduction of input items
    {
        // Compute warp scan in each warp.  The exclusive output from each lane0 is invalid.
        T inclusive_output;
        WarpScanT(temp_storage.warp_scan[warp_id]).Scan(input, inclusive_output, exclusive_output, scan_op);

        // Compute the warp-wide prefix and block-wide aggregate for each warp
        T warp_prefix = ComputeWarpPrefix(scan_op, inclusive_output, block_aggregate, initial_value);

        // Apply warp prefix to our lane's partial
        exclusive_output = scan_op(warp_prefix, exclusive_output);
        if (lane_id == 0)
            exclusive_output = warp_prefix;
    }


    /// Computes an exclusive thread block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes one input element.  the call-back functor \p block_prefix_callback_op is invoked by the first warp in the block, and the value returned by <em>lane</em><sub>0</sub> in that warp is used as the "seed" value that logically prefixes the thread block's scan inputs.  Also provides every thread with the block-wide \p block_aggregate of all inputs.
    template <
        typename ScanOp,
        typename BlockPrefixCallbackOp>
    __device__ __forceinline__ void ExclusiveScan(
        T                       input,                          ///< [in] Calling thread's input item
        T                       &exclusive_output,              ///< [out] Calling thread's output item (may be aliased to \p input)
        ScanOp                  scan_op,                        ///< [in] Binary scan operator
        BlockPrefixCallbackOp   &block_prefix_callback_op)      ///< [in-out] <b>[<em>warp</em><sub>0</sub> only]</b> Call-back functor for specifying a thread block-wide prefix to be applied to all inputs.
    {
        // Compute block-wide exclusive scan.  The exclusive output from tid0 is invalid.
        T block_aggregate;
        ExclusiveScan(input, exclusive_output, scan_op, block_aggregate);

        // Use the first warp to determine the thread block prefix, returning the result in lane0
        if (warp_id == 0)
        {
            T block_prefix = block_prefix_callback_op(block_aggregate);
            if (lane_id == 0)
            {
                // Share the prefix with all threads
                temp_storage.block_prefix = block_prefix;
                exclusive_output = block_prefix;                // The block prefix is the exclusive output for tid0
            }
        }

        CTA_SYNC();

        // Incorporate thread block prefix into outputs
        T block_prefix = temp_storage.block_prefix;
        if (linear_tid > 0)
        {
            exclusive_output = scan_op(block_prefix, exclusive_output);
        }
    }


    //---------------------------------------------------------------------
    // Inclusive scans
    //---------------------------------------------------------------------

    /// Computes an inclusive thread block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes one input element.
    template <typename ScanOp>
    __device__ __forceinline__ void InclusiveScan(
        T               input,                          ///< [in] Calling thread's input item
        T               &inclusive_output,              ///< [out] Calling thread's output item (may be aliased to \p input)
        ScanOp          scan_op)                        ///< [in] Binary scan operator
    {
        T block_aggregate;
        InclusiveScan(input, inclusive_output, scan_op, block_aggregate);
    }


    /// Computes an inclusive thread block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes one input element.  Also provides every thread with the block-wide \p block_aggregate of all inputs.
    template <typename ScanOp>
    __device__ __forceinline__ void InclusiveScan(
        T               input,                          ///< [in] Calling thread's input item
        T               &inclusive_output,              ///< [out] Calling thread's output item (may be aliased to \p input)
        ScanOp          scan_op,                        ///< [in] Binary scan operator
        T               &block_aggregate)               ///< [out] Threadblock-wide aggregate reduction of input items
    {
        WarpScanT(temp_storage.warp_scan[warp_id]).InclusiveScan(input, inclusive_output, scan_op);

        // Compute the warp-wide prefix and block-wide aggregate for each warp.  Warp prefix for warp0 is invalid.
        T warp_prefix = ComputeWarpPrefix(scan_op, inclusive_output, block_aggregate);

        // Apply warp prefix to our lane's partial
        if (warp_id != 0)
        {
            inclusive_output = scan_op(warp_prefix, inclusive_output);
        }
    }


    /// Computes an inclusive thread block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes one input element.  the call-back functor \p block_prefix_callback_op is invoked by the first warp in the block, and the value returned by <em>lane</em><sub>0</sub> in that warp is used as the "seed" value that logically prefixes the thread block's scan inputs.  Also provides every thread with the block-wide \p block_aggregate of all inputs.
    template <
        typename ScanOp,
        typename BlockPrefixCallbackOp>
    __device__ __forceinline__ void InclusiveScan(
        T                       input,                          ///< [in] Calling thread's input item
        T                       &exclusive_output,              ///< [out] Calling thread's output item (may be aliased to \p input)
        ScanOp                  scan_op,                        ///< [in] Binary scan operator
        BlockPrefixCallbackOp   &block_prefix_callback_op)      ///< [in-out] <b>[<em>warp</em><sub>0</sub> only]</b> Call-back functor for specifying a thread block-wide prefix to be applied to all inputs.
    {
        T block_aggregate;
        InclusiveScan(input, exclusive_output, scan_op, block_aggregate);

        // Use the first warp to determine the thread block prefix, returning the result in lane0
        if (warp_id == 0)
        {
            T block_prefix = block_prefix_callback_op(block_aggregate);
            if (lane_id == 0)
            {
                // Share the prefix with all threads
                temp_storage.block_prefix = block_prefix;
            }
        }

        CTA_SYNC();

        // Incorporate thread block prefix into outputs
        T block_prefix = temp_storage.block_prefix;
        exclusive_output = scan_op(block_prefix, exclusive_output);
    }


};


}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)

