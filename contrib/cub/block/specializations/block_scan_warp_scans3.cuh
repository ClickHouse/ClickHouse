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
        /// The thread block size in threads
        BLOCK_THREADS = BLOCK_DIM_X * BLOCK_DIM_Y * BLOCK_DIM_Z,

        /// Number of warp threads
        INNER_WARP_THREADS = CUB_WARP_THREADS(PTX_ARCH),
        OUTER_WARP_THREADS = BLOCK_THREADS / INNER_WARP_THREADS,

        /// Number of outer scan warps
        OUTER_WARPS = INNER_WARP_THREADS
    };

    ///  Outer WarpScan utility type
    typedef WarpScan<T, OUTER_WARP_THREADS, PTX_ARCH> OuterWarpScanT;

    ///  Inner WarpScan utility type
    typedef WarpScan<T, INNER_WARP_THREADS, PTX_ARCH> InnerWarpScanT;

    typedef typename OuterWarpScanT::TempStorage OuterScanArray[OUTER_WARPS];


    /// Shared memory storage layout type
    struct _TempStorage
    {
        union Aliasable
        {
            Uninitialized<OuterScanArray>           outer_warp_scan;  ///< Buffer for warp-synchronous outer scans
            typename InnerWarpScanT::TempStorage    inner_warp_scan;  ///< Buffer for warp-synchronous inner scan

        } aliasable;

        T                               warp_aggregates[OUTER_WARPS];

        T                               block_aggregate;                           ///< Shared prefix for the entire thread block
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
        warp_id((OUTER_WARPS == 1) ? 0 : linear_tid / OUTER_WARP_THREADS),
        lane_id((OUTER_WARPS == 1) ? linear_tid : linear_tid % OUTER_WARP_THREADS)
    {}


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
        OuterWarpScanT(temp_storage.aliasable.outer_warp_scan.Alias()[warp_id]).Scan(
            input, inclusive_output, exclusive_output, scan_op);

        // Share outer warp total
        if (lane_id == OUTER_WARP_THREADS - 1)
            temp_storage.warp_aggregates[warp_id] = inclusive_output;

        CTA_SYNC();

        if (linear_tid < INNER_WARP_THREADS)
        {
            T outer_warp_input = temp_storage.warp_aggregates[linear_tid];
            T outer_warp_exclusive;

            InnerWarpScanT(temp_storage.aliasable.inner_warp_scan).ExclusiveScan(
                outer_warp_input, outer_warp_exclusive, scan_op, block_aggregate);

            temp_storage.block_aggregate                = block_aggregate;
            temp_storage.warp_aggregates[linear_tid]    = outer_warp_exclusive;
        }

        CTA_SYNC();

        if (warp_id != 0)
        {
            // Retrieve block aggregate
            block_aggregate = temp_storage.block_aggregate;

            // Apply warp prefix to our lane's partial
            T outer_warp_exclusive = temp_storage.warp_aggregates[warp_id];
            exclusive_output = scan_op(outer_warp_exclusive, exclusive_output);
            if (lane_id == 0)
                exclusive_output = outer_warp_exclusive;
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
        OuterWarpScanT(temp_storage.aliasable.outer_warp_scan.Alias()[warp_id]).Scan(
            input, inclusive_output, exclusive_output, scan_op);

        // Share outer warp total
        if (lane_id == OUTER_WARP_THREADS - 1)
        {
            temp_storage.warp_aggregates[warp_id] = inclusive_output;
        }

        CTA_SYNC();

        if (linear_tid < INNER_WARP_THREADS)
        {
            T outer_warp_input = temp_storage.warp_aggregates[linear_tid];
            T outer_warp_exclusive;

            InnerWarpScanT(temp_storage.aliasable.inner_warp_scan).ExclusiveScan(
                outer_warp_input, outer_warp_exclusive, initial_value, scan_op, block_aggregate);

            temp_storage.block_aggregate                = block_aggregate;
            temp_storage.warp_aggregates[linear_tid]    = outer_warp_exclusive;
        }

        CTA_SYNC();

        // Retrieve block aggregate
        block_aggregate = temp_storage.block_aggregate;

        // Apply warp prefix to our lane's partial
        T outer_warp_exclusive = temp_storage.warp_aggregates[warp_id];
        exclusive_output = scan_op(outer_warp_exclusive, exclusive_output);
        if (lane_id == 0)
            exclusive_output = outer_warp_exclusive;
    }


    /// Computes an exclusive thread block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes one input element.  The call-back functor \p block_prefix_callback_op is invoked by the first warp in the block, and the value returned by <em>lane</em><sub>0</sub> in that warp is used as the "seed" value that logically prefixes the thread block's scan inputs.
    template <
        typename ScanOp,
        typename BlockPrefixCallbackOp>
    __device__ __forceinline__ void ExclusiveScan(
        T                       input,                          ///< [in] Calling thread's input item
        T                       &exclusive_output,              ///< [out] Calling thread's output item (may be aliased to \p input)
        ScanOp                  scan_op,                        ///< [in] Binary scan operator
        BlockPrefixCallbackOp   &block_prefix_callback_op)      ///< [in-out] <b>[<em>warp</em><sub>0</sub> only]</b> Call-back functor for specifying a thread block-wide prefix to be applied to all inputs.
    {
        // Compute warp scan in each warp.  The exclusive output from each lane0 is invalid.
        T inclusive_output;
        OuterWarpScanT(temp_storage.aliasable.outer_warp_scan.Alias()[warp_id]).Scan(
            input, inclusive_output, exclusive_output, scan_op);

        // Share outer warp total
        if (lane_id == OUTER_WARP_THREADS - 1)
            temp_storage.warp_aggregates[warp_id] = inclusive_output;

        CTA_SYNC();

        if (linear_tid < INNER_WARP_THREADS)
        {
            InnerWarpScanT inner_scan(temp_storage.aliasable.inner_warp_scan);

            T upsweep = temp_storage.warp_aggregates[linear_tid];
            T downsweep_prefix, block_aggregate;

            inner_scan.ExclusiveScan(upsweep, downsweep_prefix, scan_op, block_aggregate);

            // Use callback functor to get block prefix in lane0 and then broadcast to other lanes
            T block_prefix = block_prefix_callback_op(block_aggregate);
            block_prefix = inner_scan.Broadcast(block_prefix, 0);

            downsweep_prefix = scan_op(block_prefix, downsweep_prefix);
            if (linear_tid == 0)
                downsweep_prefix = block_prefix;

            temp_storage.warp_aggregates[linear_tid] = downsweep_prefix;
        }

        CTA_SYNC();

        // Apply warp prefix to our lane's partial (or assign it if partial is invalid)
        T outer_warp_exclusive = temp_storage.warp_aggregates[warp_id];
        exclusive_output = scan_op(outer_warp_exclusive, exclusive_output);
        if (lane_id == 0)
            exclusive_output = outer_warp_exclusive;
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
        // Compute warp scan in each warp.  The exclusive output from each lane0 is invalid.
        OuterWarpScanT(temp_storage.aliasable.outer_warp_scan.Alias()[warp_id]).InclusiveScan(
            input, inclusive_output, scan_op);

        // Share outer warp total
        if (lane_id == OUTER_WARP_THREADS - 1)
            temp_storage.warp_aggregates[warp_id] = inclusive_output;

        CTA_SYNC();

        if (linear_tid < INNER_WARP_THREADS)
        {
            T outer_warp_input = temp_storage.warp_aggregates[linear_tid];
            T outer_warp_exclusive;

            InnerWarpScanT(temp_storage.aliasable.inner_warp_scan).ExclusiveScan(
                outer_warp_input, outer_warp_exclusive, scan_op, block_aggregate);

            temp_storage.block_aggregate                = block_aggregate;
            temp_storage.warp_aggregates[linear_tid]    = outer_warp_exclusive;
        }

        CTA_SYNC();

        if (warp_id != 0)
        {
            // Retrieve block aggregate
            block_aggregate = temp_storage.block_aggregate;

            // Apply warp prefix to our lane's partial
            T outer_warp_exclusive = temp_storage.warp_aggregates[warp_id];
            inclusive_output = scan_op(outer_warp_exclusive, inclusive_output);
        }
    }


    /// Computes an inclusive thread block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes one input element.  the call-back functor \p block_prefix_callback_op is invoked by the first warp in the block, and the value returned by <em>lane</em><sub>0</sub> in that warp is used as the "seed" value that logically prefixes the thread block's scan inputs.
    template <
        typename ScanOp,
        typename BlockPrefixCallbackOp>
    __device__ __forceinline__ void InclusiveScan(
        T                       input,                          ///< [in] Calling thread's input item
        T                       &inclusive_output,              ///< [out] Calling thread's output item (may be aliased to \p input)
        ScanOp                  scan_op,                        ///< [in] Binary scan operator
        BlockPrefixCallbackOp   &block_prefix_callback_op)      ///< [in-out] <b>[<em>warp</em><sub>0</sub> only]</b> Call-back functor for specifying a thread block-wide prefix to be applied to all inputs.
    {
        // Compute warp scan in each warp.  The exclusive output from each lane0 is invalid.
        OuterWarpScanT(temp_storage.aliasable.outer_warp_scan.Alias()[warp_id]).InclusiveScan(
            input, inclusive_output, scan_op);

        // Share outer warp total
        if (lane_id == OUTER_WARP_THREADS - 1)
            temp_storage.warp_aggregates[warp_id] = inclusive_output;

        CTA_SYNC();

        if (linear_tid < INNER_WARP_THREADS)
        {
            InnerWarpScanT inner_scan(temp_storage.aliasable.inner_warp_scan);

            T upsweep = temp_storage.warp_aggregates[linear_tid];
            T downsweep_prefix, block_aggregate;
            inner_scan.ExclusiveScan(upsweep, downsweep_prefix, scan_op, block_aggregate);

            // Use callback functor to get block prefix in lane0 and then broadcast to other lanes
            T block_prefix = block_prefix_callback_op(block_aggregate);
            block_prefix = inner_scan.Broadcast(block_prefix, 0);

            downsweep_prefix = scan_op(block_prefix, downsweep_prefix);
            if (linear_tid == 0)
                downsweep_prefix = block_prefix;

            temp_storage.warp_aggregates[linear_tid]    = downsweep_prefix;
        }

        CTA_SYNC();

        // Apply warp prefix to our lane's partial
        T outer_warp_exclusive = temp_storage.warp_aggregates[warp_id];
        inclusive_output = scan_op(outer_warp_exclusive, inclusive_output);
    }


};


}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)

