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
 * cub::BlockScanRaking provides variants of raking-based parallel prefix scan across a CUDA thread block.
 */

#pragma once

#include "../../util_ptx.cuh"
#include "../../util_arch.cuh"
#include "../../block/block_raking_layout.cuh"
#include "../../thread/thread_reduce.cuh"
#include "../../thread/thread_scan.cuh"
#include "../../warp/warp_scan.cuh"
#include "../../util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {


/**
 * \brief BlockScanRaking provides variants of raking-based parallel prefix scan across a CUDA thread block.
 */
template <
    typename    T,              ///< Data type being scanned
    int         BLOCK_DIM_X,    ///< The thread block length in threads along the X dimension
    int         BLOCK_DIM_Y,    ///< The thread block length in threads along the Y dimension
    int         BLOCK_DIM_Z,    ///< The thread block length in threads along the Z dimension
    bool        MEMOIZE,        ///< Whether or not to buffer outer raking scan partials to incur fewer shared memory reads at the expense of higher register pressure
    int         PTX_ARCH>       ///< The PTX compute capability for which to to specialize this collective
struct BlockScanRaking
{
    //---------------------------------------------------------------------
    // Types and constants
    //---------------------------------------------------------------------

    /// Constants
    enum
    {
        /// The thread block size in threads
        BLOCK_THREADS = BLOCK_DIM_X * BLOCK_DIM_Y * BLOCK_DIM_Z,
    };

    /// Layout type for padded thread block raking grid
    typedef BlockRakingLayout<T, BLOCK_THREADS, PTX_ARCH> BlockRakingLayout;

    /// Constants
    enum
    {
        /// Number of raking threads
        RAKING_THREADS = BlockRakingLayout::RAKING_THREADS,

        /// Number of raking elements per warp synchronous raking thread
        SEGMENT_LENGTH = BlockRakingLayout::SEGMENT_LENGTH,

        /// Cooperative work can be entirely warp synchronous
        WARP_SYNCHRONOUS = (BLOCK_THREADS == RAKING_THREADS),
    };

    ///  WarpScan utility type
    typedef WarpScan<T, RAKING_THREADS, PTX_ARCH> WarpScan;

    /// Shared memory storage layout type
    struct _TempStorage
    {
        typename WarpScan::TempStorage              warp_scan;          ///< Buffer for warp-synchronous scan
        typename BlockRakingLayout::TempStorage     raking_grid;        ///< Padded thread block raking grid
        T                                           block_aggregate;    ///< Block aggregate
    };


    /// Alias wrapper allowing storage to be unioned
    struct TempStorage : Uninitialized<_TempStorage> {};


    //---------------------------------------------------------------------
    // Per-thread fields
    //---------------------------------------------------------------------

    // Thread fields
    _TempStorage    &temp_storage;
    unsigned int    linear_tid;
    T               cached_segment[SEGMENT_LENGTH];


    //---------------------------------------------------------------------
    // Utility methods
    //---------------------------------------------------------------------

    /// Templated reduction
    template <int ITERATION, typename ScanOp>
    __device__ __forceinline__ T GuardedReduce(
        T*                  raking_ptr,         ///< [in] Input array
        ScanOp              scan_op,            ///< [in] Binary reduction operator
        T                   raking_partial,     ///< [in] Prefix to seed reduction with
        Int2Type<ITERATION> /*iteration*/)
    {
        if ((BlockRakingLayout::UNGUARDED) || (((linear_tid * SEGMENT_LENGTH) + ITERATION) < BLOCK_THREADS))
        {
            T addend = raking_ptr[ITERATION];
            raking_partial = scan_op(raking_partial, addend);
        }

        return GuardedReduce(raking_ptr, scan_op, raking_partial, Int2Type<ITERATION + 1>());
    }


    /// Templated reduction (base case)
    template <typename ScanOp>
    __device__ __forceinline__ T GuardedReduce(
        T*                          /*raking_ptr*/,    ///< [in] Input array
        ScanOp                      /*scan_op*/,       ///< [in] Binary reduction operator
        T                           raking_partial,    ///< [in] Prefix to seed reduction with
        Int2Type<SEGMENT_LENGTH>    /*iteration*/)
    {
        return raking_partial;
    }


    /// Templated copy
    template <int ITERATION>
    __device__ __forceinline__ void CopySegment(
        T*                  out,            ///< [out] Out array
        T*                  in,             ///< [in] Input array
        Int2Type<ITERATION> /*iteration*/)
    {
        out[ITERATION] = in[ITERATION];
        CopySegment(out, in, Int2Type<ITERATION + 1>());
    }

 
    /// Templated copy (base case)
    __device__ __forceinline__ void CopySegment(
        T*                  /*out*/,            ///< [out] Out array
        T*                  /*in*/,             ///< [in] Input array
        Int2Type<SEGMENT_LENGTH> /*iteration*/)
    {}


    /// Performs upsweep raking reduction, returning the aggregate
    template <typename ScanOp>
    __device__ __forceinline__ T Upsweep(
        ScanOp scan_op)
    {
        T *smem_raking_ptr = BlockRakingLayout::RakingPtr(temp_storage.raking_grid, linear_tid);

        // Read data into registers
        CopySegment(cached_segment, smem_raking_ptr, Int2Type<0>());

        T raking_partial = cached_segment[0];

        return GuardedReduce(cached_segment, scan_op, raking_partial, Int2Type<1>());
    }


    /// Performs exclusive downsweep raking scan
    template <typename ScanOp>
    __device__ __forceinline__ void ExclusiveDownsweep(
        ScanOp          scan_op,
        T               raking_partial,
        bool            apply_prefix = true)
    {
        T *smem_raking_ptr = BlockRakingLayout::RakingPtr(temp_storage.raking_grid, linear_tid);

        // Read data back into registers
        if (!MEMOIZE)
        {
            CopySegment(cached_segment, smem_raking_ptr, Int2Type<0>());
        }

        internal::ThreadScanExclusive(cached_segment, cached_segment, scan_op, raking_partial, apply_prefix);

        // Write data back to smem
        CopySegment(smem_raking_ptr, cached_segment, Int2Type<0>());
    }


    /// Performs inclusive downsweep raking scan
    template <typename ScanOp>
    __device__ __forceinline__ void InclusiveDownsweep(
        ScanOp          scan_op,
        T               raking_partial,
        bool            apply_prefix = true)
    {
        T *smem_raking_ptr = BlockRakingLayout::RakingPtr(temp_storage.raking_grid, linear_tid);

        // Read data back into registers
        if (!MEMOIZE)
        {
            CopySegment(cached_segment, smem_raking_ptr, Int2Type<0>());
        }

        internal::ThreadScanInclusive(cached_segment, cached_segment, scan_op, raking_partial, apply_prefix);

        // Write data back to smem
        CopySegment(smem_raking_ptr, cached_segment, Int2Type<0>());
    }


    //---------------------------------------------------------------------
    // Constructors
    //---------------------------------------------------------------------

    /// Constructor
    __device__ __forceinline__ BlockScanRaking(
        TempStorage &temp_storage)
    :
        temp_storage(temp_storage.Alias()),
        linear_tid(RowMajorTid(BLOCK_DIM_X, BLOCK_DIM_Y, BLOCK_DIM_Z))
    {}


    //---------------------------------------------------------------------
    // Exclusive scans
    //---------------------------------------------------------------------

    /// Computes an exclusive thread block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes one input element.  With no initial value, the output computed for <em>thread</em><sub>0</sub> is undefined.
    template <typename ScanOp>
    __device__ __forceinline__ void ExclusiveScan(
        T               input,                          ///< [in] Calling thread's input item
        T               &exclusive_output,                        ///< [out] Calling thread's output item (may be aliased to \p input)
        ScanOp          scan_op)                        ///< [in] Binary scan operator
    {
        if (WARP_SYNCHRONOUS)
        {
            // Short-circuit directly to warp-synchronous scan
            WarpScan(temp_storage.warp_scan).ExclusiveScan(input, exclusive_output, scan_op);
        }
        else
        {
            // Place thread partial into shared memory raking grid
            T *placement_ptr = BlockRakingLayout::PlacementPtr(temp_storage.raking_grid, linear_tid);
            *placement_ptr = input;

            CTA_SYNC();

            // Reduce parallelism down to just raking threads
            if (linear_tid < RAKING_THREADS)
            {
                // Raking upsweep reduction across shared partials
                T upsweep_partial = Upsweep(scan_op);

                // Warp-synchronous scan
                T exclusive_partial;
                WarpScan(temp_storage.warp_scan).ExclusiveScan(upsweep_partial, exclusive_partial, scan_op);

                // Exclusive raking downsweep scan
                ExclusiveDownsweep(scan_op, exclusive_partial, (linear_tid != 0));
            }

            CTA_SYNC();

            // Grab thread prefix from shared memory
            exclusive_output = *placement_ptr;
        }
    }

    /// Computes an exclusive thread block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes one input element.
    template <typename ScanOp>
    __device__ __forceinline__ void ExclusiveScan(
        T               input,              ///< [in] Calling thread's input items
        T               &output,            ///< [out] Calling thread's output items (may be aliased to \p input)
        const T         &initial_value,     ///< [in] Initial value to seed the exclusive scan
        ScanOp          scan_op)            ///< [in] Binary scan operator
    {
        if (WARP_SYNCHRONOUS)
        {
            // Short-circuit directly to warp-synchronous scan
            WarpScan(temp_storage.warp_scan).ExclusiveScan(input, output, initial_value, scan_op);
        }
        else
        {
            // Place thread partial into shared memory raking grid
            T *placement_ptr = BlockRakingLayout::PlacementPtr(temp_storage.raking_grid, linear_tid);
            *placement_ptr = input;

            CTA_SYNC();

            // Reduce parallelism down to just raking threads
            if (linear_tid < RAKING_THREADS)
            {
                // Raking upsweep reduction across shared partials
                T upsweep_partial = Upsweep(scan_op);

                // Exclusive Warp-synchronous scan
                T exclusive_partial;
                WarpScan(temp_storage.warp_scan).ExclusiveScan(upsweep_partial, exclusive_partial, initial_value, scan_op);

                // Exclusive raking downsweep scan
                ExclusiveDownsweep(scan_op, exclusive_partial);
            }

            CTA_SYNC();

            // Grab exclusive partial from shared memory
            output = *placement_ptr;
        }
    }


    /// Computes an exclusive thread block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes one input element.  Also provides every thread with the block-wide \p block_aggregate of all inputs.  With no initial value, the output computed for <em>thread</em><sub>0</sub> is undefined.
    template <typename ScanOp>
    __device__ __forceinline__ void ExclusiveScan(
        T               input,                          ///< [in] Calling thread's input item
        T               &output,                        ///< [out] Calling thread's output item (may be aliased to \p input)
        ScanOp          scan_op,                        ///< [in] Binary scan operator
        T               &block_aggregate)               ///< [out] Threadblock-wide aggregate reduction of input items
    {
        if (WARP_SYNCHRONOUS)
        {
            // Short-circuit directly to warp-synchronous scan
            WarpScan(temp_storage.warp_scan).ExclusiveScan(input, output, scan_op, block_aggregate);
        }
        else
        {
            // Place thread partial into shared memory raking grid
            T *placement_ptr = BlockRakingLayout::PlacementPtr(temp_storage.raking_grid, linear_tid);
            *placement_ptr = input;

            CTA_SYNC();

            // Reduce parallelism down to just raking threads
            if (linear_tid < RAKING_THREADS)
            {
                // Raking upsweep reduction across shared partials
                T upsweep_partial= Upsweep(scan_op);

                // Warp-synchronous scan
                T inclusive_partial;
                T exclusive_partial;
                WarpScan(temp_storage.warp_scan).Scan(upsweep_partial, inclusive_partial, exclusive_partial, scan_op);

                // Exclusive raking downsweep scan
                ExclusiveDownsweep(scan_op, exclusive_partial, (linear_tid != 0));

                // Broadcast aggregate to all threads
                if (linear_tid == RAKING_THREADS - 1)
                    temp_storage.block_aggregate = inclusive_partial;
            }

            CTA_SYNC();

            // Grab thread prefix from shared memory
            output = *placement_ptr;

            // Retrieve block aggregate
            block_aggregate = temp_storage.block_aggregate;
        }
    }


    /// Computes an exclusive thread block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes one input element.  Also provides every thread with the block-wide \p block_aggregate of all inputs.
    template <typename ScanOp>
    __device__ __forceinline__ void ExclusiveScan(
        T               input,              ///< [in] Calling thread's input items
        T               &output,            ///< [out] Calling thread's output items (may be aliased to \p input)
        const T         &initial_value,     ///< [in] Initial value to seed the exclusive scan
        ScanOp          scan_op,            ///< [in] Binary scan operator
        T               &block_aggregate)   ///< [out] Threadblock-wide aggregate reduction of input items
    {
        if (WARP_SYNCHRONOUS)
        {
            // Short-circuit directly to warp-synchronous scan
            WarpScan(temp_storage.warp_scan).ExclusiveScan(input, output, initial_value, scan_op, block_aggregate);
        }
        else
        {
            // Place thread partial into shared memory raking grid
            T *placement_ptr = BlockRakingLayout::PlacementPtr(temp_storage.raking_grid, linear_tid);
            *placement_ptr = input;

            CTA_SYNC();

            // Reduce parallelism down to just raking threads
            if (linear_tid < RAKING_THREADS)
            {
                // Raking upsweep reduction across shared partials
                T upsweep_partial = Upsweep(scan_op);

                // Warp-synchronous scan
                T exclusive_partial;
                WarpScan(temp_storage.warp_scan).ExclusiveScan(upsweep_partial, exclusive_partial, initial_value, scan_op, block_aggregate);

                // Exclusive raking downsweep scan
                ExclusiveDownsweep(scan_op, exclusive_partial);

                // Broadcast aggregate to other threads
                if (linear_tid == 0)
                    temp_storage.block_aggregate = block_aggregate;
            }

            CTA_SYNC();

            // Grab exclusive partial from shared memory
            output = *placement_ptr;

            // Retrieve block aggregate
            block_aggregate = temp_storage.block_aggregate;
        }
    }


    /// Computes an exclusive thread block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes one input element.  the call-back functor \p block_prefix_callback_op is invoked by the first warp in the block, and the value returned by <em>lane</em><sub>0</sub> in that warp is used as the "seed" value that logically prefixes the thread block's scan inputs.  Also provides every thread with the block-wide \p block_aggregate of all inputs.
    template <
        typename ScanOp,
        typename BlockPrefixCallbackOp>
    __device__ __forceinline__ void ExclusiveScan(
        T                       input,                          ///< [in] Calling thread's input item
        T                       &output,                        ///< [out] Calling thread's output item (may be aliased to \p input)
        ScanOp                  scan_op,                        ///< [in] Binary scan operator
        BlockPrefixCallbackOp   &block_prefix_callback_op)      ///< [in-out] <b>[<em>warp</em><sub>0</sub> only]</b> Call-back functor for specifying a thread block-wide prefix to be applied to all inputs.
    {
        if (WARP_SYNCHRONOUS)
        {
            // Short-circuit directly to warp-synchronous scan
            T block_aggregate;
            WarpScan warp_scan(temp_storage.warp_scan);
            warp_scan.ExclusiveScan(input, output, scan_op, block_aggregate);

            // Obtain warp-wide prefix in lane0, then broadcast to other lanes
            T block_prefix = block_prefix_callback_op(block_aggregate);
            block_prefix = warp_scan.Broadcast(block_prefix, 0);

            output = scan_op(block_prefix, output);
            if (linear_tid == 0)
                output = block_prefix;
        }
        else
        {
            // Place thread partial into shared memory raking grid
            T *placement_ptr = BlockRakingLayout::PlacementPtr(temp_storage.raking_grid, linear_tid);
            *placement_ptr = input;

            CTA_SYNC();

            // Reduce parallelism down to just raking threads
            if (linear_tid < RAKING_THREADS)
            {
                WarpScan warp_scan(temp_storage.warp_scan);

                // Raking upsweep reduction across shared partials
                T upsweep_partial = Upsweep(scan_op);

                // Warp-synchronous scan
                T exclusive_partial, block_aggregate;
                warp_scan.ExclusiveScan(upsweep_partial, exclusive_partial, scan_op, block_aggregate);

                // Obtain block-wide prefix in lane0, then broadcast to other lanes
                T block_prefix = block_prefix_callback_op(block_aggregate);
                block_prefix = warp_scan.Broadcast(block_prefix, 0);

                // Update prefix with warpscan exclusive partial
                T downsweep_prefix = scan_op(block_prefix, exclusive_partial);
                if (linear_tid == 0)
                    downsweep_prefix = block_prefix;

                // Exclusive raking downsweep scan
                ExclusiveDownsweep(scan_op, downsweep_prefix);
            }

            CTA_SYNC();

            // Grab thread prefix from shared memory
            output = *placement_ptr;
        }
    }


    //---------------------------------------------------------------------
    // Inclusive scans
    //---------------------------------------------------------------------

    /// Computes an inclusive thread block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes one input element.
    template <typename ScanOp>
    __device__ __forceinline__ void InclusiveScan(
        T               input,                          ///< [in] Calling thread's input item
        T               &output,                        ///< [out] Calling thread's output item (may be aliased to \p input)
        ScanOp          scan_op)                        ///< [in] Binary scan operator
    {
        if (WARP_SYNCHRONOUS)
        {
            // Short-circuit directly to warp-synchronous scan
            WarpScan(temp_storage.warp_scan).InclusiveScan(input, output, scan_op);
        }
        else
        {
            // Place thread partial into shared memory raking grid
            T *placement_ptr = BlockRakingLayout::PlacementPtr(temp_storage.raking_grid, linear_tid);
            *placement_ptr = input;

            CTA_SYNC();

            // Reduce parallelism down to just raking threads
            if (linear_tid < RAKING_THREADS)
            {
                // Raking upsweep reduction across shared partials
                T upsweep_partial = Upsweep(scan_op);

                // Exclusive Warp-synchronous scan
                T exclusive_partial;
                WarpScan(temp_storage.warp_scan).ExclusiveScan(upsweep_partial, exclusive_partial, scan_op);

                // Inclusive raking downsweep scan
                InclusiveDownsweep(scan_op, exclusive_partial, (linear_tid != 0));
            }

            CTA_SYNC();

            // Grab thread prefix from shared memory
            output = *placement_ptr;
        }
    }


    /// Computes an inclusive thread block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes one input element.  Also provides every thread with the block-wide \p block_aggregate of all inputs.
    template <typename ScanOp>
    __device__ __forceinline__ void InclusiveScan(
        T               input,                          ///< [in] Calling thread's input item
        T               &output,                        ///< [out] Calling thread's output item (may be aliased to \p input)
        ScanOp          scan_op,                        ///< [in] Binary scan operator
        T               &block_aggregate)               ///< [out] Threadblock-wide aggregate reduction of input items
    {
        if (WARP_SYNCHRONOUS)
        {
            // Short-circuit directly to warp-synchronous scan
            WarpScan(temp_storage.warp_scan).InclusiveScan(input, output, scan_op, block_aggregate);
        }
        else
        {
            // Place thread partial into shared memory raking grid
            T *placement_ptr = BlockRakingLayout::PlacementPtr(temp_storage.raking_grid, linear_tid);
            *placement_ptr = input;

            CTA_SYNC();

            // Reduce parallelism down to just raking threads
            if (linear_tid < RAKING_THREADS)
            {
                // Raking upsweep reduction across shared partials
                T upsweep_partial = Upsweep(scan_op);

                // Warp-synchronous scan
                T inclusive_partial;
                T exclusive_partial;
                WarpScan(temp_storage.warp_scan).Scan(upsweep_partial, inclusive_partial, exclusive_partial, scan_op);

                // Inclusive raking downsweep scan
                InclusiveDownsweep(scan_op, exclusive_partial, (linear_tid != 0));

                // Broadcast aggregate to all threads
                if (linear_tid == RAKING_THREADS - 1)
                    temp_storage.block_aggregate = inclusive_partial;
            }

            CTA_SYNC();

            // Grab thread prefix from shared memory
            output = *placement_ptr;

            // Retrieve block aggregate
            block_aggregate = temp_storage.block_aggregate;
        }
    }


    /// Computes an inclusive thread block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes one input element.  the call-back functor \p block_prefix_callback_op is invoked by the first warp in the block, and the value returned by <em>lane</em><sub>0</sub> in that warp is used as the "seed" value that logically prefixes the thread block's scan inputs.  Also provides every thread with the block-wide \p block_aggregate of all inputs.
    template <
        typename ScanOp,
        typename BlockPrefixCallbackOp>
    __device__ __forceinline__ void InclusiveScan(
        T                       input,                          ///< [in] Calling thread's input item
        T                       &output,                        ///< [out] Calling thread's output item (may be aliased to \p input)
        ScanOp                  scan_op,                        ///< [in] Binary scan operator
        BlockPrefixCallbackOp   &block_prefix_callback_op)      ///< [in-out] <b>[<em>warp</em><sub>0</sub> only]</b> Call-back functor for specifying a thread block-wide prefix to be applied to all inputs.
    {
        if (WARP_SYNCHRONOUS)
        {
            // Short-circuit directly to warp-synchronous scan
            T block_aggregate;
            WarpScan warp_scan(temp_storage.warp_scan);
            warp_scan.InclusiveScan(input, output, scan_op, block_aggregate);

            // Obtain warp-wide prefix in lane0, then broadcast to other lanes
            T block_prefix = block_prefix_callback_op(block_aggregate);
            block_prefix = warp_scan.Broadcast(block_prefix, 0);

            // Update prefix with exclusive warpscan partial
            output = scan_op(block_prefix, output);
        }
        else
        {
            // Place thread partial into shared memory raking grid
            T *placement_ptr = BlockRakingLayout::PlacementPtr(temp_storage.raking_grid, linear_tid);
            *placement_ptr = input;

            CTA_SYNC();

            // Reduce parallelism down to just raking threads
            if (linear_tid < RAKING_THREADS)
            {
                WarpScan warp_scan(temp_storage.warp_scan);

                // Raking upsweep reduction across shared partials
                T upsweep_partial = Upsweep(scan_op);

                // Warp-synchronous scan
                T exclusive_partial, block_aggregate;
                warp_scan.ExclusiveScan(upsweep_partial, exclusive_partial, scan_op, block_aggregate);

                // Obtain block-wide prefix in lane0, then broadcast to other lanes
                T block_prefix = block_prefix_callback_op(block_aggregate);
                block_prefix = warp_scan.Broadcast(block_prefix, 0);

                // Update prefix with warpscan exclusive partial
                T downsweep_prefix = scan_op(block_prefix, exclusive_partial);
                if (linear_tid == 0)
                    downsweep_prefix = block_prefix;

                // Inclusive raking downsweep scan
                InclusiveDownsweep(scan_op, downsweep_prefix);
            }

            CTA_SYNC();

            // Grab thread prefix from shared memory
            output = *placement_ptr;
        }
    }

};


}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)

