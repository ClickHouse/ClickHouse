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
 * The cub::BlockHistogramSort class provides sorting-based methods for constructing block-wide histograms from data samples partitioned across a CUDA thread block.
 */

#pragma once

#include "../../block/block_radix_sort.cuh"
#include "../../block/block_discontinuity.cuh"
#include "../../util_ptx.cuh"
#include "../../util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {



/**
 * \brief The BlockHistogramSort class provides sorting-based methods for constructing block-wide histograms from data samples partitioned across a CUDA thread block.
 */
template <
    typename    T,                  ///< Sample type
    int         BLOCK_DIM_X,        ///< The thread block length in threads along the X dimension
    int         ITEMS_PER_THREAD,   ///< The number of samples per thread
    int         BINS,               ///< The number of bins into which histogram samples may fall
    int         BLOCK_DIM_Y,        ///< The thread block length in threads along the Y dimension
    int         BLOCK_DIM_Z,        ///< The thread block length in threads along the Z dimension
    int         PTX_ARCH>           ///< The PTX compute capability for which to to specialize this collective
struct BlockHistogramSort
{
    /// Constants
    enum
    {
        /// The thread block size in threads
        BLOCK_THREADS = BLOCK_DIM_X * BLOCK_DIM_Y * BLOCK_DIM_Z,
    };

    // Parameterize BlockRadixSort type for our thread block
    typedef BlockRadixSort<
            T,
            BLOCK_DIM_X,
            ITEMS_PER_THREAD,
            NullType,
            4,
            (PTX_ARCH >= 350) ? true : false,
            BLOCK_SCAN_WARP_SCANS,
            cudaSharedMemBankSizeFourByte,
            BLOCK_DIM_Y,
            BLOCK_DIM_Z,
            PTX_ARCH>
        BlockRadixSortT;

    // Parameterize BlockDiscontinuity type for our thread block
    typedef BlockDiscontinuity<
            T,
            BLOCK_DIM_X,
            BLOCK_DIM_Y,
            BLOCK_DIM_Z,
            PTX_ARCH>
        BlockDiscontinuityT;

    /// Shared memory
    union _TempStorage
    {
        // Storage for sorting bin values
        typename BlockRadixSortT::TempStorage sort;

        struct
        {
            // Storage for detecting discontinuities in the tile of sorted bin values
            typename BlockDiscontinuityT::TempStorage flag;

            // Storage for noting begin/end offsets of bin runs in the tile of sorted bin values
            unsigned int run_begin[BINS];
            unsigned int run_end[BINS];
        };
    };


    /// Alias wrapper allowing storage to be unioned
    struct TempStorage : Uninitialized<_TempStorage> {};


    // Thread fields
    _TempStorage &temp_storage;
    unsigned int linear_tid;


    /// Constructor
    __device__ __forceinline__ BlockHistogramSort(
        TempStorage     &temp_storage)
    :
        temp_storage(temp_storage.Alias()),
        linear_tid(RowMajorTid(BLOCK_DIM_X, BLOCK_DIM_Y, BLOCK_DIM_Z))
    {}


    // Discontinuity functor
    struct DiscontinuityOp
    {
        // Reference to temp_storage
        _TempStorage &temp_storage;

        // Constructor
        __device__ __forceinline__ DiscontinuityOp(_TempStorage &temp_storage) :
            temp_storage(temp_storage)
        {}

        // Discontinuity predicate
        __device__ __forceinline__ bool operator()(const T &a, const T &b, int b_index)
        {
            if (a != b)
            {
                // Note the begin/end offsets in shared storage
                temp_storage.run_begin[b] = b_index;
                temp_storage.run_end[a] = b_index;

                return true;
            }
            else
            {
                return false;
            }
        }
    };


    // Composite data onto an existing histogram
    template <
        typename            CounterT     >
    __device__ __forceinline__ void Composite(
        T                   (&items)[ITEMS_PER_THREAD],     ///< [in] Calling thread's input values to histogram
        CounterT            histogram[BINS])                 ///< [out] Reference to shared/device-accessible memory histogram
    {
        enum { TILE_SIZE = BLOCK_THREADS * ITEMS_PER_THREAD };

        // Sort bytes in blocked arrangement
        BlockRadixSortT(temp_storage.sort).Sort(items);

        CTA_SYNC();

        // Initialize the shared memory's run_begin and run_end for each bin
        int histo_offset = 0;

        #pragma unroll
        for(; histo_offset + BLOCK_THREADS <= BINS; histo_offset += BLOCK_THREADS)
        {
            temp_storage.run_begin[histo_offset + linear_tid] = TILE_SIZE;
            temp_storage.run_end[histo_offset + linear_tid] = TILE_SIZE;
        }
        // Finish up with guarded initialization if necessary
        if ((BINS % BLOCK_THREADS != 0) && (histo_offset + linear_tid < BINS))
        {
            temp_storage.run_begin[histo_offset + linear_tid] = TILE_SIZE;
            temp_storage.run_end[histo_offset + linear_tid] = TILE_SIZE;
        }

        CTA_SYNC();

        int flags[ITEMS_PER_THREAD];    // unused

        // Compute head flags to demarcate contiguous runs of the same bin in the sorted tile
        DiscontinuityOp flag_op(temp_storage);
        BlockDiscontinuityT(temp_storage.flag).FlagHeads(flags, items, flag_op);

        // Update begin for first item
        if (linear_tid == 0) temp_storage.run_begin[items[0]] = 0;

        CTA_SYNC();

        // Composite into histogram
        histo_offset = 0;

        #pragma unroll
        for(; histo_offset + BLOCK_THREADS <= BINS; histo_offset += BLOCK_THREADS)
        {
            int thread_offset = histo_offset + linear_tid;
            CounterT      count = temp_storage.run_end[thread_offset] - temp_storage.run_begin[thread_offset];
            histogram[thread_offset] += count;
        }

        // Finish up with guarded composition if necessary
        if ((BINS % BLOCK_THREADS != 0) && (histo_offset + linear_tid < BINS))
        {
            int thread_offset = histo_offset + linear_tid;
            CounterT      count = temp_storage.run_end[thread_offset] - temp_storage.run_begin[thread_offset];
            histogram[thread_offset] += count;
        }
    }

};

}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)

