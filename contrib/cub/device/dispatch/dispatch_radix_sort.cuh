
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
 * cub::DeviceRadixSort provides device-wide, parallel operations for computing a radix sort across a sequence of data items residing within device-accessible memory.
 */

#pragma once

#include <stdio.h>
#include <iterator>

#include "../../agent/agent_radix_sort_upsweep.cuh"
#include "../../agent/agent_radix_sort_downsweep.cuh"
#include "../../agent/agent_scan.cuh"
#include "../../block/block_radix_sort.cuh"
#include "../../grid/grid_even_share.cuh"
#include "../../util_type.cuh"
#include "../../util_debug.cuh"
#include "../../util_device.cuh"
#include "../../util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {

/******************************************************************************
 * Kernel entry points
 *****************************************************************************/

/**
 * Upsweep digit-counting kernel entry point (multi-block).  Computes privatized digit histograms, one per block.
 */
template <
    typename                ChainedPolicyT,                 ///< Chained tuning policy
    bool                    ALT_DIGIT_BITS,                 ///< Whether or not to use the alternate (lower-bits) policy
    bool                    IS_DESCENDING,                  ///< Whether or not the sorted-order is high-to-low
    typename                KeyT,                           ///< Key type
    typename                OffsetT>                        ///< Signed integer type for global offsets
__launch_bounds__ (int((ALT_DIGIT_BITS) ?
    ChainedPolicyT::ActivePolicy::AltUpsweepPolicy::BLOCK_THREADS :
    ChainedPolicyT::ActivePolicy::UpsweepPolicy::BLOCK_THREADS))
__global__ void DeviceRadixSortUpsweepKernel(
    const KeyT              *d_keys,                        ///< [in] Input keys buffer
    OffsetT                 *d_spine,                       ///< [out] Privatized (per block) digit histograms (striped, i.e., 0s counts from each block, then 1s counts from each block, etc.)
    OffsetT                 /*num_items*/,                  ///< [in] Total number of input data items
    int                     current_bit,                    ///< [in] Bit position of current radix digit
    int                     num_bits,                       ///< [in] Number of bits of current radix digit
    GridEvenShare<OffsetT>  even_share)                     ///< [in] Even-share descriptor for mapan equal number of tiles onto each thread block
{
    enum {
        TILE_ITEMS = ChainedPolicyT::ActivePolicy::AltUpsweepPolicy::BLOCK_THREADS *
                        ChainedPolicyT::ActivePolicy::AltUpsweepPolicy::ITEMS_PER_THREAD
    };

    // Parameterize AgentRadixSortUpsweep type for the current configuration
    typedef AgentRadixSortUpsweep<
            typename If<(ALT_DIGIT_BITS),
                typename ChainedPolicyT::ActivePolicy::AltUpsweepPolicy,
                typename ChainedPolicyT::ActivePolicy::UpsweepPolicy>::Type,
            KeyT,
            OffsetT>
        AgentRadixSortUpsweepT;

    // Shared memory storage
    __shared__ typename AgentRadixSortUpsweepT::TempStorage temp_storage;

    // Initialize GRID_MAPPING_RAKE even-share descriptor for this thread block
    even_share.template BlockInit<TILE_ITEMS, GRID_MAPPING_RAKE>();

    AgentRadixSortUpsweepT upsweep(temp_storage, d_keys, current_bit, num_bits);

    upsweep.ProcessRegion(even_share.block_offset, even_share.block_end);

    CTA_SYNC();

    // Write out digit counts (striped)
    upsweep.ExtractCounts<IS_DESCENDING>(d_spine, gridDim.x, blockIdx.x);
}


/**
 * Spine scan kernel entry point (single-block).  Computes an exclusive prefix sum over the privatized digit histograms
 */
template <
    typename                ChainedPolicyT,                 ///< Chained tuning policy
    typename                OffsetT>                        ///< Signed integer type for global offsets
__launch_bounds__ (int(ChainedPolicyT::ActivePolicy::ScanPolicy::BLOCK_THREADS), 1)
__global__ void RadixSortScanBinsKernel(
    OffsetT                 *d_spine,                       ///< [in,out] Privatized (per block) digit histograms (striped, i.e., 0s counts from each block, then 1s counts from each block, etc.)
    int                     num_counts)                     ///< [in] Total number of bin-counts
{
    // Parameterize the AgentScan type for the current configuration
    typedef AgentScan<
            typename ChainedPolicyT::ActivePolicy::ScanPolicy,
            OffsetT*,
            OffsetT*,
            cub::Sum,
            OffsetT,
            OffsetT>
        AgentScanT;

    // Shared memory storage
    __shared__ typename AgentScanT::TempStorage temp_storage;

    // Block scan instance
    AgentScanT block_scan(temp_storage, d_spine, d_spine, cub::Sum(), OffsetT(0)) ;

    // Process full input tiles
    int block_offset = 0;
    BlockScanRunningPrefixOp<OffsetT, Sum> prefix_op(0, Sum());
    while (block_offset + AgentScanT::TILE_ITEMS <= num_counts)
    {
        block_scan.template ConsumeTile<false, false>(block_offset, prefix_op);
        block_offset += AgentScanT::TILE_ITEMS;
    }
}


/**
 * Downsweep pass kernel entry point (multi-block).  Scatters keys (and values) into corresponding bins for the current digit place.
 */
template <
    typename                ChainedPolicyT,                 ///< Chained tuning policy
    bool                    ALT_DIGIT_BITS,                 ///< Whether or not to use the alternate (lower-bits) policy
    bool                    IS_DESCENDING,                  ///< Whether or not the sorted-order is high-to-low
    typename                KeyT,                           ///< Key type
    typename                ValueT,                         ///< Value type
    typename                OffsetT>                        ///< Signed integer type for global offsets
__launch_bounds__ (int((ALT_DIGIT_BITS) ?
    ChainedPolicyT::ActivePolicy::AltDownsweepPolicy::BLOCK_THREADS :
    ChainedPolicyT::ActivePolicy::DownsweepPolicy::BLOCK_THREADS))
__global__ void DeviceRadixSortDownsweepKernel(
    const KeyT              *d_keys_in,                     ///< [in] Input keys buffer
    KeyT                    *d_keys_out,                    ///< [in] Output keys buffer
    const ValueT            *d_values_in,                   ///< [in] Input values buffer
    ValueT                  *d_values_out,                  ///< [in] Output values buffer
    OffsetT                 *d_spine,                       ///< [in] Scan of privatized (per block) digit histograms (striped, i.e., 0s counts from each block, then 1s counts from each block, etc.)
    OffsetT                 num_items,                      ///< [in] Total number of input data items
    int                     current_bit,                    ///< [in] Bit position of current radix digit
    int                     num_bits,                       ///< [in] Number of bits of current radix digit
    GridEvenShare<OffsetT>  even_share)                     ///< [in] Even-share descriptor for mapan equal number of tiles onto each thread block
{
    enum {
        TILE_ITEMS = ChainedPolicyT::ActivePolicy::AltUpsweepPolicy::BLOCK_THREADS *
                        ChainedPolicyT::ActivePolicy::AltUpsweepPolicy::ITEMS_PER_THREAD
    };

    // Parameterize AgentRadixSortDownsweep type for the current configuration
    typedef AgentRadixSortDownsweep<
            typename If<(ALT_DIGIT_BITS),
                typename ChainedPolicyT::ActivePolicy::AltDownsweepPolicy,
                typename ChainedPolicyT::ActivePolicy::DownsweepPolicy>::Type,
            IS_DESCENDING,
            KeyT,
            ValueT,
            OffsetT>
        AgentRadixSortDownsweepT;

    // Shared memory storage
    __shared__  typename AgentRadixSortDownsweepT::TempStorage temp_storage;

    // Initialize even-share descriptor for this thread block
    even_share.template BlockInit<TILE_ITEMS, GRID_MAPPING_RAKE>();

    // Process input tiles
    AgentRadixSortDownsweepT(temp_storage, num_items, d_spine, d_keys_in, d_keys_out, d_values_in, d_values_out, current_bit, num_bits).ProcessRegion(
        even_share.block_offset,
        even_share.block_end);
}


/**
 * Single pass kernel entry point (single-block).  Fully sorts a tile of input.
 */
template <
    typename                ChainedPolicyT,                 ///< Chained tuning policy
    bool                    IS_DESCENDING,                  ///< Whether or not the sorted-order is high-to-low
    typename                KeyT,                           ///< Key type
    typename                ValueT,                         ///< Value type
    typename                OffsetT>                        ///< Signed integer type for global offsets
__launch_bounds__ (int(ChainedPolicyT::ActivePolicy::SingleTilePolicy::BLOCK_THREADS), 1)
__global__ void DeviceRadixSortSingleTileKernel(
    const KeyT              *d_keys_in,                     ///< [in] Input keys buffer
    KeyT                    *d_keys_out,                    ///< [in] Output keys buffer
    const ValueT            *d_values_in,                   ///< [in] Input values buffer
    ValueT                  *d_values_out,                  ///< [in] Output values buffer
    OffsetT                 num_items,                      ///< [in] Total number of input data items
    int                     current_bit,                    ///< [in] Bit position of current radix digit
    int                     end_bit)                        ///< [in] The past-the-end (most-significant) bit index needed for key comparison
{
    // Constants
    enum
    {
        BLOCK_THREADS           = ChainedPolicyT::ActivePolicy::SingleTilePolicy::BLOCK_THREADS,
        ITEMS_PER_THREAD        = ChainedPolicyT::ActivePolicy::SingleTilePolicy::ITEMS_PER_THREAD,
        KEYS_ONLY               = Equals<ValueT, NullType>::VALUE,
    };

    // BlockRadixSort type
    typedef BlockRadixSort<
            KeyT,
            BLOCK_THREADS,
            ITEMS_PER_THREAD,
            ValueT,
            ChainedPolicyT::ActivePolicy::SingleTilePolicy::RADIX_BITS,
            (ChainedPolicyT::ActivePolicy::SingleTilePolicy::RANK_ALGORITHM == RADIX_RANK_MEMOIZE),
            ChainedPolicyT::ActivePolicy::SingleTilePolicy::SCAN_ALGORITHM>
        BlockRadixSortT;

    // BlockLoad type (keys)
    typedef BlockLoad<
        KeyT,
        BLOCK_THREADS,
        ITEMS_PER_THREAD,
        ChainedPolicyT::ActivePolicy::SingleTilePolicy::LOAD_ALGORITHM> BlockLoadKeys;

    // BlockLoad type (values)
    typedef BlockLoad<
        ValueT,
        BLOCK_THREADS,
        ITEMS_PER_THREAD,
        ChainedPolicyT::ActivePolicy::SingleTilePolicy::LOAD_ALGORITHM> BlockLoadValues;

    // Unsigned word for key bits
    typedef typename Traits<KeyT>::UnsignedBits UnsignedBitsT;

    // Shared memory storage
    __shared__ union TempStorage
    {
        typename BlockRadixSortT::TempStorage       sort;
        typename BlockLoadKeys::TempStorage         load_keys;
        typename BlockLoadValues::TempStorage       load_values;

    } temp_storage;

    // Keys and values for the block
    KeyT            keys[ITEMS_PER_THREAD];
    ValueT          values[ITEMS_PER_THREAD];

    // Get default (min/max) value for out-of-bounds keys
    UnsignedBitsT   default_key_bits = (IS_DESCENDING) ? Traits<KeyT>::LOWEST_KEY : Traits<KeyT>::MAX_KEY;
    KeyT            default_key = reinterpret_cast<KeyT&>(default_key_bits);

    // Load keys
    BlockLoadKeys(temp_storage.load_keys).Load(d_keys_in, keys, num_items, default_key);

    CTA_SYNC();

    // Load values
    if (!KEYS_ONLY)
    {
        BlockLoadValues(temp_storage.load_values).Load(d_values_in, values, num_items);

        CTA_SYNC();
    }

    // Sort tile
    BlockRadixSortT(temp_storage.sort).SortBlockedToStriped(
        keys,
        values,
        current_bit,
        end_bit,
        Int2Type<IS_DESCENDING>(),
        Int2Type<KEYS_ONLY>());

    // Store keys and values
    #pragma unroll
    for (int ITEM = 0; ITEM < ITEMS_PER_THREAD; ++ITEM)
    {
        int item_offset = ITEM * BLOCK_THREADS + threadIdx.x;
        if (item_offset < num_items)
        {
            d_keys_out[item_offset] = keys[ITEM];
            if (!KEYS_ONLY)
                d_values_out[item_offset] = values[ITEM];
        }
    }
}


/**
 * Segmented radix sorting pass (one block per segment)
 */
template <
    typename                ChainedPolicyT,                 ///< Chained tuning policy
    bool                    ALT_DIGIT_BITS,                 ///< Whether or not to use the alternate (lower-bits) policy
    bool                    IS_DESCENDING,                  ///< Whether or not the sorted-order is high-to-low
    typename                KeyT,                           ///< Key type
    typename                ValueT,                         ///< Value type
    typename                OffsetIteratorT,                ///< Random-access input iterator type for reading segment offsets \iterator
    typename                OffsetT>                        ///< Signed integer type for global offsets
__launch_bounds__ (int((ALT_DIGIT_BITS) ?
    ChainedPolicyT::ActivePolicy::AltSegmentedPolicy::BLOCK_THREADS :
    ChainedPolicyT::ActivePolicy::SegmentedPolicy::BLOCK_THREADS))
__global__ void DeviceSegmentedRadixSortKernel(
    const KeyT              *d_keys_in,                     ///< [in] Input keys buffer
    KeyT                    *d_keys_out,                    ///< [in] Output keys buffer
    const ValueT            *d_values_in,                   ///< [in] Input values buffer
    ValueT                  *d_values_out,                  ///< [in] Output values buffer
    OffsetIteratorT         d_begin_offsets,                ///< [in] Pointer to the sequence of beginning offsets of length \p num_segments, such that <tt>d_begin_offsets[i]</tt> is the first element of the <em>i</em><sup>th</sup> data segment in <tt>d_keys_*</tt> and <tt>d_values_*</tt>
    OffsetIteratorT         d_end_offsets,                  ///< [in] Pointer to the sequence of ending offsets of length \p num_segments, such that <tt>d_end_offsets[i]-1</tt> is the last element of the <em>i</em><sup>th</sup> data segment in <tt>d_keys_*</tt> and <tt>d_values_*</tt>.  If <tt>d_end_offsets[i]-1</tt> <= <tt>d_begin_offsets[i]</tt>, the <em>i</em><sup>th</sup> is considered empty.
    int                     /*num_segments*/,               ///< [in] The number of segments that comprise the sorting data
    int                     current_bit,                    ///< [in] Bit position of current radix digit
    int                     pass_bits)                      ///< [in] Number of bits of current radix digit
{
    //
    // Constants
    //

    typedef typename If<(ALT_DIGIT_BITS),
        typename ChainedPolicyT::ActivePolicy::AltSegmentedPolicy,
        typename ChainedPolicyT::ActivePolicy::SegmentedPolicy>::Type SegmentedPolicyT;

    enum
    {
        BLOCK_THREADS       = SegmentedPolicyT::BLOCK_THREADS,
        ITEMS_PER_THREAD    = SegmentedPolicyT::ITEMS_PER_THREAD,
        RADIX_BITS          = SegmentedPolicyT::RADIX_BITS,
        TILE_ITEMS          = BLOCK_THREADS * ITEMS_PER_THREAD,
        RADIX_DIGITS        = 1 << RADIX_BITS,
        KEYS_ONLY           = Equals<ValueT, NullType>::VALUE,
    };

    // Upsweep type
    typedef AgentRadixSortUpsweep<
            AgentRadixSortUpsweepPolicy<BLOCK_THREADS, ITEMS_PER_THREAD, SegmentedPolicyT::LOAD_MODIFIER, RADIX_BITS>,
            KeyT,
            OffsetT>
        BlockUpsweepT;

    // Digit-scan type
    typedef BlockScan<OffsetT, BLOCK_THREADS> DigitScanT;

    // Downsweep type
    typedef AgentRadixSortDownsweep<SegmentedPolicyT, IS_DESCENDING, KeyT, ValueT, OffsetT> BlockDownsweepT;

    enum
    {
        /// Number of bin-starting offsets tracked per thread
        BINS_TRACKED_PER_THREAD = BlockDownsweepT::BINS_TRACKED_PER_THREAD
    };

    //
    // Process input tiles
    //

    // Shared memory storage
    __shared__ union
    {
        typename BlockUpsweepT::TempStorage     upsweep;
        typename BlockDownsweepT::TempStorage   downsweep;
        struct
        {
            volatile OffsetT                        reverse_counts_in[RADIX_DIGITS];
            volatile OffsetT                        reverse_counts_out[RADIX_DIGITS];
            typename DigitScanT::TempStorage        scan;
        };

    } temp_storage;

    OffsetT segment_begin   = d_begin_offsets[blockIdx.x];
    OffsetT segment_end     = d_end_offsets[blockIdx.x];
    OffsetT num_items       = segment_end - segment_begin;

    // Check if empty segment
    if (num_items <= 0)
        return;

    // Upsweep
    BlockUpsweepT upsweep(temp_storage.upsweep, d_keys_in, current_bit, pass_bits);
    upsweep.ProcessRegion(segment_begin, segment_end);

    CTA_SYNC();

    // The count of each digit value in this pass (valid in the first RADIX_DIGITS threads)
    OffsetT bin_count[BINS_TRACKED_PER_THREAD];
    upsweep.ExtractCounts(bin_count);

    CTA_SYNC();

    if (IS_DESCENDING)
    {
        // Reverse bin counts
        #pragma unroll
        for (int track = 0; track < BINS_TRACKED_PER_THREAD; ++track)
        {
            int bin_idx = (threadIdx.x * BINS_TRACKED_PER_THREAD) + track;

            if ((BLOCK_THREADS == RADIX_DIGITS) || (bin_idx < RADIX_DIGITS))
                temp_storage.reverse_counts_in[bin_idx] = bin_count[track];
        }

        CTA_SYNC();

        #pragma unroll
        for (int track = 0; track < BINS_TRACKED_PER_THREAD; ++track)
        {
            int bin_idx = (threadIdx.x * BINS_TRACKED_PER_THREAD) + track;

            if ((BLOCK_THREADS == RADIX_DIGITS) || (bin_idx < RADIX_DIGITS))
                bin_count[track] = temp_storage.reverse_counts_in[RADIX_DIGITS - bin_idx - 1];
        }
    }

    // Scan
    OffsetT bin_offset[BINS_TRACKED_PER_THREAD];     // The global scatter base offset for each digit value in this pass (valid in the first RADIX_DIGITS threads)
    DigitScanT(temp_storage.scan).ExclusiveSum(bin_count, bin_offset);

    #pragma unroll
    for (int track = 0; track < BINS_TRACKED_PER_THREAD; ++track)
    {
        bin_offset[track] += segment_begin;
    }

    if (IS_DESCENDING)
    {
        // Reverse bin offsets
        #pragma unroll
        for (int track = 0; track < BINS_TRACKED_PER_THREAD; ++track)
        {
            int bin_idx = (threadIdx.x * BINS_TRACKED_PER_THREAD) + track;

            if ((BLOCK_THREADS == RADIX_DIGITS) || (bin_idx < RADIX_DIGITS))
                temp_storage.reverse_counts_out[threadIdx.x] = bin_offset[track];
        }

        CTA_SYNC();

        #pragma unroll
        for (int track = 0; track < BINS_TRACKED_PER_THREAD; ++track)
        {
            int bin_idx = (threadIdx.x * BINS_TRACKED_PER_THREAD) + track;

            if ((BLOCK_THREADS == RADIX_DIGITS) || (bin_idx < RADIX_DIGITS))
                bin_offset[track] = temp_storage.reverse_counts_out[RADIX_DIGITS - bin_idx - 1];
        }
    }

    CTA_SYNC();

    // Downsweep
    BlockDownsweepT downsweep(temp_storage.downsweep, bin_offset, num_items, d_keys_in, d_keys_out, d_values_in, d_values_out, current_bit, pass_bits);
    downsweep.ProcessRegion(segment_begin, segment_end);
}



/******************************************************************************
 * Policy
 ******************************************************************************/

/**
 * Tuning policy for kernel specialization
 */
template <
    typename KeyT,          ///< Key type
    typename ValueT,        ///< Value type
    typename OffsetT>       ///< Signed integer type for global offsets
struct DeviceRadixSortPolicy
{
    //------------------------------------------------------------------------------
    // Constants
    //------------------------------------------------------------------------------

    enum
    {
        // Whether this is a keys-only (or key-value) sort
        KEYS_ONLY = (Equals<ValueT, NullType>::VALUE),

        // Relative size of KeyT type to a 4-byte word
        SCALE_FACTOR_4B = (CUB_MAX(sizeof(KeyT), sizeof(ValueT)) + 3) / 4,
    };

    //------------------------------------------------------------------------------
    // Architecture-specific tuning policies
    //------------------------------------------------------------------------------

    /// SM13
    struct Policy130 : ChainedPolicy<130, Policy130, Policy130>
    {
        enum {
            PRIMARY_RADIX_BITS      = 5,
            ALT_RADIX_BITS          = PRIMARY_RADIX_BITS - 1,
        };

        // Keys-only upsweep policies
        typedef AgentRadixSortUpsweepPolicy <128, CUB_MAX(1, 19 / SCALE_FACTOR_4B), LOAD_DEFAULT, PRIMARY_RADIX_BITS>   UpsweepPolicyKeys;
        typedef AgentRadixSortUpsweepPolicy <128, CUB_MAX(1, 15 / SCALE_FACTOR_4B), LOAD_DEFAULT, ALT_RADIX_BITS>       AltUpsweepPolicyKeys;

        // Key-value pairs upsweep policies
        typedef AgentRadixSortUpsweepPolicy <128, CUB_MAX(1, 19 / SCALE_FACTOR_4B), LOAD_DEFAULT, PRIMARY_RADIX_BITS>   UpsweepPolicyPairs;
        typedef AgentRadixSortUpsweepPolicy <128, CUB_MAX(1, 15 / SCALE_FACTOR_4B), LOAD_DEFAULT, ALT_RADIX_BITS>       AltUpsweepPolicyPairs;

        // Upsweep policies
        typedef typename If<KEYS_ONLY, UpsweepPolicyKeys, UpsweepPolicyPairs>::Type         UpsweepPolicy;
        typedef typename If<KEYS_ONLY, AltUpsweepPolicyKeys, AltUpsweepPolicyPairs>::Type   AltUpsweepPolicy;

        // Scan policy
        typedef AgentScanPolicy <256, 4, BLOCK_LOAD_VECTORIZE, LOAD_DEFAULT, BLOCK_STORE_VECTORIZE, BLOCK_SCAN_WARP_SCANS> ScanPolicy;

        // Keys-only downsweep policies
        typedef AgentRadixSortDownsweepPolicy <64, CUB_MAX(1, 19 / SCALE_FACTOR_4B), BLOCK_LOAD_WARP_TRANSPOSE, LOAD_DEFAULT, RADIX_RANK_BASIC, BLOCK_SCAN_WARP_SCANS, PRIMARY_RADIX_BITS>    DownsweepPolicyKeys;
        typedef AgentRadixSortDownsweepPolicy <128, CUB_MAX(1, 15 / SCALE_FACTOR_4B), BLOCK_LOAD_WARP_TRANSPOSE, LOAD_DEFAULT, RADIX_RANK_BASIC, BLOCK_SCAN_WARP_SCANS, ALT_RADIX_BITS>       AltDownsweepPolicyKeys;

        // Key-value pairs downsweep policies
        typedef AgentRadixSortDownsweepPolicy <64, CUB_MAX(1, 19 / SCALE_FACTOR_4B), BLOCK_LOAD_WARP_TRANSPOSE, LOAD_DEFAULT, RADIX_RANK_BASIC, BLOCK_SCAN_WARP_SCANS, PRIMARY_RADIX_BITS>    DownsweepPolicyPairs;
        typedef AgentRadixSortDownsweepPolicy <128, CUB_MAX(1, 15 / SCALE_FACTOR_4B), BLOCK_LOAD_WARP_TRANSPOSE, LOAD_DEFAULT, RADIX_RANK_BASIC, BLOCK_SCAN_WARP_SCANS, ALT_RADIX_BITS>       AltDownsweepPolicyPairs;

        // Downsweep policies
        typedef typename If<KEYS_ONLY, DownsweepPolicyKeys, DownsweepPolicyPairs>::Type         DownsweepPolicy;
        typedef typename If<KEYS_ONLY, AltDownsweepPolicyKeys, AltDownsweepPolicyPairs>::Type   AltDownsweepPolicy;

        // Single-tile policy
        typedef DownsweepPolicy SingleTilePolicy;

        // Segmented policies
        typedef DownsweepPolicy     SegmentedPolicy;
        typedef AltDownsweepPolicy  AltSegmentedPolicy;
    };

    /// SM20
    struct Policy200 : ChainedPolicy<200, Policy200, Policy130>
    {
        enum {
            PRIMARY_RADIX_BITS      = 5,
            ALT_RADIX_BITS          = PRIMARY_RADIX_BITS - 1,
        };

        // Keys-only upsweep policies
        typedef AgentRadixSortUpsweepPolicy <64, CUB_MAX(1, 18 / SCALE_FACTOR_4B), LOAD_DEFAULT, PRIMARY_RADIX_BITS>    UpsweepPolicyKeys;
        typedef AgentRadixSortUpsweepPolicy <64, CUB_MAX(1, 18 / SCALE_FACTOR_4B), LOAD_DEFAULT, ALT_RADIX_BITS>        AltUpsweepPolicyKeys;

        // Key-value pairs upsweep policies
        typedef AgentRadixSortUpsweepPolicy <128, CUB_MAX(1, 13 / SCALE_FACTOR_4B), LOAD_DEFAULT, PRIMARY_RADIX_BITS>   UpsweepPolicyPairs;
        typedef AgentRadixSortUpsweepPolicy <128, CUB_MAX(1, 13 / SCALE_FACTOR_4B), LOAD_DEFAULT, ALT_RADIX_BITS>       AltUpsweepPolicyPairs;

        // Upsweep policies
        typedef typename If<KEYS_ONLY, UpsweepPolicyKeys, UpsweepPolicyPairs>::Type         UpsweepPolicy;
        typedef typename If<KEYS_ONLY, AltUpsweepPolicyKeys, AltUpsweepPolicyPairs>::Type   AltUpsweepPolicy;

        // Scan policy
        typedef AgentScanPolicy <512, 4, BLOCK_LOAD_VECTORIZE, LOAD_DEFAULT, BLOCK_STORE_VECTORIZE, BLOCK_SCAN_RAKING_MEMOIZE> ScanPolicy;

        // Keys-only downsweep policies
        typedef AgentRadixSortDownsweepPolicy <64, CUB_MAX(1, 18 / SCALE_FACTOR_4B), BLOCK_LOAD_WARP_TRANSPOSE, LOAD_DEFAULT, RADIX_RANK_BASIC, BLOCK_SCAN_WARP_SCANS, PRIMARY_RADIX_BITS>    DownsweepPolicyKeys;
        typedef AgentRadixSortDownsweepPolicy <64, CUB_MAX(1, 18 / SCALE_FACTOR_4B), BLOCK_LOAD_WARP_TRANSPOSE, LOAD_DEFAULT, RADIX_RANK_BASIC, BLOCK_SCAN_WARP_SCANS, ALT_RADIX_BITS>        AltDownsweepPolicyKeys;

        // Key-value pairs downsweep policies
        typedef AgentRadixSortDownsweepPolicy <128, CUB_MAX(1, 13 / SCALE_FACTOR_4B), BLOCK_LOAD_WARP_TRANSPOSE, LOAD_DEFAULT, RADIX_RANK_BASIC, BLOCK_SCAN_WARP_SCANS, PRIMARY_RADIX_BITS>   DownsweepPolicyPairs;
        typedef AgentRadixSortDownsweepPolicy <128, CUB_MAX(1, 13 / SCALE_FACTOR_4B), BLOCK_LOAD_WARP_TRANSPOSE, LOAD_DEFAULT, RADIX_RANK_BASIC, BLOCK_SCAN_WARP_SCANS, ALT_RADIX_BITS>       AltDownsweepPolicyPairs;

        // Downsweep policies
        typedef typename If<KEYS_ONLY, DownsweepPolicyKeys, DownsweepPolicyPairs>::Type         DownsweepPolicy;
        typedef typename If<KEYS_ONLY, AltDownsweepPolicyKeys, AltDownsweepPolicyPairs>::Type   AltDownsweepPolicy;

        // Single-tile policy
        typedef DownsweepPolicy SingleTilePolicy;

        // Segmented policies
        typedef DownsweepPolicy     SegmentedPolicy;
        typedef AltDownsweepPolicy  AltSegmentedPolicy;
    };

    /// SM30
    struct Policy300 : ChainedPolicy<300, Policy300, Policy200>
    {
        enum {
            PRIMARY_RADIX_BITS      = 5,
            ALT_RADIX_BITS          = PRIMARY_RADIX_BITS - 1,
        };

        // Keys-only upsweep policies
        typedef AgentRadixSortUpsweepPolicy <256, CUB_MAX(1, 7 / SCALE_FACTOR_4B), LOAD_DEFAULT, PRIMARY_RADIX_BITS>    UpsweepPolicyKeys;
        typedef AgentRadixSortUpsweepPolicy <256, CUB_MAX(1, 7 / SCALE_FACTOR_4B), LOAD_DEFAULT, ALT_RADIX_BITS>        AltUpsweepPolicyKeys;

        // Key-value pairs upsweep policies
        typedef AgentRadixSortUpsweepPolicy <256, CUB_MAX(1, 5 / SCALE_FACTOR_4B), LOAD_DEFAULT, PRIMARY_RADIX_BITS>    UpsweepPolicyPairs;
        typedef AgentRadixSortUpsweepPolicy <256, CUB_MAX(1, 5 / SCALE_FACTOR_4B), LOAD_DEFAULT, ALT_RADIX_BITS>        AltUpsweepPolicyPairs;

        // Upsweep policies
        typedef typename If<KEYS_ONLY, UpsweepPolicyKeys, UpsweepPolicyPairs>::Type         UpsweepPolicy;
        typedef typename If<KEYS_ONLY, AltUpsweepPolicyKeys, AltUpsweepPolicyPairs>::Type   AltUpsweepPolicy;

        // Scan policy
        typedef AgentScanPolicy <1024, 4, BLOCK_LOAD_VECTORIZE, LOAD_DEFAULT, BLOCK_STORE_VECTORIZE, BLOCK_SCAN_WARP_SCANS> ScanPolicy;

        // Keys-only downsweep policies
        typedef AgentRadixSortDownsweepPolicy <128, CUB_MAX(1, 14 / SCALE_FACTOR_4B), BLOCK_LOAD_WARP_TRANSPOSE, LOAD_DEFAULT, RADIX_RANK_BASIC, BLOCK_SCAN_WARP_SCANS, PRIMARY_RADIX_BITS>   DownsweepPolicyKeys;
        typedef AgentRadixSortDownsweepPolicy <128, CUB_MAX(1, 14 / SCALE_FACTOR_4B), BLOCK_LOAD_WARP_TRANSPOSE, LOAD_DEFAULT, RADIX_RANK_BASIC, BLOCK_SCAN_WARP_SCANS, ALT_RADIX_BITS>       AltDownsweepPolicyKeys;

        // Key-value pairs downsweep policies
        typedef AgentRadixSortDownsweepPolicy <128, CUB_MAX(1, 10 / SCALE_FACTOR_4B), BLOCK_LOAD_TRANSPOSE, LOAD_DEFAULT, RADIX_RANK_BASIC, BLOCK_SCAN_WARP_SCANS, PRIMARY_RADIX_BITS>    DownsweepPolicyPairs;
        typedef AgentRadixSortDownsweepPolicy <128, CUB_MAX(1, 10 / SCALE_FACTOR_4B), BLOCK_LOAD_TRANSPOSE, LOAD_DEFAULT, RADIX_RANK_BASIC, BLOCK_SCAN_WARP_SCANS, ALT_RADIX_BITS>        AltDownsweepPolicyPairs;

        // Downsweep policies
        typedef typename If<KEYS_ONLY, DownsweepPolicyKeys, DownsweepPolicyPairs>::Type         DownsweepPolicy;
        typedef typename If<KEYS_ONLY, AltDownsweepPolicyKeys, AltDownsweepPolicyPairs>::Type   AltDownsweepPolicy;

        // Single-tile policy
        typedef DownsweepPolicy SingleTilePolicy;

        // Segmented policies
        typedef DownsweepPolicy     SegmentedPolicy;
        typedef AltDownsweepPolicy  AltSegmentedPolicy;
    };


    /// SM35
    struct Policy350 : ChainedPolicy<350, Policy350, Policy300>
    {
        enum {
            PRIMARY_RADIX_BITS      = 6,    // 1.72B 32b keys/s, 1.17B 32b pairs/s, 1.55B 32b segmented keys/s (K40m)
        };

        // Scan policy
        typedef AgentScanPolicy <1024, 4, BLOCK_LOAD_VECTORIZE, LOAD_DEFAULT, BLOCK_STORE_VECTORIZE, BLOCK_SCAN_WARP_SCANS> ScanPolicy;

        // Keys-only downsweep policies
        typedef AgentRadixSortDownsweepPolicy <128,   CUB_MAX(1, 9 / SCALE_FACTOR_4B), BLOCK_LOAD_WARP_TRANSPOSE, LOAD_LDG, RADIX_RANK_MATCH, BLOCK_SCAN_WARP_SCANS, PRIMARY_RADIX_BITS> DownsweepPolicyKeys;
        typedef AgentRadixSortDownsweepPolicy <64,   CUB_MAX(1, 18 / SCALE_FACTOR_4B), BLOCK_LOAD_DIRECT, LOAD_LDG, RADIX_RANK_MEMOIZE, BLOCK_SCAN_WARP_SCANS, PRIMARY_RADIX_BITS - 1> AltDownsweepPolicyKeys;

        // Key-value pairs downsweep policies
        typedef DownsweepPolicyKeys DownsweepPolicyPairs;
        typedef AgentRadixSortDownsweepPolicy <128,  CUB_MAX(1, 15 / SCALE_FACTOR_4B), BLOCK_LOAD_DIRECT, LOAD_LDG, RADIX_RANK_MEMOIZE, BLOCK_SCAN_WARP_SCANS, PRIMARY_RADIX_BITS - 1> AltDownsweepPolicyPairs;

        // Downsweep policies
        typedef typename If<KEYS_ONLY, DownsweepPolicyKeys, DownsweepPolicyPairs>::Type DownsweepPolicy;
        typedef typename If<KEYS_ONLY, AltDownsweepPolicyKeys, AltDownsweepPolicyPairs>::Type AltDownsweepPolicy;

        // Upsweep policies
        typedef DownsweepPolicy UpsweepPolicy;
        typedef AltDownsweepPolicy AltUpsweepPolicy;

        // Single-tile policy
        typedef DownsweepPolicy SingleTilePolicy;

        // Segmented policies
        typedef DownsweepPolicy     SegmentedPolicy;
        typedef AltDownsweepPolicy  AltSegmentedPolicy;


    };


    /// SM50
    struct Policy500 : ChainedPolicy<500, Policy500, Policy350>
    {
        enum {
            PRIMARY_RADIX_BITS      = 7,    // 3.5B 32b keys/s, 1.92B 32b pairs/s (TitanX)
            SINGLE_TILE_RADIX_BITS  = 6,
            SEGMENTED_RADIX_BITS    = 6,    // 3.1B 32b segmented keys/s (TitanX)
        };

        // ScanPolicy
        typedef AgentScanPolicy <512, 23, BLOCK_LOAD_WARP_TRANSPOSE, LOAD_DEFAULT, BLOCK_STORE_WARP_TRANSPOSE, BLOCK_SCAN_RAKING_MEMOIZE> ScanPolicy;

        // Downsweep policies
        typedef AgentRadixSortDownsweepPolicy <160, CUB_MAX(1, 39 / SCALE_FACTOR_4B),  BLOCK_LOAD_WARP_TRANSPOSE, LOAD_DEFAULT, RADIX_RANK_BASIC, BLOCK_SCAN_WARP_SCANS, PRIMARY_RADIX_BITS>  DownsweepPolicy;
        typedef AgentRadixSortDownsweepPolicy <256, CUB_MAX(1, 16 / SCALE_FACTOR_4B),  BLOCK_LOAD_DIRECT, LOAD_LDG, RADIX_RANK_MEMOIZE, BLOCK_SCAN_RAKING_MEMOIZE, PRIMARY_RADIX_BITS - 1>   AltDownsweepPolicy;

        // Upsweep policies
        typedef DownsweepPolicy UpsweepPolicy;
        typedef AltDownsweepPolicy AltUpsweepPolicy;

        // Single-tile policy
        typedef AgentRadixSortDownsweepPolicy <256, CUB_MAX(1, 19 / SCALE_FACTOR_4B),  BLOCK_LOAD_DIRECT, LOAD_LDG, RADIX_RANK_MEMOIZE, BLOCK_SCAN_WARP_SCANS, SINGLE_TILE_RADIX_BITS> SingleTilePolicy;

        // Segmented policies
        typedef AgentRadixSortDownsweepPolicy <192, CUB_MAX(1, 31 / SCALE_FACTOR_4B),  BLOCK_LOAD_WARP_TRANSPOSE, LOAD_DEFAULT, RADIX_RANK_MEMOIZE, BLOCK_SCAN_WARP_SCANS, SEGMENTED_RADIX_BITS>   SegmentedPolicy;
        typedef AgentRadixSortDownsweepPolicy <256, CUB_MAX(1, 11 / SCALE_FACTOR_4B),  BLOCK_LOAD_WARP_TRANSPOSE, LOAD_DEFAULT, RADIX_RANK_MEMOIZE, BLOCK_SCAN_WARP_SCANS, SEGMENTED_RADIX_BITS - 1>       AltSegmentedPolicy;
    };


    /// SM60 (GP100)
    struct Policy600 : ChainedPolicy<600, Policy600, Policy500>
    {
        enum {
            PRIMARY_RADIX_BITS      = 7,    // 6.9B 32b keys/s (Quadro P100)
            SINGLE_TILE_RADIX_BITS  = 6,
            SEGMENTED_RADIX_BITS    = 6,    // 5.9B 32b segmented keys/s (Quadro P100)
        };

        // ScanPolicy
        typedef AgentScanPolicy <512, 23, BLOCK_LOAD_WARP_TRANSPOSE, LOAD_DEFAULT, BLOCK_STORE_WARP_TRANSPOSE, BLOCK_SCAN_RAKING_MEMOIZE> ScanPolicy;

        // Downsweep policies
        typedef AgentRadixSortDownsweepPolicy <256, CUB_MAX(1, 25 / SCALE_FACTOR_4B),  BLOCK_LOAD_TRANSPOSE, LOAD_DEFAULT, RADIX_RANK_MATCH, BLOCK_SCAN_WARP_SCANS, PRIMARY_RADIX_BITS>   DownsweepPolicy;
        typedef AgentRadixSortDownsweepPolicy <192, CUB_MAX(1, 39 / SCALE_FACTOR_4B),  BLOCK_LOAD_TRANSPOSE, LOAD_DEFAULT, RADIX_RANK_MEMOIZE, BLOCK_SCAN_WARP_SCANS, PRIMARY_RADIX_BITS - 1>   AltDownsweepPolicy;

        // Upsweep policies
        typedef DownsweepPolicy UpsweepPolicy;
        typedef AltDownsweepPolicy AltUpsweepPolicy;

        // Single-tile policy
        typedef AgentRadixSortDownsweepPolicy <256, CUB_MAX(1, 19 / SCALE_FACTOR_4B),  BLOCK_LOAD_DIRECT, LOAD_LDG, RADIX_RANK_MEMOIZE, BLOCK_SCAN_WARP_SCANS, SINGLE_TILE_RADIX_BITS>          SingleTilePolicy;

        // Segmented policies
        typedef AgentRadixSortDownsweepPolicy <192, CUB_MAX(1, 39 / SCALE_FACTOR_4B),  BLOCK_LOAD_TRANSPOSE, LOAD_DEFAULT, RADIX_RANK_MEMOIZE, BLOCK_SCAN_WARP_SCANS, SEGMENTED_RADIX_BITS>     SegmentedPolicy;
        typedef AgentRadixSortDownsweepPolicy <384, CUB_MAX(1, 11 / SCALE_FACTOR_4B),  BLOCK_LOAD_TRANSPOSE, LOAD_DEFAULT, RADIX_RANK_MEMOIZE, BLOCK_SCAN_WARP_SCANS, SEGMENTED_RADIX_BITS - 1> AltSegmentedPolicy;

    };


    /// SM61 (GP104)
    struct Policy610 : ChainedPolicy<610, Policy610, Policy600>
    {
        enum {
            PRIMARY_RADIX_BITS      = 7,    // 3.4B 32b keys/s, 1.83B 32b pairs/s (1080)
            SINGLE_TILE_RADIX_BITS  = 6,
            SEGMENTED_RADIX_BITS    = 6,    // 3.3B 32b segmented keys/s (1080)
        };

        // ScanPolicy
        typedef AgentScanPolicy <512, 23, BLOCK_LOAD_WARP_TRANSPOSE, LOAD_DEFAULT, BLOCK_STORE_WARP_TRANSPOSE, BLOCK_SCAN_RAKING_MEMOIZE> ScanPolicy;

        // Downsweep policies
        typedef AgentRadixSortDownsweepPolicy <384, CUB_MAX(1, 31 / SCALE_FACTOR_4B),  BLOCK_LOAD_DIRECT,       LOAD_DEFAULT,       RADIX_RANK_MATCH,   BLOCK_SCAN_RAKING_MEMOIZE, PRIMARY_RADIX_BITS>   DownsweepPolicy;
        typedef AgentRadixSortDownsweepPolicy <256, CUB_MAX(1, 35 / SCALE_FACTOR_4B),  BLOCK_LOAD_TRANSPOSE,    LOAD_DEFAULT,   RADIX_RANK_MEMOIZE, BLOCK_SCAN_RAKING_MEMOIZE, PRIMARY_RADIX_BITS - 1>   AltDownsweepPolicy;

        // Upsweep policies
        typedef AgentRadixSortUpsweepPolicy <128, CUB_MAX(1, 16 / SCALE_FACTOR_4B), LOAD_LDG, PRIMARY_RADIX_BITS>        UpsweepPolicy;
        typedef AgentRadixSortUpsweepPolicy <128, CUB_MAX(1, 16 / SCALE_FACTOR_4B), LOAD_LDG, PRIMARY_RADIX_BITS - 1>    AltUpsweepPolicy;

        // Single-tile policy
        typedef AgentRadixSortDownsweepPolicy <256, CUB_MAX(1, 19 / SCALE_FACTOR_4B),  BLOCK_LOAD_DIRECT, LOAD_LDG, RADIX_RANK_MEMOIZE, BLOCK_SCAN_WARP_SCANS, SINGLE_TILE_RADIX_BITS>          SingleTilePolicy;

        // Segmented policies
        typedef AgentRadixSortDownsweepPolicy <192, CUB_MAX(1, 39 / SCALE_FACTOR_4B),  BLOCK_LOAD_TRANSPOSE, LOAD_DEFAULT, RADIX_RANK_MEMOIZE, BLOCK_SCAN_WARP_SCANS, SEGMENTED_RADIX_BITS>     SegmentedPolicy;
        typedef AgentRadixSortDownsweepPolicy <384, CUB_MAX(1, 11 / SCALE_FACTOR_4B),  BLOCK_LOAD_TRANSPOSE, LOAD_DEFAULT, RADIX_RANK_MEMOIZE, BLOCK_SCAN_WARP_SCANS, SEGMENTED_RADIX_BITS - 1> AltSegmentedPolicy;
    };


    /// SM62 (Tegra, less RF)
    struct Policy620 : ChainedPolicy<620, Policy620, Policy610>
    {
        enum {
            PRIMARY_RADIX_BITS      = 5,
            ALT_RADIX_BITS          = PRIMARY_RADIX_BITS - 1,
        };

        // ScanPolicy
        typedef AgentScanPolicy <512, 23, BLOCK_LOAD_WARP_TRANSPOSE, LOAD_DEFAULT, BLOCK_STORE_WARP_TRANSPOSE, BLOCK_SCAN_RAKING_MEMOIZE> ScanPolicy;

        // Downsweep policies
        typedef AgentRadixSortDownsweepPolicy <256, CUB_MAX(1, 16 / SCALE_FACTOR_4B),  BLOCK_LOAD_DIRECT, LOAD_DEFAULT, RADIX_RANK_MEMOIZE, BLOCK_SCAN_RAKING_MEMOIZE, PRIMARY_RADIX_BITS>   DownsweepPolicy;
        typedef AgentRadixSortDownsweepPolicy <256, CUB_MAX(1, 16 / SCALE_FACTOR_4B),  BLOCK_LOAD_DIRECT, LOAD_DEFAULT, RADIX_RANK_MEMOIZE, BLOCK_SCAN_RAKING_MEMOIZE, ALT_RADIX_BITS>       AltDownsweepPolicy;

        // Upsweep policies
        typedef DownsweepPolicy UpsweepPolicy;
        typedef AltDownsweepPolicy AltUpsweepPolicy;

        // Single-tile policy
        typedef AgentRadixSortDownsweepPolicy <256, CUB_MAX(1, 19 / SCALE_FACTOR_4B),  BLOCK_LOAD_DIRECT, LOAD_LDG, RADIX_RANK_MEMOIZE, BLOCK_SCAN_WARP_SCANS, PRIMARY_RADIX_BITS> SingleTilePolicy;

        // Segmented policies
        typedef DownsweepPolicy     SegmentedPolicy;
        typedef AltDownsweepPolicy  AltSegmentedPolicy;
    };


    /// SM70 (GV100)
    struct Policy700 : ChainedPolicy<700, Policy700, Policy620>
    {
        enum {
            PRIMARY_RADIX_BITS      = 6,    // 7.62B 32b keys/s (GV100)
            SINGLE_TILE_RADIX_BITS  = 6,
            SEGMENTED_RADIX_BITS    = 6,    // 8.7B 32b segmented keys/s (GV100)
        };

        // ScanPolicy
        typedef AgentScanPolicy <512, 23, BLOCK_LOAD_WARP_TRANSPOSE, LOAD_DEFAULT, BLOCK_STORE_WARP_TRANSPOSE, BLOCK_SCAN_RAKING_MEMOIZE> ScanPolicy;

        // Downsweep policies
        typedef AgentRadixSortDownsweepPolicy <256, CUB_MAX(1, 47 / SCALE_FACTOR_4B),  BLOCK_LOAD_TRANSPOSE, LOAD_DEFAULT, RADIX_RANK_MEMOIZE, BLOCK_SCAN_WARP_SCANS, PRIMARY_RADIX_BITS>   DownsweepPolicy;
        typedef AgentRadixSortDownsweepPolicy <384, CUB_MAX(1, 29 / SCALE_FACTOR_4B),  BLOCK_LOAD_TRANSPOSE, LOAD_DEFAULT, RADIX_RANK_MEMOIZE, BLOCK_SCAN_WARP_SCANS, PRIMARY_RADIX_BITS - 1>   AltDownsweepPolicy;

        // Upsweep policies
        typedef AgentRadixSortDownsweepPolicy <128, CUB_MAX(1, 47 / SCALE_FACTOR_4B),  BLOCK_LOAD_TRANSPOSE, LOAD_DEFAULT, RADIX_RANK_MATCH, BLOCK_SCAN_WARP_SCANS, PRIMARY_RADIX_BITS>  UpsweepPolicy;
        typedef AgentRadixSortDownsweepPolicy <128, CUB_MAX(1, 29 / SCALE_FACTOR_4B),  BLOCK_LOAD_TRANSPOSE, LOAD_DEFAULT, RADIX_RANK_MATCH, BLOCK_SCAN_WARP_SCANS, PRIMARY_RADIX_BITS - 1>  AltUpsweepPolicy;

        // Single-tile policy
        typedef AgentRadixSortDownsweepPolicy <256, CUB_MAX(1, 19 / SCALE_FACTOR_4B),  BLOCK_LOAD_DIRECT, LOAD_LDG, RADIX_RANK_MEMOIZE, BLOCK_SCAN_WARP_SCANS, SINGLE_TILE_RADIX_BITS>          SingleTilePolicy;

        // Segmented policies
        typedef AgentRadixSortDownsweepPolicy <192, CUB_MAX(1, 39 / SCALE_FACTOR_4B),  BLOCK_LOAD_TRANSPOSE, LOAD_DEFAULT, RADIX_RANK_MEMOIZE, BLOCK_SCAN_WARP_SCANS, SEGMENTED_RADIX_BITS>     SegmentedPolicy;
        typedef AgentRadixSortDownsweepPolicy <384, CUB_MAX(1, 11 / SCALE_FACTOR_4B),  BLOCK_LOAD_TRANSPOSE, LOAD_DEFAULT, RADIX_RANK_MEMOIZE, BLOCK_SCAN_WARP_SCANS, SEGMENTED_RADIX_BITS - 1> AltSegmentedPolicy;
    };


    /// MaxPolicy
    typedef Policy700 MaxPolicy;


};



/******************************************************************************
 * Single-problem dispatch
 ******************************************************************************/

/**
 * Utility class for dispatching the appropriately-tuned kernels for device-wide radix sort
 */
template <
    bool     IS_DESCENDING, ///< Whether or not the sorted-order is high-to-low
    typename KeyT,          ///< Key type
    typename ValueT,        ///< Value type
    typename OffsetT>       ///< Signed integer type for global offsets
struct DispatchRadixSort :
    DeviceRadixSortPolicy<KeyT, ValueT, OffsetT>
{
    //------------------------------------------------------------------------------
    // Constants
    //------------------------------------------------------------------------------

    enum
    {
        // Whether this is a keys-only (or key-value) sort
        KEYS_ONLY = (Equals<ValueT, NullType>::VALUE),
    };


    //------------------------------------------------------------------------------
    // Problem state
    //------------------------------------------------------------------------------

    void                    *d_temp_storage;        ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
    size_t                  &temp_storage_bytes;    ///< [in,out] Reference to size in bytes of \p d_temp_storage allocation
    DoubleBuffer<KeyT>      &d_keys;                ///< [in,out] Double-buffer whose current buffer contains the unsorted input keys and, upon return, is updated to point to the sorted output keys
    DoubleBuffer<ValueT>    &d_values;              ///< [in,out] Double-buffer whose current buffer contains the unsorted input values and, upon return, is updated to point to the sorted output values
    OffsetT                 num_items;              ///< [in] Number of items to sort
    int                     begin_bit;              ///< [in] The beginning (least-significant) bit index needed for key comparison
    int                     end_bit;                ///< [in] The past-the-end (most-significant) bit index needed for key comparison
    cudaStream_t            stream;                 ///< [in] CUDA stream to launch kernels within.  Default is stream<sub>0</sub>.
    bool                    debug_synchronous;      ///< [in] Whether or not to synchronize the stream after every kernel launch to check for errors.  Also causes launch configurations to be printed to the console.  Default is \p false.
    int                     ptx_version;            ///< [in] PTX version
    bool                    is_overwrite_okay;      ///< [in] Whether is okay to overwrite source buffers


    //------------------------------------------------------------------------------
    // Constructor
    //------------------------------------------------------------------------------

    /// Constructor
    CUB_RUNTIME_FUNCTION __forceinline__
    DispatchRadixSort(
        void*                   d_temp_storage,
        size_t                  &temp_storage_bytes,
        DoubleBuffer<KeyT>      &d_keys,
        DoubleBuffer<ValueT>    &d_values,
        OffsetT                 num_items,
        int                     begin_bit,
        int                     end_bit,
        bool                    is_overwrite_okay,
        cudaStream_t            stream,
        bool                    debug_synchronous,
        int                     ptx_version)
    :
        d_temp_storage(d_temp_storage),
        temp_storage_bytes(temp_storage_bytes),
        d_keys(d_keys),
        d_values(d_values),
        num_items(num_items),
        begin_bit(begin_bit),
        end_bit(end_bit),
        stream(stream),
        debug_synchronous(debug_synchronous),
        ptx_version(ptx_version),
        is_overwrite_okay(is_overwrite_okay)
    {}


    //------------------------------------------------------------------------------
    // Small-problem (single tile) invocation
    //------------------------------------------------------------------------------

    /// Invoke a single block to sort in-core
    template <
        typename                ActivePolicyT,          ///< Umbrella policy active for the target device
        typename                SingleTileKernelT>      ///< Function type of cub::DeviceRadixSortSingleTileKernel
    CUB_RUNTIME_FUNCTION __forceinline__
    cudaError_t InvokeSingleTile(
        SingleTileKernelT       single_tile_kernel)     ///< [in] Kernel function pointer to parameterization of cub::DeviceRadixSortSingleTileKernel
    {
#ifndef CUB_RUNTIME_ENABLED
        (void)single_tile_kernel;
        // Kernel launch not supported from this device
        return CubDebug(cudaErrorNotSupported );
#else
        cudaError error = cudaSuccess;
        do
        {
            // Return if the caller is simply requesting the size of the storage allocation
            if (d_temp_storage == NULL)
            {
                temp_storage_bytes = 1;
                break;
            }

            // Return if empty problem
            if (num_items == 0)
                break;

            // Log single_tile_kernel configuration
            if (debug_synchronous)
                _CubLog("Invoking single_tile_kernel<<<%d, %d, 0, %lld>>>(), %d items per thread, %d SM occupancy, current bit %d, bit_grain %d\n",
                    1, ActivePolicyT::SingleTilePolicy::BLOCK_THREADS, (long long) stream,
                    ActivePolicyT::SingleTilePolicy::ITEMS_PER_THREAD, 1, begin_bit, ActivePolicyT::SingleTilePolicy::RADIX_BITS);

            // Invoke upsweep_kernel with same grid size as downsweep_kernel
            single_tile_kernel<<<1, ActivePolicyT::SingleTilePolicy::BLOCK_THREADS, 0, stream>>>(
                d_keys.Current(),
                d_keys.Alternate(),
                d_values.Current(),
                d_values.Alternate(),
                num_items,
                begin_bit,
                end_bit);

            // Check for failure to launch
            if (CubDebug(error = cudaPeekAtLastError())) break;

            // Sync the stream if specified to flush runtime errors
            if (debug_synchronous && (CubDebug(error = SyncStream(stream)))) break;

            // Update selector
            d_keys.selector ^= 1;
            d_values.selector ^= 1;
        }
        while (0);

        return error;

#endif // CUB_RUNTIME_ENABLED
    }


    //------------------------------------------------------------------------------
    // Normal problem size invocation
    //------------------------------------------------------------------------------

    /**
     * Invoke a three-kernel sorting pass at the current bit.
     */
    template <typename PassConfigT>
    CUB_RUNTIME_FUNCTION __forceinline__
    cudaError_t InvokePass(
        const KeyT      *d_keys_in,
        KeyT            *d_keys_out,
        const ValueT    *d_values_in,
        ValueT          *d_values_out,
        OffsetT         *d_spine,
        int             spine_length,
        int             &current_bit,
        PassConfigT     &pass_config)
    {
        cudaError error = cudaSuccess;
        do
        {
            int pass_bits = CUB_MIN(pass_config.radix_bits, (end_bit - current_bit));

            // Log upsweep_kernel configuration
            if (debug_synchronous)
                _CubLog("Invoking upsweep_kernel<<<%d, %d, 0, %lld>>>(), %d items per thread, %d SM occupancy, current bit %d, bit_grain %d\n",
                pass_config.even_share.grid_size, pass_config.upsweep_config.block_threads, (long long) stream,
                pass_config.upsweep_config.items_per_thread, pass_config.upsweep_config.sm_occupancy, current_bit, pass_bits);

            // Invoke upsweep_kernel with same grid size as downsweep_kernel
            pass_config.upsweep_kernel<<<pass_config.even_share.grid_size, pass_config.upsweep_config.block_threads, 0, stream>>>(
                d_keys_in,
                d_spine,
                num_items,
                current_bit,
                pass_bits,
                pass_config.even_share);

            // Check for failure to launch
            if (CubDebug(error = cudaPeekAtLastError())) break;

            // Sync the stream if specified to flush runtime errors
            if (debug_synchronous && (CubDebug(error = SyncStream(stream)))) break;

            // Log scan_kernel configuration
            if (debug_synchronous) _CubLog("Invoking scan_kernel<<<%d, %d, 0, %lld>>>(), %d items per thread\n",
                1, pass_config.scan_config.block_threads, (long long) stream, pass_config.scan_config.items_per_thread);

            // Invoke scan_kernel
            pass_config.scan_kernel<<<1, pass_config.scan_config.block_threads, 0, stream>>>(
                d_spine,
                spine_length);

            // Check for failure to launch
            if (CubDebug(error = cudaPeekAtLastError())) break;

            // Sync the stream if specified to flush runtime errors
            if (debug_synchronous && (CubDebug(error = SyncStream(stream)))) break;

            // Log downsweep_kernel configuration
            if (debug_synchronous) _CubLog("Invoking downsweep_kernel<<<%d, %d, 0, %lld>>>(), %d items per thread, %d SM occupancy\n",
                pass_config.even_share.grid_size, pass_config.downsweep_config.block_threads, (long long) stream,
                pass_config.downsweep_config.items_per_thread, pass_config.downsweep_config.sm_occupancy);

            // Invoke downsweep_kernel
            pass_config.downsweep_kernel<<<pass_config.even_share.grid_size, pass_config.downsweep_config.block_threads, 0, stream>>>(
                d_keys_in,
                d_keys_out,
                d_values_in,
                d_values_out,
                d_spine,
                num_items,
                current_bit,
                pass_bits,
                pass_config.even_share);

            // Check for failure to launch
            if (CubDebug(error = cudaPeekAtLastError())) break;

            // Sync the stream if specified to flush runtime errors
            if (debug_synchronous && (CubDebug(error = SyncStream(stream)))) break;

            // Update current bit
            current_bit += pass_bits;
        }
        while (0);

        return error;
    }



    /// Pass configuration structure
    template <
        typename UpsweepKernelT,
        typename ScanKernelT,
        typename DownsweepKernelT>
    struct PassConfig
    {
        UpsweepKernelT          upsweep_kernel;
        KernelConfig            upsweep_config;
        ScanKernelT             scan_kernel;
        KernelConfig            scan_config;
        DownsweepKernelT        downsweep_kernel;
        KernelConfig            downsweep_config;
        int                     radix_bits;
        int                     radix_digits;
        int                     max_downsweep_grid_size;
        GridEvenShare<OffsetT>  even_share;

        /// Initialize pass configuration
        template <
            typename UpsweepPolicyT,
            typename ScanPolicyT,
            typename DownsweepPolicyT>
        CUB_RUNTIME_FUNCTION __forceinline__
        cudaError_t InitPassConfig(
            UpsweepKernelT      upsweep_kernel,
            ScanKernelT         scan_kernel,
            DownsweepKernelT    downsweep_kernel,
            int                 ptx_version,
            int                 sm_count,
            int                 num_items)
        {
            cudaError error = cudaSuccess;
            do
            {
                this->upsweep_kernel    = upsweep_kernel;
                this->scan_kernel       = scan_kernel;
                this->downsweep_kernel  = downsweep_kernel;
                radix_bits              = DownsweepPolicyT::RADIX_BITS;
                radix_digits            = 1 << radix_bits;

                if (CubDebug(error = upsweep_config.Init<UpsweepPolicyT>(upsweep_kernel))) break;
                if (CubDebug(error = scan_config.Init<ScanPolicyT>(scan_kernel))) break;
                if (CubDebug(error = downsweep_config.Init<DownsweepPolicyT>(downsweep_kernel))) break;

                max_downsweep_grid_size = (downsweep_config.sm_occupancy * sm_count) * CUB_SUBSCRIPTION_FACTOR(ptx_version);

                even_share.DispatchInit(
                    num_items,
                    max_downsweep_grid_size,
                    CUB_MAX(downsweep_config.tile_size, upsweep_config.tile_size));

            }
            while (0);
            return error;
        }

    };


    /// Invocation (run multiple digit passes)
    template <
        typename            ActivePolicyT,          ///< Umbrella policy active for the target device
        typename            UpsweepKernelT,         ///< Function type of cub::DeviceRadixSortUpsweepKernel
        typename            ScanKernelT,            ///< Function type of cub::SpineScanKernel
        typename            DownsweepKernelT>       ///< Function type of cub::DeviceRadixSortDownsweepKernel
    CUB_RUNTIME_FUNCTION __forceinline__
    cudaError_t InvokePasses(
        UpsweepKernelT      upsweep_kernel,         ///< [in] Kernel function pointer to parameterization of cub::DeviceRadixSortUpsweepKernel
        UpsweepKernelT      alt_upsweep_kernel,     ///< [in] Alternate kernel function pointer to parameterization of cub::DeviceRadixSortUpsweepKernel
        ScanKernelT         scan_kernel,            ///< [in] Kernel function pointer to parameterization of cub::SpineScanKernel
        DownsweepKernelT    downsweep_kernel,       ///< [in] Kernel function pointer to parameterization of cub::DeviceRadixSortDownsweepKernel
        DownsweepKernelT    alt_downsweep_kernel)   ///< [in] Alternate kernel function pointer to parameterization of cub::DeviceRadixSortDownsweepKernel
    {
#ifndef CUB_RUNTIME_ENABLED
        (void)upsweep_kernel;
        (void)alt_upsweep_kernel;
        (void)scan_kernel;
        (void)downsweep_kernel;
        (void)alt_downsweep_kernel;

        // Kernel launch not supported from this device
        return CubDebug(cudaErrorNotSupported );
#else

        cudaError error = cudaSuccess;
        do
        {
            // Get device ordinal
            int device_ordinal;
            if (CubDebug(error = cudaGetDevice(&device_ordinal))) break;

            // Get SM count
            int sm_count;
            if (CubDebug(error = cudaDeviceGetAttribute (&sm_count, cudaDevAttrMultiProcessorCount, device_ordinal))) break;

            // Init regular and alternate-digit kernel configurations
            PassConfig<UpsweepKernelT, ScanKernelT, DownsweepKernelT> pass_config, alt_pass_config;
            if ((error = pass_config.template InitPassConfig<
                    typename ActivePolicyT::UpsweepPolicy, 
                    typename ActivePolicyT::ScanPolicy, 
                    typename ActivePolicyT::DownsweepPolicy>(
                upsweep_kernel, scan_kernel, downsweep_kernel, ptx_version, sm_count, num_items))) break;

            if ((error = alt_pass_config.template InitPassConfig<
                    typename ActivePolicyT::AltUpsweepPolicy, 
                    typename ActivePolicyT::ScanPolicy, 
                    typename ActivePolicyT::AltDownsweepPolicy>(
                alt_upsweep_kernel, scan_kernel, alt_downsweep_kernel, ptx_version, sm_count, num_items))) break;

            // Get maximum spine length
            int max_grid_size       = CUB_MAX(pass_config.max_downsweep_grid_size, alt_pass_config.max_downsweep_grid_size);
            int spine_length        = (max_grid_size * pass_config.radix_digits) + pass_config.scan_config.tile_size;

            // Temporary storage allocation requirements
            void* allocations[3];
            size_t allocation_sizes[3] =
            {
                spine_length * sizeof(OffsetT),                                         // bytes needed for privatized block digit histograms
                (is_overwrite_okay) ? 0 : num_items * sizeof(KeyT),                     // bytes needed for 3rd keys buffer
                (is_overwrite_okay || (KEYS_ONLY)) ? 0 : num_items * sizeof(ValueT),    // bytes needed for 3rd values buffer
            };

            // Alias the temporary allocations from the single storage blob (or compute the necessary size of the blob)
            if (CubDebug(error = AliasTemporaries(d_temp_storage, temp_storage_bytes, allocations, allocation_sizes))) break;

            // Return if the caller is simply requesting the size of the storage allocation
            if (d_temp_storage == NULL)
                return cudaSuccess;

            // Pass planning.  Run passes of the alternate digit-size configuration until we have an even multiple of our preferred digit size
            int num_bits            = end_bit - begin_bit;
            int num_passes          = (num_bits + pass_config.radix_bits - 1) / pass_config.radix_bits;
            bool is_num_passes_odd  = num_passes & 1;
            int max_alt_passes      = (num_passes * pass_config.radix_bits) - num_bits;
            int alt_end_bit         = CUB_MIN(end_bit, begin_bit + (max_alt_passes * alt_pass_config.radix_bits));

            // Alias the temporary storage allocations
            OffsetT *d_spine = static_cast<OffsetT*>(allocations[0]);

            DoubleBuffer<KeyT> d_keys_remaining_passes(
                (is_overwrite_okay || is_num_passes_odd) ? d_keys.Alternate() : static_cast<KeyT*>(allocations[1]),
                (is_overwrite_okay) ? d_keys.Current() : (is_num_passes_odd) ? static_cast<KeyT*>(allocations[1]) : d_keys.Alternate());

            DoubleBuffer<ValueT> d_values_remaining_passes(
                (is_overwrite_okay || is_num_passes_odd) ? d_values.Alternate() : static_cast<ValueT*>(allocations[2]),
                (is_overwrite_okay) ? d_values.Current() : (is_num_passes_odd) ? static_cast<ValueT*>(allocations[2]) : d_values.Alternate());

            // Run first pass, consuming from the input's current buffers
            int current_bit = begin_bit;
            if (CubDebug(error = InvokePass(
                d_keys.Current(), d_keys_remaining_passes.Current(),
                d_values.Current(), d_values_remaining_passes.Current(),
                d_spine, spine_length, current_bit,
                (current_bit < alt_end_bit) ? alt_pass_config : pass_config))) break;

            // Run remaining passes
            while (current_bit < end_bit)
            {
                if (CubDebug(error = InvokePass(
                    d_keys_remaining_passes.d_buffers[d_keys_remaining_passes.selector],    d_keys_remaining_passes.d_buffers[d_keys_remaining_passes.selector ^ 1],
                    d_values_remaining_passes.d_buffers[d_keys_remaining_passes.selector],  d_values_remaining_passes.d_buffers[d_keys_remaining_passes.selector ^ 1],
                    d_spine, spine_length, current_bit,
                    (current_bit < alt_end_bit) ? alt_pass_config : pass_config))) break;;

                // Invert selectors
                d_keys_remaining_passes.selector ^= 1;
                d_values_remaining_passes.selector ^= 1;
            }

            // Update selector
            if (!is_overwrite_okay) {
                num_passes = 1; // Sorted data always ends up in the other vector
            }

            d_keys.selector = (d_keys.selector + num_passes) & 1;
            d_values.selector = (d_values.selector + num_passes) & 1;
        }
        while (0);

        return error;

#endif // CUB_RUNTIME_ENABLED
    }


    //------------------------------------------------------------------------------
    // Chained policy invocation
    //------------------------------------------------------------------------------

    /// Invocation
    template <typename ActivePolicyT>
    CUB_RUNTIME_FUNCTION __forceinline__
    cudaError_t Invoke()
    {
        typedef typename DispatchRadixSort::MaxPolicy       MaxPolicyT;
        typedef typename ActivePolicyT::SingleTilePolicy    SingleTilePolicyT;

        // Force kernel code-generation in all compiler passes
        if (num_items <= (SingleTilePolicyT::BLOCK_THREADS * SingleTilePolicyT::ITEMS_PER_THREAD))
        {
            // Small, single tile size
            return InvokeSingleTile<ActivePolicyT>(
                DeviceRadixSortSingleTileKernel<MaxPolicyT, IS_DESCENDING, KeyT, ValueT, OffsetT>);
        }
        else
        {
            // Regular size
            return InvokePasses<ActivePolicyT>(
                DeviceRadixSortUpsweepKernel<   MaxPolicyT, false,   IS_DESCENDING, KeyT, OffsetT>,
                DeviceRadixSortUpsweepKernel<   MaxPolicyT, true,    IS_DESCENDING, KeyT, OffsetT>,
                RadixSortScanBinsKernel<        MaxPolicyT, OffsetT>,
                DeviceRadixSortDownsweepKernel< MaxPolicyT, false,   IS_DESCENDING, KeyT, ValueT, OffsetT>,
                DeviceRadixSortDownsweepKernel< MaxPolicyT, true,    IS_DESCENDING, KeyT, ValueT, OffsetT>);
        }
    }


    //------------------------------------------------------------------------------
    // Dispatch entrypoints
    //------------------------------------------------------------------------------

    /**
     * Internal dispatch routine
     */
    CUB_RUNTIME_FUNCTION __forceinline__
    static cudaError_t Dispatch(
        void*                   d_temp_storage,         ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
        size_t                  &temp_storage_bytes,    ///< [in,out] Reference to size in bytes of \p d_temp_storage allocation
        DoubleBuffer<KeyT>      &d_keys,                ///< [in,out] Double-buffer whose current buffer contains the unsorted input keys and, upon return, is updated to point to the sorted output keys
        DoubleBuffer<ValueT>    &d_values,              ///< [in,out] Double-buffer whose current buffer contains the unsorted input values and, upon return, is updated to point to the sorted output values
        OffsetT                 num_items,              ///< [in] Number of items to sort
        int                     begin_bit,              ///< [in] The beginning (least-significant) bit index needed for key comparison
        int                     end_bit,                ///< [in] The past-the-end (most-significant) bit index needed for key comparison
        bool                    is_overwrite_okay,      ///< [in] Whether is okay to overwrite source buffers
        cudaStream_t            stream,                 ///< [in] CUDA stream to launch kernels within.  Default is stream<sub>0</sub>.
        bool                    debug_synchronous)      ///< [in] Whether or not to synchronize the stream after every kernel launch to check for errors.  Also causes launch configurations to be printed to the console.  Default is \p false.
    {
        typedef typename DispatchRadixSort::MaxPolicy MaxPolicyT;

        cudaError_t error;
        do {
            // Get PTX version
            int ptx_version;
            if (CubDebug(error = PtxVersion(ptx_version))) break;

            // Create dispatch functor
            DispatchRadixSort dispatch(
                d_temp_storage, temp_storage_bytes,
                d_keys, d_values,
                num_items, begin_bit, end_bit, is_overwrite_okay,
                stream, debug_synchronous, ptx_version);

            // Dispatch to chained policy
            if (CubDebug(error = MaxPolicyT::Invoke(ptx_version, dispatch))) break;

        } while (0);

        return error;
    }
};




/******************************************************************************
 * Segmented dispatch
 ******************************************************************************/

/**
 * Utility class for dispatching the appropriately-tuned kernels for segmented device-wide radix sort
 */
template <
    bool     IS_DESCENDING,     ///< Whether or not the sorted-order is high-to-low
    typename KeyT,              ///< Key type
    typename ValueT,            ///< Value type
    typename OffsetIteratorT,   ///< Random-access input iterator type for reading segment offsets \iterator
    typename OffsetT>           ///< Signed integer type for global offsets
struct DispatchSegmentedRadixSort :
    DeviceRadixSortPolicy<KeyT, ValueT, OffsetT>
{
    //------------------------------------------------------------------------------
    // Constants
    //------------------------------------------------------------------------------

    enum
    {
        // Whether this is a keys-only (or key-value) sort
        KEYS_ONLY = (Equals<ValueT, NullType>::VALUE),
    };


    //------------------------------------------------------------------------------
    // Parameter members
    //------------------------------------------------------------------------------

    void                    *d_temp_storage;        ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
    size_t                  &temp_storage_bytes;    ///< [in,out] Reference to size in bytes of \p d_temp_storage allocation
    DoubleBuffer<KeyT>      &d_keys;                ///< [in,out] Double-buffer whose current buffer contains the unsorted input keys and, upon return, is updated to point to the sorted output keys
    DoubleBuffer<ValueT>    &d_values;              ///< [in,out] Double-buffer whose current buffer contains the unsorted input values and, upon return, is updated to point to the sorted output values
    OffsetT                 num_items;              ///< [in] Number of items to sort
    OffsetT                 num_segments;           ///< [in] The number of segments that comprise the sorting data
    OffsetIteratorT         d_begin_offsets;        ///< [in] Pointer to the sequence of beginning offsets of length \p num_segments, such that <tt>d_begin_offsets[i]</tt> is the first element of the <em>i</em><sup>th</sup> data segment in <tt>d_keys_*</tt> and <tt>d_values_*</tt>
    OffsetIteratorT         d_end_offsets;          ///< [in] Pointer to the sequence of ending offsets of length \p num_segments, such that <tt>d_end_offsets[i]-1</tt> is the last element of the <em>i</em><sup>th</sup> data segment in <tt>d_keys_*</tt> and <tt>d_values_*</tt>.  If <tt>d_end_offsets[i]-1</tt> <= <tt>d_begin_offsets[i]</tt>, the <em>i</em><sup>th</sup> is considered empty.
    int                     begin_bit;              ///< [in] The beginning (least-significant) bit index needed for key comparison
    int                     end_bit;                ///< [in] The past-the-end (most-significant) bit index needed for key comparison
    cudaStream_t            stream;                 ///< [in] CUDA stream to launch kernels within.  Default is stream<sub>0</sub>.
    bool                    debug_synchronous;      ///< [in] Whether or not to synchronize the stream after every kernel launch to check for errors.  Also causes launch configurations to be printed to the console.  Default is \p false.
    int                     ptx_version;            ///< [in] PTX version
    bool                    is_overwrite_okay;      ///< [in] Whether is okay to overwrite source buffers


    //------------------------------------------------------------------------------
    // Constructors
    //------------------------------------------------------------------------------

    /// Constructor
    CUB_RUNTIME_FUNCTION __forceinline__
    DispatchSegmentedRadixSort(
        void*                   d_temp_storage,
        size_t                  &temp_storage_bytes,
        DoubleBuffer<KeyT>      &d_keys,
        DoubleBuffer<ValueT>    &d_values,
        OffsetT                 num_items,
        OffsetT                 num_segments,
        OffsetIteratorT         d_begin_offsets,
        OffsetIteratorT         d_end_offsets,
        int                     begin_bit,
        int                     end_bit,
        bool                    is_overwrite_okay,
        cudaStream_t            stream,
        bool                    debug_synchronous,
        int                     ptx_version)
    :
        d_temp_storage(d_temp_storage),
        temp_storage_bytes(temp_storage_bytes),
        d_keys(d_keys),
        d_values(d_values),
        num_items(num_items),
        num_segments(num_segments),
        d_begin_offsets(d_begin_offsets),
        d_end_offsets(d_end_offsets),
        begin_bit(begin_bit),
        end_bit(end_bit),
        is_overwrite_okay(is_overwrite_okay),
        stream(stream),
        debug_synchronous(debug_synchronous),
        ptx_version(ptx_version)
    {}


    //------------------------------------------------------------------------------
    // Multi-segment invocation
    //------------------------------------------------------------------------------

    /// Invoke a three-kernel sorting pass at the current bit.
    template <typename PassConfigT>
    CUB_RUNTIME_FUNCTION __forceinline__
    cudaError_t InvokePass(
        const KeyT      *d_keys_in,
        KeyT            *d_keys_out,
        const ValueT    *d_values_in,
        ValueT          *d_values_out,
        int             &current_bit,
        PassConfigT     &pass_config)
    {
        cudaError error = cudaSuccess;
        do
        {
            int pass_bits = CUB_MIN(pass_config.radix_bits, (end_bit - current_bit));

            // Log kernel configuration
            if (debug_synchronous)
                _CubLog("Invoking segmented_kernels<<<%d, %d, 0, %lld>>>(), %d items per thread, %d SM occupancy, current bit %d, bit_grain %d\n",
                    num_segments, pass_config.segmented_config.block_threads, (long long) stream,
                pass_config.segmented_config.items_per_thread, pass_config.segmented_config.sm_occupancy, current_bit, pass_bits);

            pass_config.segmented_kernel<<<num_segments, pass_config.segmented_config.block_threads, 0, stream>>>(
                d_keys_in, d_keys_out,
                d_values_in,  d_values_out,
                d_begin_offsets, d_end_offsets, num_segments,
                current_bit, pass_bits);

            // Check for failure to launch
            if (CubDebug(error = cudaPeekAtLastError())) break;

            // Sync the stream if specified to flush runtime errors
            if (debug_synchronous && (CubDebug(error = SyncStream(stream)))) break;

            // Update current bit
            current_bit += pass_bits;
        }
        while (0);

        return error;
    }


    /// PassConfig data structure
    template <typename SegmentedKernelT>
    struct PassConfig
    {
        SegmentedKernelT    segmented_kernel;
        KernelConfig        segmented_config;
        int                 radix_bits;
        int                 radix_digits;

        /// Initialize pass configuration
        template <typename SegmentedPolicyT>
        CUB_RUNTIME_FUNCTION __forceinline__
        cudaError_t InitPassConfig(SegmentedKernelT segmented_kernel)
        {
            this->segmented_kernel  = segmented_kernel;
            this->radix_bits        = SegmentedPolicyT::RADIX_BITS;
            this->radix_digits      = 1 << radix_bits;

            return CubDebug(segmented_config.Init<SegmentedPolicyT>(segmented_kernel));
        }
    };


    /// Invocation (run multiple digit passes)
    template <
        typename                ActivePolicyT,          ///< Umbrella policy active for the target device
        typename                SegmentedKernelT>       ///< Function type of cub::DeviceSegmentedRadixSortKernel
    CUB_RUNTIME_FUNCTION __forceinline__
    cudaError_t InvokePasses(
        SegmentedKernelT     segmented_kernel,          ///< [in] Kernel function pointer to parameterization of cub::DeviceSegmentedRadixSortKernel
        SegmentedKernelT     alt_segmented_kernel)      ///< [in] Alternate kernel function pointer to parameterization of cub::DeviceSegmentedRadixSortKernel
    {
#ifndef CUB_RUNTIME_ENABLED
      (void)segmented_kernel;
      (void)alt_segmented_kernel;

        // Kernel launch not supported from this device
        return CubDebug(cudaErrorNotSupported );
#else

        cudaError error = cudaSuccess;
        do
        {
            // Init regular and alternate kernel configurations
            PassConfig<SegmentedKernelT> pass_config, alt_pass_config;
            if ((error = pass_config.template       InitPassConfig<typename ActivePolicyT::SegmentedPolicy>(segmented_kernel))) break;
            if ((error = alt_pass_config.template   InitPassConfig<typename ActivePolicyT::AltSegmentedPolicy>(alt_segmented_kernel))) break;

            // Temporary storage allocation requirements
            void* allocations[2];
            size_t allocation_sizes[2] =
            {
                (is_overwrite_okay) ? 0 : num_items * sizeof(KeyT),                      // bytes needed for 3rd keys buffer
                (is_overwrite_okay || (KEYS_ONLY)) ? 0 : num_items * sizeof(ValueT),     // bytes needed for 3rd values buffer
            };

            // Alias the temporary allocations from the single storage blob (or compute the necessary size of the blob)
            if (CubDebug(error = AliasTemporaries(d_temp_storage, temp_storage_bytes, allocations, allocation_sizes))) break;

            // Return if the caller is simply requesting the size of the storage allocation
            if (d_temp_storage == NULL)
            {
                if (temp_storage_bytes == 0)
                    temp_storage_bytes = 1;
                return cudaSuccess;
            }

            // Pass planning.  Run passes of the alternate digit-size configuration until we have an even multiple of our preferred digit size
            int radix_bits          = ActivePolicyT::SegmentedPolicy::RADIX_BITS;
            int alt_radix_bits      = ActivePolicyT::AltSegmentedPolicy::RADIX_BITS;
            int num_bits            = end_bit - begin_bit;
            int num_passes          = (num_bits + radix_bits - 1) / radix_bits;
            bool is_num_passes_odd  = num_passes & 1;
            int max_alt_passes      = (num_passes * radix_bits) - num_bits;
            int alt_end_bit         = CUB_MIN(end_bit, begin_bit + (max_alt_passes * alt_radix_bits));

            DoubleBuffer<KeyT> d_keys_remaining_passes(
                (is_overwrite_okay || is_num_passes_odd) ? d_keys.Alternate() : static_cast<KeyT*>(allocations[0]),
                (is_overwrite_okay) ? d_keys.Current() : (is_num_passes_odd) ? static_cast<KeyT*>(allocations[0]) : d_keys.Alternate());

            DoubleBuffer<ValueT> d_values_remaining_passes(
                (is_overwrite_okay || is_num_passes_odd) ? d_values.Alternate() : static_cast<ValueT*>(allocations[1]),
                (is_overwrite_okay) ? d_values.Current() : (is_num_passes_odd) ? static_cast<ValueT*>(allocations[1]) : d_values.Alternate());

            // Run first pass, consuming from the input's current buffers
            int current_bit = begin_bit;

            if (CubDebug(error = InvokePass(
                d_keys.Current(), d_keys_remaining_passes.Current(),
                d_values.Current(), d_values_remaining_passes.Current(),
                current_bit,
                (current_bit < alt_end_bit) ? alt_pass_config : pass_config))) break;

            // Run remaining passes
            while (current_bit < end_bit)
            {
                if (CubDebug(error = InvokePass(
                    d_keys_remaining_passes.d_buffers[d_keys_remaining_passes.selector],    d_keys_remaining_passes.d_buffers[d_keys_remaining_passes.selector ^ 1],
                    d_values_remaining_passes.d_buffers[d_keys_remaining_passes.selector],  d_values_remaining_passes.d_buffers[d_keys_remaining_passes.selector ^ 1],
                    current_bit,
                    (current_bit < alt_end_bit) ? alt_pass_config : pass_config))) break;

                // Invert selectors and update current bit
                d_keys_remaining_passes.selector ^= 1;
                d_values_remaining_passes.selector ^= 1;
            }

            // Update selector
            if (!is_overwrite_okay) {
                num_passes = 1; // Sorted data always ends up in the other vector
            }

            d_keys.selector = (d_keys.selector + num_passes) & 1;
            d_values.selector = (d_values.selector + num_passes) & 1;
        }
        while (0);

        return error;

#endif // CUB_RUNTIME_ENABLED
    }


    //------------------------------------------------------------------------------
    // Chained policy invocation
    //------------------------------------------------------------------------------

    /// Invocation
    template <typename ActivePolicyT>
    CUB_RUNTIME_FUNCTION __forceinline__
    cudaError_t Invoke()
    {
        typedef typename DispatchSegmentedRadixSort::MaxPolicy MaxPolicyT;

        // Force kernel code-generation in all compiler passes
        return InvokePasses<ActivePolicyT>(
            DeviceSegmentedRadixSortKernel<MaxPolicyT, false,   IS_DESCENDING, KeyT, ValueT, OffsetIteratorT, OffsetT>,
            DeviceSegmentedRadixSortKernel<MaxPolicyT, true,    IS_DESCENDING, KeyT, ValueT, OffsetIteratorT, OffsetT>);
    }


    //------------------------------------------------------------------------------
    // Dispatch entrypoints
    //------------------------------------------------------------------------------


    /// Internal dispatch routine
    CUB_RUNTIME_FUNCTION __forceinline__
    static cudaError_t Dispatch(
        void*                   d_temp_storage,         ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
        size_t                  &temp_storage_bytes,    ///< [in,out] Reference to size in bytes of \p d_temp_storage allocation
        DoubleBuffer<KeyT>      &d_keys,                ///< [in,out] Double-buffer whose current buffer contains the unsorted input keys and, upon return, is updated to point to the sorted output keys
        DoubleBuffer<ValueT>    &d_values,              ///< [in,out] Double-buffer whose current buffer contains the unsorted input values and, upon return, is updated to point to the sorted output values
        int                     num_items,              ///< [in] Number of items to sort
        int                     num_segments,           ///< [in] The number of segments that comprise the sorting data
        OffsetIteratorT         d_begin_offsets,        ///< [in] Pointer to the sequence of beginning offsets of length \p num_segments, such that <tt>d_begin_offsets[i]</tt> is the first element of the <em>i</em><sup>th</sup> data segment in <tt>d_keys_*</tt> and <tt>d_values_*</tt>
        OffsetIteratorT         d_end_offsets,          ///< [in] Pointer to the sequence of ending offsets of length \p num_segments, such that <tt>d_end_offsets[i]-1</tt> is the last element of the <em>i</em><sup>th</sup> data segment in <tt>d_keys_*</tt> and <tt>d_values_*</tt>.  If <tt>d_end_offsets[i]-1</tt> <= <tt>d_begin_offsets[i]</tt>, the <em>i</em><sup>th</sup> is considered empty.
        int                     begin_bit,              ///< [in] The beginning (least-significant) bit index needed for key comparison
        int                     end_bit,                ///< [in] The past-the-end (most-significant) bit index needed for key comparison
        bool                    is_overwrite_okay,      ///< [in] Whether is okay to overwrite source buffers
        cudaStream_t            stream,                 ///< [in] CUDA stream to launch kernels within.  Default is stream<sub>0</sub>.
        bool                    debug_synchronous)      ///< [in] Whether or not to synchronize the stream after every kernel launch to check for errors.  Also causes launch configurations to be printed to the console.  Default is \p false.
    {
        typedef typename DispatchSegmentedRadixSort::MaxPolicy MaxPolicyT;

        cudaError_t error;
        do {
            // Get PTX version
            int ptx_version;
            if (CubDebug(error = PtxVersion(ptx_version))) break;

            // Create dispatch functor
            DispatchSegmentedRadixSort dispatch(
                d_temp_storage, temp_storage_bytes,
                d_keys, d_values,
                num_items, num_segments, d_begin_offsets, d_end_offsets,
                begin_bit, end_bit, is_overwrite_okay,
                stream, debug_synchronous, ptx_version);

            // Dispatch to chained policy
            if (CubDebug(error = MaxPolicyT::Invoke(ptx_version, dispatch))) break;

        } while (0);

        return error;
    }
};


}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)


