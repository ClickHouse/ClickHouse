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
 * AgentRadixSortUpsweep implements a stateful abstraction of CUDA thread blocks for participating in device-wide radix sort upsweep .
 */

#pragma once

#include "../thread/thread_reduce.cuh"
#include "../thread/thread_load.cuh"
#include "../warp/warp_reduce.cuh"
#include "../block/block_load.cuh"
#include "../util_type.cuh"
#include "../iterator/cache_modified_input_iterator.cuh"
#include "../util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {

/******************************************************************************
 * Tuning policy types
 ******************************************************************************/

/**
 * Parameterizable tuning policy type for AgentRadixSortUpsweep
 */
template <
    int                 _BLOCK_THREADS,     ///< Threads per thread block
    int                 _ITEMS_PER_THREAD,  ///< Items per thread (per tile of input)
    CacheLoadModifier   _LOAD_MODIFIER,     ///< Cache load modifier for reading keys
    int                 _RADIX_BITS>        ///< The number of radix bits, i.e., log2(bins)
struct AgentRadixSortUpsweepPolicy
{
    enum
    {
        BLOCK_THREADS       = _BLOCK_THREADS,       ///< Threads per thread block
        ITEMS_PER_THREAD    = _ITEMS_PER_THREAD,    ///< Items per thread (per tile of input)
        RADIX_BITS          = _RADIX_BITS,          ///< The number of radix bits, i.e., log2(bins)
    };

    static const CacheLoadModifier LOAD_MODIFIER = _LOAD_MODIFIER;      ///< Cache load modifier for reading keys
};


/******************************************************************************
 * Thread block abstractions
 ******************************************************************************/

/**
 * \brief AgentRadixSortUpsweep implements a stateful abstraction of CUDA thread blocks for participating in device-wide radix sort upsweep .
 */
template <
    typename AgentRadixSortUpsweepPolicy,   ///< Parameterized AgentRadixSortUpsweepPolicy tuning policy type
    typename KeyT,                          ///< KeyT type
    typename OffsetT>                       ///< Signed integer type for global offsets
struct AgentRadixSortUpsweep
{

    //---------------------------------------------------------------------
    // Type definitions and constants
    //---------------------------------------------------------------------

    typedef typename Traits<KeyT>::UnsignedBits UnsignedBits;

    // Integer type for digit counters (to be packed into words of PackedCounters)
    typedef unsigned char DigitCounter;

    // Integer type for packing DigitCounters into columns of shared memory banks
    typedef unsigned int PackedCounter;

    static const CacheLoadModifier LOAD_MODIFIER = AgentRadixSortUpsweepPolicy::LOAD_MODIFIER;

    enum
    {
        RADIX_BITS              = AgentRadixSortUpsweepPolicy::RADIX_BITS,
        BLOCK_THREADS           = AgentRadixSortUpsweepPolicy::BLOCK_THREADS,
        KEYS_PER_THREAD         = AgentRadixSortUpsweepPolicy::ITEMS_PER_THREAD,

        RADIX_DIGITS            = 1 << RADIX_BITS,

        LOG_WARP_THREADS        = CUB_PTX_LOG_WARP_THREADS,
        WARP_THREADS            = 1 << LOG_WARP_THREADS,
        WARPS                   = (BLOCK_THREADS + WARP_THREADS - 1) / WARP_THREADS,

        TILE_ITEMS              = BLOCK_THREADS * KEYS_PER_THREAD,

        BYTES_PER_COUNTER       = sizeof(DigitCounter),
        LOG_BYTES_PER_COUNTER   = Log2<BYTES_PER_COUNTER>::VALUE,

        PACKING_RATIO           = sizeof(PackedCounter) / sizeof(DigitCounter),
        LOG_PACKING_RATIO       = Log2<PACKING_RATIO>::VALUE,

        LOG_COUNTER_LANES       = CUB_MAX(0, RADIX_BITS - LOG_PACKING_RATIO),
        COUNTER_LANES           = 1 << LOG_COUNTER_LANES,

        // To prevent counter overflow, we must periodically unpack and aggregate the
        // digit counters back into registers.  Each counter lane is assigned to a
        // warp for aggregation.

        LANES_PER_WARP          = CUB_MAX(1, (COUNTER_LANES + WARPS - 1) / WARPS),

        // Unroll tiles in batches without risk of counter overflow
        UNROLL_COUNT            = CUB_MIN(64, 255 / KEYS_PER_THREAD),
        UNROLLED_ELEMENTS       = UNROLL_COUNT * TILE_ITEMS,
    };


    // Input iterator wrapper type (for applying cache modifier)s
    typedef CacheModifiedInputIterator<LOAD_MODIFIER, UnsignedBits, OffsetT> KeysItr;

    /**
     * Shared memory storage layout
     */
    union __align__(16) _TempStorage
    {
        DigitCounter    thread_counters[COUNTER_LANES][BLOCK_THREADS][PACKING_RATIO];
        PackedCounter   packed_thread_counters[COUNTER_LANES][BLOCK_THREADS];
        OffsetT         block_counters[WARP_THREADS][RADIX_DIGITS];
    };


    /// Alias wrapper allowing storage to be unioned
    struct TempStorage : Uninitialized<_TempStorage> {};


    //---------------------------------------------------------------------
    // Thread fields (aggregate state bundle)
    //---------------------------------------------------------------------

    // Shared storage for this CTA
    _TempStorage    &temp_storage;

    // Thread-local counters for periodically aggregating composite-counter lanes
    OffsetT         local_counts[LANES_PER_WARP][PACKING_RATIO];

    // Input and output device pointers
    KeysItr         d_keys_in;

    // The least-significant bit position of the current digit to extract
    int             current_bit;

    // Number of bits in current digit
    int             num_bits;



    //---------------------------------------------------------------------
    // Helper structure for templated iteration
    //---------------------------------------------------------------------

    // Iterate
    template <int COUNT, int MAX>
    struct Iterate
    {
        // BucketKeys
        static __device__ __forceinline__ void BucketKeys(
            AgentRadixSortUpsweep       &cta,
            UnsignedBits                keys[KEYS_PER_THREAD])
        {
            cta.Bucket(keys[COUNT]);

            // Next
            Iterate<COUNT + 1, MAX>::BucketKeys(cta, keys);
        }
    };

    // Terminate
    template <int MAX>
    struct Iterate<MAX, MAX>
    {
        // BucketKeys
        static __device__ __forceinline__ void BucketKeys(AgentRadixSortUpsweep &/*cta*/, UnsignedBits /*keys*/[KEYS_PER_THREAD]) {}
    };


    //---------------------------------------------------------------------
    // Utility methods
    //---------------------------------------------------------------------

    /**
     * Decode a key and increment corresponding smem digit counter
     */
    __device__ __forceinline__ void Bucket(UnsignedBits key)
    {
        // Perform transform op
        UnsignedBits converted_key = Traits<KeyT>::TwiddleIn(key);

        // Extract current digit bits
        UnsignedBits digit = BFE(converted_key, current_bit, num_bits);

        // Get sub-counter offset
        UnsignedBits sub_counter = digit & (PACKING_RATIO - 1);

        // Get row offset
        UnsignedBits row_offset = digit >> LOG_PACKING_RATIO;

        // Increment counter
        temp_storage.thread_counters[row_offset][threadIdx.x][sub_counter]++;
    }


    /**
     * Reset composite counters
     */
    __device__ __forceinline__ void ResetDigitCounters()
    {
        #pragma unroll
        for (int LANE = 0; LANE < COUNTER_LANES; LANE++)
        {
            temp_storage.packed_thread_counters[LANE][threadIdx.x] = 0;
        }
    }


    /**
     * Reset the unpacked counters in each thread
     */
    __device__ __forceinline__ void ResetUnpackedCounters()
    {
        #pragma unroll
        for (int LANE = 0; LANE < LANES_PER_WARP; LANE++)
        {
            #pragma unroll
            for (int UNPACKED_COUNTER = 0; UNPACKED_COUNTER < PACKING_RATIO; UNPACKED_COUNTER++)
            {
                local_counts[LANE][UNPACKED_COUNTER] = 0;
            }
        }
    }


    /**
     * Extracts and aggregates the digit counters for each counter lane
     * owned by this warp
     */
    __device__ __forceinline__ void UnpackDigitCounts()
    {
        unsigned int warp_id = threadIdx.x >> LOG_WARP_THREADS;
        unsigned int warp_tid = LaneId();

        #pragma unroll
        for (int LANE = 0; LANE < LANES_PER_WARP; LANE++)
        {
            const int counter_lane = (LANE * WARPS) + warp_id;
            if (counter_lane < COUNTER_LANES)
            {
                #pragma unroll
                for (int PACKED_COUNTER = 0; PACKED_COUNTER < BLOCK_THREADS; PACKED_COUNTER += WARP_THREADS)
                {
                    #pragma unroll
                    for (int UNPACKED_COUNTER = 0; UNPACKED_COUNTER < PACKING_RATIO; UNPACKED_COUNTER++)
                    {
                        OffsetT counter = temp_storage.thread_counters[counter_lane][warp_tid + PACKED_COUNTER][UNPACKED_COUNTER];
                        local_counts[LANE][UNPACKED_COUNTER] += counter;
                    }
                }
            }
        }
    }


    /**
     * Processes a single, full tile
     */
    __device__ __forceinline__ void ProcessFullTile(OffsetT block_offset)
    {
        // Tile of keys
        UnsignedBits keys[KEYS_PER_THREAD];

        LoadDirectStriped<BLOCK_THREADS>(threadIdx.x, d_keys_in + block_offset, keys);

        // Prevent hoisting
        CTA_SYNC();

        // Bucket tile of keys
        Iterate<0, KEYS_PER_THREAD>::BucketKeys(*this, keys);
    }


    /**
     * Processes a single load (may have some threads masked off)
     */
    __device__ __forceinline__ void ProcessPartialTile(
        OffsetT block_offset,
        const OffsetT &block_end)
    {
        // Process partial tile if necessary using single loads
        block_offset += threadIdx.x;
        while (block_offset < block_end)
        {
            // Load and bucket key
            UnsignedBits key = d_keys_in[block_offset];
            Bucket(key);
            block_offset += BLOCK_THREADS;
        }
    }


    //---------------------------------------------------------------------
    // Interface
    //---------------------------------------------------------------------

    /**
     * Constructor
     */
    __device__ __forceinline__ AgentRadixSortUpsweep(
        TempStorage &temp_storage,
        const KeyT  *d_keys_in,
        int         current_bit,
        int         num_bits)
    :
        temp_storage(temp_storage.Alias()),
        d_keys_in(reinterpret_cast<const UnsignedBits*>(d_keys_in)),
        current_bit(current_bit),
        num_bits(num_bits)
    {}


    /**
     * Compute radix digit histograms from a segment of input tiles.
     */
    __device__ __forceinline__ void ProcessRegion(
        OffsetT          block_offset,
        const OffsetT    &block_end)
    {
        // Reset digit counters in smem and unpacked counters in registers
        ResetDigitCounters();
        ResetUnpackedCounters();

        // Unroll batches of full tiles
        while (block_offset + UNROLLED_ELEMENTS <= block_end)
        {
            for (int i = 0; i < UNROLL_COUNT; ++i)
            {
                ProcessFullTile(block_offset);
                block_offset += TILE_ITEMS;
            }

            CTA_SYNC();

            // Aggregate back into local_count registers to prevent overflow
            UnpackDigitCounts();

            CTA_SYNC();

            // Reset composite counters in lanes
            ResetDigitCounters();
        }

        // Unroll single full tiles
        while (block_offset + TILE_ITEMS <= block_end)
        {
            ProcessFullTile(block_offset);
            block_offset += TILE_ITEMS;
        }

        // Process partial tile if necessary
        ProcessPartialTile(
            block_offset,
            block_end);

        CTA_SYNC();

        // Aggregate back into local_count registers
        UnpackDigitCounts();
    }


    /**
     * Extract counts (saving them to the external array)
     */
    template <bool IS_DESCENDING>
    __device__ __forceinline__ void ExtractCounts(
        OffsetT     *counters,
        int         bin_stride = 1,
        int         bin_offset = 0)
    {
        unsigned int warp_id    = threadIdx.x >> LOG_WARP_THREADS;
        unsigned int warp_tid   = LaneId();

        // Place unpacked digit counters in shared memory
        #pragma unroll
        for (int LANE = 0; LANE < LANES_PER_WARP; LANE++)
        {
            int counter_lane = (LANE * WARPS) + warp_id;
            if (counter_lane < COUNTER_LANES)
            {
                int digit_row = counter_lane << LOG_PACKING_RATIO;

                #pragma unroll
                for (int UNPACKED_COUNTER = 0; UNPACKED_COUNTER < PACKING_RATIO; UNPACKED_COUNTER++)
                {
                    int bin_idx = digit_row + UNPACKED_COUNTER;

                    temp_storage.block_counters[warp_tid][bin_idx] =
                        local_counts[LANE][UNPACKED_COUNTER];
                }
            }
        }

        CTA_SYNC();

        // Rake-reduce bin_count reductions

        // Whole blocks
        #pragma unroll
        for (int BIN_BASE   = RADIX_DIGITS % BLOCK_THREADS;
            (BIN_BASE + BLOCK_THREADS) <= RADIX_DIGITS;
            BIN_BASE += BLOCK_THREADS)
        {
            int bin_idx = BIN_BASE + threadIdx.x;

            OffsetT bin_count = 0;
            #pragma unroll
            for (int i = 0; i < WARP_THREADS; ++i)
                bin_count += temp_storage.block_counters[i][bin_idx];

            if (IS_DESCENDING)
                bin_idx = RADIX_DIGITS - bin_idx - 1;

            counters[(bin_stride * bin_idx) + bin_offset] = bin_count;
        }

        // Remainder
        if ((RADIX_DIGITS % BLOCK_THREADS != 0) && (threadIdx.x < RADIX_DIGITS))
        {
            int bin_idx = threadIdx.x;

            OffsetT bin_count = 0;
            #pragma unroll
            for (int i = 0; i < WARP_THREADS; ++i)
                bin_count += temp_storage.block_counters[i][bin_idx];

            if (IS_DESCENDING)
                bin_idx = RADIX_DIGITS - bin_idx - 1;

            counters[(bin_stride * bin_idx) + bin_offset] = bin_count;
        }
    }


    /**
     * Extract counts
     */
    template <int BINS_TRACKED_PER_THREAD>
    __device__ __forceinline__ void ExtractCounts(
        OffsetT (&bin_count)[BINS_TRACKED_PER_THREAD])  ///< [out] The exclusive prefix sum for the digits [(threadIdx.x * BINS_TRACKED_PER_THREAD) ... (threadIdx.x * BINS_TRACKED_PER_THREAD) + BINS_TRACKED_PER_THREAD - 1]
    {
        unsigned int warp_id    = threadIdx.x >> LOG_WARP_THREADS;
        unsigned int warp_tid   = LaneId();

        // Place unpacked digit counters in shared memory
        #pragma unroll
        for (int LANE = 0; LANE < LANES_PER_WARP; LANE++)
        {
            int counter_lane = (LANE * WARPS) + warp_id;
            if (counter_lane < COUNTER_LANES)
            {
                int digit_row = counter_lane << LOG_PACKING_RATIO;

                #pragma unroll
                for (int UNPACKED_COUNTER = 0; UNPACKED_COUNTER < PACKING_RATIO; UNPACKED_COUNTER++)
                {
                    int bin_idx = digit_row + UNPACKED_COUNTER;

                    temp_storage.block_counters[warp_tid][bin_idx] =
                        local_counts[LANE][UNPACKED_COUNTER];
                }
            }
        }

        CTA_SYNC();

        // Rake-reduce bin_count reductions
        #pragma unroll
        for (int track = 0; track < BINS_TRACKED_PER_THREAD; ++track)
        {
            int bin_idx = (threadIdx.x * BINS_TRACKED_PER_THREAD) + track;

            if ((BLOCK_THREADS == RADIX_DIGITS) || (bin_idx < RADIX_DIGITS))
            {
                bin_count[track] = 0;

                #pragma unroll
                for (int i = 0; i < WARP_THREADS; ++i)
                    bin_count[track] += temp_storage.block_counters[i][bin_idx];
            }
        }
    }

};


}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)

