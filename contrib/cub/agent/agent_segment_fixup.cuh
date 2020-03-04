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
 * cub::AgentSegmentFixup implements a stateful abstraction of CUDA thread blocks for participating in device-wide reduce-value-by-key.
 */

#pragma once

#include <iterator>

#include "single_pass_scan_operators.cuh"
#include "../block/block_load.cuh"
#include "../block/block_store.cuh"
#include "../block/block_scan.cuh"
#include "../block/block_discontinuity.cuh"
#include "../iterator/cache_modified_input_iterator.cuh"
#include "../iterator/constant_input_iterator.cuh"
#include "../util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {


/******************************************************************************
 * Tuning policy types
 ******************************************************************************/

/**
 * Parameterizable tuning policy type for AgentSegmentFixup
 */
template <
    int                         _BLOCK_THREADS,                 ///< Threads per thread block
    int                         _ITEMS_PER_THREAD,              ///< Items per thread (per tile of input)
    BlockLoadAlgorithm          _LOAD_ALGORITHM,                ///< The BlockLoad algorithm to use
    CacheLoadModifier           _LOAD_MODIFIER,                 ///< Cache load modifier for reading input elements
    BlockScanAlgorithm          _SCAN_ALGORITHM>                ///< The BlockScan algorithm to use
struct AgentSegmentFixupPolicy
{
    enum
    {
        BLOCK_THREADS           = _BLOCK_THREADS,               ///< Threads per thread block
        ITEMS_PER_THREAD        = _ITEMS_PER_THREAD,            ///< Items per thread (per tile of input)
    };

    static const BlockLoadAlgorithm     LOAD_ALGORITHM          = _LOAD_ALGORITHM;      ///< The BlockLoad algorithm to use
    static const CacheLoadModifier      LOAD_MODIFIER           = _LOAD_MODIFIER;       ///< Cache load modifier for reading input elements
    static const BlockScanAlgorithm     SCAN_ALGORITHM          = _SCAN_ALGORITHM;      ///< The BlockScan algorithm to use
};


/******************************************************************************
 * Thread block abstractions
 ******************************************************************************/

/**
 * \brief AgentSegmentFixup implements a stateful abstraction of CUDA thread blocks for participating in device-wide reduce-value-by-key
 */
template <
    typename    AgentSegmentFixupPolicyT,       ///< Parameterized AgentSegmentFixupPolicy tuning policy type
    typename    PairsInputIteratorT,            ///< Random-access input iterator type for keys
    typename    AggregatesOutputIteratorT,      ///< Random-access output iterator type for values
    typename    EqualityOpT,                    ///< KeyT equality operator type
    typename    ReductionOpT,                   ///< ValueT reduction operator type
    typename    OffsetT>                        ///< Signed integer type for global offsets
struct AgentSegmentFixup
{
    //---------------------------------------------------------------------
    // Types and constants
    //---------------------------------------------------------------------

    // Data type of key-value input iterator
    typedef typename std::iterator_traits<PairsInputIteratorT>::value_type KeyValuePairT;

    // Value type
    typedef typename KeyValuePairT::Value ValueT;

    // Tile status descriptor interface type
    typedef ReduceByKeyScanTileState<ValueT, OffsetT> ScanTileStateT;

    // Constants
    enum
    {
        BLOCK_THREADS       = AgentSegmentFixupPolicyT::BLOCK_THREADS,
        ITEMS_PER_THREAD    = AgentSegmentFixupPolicyT::ITEMS_PER_THREAD,
        TILE_ITEMS          = BLOCK_THREADS * ITEMS_PER_THREAD,

        // Whether or not do fixup using RLE + global atomics
        USE_ATOMIC_FIXUP    = (CUB_PTX_ARCH >= 350) && 
                                (Equals<ValueT, float>::VALUE || 
                                 Equals<ValueT, int>::VALUE ||
                                 Equals<ValueT, unsigned int>::VALUE ||
                                 Equals<ValueT, unsigned long long>::VALUE),

        // Whether or not the scan operation has a zero-valued identity value (true if we're performing addition on a primitive type)
        HAS_IDENTITY_ZERO   = (Equals<ReductionOpT, cub::Sum>::VALUE) && (Traits<ValueT>::PRIMITIVE),
    };

    // Cache-modified Input iterator wrapper type (for applying cache modifier) for keys
    typedef typename If<IsPointer<PairsInputIteratorT>::VALUE,
            CacheModifiedInputIterator<AgentSegmentFixupPolicyT::LOAD_MODIFIER, KeyValuePairT, OffsetT>,    // Wrap the native input pointer with CacheModifiedValuesInputIterator
            PairsInputIteratorT>::Type                                                                      // Directly use the supplied input iterator type
        WrappedPairsInputIteratorT;

    // Cache-modified Input iterator wrapper type (for applying cache modifier) for fixup values
    typedef typename If<IsPointer<AggregatesOutputIteratorT>::VALUE,
            CacheModifiedInputIterator<AgentSegmentFixupPolicyT::LOAD_MODIFIER, ValueT, OffsetT>,    // Wrap the native input pointer with CacheModifiedValuesInputIterator
            AggregatesOutputIteratorT>::Type                                                        // Directly use the supplied input iterator type
        WrappedFixupInputIteratorT;

    // Reduce-value-by-segment scan operator
    typedef ReduceByKeyOp<cub::Sum> ReduceBySegmentOpT;

    // Parameterized BlockLoad type for pairs
    typedef BlockLoad<
            KeyValuePairT,
            BLOCK_THREADS,
            ITEMS_PER_THREAD,
            AgentSegmentFixupPolicyT::LOAD_ALGORITHM>
        BlockLoadPairs;

    // Parameterized BlockScan type
    typedef BlockScan<
            KeyValuePairT,
            BLOCK_THREADS,
            AgentSegmentFixupPolicyT::SCAN_ALGORITHM>
        BlockScanT;

    // Callback type for obtaining tile prefix during block scan
    typedef TilePrefixCallbackOp<
            KeyValuePairT,
            ReduceBySegmentOpT,
            ScanTileStateT>
        TilePrefixCallbackOpT;

    // Shared memory type for this thread block
    union _TempStorage
    {
        struct
        {
            typename BlockScanT::TempStorage                scan;           // Smem needed for tile scanning
            typename TilePrefixCallbackOpT::TempStorage     prefix;         // Smem needed for cooperative prefix callback
        };

        // Smem needed for loading keys
        typename BlockLoadPairs::TempStorage load_pairs;
    };

    // Alias wrapper allowing storage to be unioned
    struct TempStorage : Uninitialized<_TempStorage> {};


    //---------------------------------------------------------------------
    // Per-thread fields
    //---------------------------------------------------------------------

    _TempStorage&                   temp_storage;       ///< Reference to temp_storage
    WrappedPairsInputIteratorT      d_pairs_in;          ///< Input keys
    AggregatesOutputIteratorT       d_aggregates_out;   ///< Output value aggregates
    WrappedFixupInputIteratorT      d_fixup_in;         ///< Fixup input values
    InequalityWrapper<EqualityOpT>  inequality_op;      ///< KeyT inequality operator
    ReductionOpT                    reduction_op;       ///< Reduction operator
    ReduceBySegmentOpT              scan_op;            ///< Reduce-by-segment scan operator


    //---------------------------------------------------------------------
    // Constructor
    //---------------------------------------------------------------------

    // Constructor
    __device__ __forceinline__
    AgentSegmentFixup(
        TempStorage&                temp_storage,       ///< Reference to temp_storage
        PairsInputIteratorT         d_pairs_in,          ///< Input keys
        AggregatesOutputIteratorT   d_aggregates_out,   ///< Output value aggregates
        EqualityOpT                 equality_op,        ///< KeyT equality operator
        ReductionOpT                reduction_op)       ///< ValueT reduction operator
    :
        temp_storage(temp_storage.Alias()),
        d_pairs_in(d_pairs_in),
        d_aggregates_out(d_aggregates_out),
        d_fixup_in(d_aggregates_out),
        inequality_op(equality_op),
        reduction_op(reduction_op),
        scan_op(reduction_op)
    {}


    //---------------------------------------------------------------------
    // Cooperatively scan a device-wide sequence of tiles with other CTAs
    //---------------------------------------------------------------------


    /**
     * Process input tile.  Specialized for atomic-fixup
     */
    template <bool IS_LAST_TILE>
    __device__ __forceinline__ void ConsumeTile(
        OffsetT             num_remaining,      ///< Number of global input items remaining (including this tile)
        int                 tile_idx,           ///< Tile index
        OffsetT             tile_offset,        ///< Tile offset
        ScanTileStateT&     tile_state,         ///< Global tile state descriptor
        Int2Type<true>      use_atomic_fixup)   ///< Marker whether to use atomicAdd (instead of reduce-by-key)
    {
        KeyValuePairT   pairs[ITEMS_PER_THREAD];

        // Load pairs
        KeyValuePairT oob_pair;
        oob_pair.key = -1;

        if (IS_LAST_TILE)
            BlockLoadPairs(temp_storage.load_pairs).Load(d_pairs_in + tile_offset, pairs, num_remaining, oob_pair);
        else
            BlockLoadPairs(temp_storage.load_pairs).Load(d_pairs_in + tile_offset, pairs);

        // RLE 
        #pragma unroll
        for (int ITEM = 1; ITEM < ITEMS_PER_THREAD; ++ITEM)
        {
            ValueT* d_scatter = d_aggregates_out + pairs[ITEM - 1].key;
            if (pairs[ITEM].key != pairs[ITEM - 1].key)
                atomicAdd(d_scatter, pairs[ITEM - 1].value);
            else
                pairs[ITEM].value = reduction_op(pairs[ITEM - 1].value, pairs[ITEM].value);
        }

        // Flush last item if valid
        ValueT* d_scatter = d_aggregates_out + pairs[ITEMS_PER_THREAD - 1].key;
        if ((!IS_LAST_TILE) || (pairs[ITEMS_PER_THREAD - 1].key >= 0))
            atomicAdd(d_scatter, pairs[ITEMS_PER_THREAD - 1].value);
    }


    /**
     * Process input tile.  Specialized for reduce-by-key fixup
     */
    template <bool IS_LAST_TILE>
    __device__ __forceinline__ void ConsumeTile(
        OffsetT             num_remaining,      ///< Number of global input items remaining (including this tile)
        int                 tile_idx,           ///< Tile index
        OffsetT             tile_offset,        ///< Tile offset
        ScanTileStateT&     tile_state,         ///< Global tile state descriptor
        Int2Type<false>     use_atomic_fixup)   ///< Marker whether to use atomicAdd (instead of reduce-by-key)
    {
        KeyValuePairT   pairs[ITEMS_PER_THREAD];
        KeyValuePairT   scatter_pairs[ITEMS_PER_THREAD];

        // Load pairs
        KeyValuePairT oob_pair;
        oob_pair.key = -1;

        if (IS_LAST_TILE)
            BlockLoadPairs(temp_storage.load_pairs).Load(d_pairs_in + tile_offset, pairs, num_remaining, oob_pair);
        else
            BlockLoadPairs(temp_storage.load_pairs).Load(d_pairs_in + tile_offset, pairs);

        CTA_SYNC();

        KeyValuePairT tile_aggregate;
        if (tile_idx == 0)
        {
            // Exclusive scan of values and segment_flags
            BlockScanT(temp_storage.scan).ExclusiveScan(pairs, scatter_pairs, scan_op, tile_aggregate);

            // Update tile status if this is not the last tile
            if (threadIdx.x == 0)
            {
                // Set first segment id to not trigger a flush (invalid from exclusive scan)
                scatter_pairs[0].key = pairs[0].key;

                if (!IS_LAST_TILE)
                    tile_state.SetInclusive(0, tile_aggregate);

            }
        }
        else
        {
            // Exclusive scan of values and segment_flags
            TilePrefixCallbackOpT prefix_op(tile_state, temp_storage.prefix, scan_op, tile_idx);
            BlockScanT(temp_storage.scan).ExclusiveScan(pairs, scatter_pairs, scan_op, prefix_op);
            tile_aggregate = prefix_op.GetBlockAggregate();
        }

        // Scatter updated values
        #pragma unroll
        for (int ITEM = 0; ITEM < ITEMS_PER_THREAD; ++ITEM)
        {
            if (scatter_pairs[ITEM].key != pairs[ITEM].key)
            {
                // Update the value at the key location
                ValueT value    = d_fixup_in[scatter_pairs[ITEM].key];
                value           = reduction_op(value, scatter_pairs[ITEM].value);

                d_aggregates_out[scatter_pairs[ITEM].key] = value;
            }
        }

        // Finalize the last item
        if (IS_LAST_TILE)
        {
            // Last thread will output final count and last item, if necessary
            if (threadIdx.x == BLOCK_THREADS - 1)
            {
                // If the last tile is a whole tile, the inclusive prefix contains accumulated value reduction for the last segment
                if (num_remaining == TILE_ITEMS)
                {
                    // Update the value at the key location
                    OffsetT last_key = pairs[ITEMS_PER_THREAD - 1].key;
                    d_aggregates_out[last_key] = reduction_op(tile_aggregate.value, d_fixup_in[last_key]);
                }
            }
        }
    }


    /**
     * Scan tiles of items as part of a dynamic chained scan
     */
    __device__ __forceinline__ void ConsumeRange(
        int                 num_items,          ///< Total number of input items
        int                 num_tiles,          ///< Total number of input tiles
        ScanTileStateT&     tile_state)         ///< Global tile state descriptor
    {
        // Blocks are launched in increasing order, so just assign one tile per block
        int     tile_idx        = (blockIdx.x * gridDim.y) + blockIdx.y;    // Current tile index
        OffsetT tile_offset     = tile_idx * TILE_ITEMS;                    // Global offset for the current tile
        OffsetT num_remaining   = num_items - tile_offset;                  // Remaining items (including this tile)

        if (num_remaining > TILE_ITEMS)
        {
            // Not the last tile (full)
            ConsumeTile<false>(num_remaining, tile_idx, tile_offset, tile_state, Int2Type<USE_ATOMIC_FIXUP>());
        }
        else if (num_remaining > 0)
        {
            // The last tile (possibly partially-full)
            ConsumeTile<true>(num_remaining, tile_idx, tile_offset, tile_state, Int2Type<USE_ATOMIC_FIXUP>());
        }
    }

};


}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)

