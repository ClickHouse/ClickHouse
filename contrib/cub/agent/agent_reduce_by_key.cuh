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
 * cub::AgentReduceByKey implements a stateful abstraction of CUDA thread blocks for participating in device-wide reduce-value-by-key.
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
 * Parameterizable tuning policy type for AgentReduceByKey
 */
template <
    int                         _BLOCK_THREADS,                 ///< Threads per thread block
    int                         _ITEMS_PER_THREAD,              ///< Items per thread (per tile of input)
    BlockLoadAlgorithm          _LOAD_ALGORITHM,                ///< The BlockLoad algorithm to use
    CacheLoadModifier           _LOAD_MODIFIER,                 ///< Cache load modifier for reading input elements
    BlockScanAlgorithm          _SCAN_ALGORITHM>                ///< The BlockScan algorithm to use
struct AgentReduceByKeyPolicy
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
 * \brief AgentReduceByKey implements a stateful abstraction of CUDA thread blocks for participating in device-wide reduce-value-by-key
 */
template <
    typename    AgentReduceByKeyPolicyT,        ///< Parameterized AgentReduceByKeyPolicy tuning policy type
    typename    KeysInputIteratorT,             ///< Random-access input iterator type for keys
    typename    UniqueOutputIteratorT,          ///< Random-access output iterator type for keys
    typename    ValuesInputIteratorT,           ///< Random-access input iterator type for values
    typename    AggregatesOutputIteratorT,      ///< Random-access output iterator type for values
    typename    NumRunsOutputIteratorT,         ///< Output iterator type for recording number of items selected
    typename    EqualityOpT,                    ///< KeyT equality operator type
    typename    ReductionOpT,                   ///< ValueT reduction operator type
    typename    OffsetT>                        ///< Signed integer type for global offsets
struct AgentReduceByKey
{
    //---------------------------------------------------------------------
    // Types and constants
    //---------------------------------------------------------------------

    // The input keys type
    typedef typename std::iterator_traits<KeysInputIteratorT>::value_type KeyInputT;

    // The output keys type
    typedef typename If<(Equals<typename std::iterator_traits<UniqueOutputIteratorT>::value_type, void>::VALUE),    // KeyOutputT =  (if output iterator's value type is void) ?
        typename std::iterator_traits<KeysInputIteratorT>::value_type,                                              // ... then the input iterator's value type,
        typename std::iterator_traits<UniqueOutputIteratorT>::value_type>::Type KeyOutputT;                         // ... else the output iterator's value type

    // The input values type
    typedef typename std::iterator_traits<ValuesInputIteratorT>::value_type ValueInputT;

    // The output values type
    typedef typename If<(Equals<typename std::iterator_traits<AggregatesOutputIteratorT>::value_type, void>::VALUE),    // ValueOutputT =  (if output iterator's value type is void) ?
        typename std::iterator_traits<ValuesInputIteratorT>::value_type,                                                // ... then the input iterator's value type,
        typename std::iterator_traits<AggregatesOutputIteratorT>::value_type>::Type ValueOutputT;                       // ... else the output iterator's value type

    // Tuple type for scanning (pairs accumulated segment-value with segment-index)
    typedef KeyValuePair<OffsetT, ValueOutputT> OffsetValuePairT;

    // Tuple type for pairing keys and values
    typedef KeyValuePair<KeyOutputT, ValueOutputT> KeyValuePairT;

    // Tile status descriptor interface type
    typedef ReduceByKeyScanTileState<ValueOutputT, OffsetT> ScanTileStateT;

    // Guarded inequality functor
    template <typename _EqualityOpT>
    struct GuardedInequalityWrapper
    {
        _EqualityOpT     op;             ///< Wrapped equality operator
        int             num_remaining;  ///< Items remaining

        /// Constructor
        __host__ __device__ __forceinline__
        GuardedInequalityWrapper(_EqualityOpT op, int num_remaining) : op(op), num_remaining(num_remaining) {}

        /// Boolean inequality operator, returns <tt>(a != b)</tt>
        template <typename T>
        __host__ __device__ __forceinline__ bool operator()(const T &a, const T &b, int idx) const
        {
            if (idx < num_remaining)
                return !op(a, b);   // In bounds

            // Return true if first out-of-bounds item, false otherwise
            return (idx == num_remaining);
       }
    };


    // Constants
    enum
    {
        BLOCK_THREADS       = AgentReduceByKeyPolicyT::BLOCK_THREADS,
        ITEMS_PER_THREAD    = AgentReduceByKeyPolicyT::ITEMS_PER_THREAD,
        TILE_ITEMS          = BLOCK_THREADS * ITEMS_PER_THREAD,
        TWO_PHASE_SCATTER   = (ITEMS_PER_THREAD > 1),

        // Whether or not the scan operation has a zero-valued identity value (true if we're performing addition on a primitive type)
        HAS_IDENTITY_ZERO   = (Equals<ReductionOpT, cub::Sum>::VALUE) && (Traits<ValueOutputT>::PRIMITIVE),
    };

    // Cache-modified Input iterator wrapper type (for applying cache modifier) for keys
    typedef typename If<IsPointer<KeysInputIteratorT>::VALUE,
            CacheModifiedInputIterator<AgentReduceByKeyPolicyT::LOAD_MODIFIER, KeyInputT, OffsetT>,     // Wrap the native input pointer with CacheModifiedValuesInputIterator
            KeysInputIteratorT>::Type                                                                   // Directly use the supplied input iterator type
        WrappedKeysInputIteratorT;

    // Cache-modified Input iterator wrapper type (for applying cache modifier) for values
    typedef typename If<IsPointer<ValuesInputIteratorT>::VALUE,
            CacheModifiedInputIterator<AgentReduceByKeyPolicyT::LOAD_MODIFIER, ValueInputT, OffsetT>,   // Wrap the native input pointer with CacheModifiedValuesInputIterator
            ValuesInputIteratorT>::Type                                                                 // Directly use the supplied input iterator type
        WrappedValuesInputIteratorT;

    // Cache-modified Input iterator wrapper type (for applying cache modifier) for fixup values
    typedef typename If<IsPointer<AggregatesOutputIteratorT>::VALUE,
            CacheModifiedInputIterator<AgentReduceByKeyPolicyT::LOAD_MODIFIER, ValueInputT, OffsetT>,   // Wrap the native input pointer with CacheModifiedValuesInputIterator
            AggregatesOutputIteratorT>::Type                                                            // Directly use the supplied input iterator type
        WrappedFixupInputIteratorT;

    // Reduce-value-by-segment scan operator
    typedef ReduceBySegmentOp<ReductionOpT> ReduceBySegmentOpT;

    // Parameterized BlockLoad type for keys
    typedef BlockLoad<
            KeyOutputT,
            BLOCK_THREADS,
            ITEMS_PER_THREAD,
            AgentReduceByKeyPolicyT::LOAD_ALGORITHM>
        BlockLoadKeysT;

    // Parameterized BlockLoad type for values
    typedef BlockLoad<
            ValueOutputT,
            BLOCK_THREADS,
            ITEMS_PER_THREAD,
            AgentReduceByKeyPolicyT::LOAD_ALGORITHM>
        BlockLoadValuesT;

    // Parameterized BlockDiscontinuity type for keys
    typedef BlockDiscontinuity<
            KeyOutputT,
            BLOCK_THREADS>
        BlockDiscontinuityKeys;

    // Parameterized BlockScan type
    typedef BlockScan<
            OffsetValuePairT,
            BLOCK_THREADS,
            AgentReduceByKeyPolicyT::SCAN_ALGORITHM>
        BlockScanT;

    // Callback type for obtaining tile prefix during block scan
    typedef TilePrefixCallbackOp<
            OffsetValuePairT,
            ReduceBySegmentOpT,
            ScanTileStateT>
        TilePrefixCallbackOpT;

    // Key and value exchange types
    typedef KeyOutputT    KeyExchangeT[TILE_ITEMS + 1];
    typedef ValueOutputT  ValueExchangeT[TILE_ITEMS + 1];

    // Shared memory type for this thread block
    union _TempStorage
    {
        struct
        {
            typename BlockScanT::TempStorage                scan;           // Smem needed for tile scanning
            typename TilePrefixCallbackOpT::TempStorage     prefix;         // Smem needed for cooperative prefix callback
            typename BlockDiscontinuityKeys::TempStorage    discontinuity;  // Smem needed for discontinuity detection
        };

        // Smem needed for loading keys
        typename BlockLoadKeysT::TempStorage load_keys;

        // Smem needed for loading values
        typename BlockLoadValuesT::TempStorage load_values;

        // Smem needed for compacting key value pairs(allows non POD items in this union)
        Uninitialized<KeyValuePairT[TILE_ITEMS + 1]> raw_exchange;
    };

    // Alias wrapper allowing storage to be unioned
    struct TempStorage : Uninitialized<_TempStorage> {};


    //---------------------------------------------------------------------
    // Per-thread fields
    //---------------------------------------------------------------------

    _TempStorage&                   temp_storage;       ///< Reference to temp_storage
    WrappedKeysInputIteratorT       d_keys_in;          ///< Input keys
    UniqueOutputIteratorT           d_unique_out;       ///< Unique output keys
    WrappedValuesInputIteratorT     d_values_in;        ///< Input values
    AggregatesOutputIteratorT       d_aggregates_out;   ///< Output value aggregates
    NumRunsOutputIteratorT          d_num_runs_out;     ///< Output pointer for total number of segments identified
    EqualityOpT                     equality_op;        ///< KeyT equality operator
    ReductionOpT                    reduction_op;       ///< Reduction operator
    ReduceBySegmentOpT              scan_op;            ///< Reduce-by-segment scan operator


    //---------------------------------------------------------------------
    // Constructor
    //---------------------------------------------------------------------

    // Constructor
    __device__ __forceinline__
    AgentReduceByKey(
        TempStorage&                temp_storage,       ///< Reference to temp_storage
        KeysInputIteratorT          d_keys_in,          ///< Input keys
        UniqueOutputIteratorT       d_unique_out,       ///< Unique output keys
        ValuesInputIteratorT        d_values_in,        ///< Input values
        AggregatesOutputIteratorT   d_aggregates_out,   ///< Output value aggregates
        NumRunsOutputIteratorT      d_num_runs_out,     ///< Output pointer for total number of segments identified
        EqualityOpT                 equality_op,        ///< KeyT equality operator
        ReductionOpT                reduction_op)       ///< ValueT reduction operator
    :
        temp_storage(temp_storage.Alias()),
        d_keys_in(d_keys_in),
        d_unique_out(d_unique_out),
        d_values_in(d_values_in),
        d_aggregates_out(d_aggregates_out),
        d_num_runs_out(d_num_runs_out),
        equality_op(equality_op),
        reduction_op(reduction_op),
        scan_op(reduction_op)
    {}


    //---------------------------------------------------------------------
    // Scatter utility methods
    //---------------------------------------------------------------------

    /**
     * Directly scatter flagged items to output offsets
     */
    __device__ __forceinline__ void ScatterDirect(
        KeyValuePairT   (&scatter_items)[ITEMS_PER_THREAD],
        OffsetT         (&segment_flags)[ITEMS_PER_THREAD],
        OffsetT         (&segment_indices)[ITEMS_PER_THREAD])
    {
        // Scatter flagged keys and values
        #pragma unroll
        for (int ITEM = 0; ITEM < ITEMS_PER_THREAD; ++ITEM)
        {
            if (segment_flags[ITEM])
            {
                d_unique_out[segment_indices[ITEM]]     = scatter_items[ITEM].key;
                d_aggregates_out[segment_indices[ITEM]] = scatter_items[ITEM].value;
            }
        }
    }


    /**
     * 2-phase scatter flagged items to output offsets
     *
     * The exclusive scan causes each head flag to be paired with the previous
     * value aggregate: the scatter offsets must be decremented for value aggregates
     */
    __device__ __forceinline__ void ScatterTwoPhase(
        KeyValuePairT   (&scatter_items)[ITEMS_PER_THREAD],
        OffsetT         (&segment_flags)[ITEMS_PER_THREAD],
        OffsetT         (&segment_indices)[ITEMS_PER_THREAD],
        OffsetT         num_tile_segments,
        OffsetT         num_tile_segments_prefix)
    {
        CTA_SYNC();

        // Compact and scatter pairs
        #pragma unroll
        for (int ITEM = 0; ITEM < ITEMS_PER_THREAD; ++ITEM)
        {
            if (segment_flags[ITEM])
            {
                temp_storage.raw_exchange.Alias()[segment_indices[ITEM] - num_tile_segments_prefix] = scatter_items[ITEM];
            }
        }

        CTA_SYNC();

        for (int item = threadIdx.x; item < num_tile_segments; item += BLOCK_THREADS)
        {
            KeyValuePairT pair                                  = temp_storage.raw_exchange.Alias()[item];
            d_unique_out[num_tile_segments_prefix + item]       = pair.key;
            d_aggregates_out[num_tile_segments_prefix + item]   = pair.value;
        }
    }


    /**
     * Scatter flagged items
     */
    __device__ __forceinline__ void Scatter(
        KeyValuePairT   (&scatter_items)[ITEMS_PER_THREAD],
        OffsetT         (&segment_flags)[ITEMS_PER_THREAD],
        OffsetT         (&segment_indices)[ITEMS_PER_THREAD],
        OffsetT         num_tile_segments,
        OffsetT         num_tile_segments_prefix)
    {
        // Do a one-phase scatter if (a) two-phase is disabled or (b) the average number of selected items per thread is less than one
        if (TWO_PHASE_SCATTER && (num_tile_segments > BLOCK_THREADS))
        {
            ScatterTwoPhase(
                scatter_items,
                segment_flags,
                segment_indices,
                num_tile_segments,
                num_tile_segments_prefix);
        }
        else
        {
            ScatterDirect(
                scatter_items,
                segment_flags,
                segment_indices);
        }
    }


    //---------------------------------------------------------------------
    // Cooperatively scan a device-wide sequence of tiles with other CTAs
    //---------------------------------------------------------------------

    /**
     * Process a tile of input (dynamic chained scan)
     */
    template <bool IS_LAST_TILE>                ///< Whether the current tile is the last tile
    __device__ __forceinline__ void ConsumeTile(
        OffsetT             num_remaining,      ///< Number of global input items remaining (including this tile)
        int                 tile_idx,           ///< Tile index
        OffsetT             tile_offset,        ///< Tile offset
        ScanTileStateT&     tile_state)         ///< Global tile state descriptor
    {
        KeyOutputT          keys[ITEMS_PER_THREAD];             // Tile keys
        KeyOutputT          prev_keys[ITEMS_PER_THREAD];        // Tile keys shuffled up
        ValueOutputT        values[ITEMS_PER_THREAD];           // Tile values
        OffsetT             head_flags[ITEMS_PER_THREAD];       // Segment head flags
        OffsetT             segment_indices[ITEMS_PER_THREAD];  // Segment indices
        OffsetValuePairT    scan_items[ITEMS_PER_THREAD];       // Zipped values and segment flags|indices
        KeyValuePairT       scatter_items[ITEMS_PER_THREAD];    // Zipped key value pairs for scattering

        // Load keys
        if (IS_LAST_TILE)
            BlockLoadKeysT(temp_storage.load_keys).Load(d_keys_in + tile_offset, keys, num_remaining);
        else
            BlockLoadKeysT(temp_storage.load_keys).Load(d_keys_in + tile_offset, keys);

        // Load tile predecessor key in first thread
        KeyOutputT tile_predecessor;
        if (threadIdx.x == 0)
        {
            tile_predecessor = (tile_idx == 0) ?
                keys[0] :                       // First tile gets repeat of first item (thus first item will not be flagged as a head)
                d_keys_in[tile_offset - 1];     // Subsequent tiles get last key from previous tile
        }

        CTA_SYNC();

        // Load values
        if (IS_LAST_TILE)
            BlockLoadValuesT(temp_storage.load_values).Load(d_values_in + tile_offset, values, num_remaining);
        else
            BlockLoadValuesT(temp_storage.load_values).Load(d_values_in + tile_offset, values);

        CTA_SYNC();

        // Initialize head-flags and shuffle up the previous keys
        if (IS_LAST_TILE)
        {
            // Use custom flag operator to additionally flag the first out-of-bounds item
            GuardedInequalityWrapper<EqualityOpT> flag_op(equality_op, num_remaining);
            BlockDiscontinuityKeys(temp_storage.discontinuity).FlagHeads(
                head_flags, keys, prev_keys, flag_op, tile_predecessor);
        }
        else
        {
            InequalityWrapper<EqualityOpT> flag_op(equality_op);
            BlockDiscontinuityKeys(temp_storage.discontinuity).FlagHeads(
                head_flags, keys, prev_keys, flag_op, tile_predecessor);
        }

        // Zip values and head flags
        #pragma unroll
        for (int ITEM = 0; ITEM < ITEMS_PER_THREAD; ++ITEM)
        {
            scan_items[ITEM].value  = values[ITEM];
            scan_items[ITEM].key    = head_flags[ITEM];
        }

        // Perform exclusive tile scan
        OffsetValuePairT    block_aggregate;        // Inclusive block-wide scan aggregate
        OffsetT             num_segments_prefix;    // Number of segments prior to this tile
        ValueOutputT        total_aggregate;        // The tile prefix folded with block_aggregate
        if (tile_idx == 0)
        {
            // Scan first tile
            BlockScanT(temp_storage.scan).ExclusiveScan(scan_items, scan_items, scan_op, block_aggregate);
            num_segments_prefix     = 0;
            total_aggregate         = block_aggregate.value;

            // Update tile status if there are successor tiles
            if ((!IS_LAST_TILE) && (threadIdx.x == 0))
                tile_state.SetInclusive(0, block_aggregate);
        }
        else
        {
            // Scan non-first tile
            TilePrefixCallbackOpT prefix_op(tile_state, temp_storage.prefix, scan_op, tile_idx);
            BlockScanT(temp_storage.scan).ExclusiveScan(scan_items, scan_items, scan_op, prefix_op);

            block_aggregate         = prefix_op.GetBlockAggregate();
            num_segments_prefix     = prefix_op.GetExclusivePrefix().key;
            total_aggregate         = reduction_op(
                                        prefix_op.GetExclusivePrefix().value,
                                        block_aggregate.value);
        }

        // Rezip scatter items and segment indices
        #pragma unroll
        for (int ITEM = 0; ITEM < ITEMS_PER_THREAD; ++ITEM)
        {
            scatter_items[ITEM].key     = prev_keys[ITEM];
            scatter_items[ITEM].value   = scan_items[ITEM].value;
            segment_indices[ITEM]       = scan_items[ITEM].key;
        }

        // At this point, each flagged segment head has:
        //  - The key for the previous segment
        //  - The reduced value from the previous segment
        //  - The segment index for the reduced value

        // Scatter flagged keys and values
        OffsetT num_tile_segments = block_aggregate.key;
        Scatter(scatter_items, head_flags, segment_indices, num_tile_segments, num_segments_prefix);

        // Last thread in last tile will output final count (and last pair, if necessary)
        if ((IS_LAST_TILE) && (threadIdx.x == BLOCK_THREADS - 1))
        {
            OffsetT num_segments = num_segments_prefix + num_tile_segments;

            // If the last tile is a whole tile, output the final_value
            if (num_remaining == TILE_ITEMS)
            {
                d_unique_out[num_segments]      = keys[ITEMS_PER_THREAD - 1];
                d_aggregates_out[num_segments]  = total_aggregate;
                num_segments++;
            }

            // Output the total number of items selected
            *d_num_runs_out = num_segments;
        }
    }


    /**
     * Scan tiles of items as part of a dynamic chained scan
     */
    __device__ __forceinline__ void ConsumeRange(
        int                 num_items,          ///< Total number of input items
        ScanTileStateT&     tile_state,         ///< Global tile state descriptor
        int                 start_tile)         ///< The starting tile for the current grid
    {
        // Blocks are launched in increasing order, so just assign one tile per block
        int     tile_idx        = start_tile + blockIdx.x;          // Current tile index
        OffsetT tile_offset     = OffsetT(TILE_ITEMS) * tile_idx;   // Global offset for the current tile
        OffsetT num_remaining   = num_items - tile_offset;          // Remaining items (including this tile)

        if (num_remaining > TILE_ITEMS)
        {
            // Not last tile
            ConsumeTile<false>(num_remaining, tile_idx, tile_offset, tile_state);
        }
        else if (num_remaining > 0)
        {
            // Last tile
            ConsumeTile<true>(num_remaining, tile_idx, tile_offset, tile_state);
        }
    }

};


}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)

