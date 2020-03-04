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
 * cub::AgentScan implements a stateful abstraction of CUDA thread blocks for participating in device-wide prefix scan .
 */

#pragma once

#include <iterator>

#include "single_pass_scan_operators.cuh"
#include "../block/block_load.cuh"
#include "../block/block_store.cuh"
#include "../block/block_scan.cuh"
#include "../grid/grid_queue.cuh"
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
 * Parameterizable tuning policy type for AgentScan
 */
template <
    int                         _BLOCK_THREADS,                 ///< Threads per thread block
    int                         _ITEMS_PER_THREAD,              ///< Items per thread (per tile of input)
    BlockLoadAlgorithm          _LOAD_ALGORITHM,                ///< The BlockLoad algorithm to use
    CacheLoadModifier           _LOAD_MODIFIER,                 ///< Cache load modifier for reading input elements
    BlockStoreAlgorithm         _STORE_ALGORITHM,               ///< The BlockStore algorithm to use
    BlockScanAlgorithm          _SCAN_ALGORITHM>                ///< The BlockScan algorithm to use
struct AgentScanPolicy
{
    enum
    {
        BLOCK_THREADS           = _BLOCK_THREADS,               ///< Threads per thread block
        ITEMS_PER_THREAD        = _ITEMS_PER_THREAD,            ///< Items per thread (per tile of input)
    };

    static const BlockLoadAlgorithm     LOAD_ALGORITHM          = _LOAD_ALGORITHM;          ///< The BlockLoad algorithm to use
    static const CacheLoadModifier      LOAD_MODIFIER           = _LOAD_MODIFIER;           ///< Cache load modifier for reading input elements
    static const BlockStoreAlgorithm    STORE_ALGORITHM         = _STORE_ALGORITHM;         ///< The BlockStore algorithm to use
    static const BlockScanAlgorithm     SCAN_ALGORITHM          = _SCAN_ALGORITHM;          ///< The BlockScan algorithm to use
};




/******************************************************************************
 * Thread block abstractions
 ******************************************************************************/

/**
 * \brief AgentScan implements a stateful abstraction of CUDA thread blocks for participating in device-wide prefix scan .
 */
template <
    typename AgentScanPolicyT,      ///< Parameterized AgentScanPolicyT tuning policy type
    typename InputIteratorT,        ///< Random-access input iterator type
    typename OutputIteratorT,       ///< Random-access output iterator type
    typename ScanOpT,               ///< Scan functor type
    typename InitValueT,            ///< The init_value element for ScanOpT type (cub::NullType for inclusive scan)
    typename OffsetT>               ///< Signed integer type for global offsets
struct AgentScan
{
    //---------------------------------------------------------------------
    // Types and constants
    //---------------------------------------------------------------------

    // The input value type
    typedef typename std::iterator_traits<InputIteratorT>::value_type InputT;

    // The output value type
    typedef typename If<(Equals<typename std::iterator_traits<OutputIteratorT>::value_type, void>::VALUE),  // OutputT =  (if output iterator's value type is void) ?
        typename std::iterator_traits<InputIteratorT>::value_type,                                          // ... then the input iterator's value type,
        typename std::iterator_traits<OutputIteratorT>::value_type>::Type OutputT;                          // ... else the output iterator's value type

    // Tile status descriptor interface type
    typedef ScanTileState<OutputT> ScanTileStateT;

    // Input iterator wrapper type (for applying cache modifier)
    typedef typename If<IsPointer<InputIteratorT>::VALUE,
            CacheModifiedInputIterator<AgentScanPolicyT::LOAD_MODIFIER, InputT, OffsetT>,   // Wrap the native input pointer with CacheModifiedInputIterator
            InputIteratorT>::Type                                                           // Directly use the supplied input iterator type
        WrappedInputIteratorT;

    // Constants
    enum
    {
        IS_INCLUSIVE        = Equals<InitValueT, NullType>::VALUE,            // Inclusive scan if no init_value type is provided
        BLOCK_THREADS       = AgentScanPolicyT::BLOCK_THREADS,
        ITEMS_PER_THREAD    = AgentScanPolicyT::ITEMS_PER_THREAD,
        TILE_ITEMS          = BLOCK_THREADS * ITEMS_PER_THREAD,
    };

    // Parameterized BlockLoad type
    typedef BlockLoad<
            OutputT,
            AgentScanPolicyT::BLOCK_THREADS,
            AgentScanPolicyT::ITEMS_PER_THREAD,
            AgentScanPolicyT::LOAD_ALGORITHM>
        BlockLoadT;

    // Parameterized BlockStore type
    typedef BlockStore<
            OutputT,
            AgentScanPolicyT::BLOCK_THREADS,
            AgentScanPolicyT::ITEMS_PER_THREAD,
            AgentScanPolicyT::STORE_ALGORITHM>
        BlockStoreT;

    // Parameterized BlockScan type
    typedef BlockScan<
            OutputT,
            AgentScanPolicyT::BLOCK_THREADS,
            AgentScanPolicyT::SCAN_ALGORITHM>
        BlockScanT;

    // Callback type for obtaining tile prefix during block scan
    typedef TilePrefixCallbackOp<
            OutputT,
            ScanOpT,
            ScanTileStateT>
        TilePrefixCallbackOpT;

    // Stateful BlockScan prefix callback type for managing a running total while scanning consecutive tiles
    typedef BlockScanRunningPrefixOp<
            OutputT,
            ScanOpT>
        RunningPrefixCallbackOp;

    // Shared memory type for this thread block
    union _TempStorage
    {
        typename BlockLoadT::TempStorage    load;       // Smem needed for tile loading
        typename BlockStoreT::TempStorage   store;      // Smem needed for tile storing

        struct
        {
            typename TilePrefixCallbackOpT::TempStorage  prefix;     // Smem needed for cooperative prefix callback
            typename BlockScanT::TempStorage             scan;       // Smem needed for tile scanning
        };
    };

    // Alias wrapper allowing storage to be unioned
    struct TempStorage : Uninitialized<_TempStorage> {};


    //---------------------------------------------------------------------
    // Per-thread fields
    //---------------------------------------------------------------------

    _TempStorage&               temp_storage;       ///< Reference to temp_storage
    WrappedInputIteratorT       d_in;               ///< Input data
    OutputIteratorT             d_out;              ///< Output data
    ScanOpT                     scan_op;            ///< Binary scan operator
    InitValueT                  init_value;         ///< The init_value element for ScanOpT


    //---------------------------------------------------------------------
    // Block scan utility methods
    //---------------------------------------------------------------------

    /**
     * Exclusive scan specialization (first tile)
     */
    __device__ __forceinline__
    void ScanTile(
        OutputT             (&items)[ITEMS_PER_THREAD],
        OutputT             init_value,
        ScanOpT             scan_op,
        OutputT             &block_aggregate,
        Int2Type<false>     /*is_inclusive*/)
    {
        BlockScanT(temp_storage.scan).ExclusiveScan(items, items, init_value, scan_op, block_aggregate);
        block_aggregate = scan_op(init_value, block_aggregate);
    }


    /**
     * Inclusive scan specialization (first tile)
     */
    __device__ __forceinline__
    void ScanTile(
        OutputT             (&items)[ITEMS_PER_THREAD],
        InitValueT          /*init_value*/,
        ScanOpT             scan_op,
        OutputT             &block_aggregate,
        Int2Type<true>      /*is_inclusive*/)
    {
        BlockScanT(temp_storage.scan).InclusiveScan(items, items, scan_op, block_aggregate);
    }


    /**
     * Exclusive scan specialization (subsequent tiles)
     */
    template <typename PrefixCallback>
    __device__ __forceinline__
    void ScanTile(
        OutputT             (&items)[ITEMS_PER_THREAD],
        ScanOpT             scan_op,
        PrefixCallback      &prefix_op,
        Int2Type<false>     /*is_inclusive*/)
    {
        BlockScanT(temp_storage.scan).ExclusiveScan(items, items, scan_op, prefix_op);
    }


    /**
     * Inclusive scan specialization (subsequent tiles)
     */
    template <typename PrefixCallback>
    __device__ __forceinline__
    void ScanTile(
        OutputT             (&items)[ITEMS_PER_THREAD],
        ScanOpT             scan_op,
        PrefixCallback      &prefix_op,
        Int2Type<true>      /*is_inclusive*/)
    {
        BlockScanT(temp_storage.scan).InclusiveScan(items, items, scan_op, prefix_op);
    }


    //---------------------------------------------------------------------
    // Constructor
    //---------------------------------------------------------------------

    // Constructor
    __device__ __forceinline__
    AgentScan(
        TempStorage&    temp_storage,       ///< Reference to temp_storage
        InputIteratorT  d_in,               ///< Input data
        OutputIteratorT d_out,              ///< Output data
        ScanOpT         scan_op,            ///< Binary scan operator
        InitValueT      init_value)         ///< Initial value to seed the exclusive scan
    :
        temp_storage(temp_storage.Alias()),
        d_in(d_in),
        d_out(d_out),
        scan_op(scan_op),
        init_value(init_value)
    {}


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
        // Load items
        OutputT items[ITEMS_PER_THREAD];

        if (IS_LAST_TILE)
            BlockLoadT(temp_storage.load).Load(d_in + tile_offset, items, num_remaining);
        else
            BlockLoadT(temp_storage.load).Load(d_in + tile_offset, items);

        CTA_SYNC();

        // Perform tile scan
        if (tile_idx == 0)
        {
            // Scan first tile
            OutputT block_aggregate;
            ScanTile(items, init_value, scan_op, block_aggregate, Int2Type<IS_INCLUSIVE>());
            if ((!IS_LAST_TILE) && (threadIdx.x == 0))
                tile_state.SetInclusive(0, block_aggregate);
        }
        else
        {
            // Scan non-first tile
            TilePrefixCallbackOpT prefix_op(tile_state, temp_storage.prefix, scan_op, tile_idx);
            ScanTile(items, scan_op, prefix_op, Int2Type<IS_INCLUSIVE>());
        }

        CTA_SYNC();

        // Store items
        if (IS_LAST_TILE)
            BlockStoreT(temp_storage.store).Store(d_out + tile_offset, items, num_remaining);
        else
            BlockStoreT(temp_storage.store).Store(d_out + tile_offset, items);
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


    //---------------------------------------------------------------------
    // Scan an sequence of consecutive tiles (independent of other thread blocks)
    //---------------------------------------------------------------------

    /**
     * Process a tile of input
     */
    template <
        bool                        IS_FIRST_TILE,
        bool                        IS_LAST_TILE>
    __device__ __forceinline__ void ConsumeTile(
        OffsetT                     tile_offset,                ///< Tile offset
        RunningPrefixCallbackOp&    prefix_op,                  ///< Running prefix operator
        int                         valid_items = TILE_ITEMS)   ///< Number of valid items in the tile
    {
        // Load items
        OutputT items[ITEMS_PER_THREAD];

        if (IS_LAST_TILE)
            BlockLoadT(temp_storage.load).Load(d_in + tile_offset, items, valid_items);
        else
            BlockLoadT(temp_storage.load).Load(d_in + tile_offset, items);

        CTA_SYNC();

        // Block scan
        if (IS_FIRST_TILE)
        {
            OutputT block_aggregate;
            ScanTile(items, init_value, scan_op, block_aggregate, Int2Type<IS_INCLUSIVE>());
            prefix_op.running_total = block_aggregate;
        }
        else
        {
            ScanTile(items, scan_op, prefix_op, Int2Type<IS_INCLUSIVE>());
        }

        CTA_SYNC();

        // Store items
        if (IS_LAST_TILE)
            BlockStoreT(temp_storage.store).Store(d_out + tile_offset, items, valid_items);
        else
            BlockStoreT(temp_storage.store).Store(d_out + tile_offset, items);
    }


    /**
     * Scan a consecutive share of input tiles
     */
    __device__ __forceinline__ void ConsumeRange(
        OffsetT  range_offset,      ///< [in] Threadblock begin offset (inclusive)
        OffsetT  range_end)         ///< [in] Threadblock end offset (exclusive)
    {
        BlockScanRunningPrefixOp<OutputT, ScanOpT> prefix_op(scan_op);

        if (range_offset + TILE_ITEMS <= range_end)
        {
            // Consume first tile of input (full)
            ConsumeTile<true, true>(range_offset, prefix_op);
            range_offset += TILE_ITEMS;

            // Consume subsequent full tiles of input
            while (range_offset + TILE_ITEMS <= range_end)
            {
                ConsumeTile<false, true>(range_offset, prefix_op);
                range_offset += TILE_ITEMS;
            }

            // Consume a partially-full tile
            if (range_offset < range_end)
            {
                int valid_items = range_end - range_offset;
                ConsumeTile<false, false>(range_offset, prefix_op, valid_items);
            }
        }
        else
        {
            // Consume the first tile of input (partially-full)
            int valid_items = range_end - range_offset;
            ConsumeTile<true, false>(range_offset, prefix_op, valid_items);
        }
    }


    /**
     * Scan a consecutive share of input tiles, seeded with the specified prefix value
     */
    __device__ __forceinline__ void ConsumeRange(
        OffsetT range_offset,                       ///< [in] Threadblock begin offset (inclusive)
        OffsetT range_end,                          ///< [in] Threadblock end offset (exclusive)
        OutputT prefix)                             ///< [in] The prefix to apply to the scan segment
    {
        BlockScanRunningPrefixOp<OutputT, ScanOpT> prefix_op(prefix, scan_op);

        // Consume full tiles of input
        while (range_offset + TILE_ITEMS <= range_end)
        {
            ConsumeTile<true, false>(range_offset, prefix_op);
            range_offset += TILE_ITEMS;
        }

        // Consume a partially-full tile
        if (range_offset < range_end)
        {
            int valid_items = range_end - range_offset;
            ConsumeTile<false, false>(range_offset, prefix_op, valid_items);
        }
    }

};


}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)

