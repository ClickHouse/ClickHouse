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
 * cub::GridEvenShare is a descriptor utility for distributing input among CUDA thread blocks in an "even-share" fashion.  Each thread block gets roughly the same number of fixed-size work units (grains).
 */


#pragma once

#include "../util_namespace.cuh"
#include "../util_macro.cuh"
#include "grid_mapping.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {


/**
 * \addtogroup GridModule
 * @{
 */


/**
 * \brief GridEvenShare is a descriptor utility for distributing input among
 * CUDA thread blocks in an "even-share" fashion.  Each thread block gets roughly
 * the same number of input tiles.
 *
 * \par Overview
 * Each thread block is assigned a consecutive sequence of input tiles.  To help
 * preserve alignment and eliminate the overhead of guarded loads for all but the
 * last thread block, to GridEvenShare assigns one of three different amounts of
 * work to a given thread block: "big", "normal", or "last".  The "big" workloads
 * are one scheduling grain larger than "normal".  The "last" work unit for the
 * last thread block may be partially-full if the input is not an even multiple of
 * the scheduling grain size.
 *
 * \par
 * Before invoking a child grid, a parent thread will typically construct an
 * instance of GridEvenShare.  The instance can be passed to child thread blocks
 * which can initialize their per-thread block offsets using \p BlockInit().
 */
template <typename OffsetT>
struct GridEvenShare
{
private:

    OffsetT     total_tiles;
    int         big_shares;
    OffsetT     big_share_items;
    OffsetT     normal_share_items;
    OffsetT     normal_base_offset;

public:

    /// Total number of input items
    OffsetT     num_items;

    /// Grid size in thread blocks
    int         grid_size;

    /// OffsetT into input marking the beginning of the owning thread block's segment of input tiles
    OffsetT     block_offset;

    /// OffsetT into input of marking the end (one-past) of the owning thread block's segment of input tiles
    OffsetT     block_end;

    /// Stride between input tiles
    OffsetT     block_stride;


    /**
     * \brief Constructor.
     */
    __host__ __device__ __forceinline__ GridEvenShare() :
        total_tiles(0),
        big_shares(0),
        big_share_items(0),
        normal_share_items(0),
        normal_base_offset(0),
        num_items(0),
        grid_size(0),
        block_offset(0),
        block_end(0),
        block_stride(0)
    {}


    /**
     * \brief Dispatch initializer. To be called prior prior to kernel launch.
     */
    __host__ __device__ __forceinline__ void DispatchInit(
        OffsetT num_items,          ///< Total number of input items
        int     max_grid_size,      ///< Maximum grid size allowable (actual grid size may be less if not warranted by the the number of input items)
        int     tile_items)         ///< Number of data items per input tile
    {
        this->block_offset          = num_items;    // Initialize past-the-end
        this->block_end             = num_items;    // Initialize past-the-end
        this->num_items             = num_items;
        this->total_tiles           = (num_items + tile_items - 1) / tile_items;
        this->grid_size             = CUB_MIN(total_tiles, max_grid_size);
        OffsetT avg_tiles_per_block = total_tiles / grid_size;
        this->big_shares            = total_tiles - (avg_tiles_per_block * grid_size);        // leftover grains go to big blocks
        this->normal_share_items    = avg_tiles_per_block * tile_items;
        this->normal_base_offset    = big_shares * tile_items;
        this->big_share_items       = normal_share_items + tile_items;
    }


    /**
     * \brief Initializes ranges for the specified thread block index.  Specialized
     * for a "raking" access pattern in which each thread block is assigned a
     * consecutive sequence of input tiles.
     */
    template <int TILE_ITEMS>
    __device__ __forceinline__ void BlockInit(
        int block_id,
        Int2Type<GRID_MAPPING_RAKE> /*strategy_tag*/)
    {
        block_stride = TILE_ITEMS;
        if (block_id < big_shares)
        {
            // This thread block gets a big share of grains (avg_tiles_per_block + 1)
            block_offset = (block_id * big_share_items);
            block_end = block_offset + big_share_items;
        }
        else if (block_id < total_tiles)
        {
            // This thread block gets a normal share of grains (avg_tiles_per_block)
            block_offset = normal_base_offset + (block_id * normal_share_items);
            block_end = CUB_MIN(num_items, block_offset + normal_share_items);
        }
        // Else default past-the-end
    }


    /**
     * \brief Block-initialization, specialized for a "raking" access
     * pattern in which each thread block is assigned a consecutive sequence
     * of input tiles.
     */
    template <int TILE_ITEMS>
    __device__ __forceinline__ void BlockInit(
        int block_id,
        Int2Type<GRID_MAPPING_STRIP_MINE> /*strategy_tag*/)
    {
        block_stride = grid_size * TILE_ITEMS;
        block_offset = (block_id * TILE_ITEMS);
        block_end = num_items;
    }


    /**
     * \brief Block-initialization, specialized for "strip mining" access
     * pattern in which the input tiles assigned to each thread block are
     * separated by a stride equal to the the extent of the grid.
     */
    template <
        int TILE_ITEMS,
        GridMappingStrategy STRATEGY>
    __device__ __forceinline__ void BlockInit()
    {
        BlockInit<TILE_ITEMS>(blockIdx.x, Int2Type<STRATEGY>());
    }


    /**
     * \brief Block-initialization, specialized for a "raking" access
     * pattern in which each thread block is assigned a consecutive sequence
     * of input tiles.
     */
    template <int TILE_ITEMS>
    __device__ __forceinline__ void BlockInit(
        OffsetT block_offset,                       ///< [in] Threadblock begin offset (inclusive)
        OffsetT block_end)                          ///< [in] Threadblock end offset (exclusive)
    {
        this->block_offset = block_offset;
        this->block_end = block_end;
        this->block_stride = TILE_ITEMS;
    }


};





/** @} */       // end group GridModule

}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)
