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
 * cub::AgentSpmv implements a stateful abstraction of CUDA thread blocks for participating in device-wide SpMV.
 */

#pragma once

#include <iterator>

#include "../util_type.cuh"
#include "../block/block_reduce.cuh"
#include "../block/block_scan.cuh"
#include "../block/block_exchange.cuh"
#include "../thread/thread_search.cuh"
#include "../thread/thread_operators.cuh"
#include "../iterator/cache_modified_input_iterator.cuh"
#include "../iterator/counting_input_iterator.cuh"
#include "../iterator/tex_ref_input_iterator.cuh"
#include "../util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {


/******************************************************************************
 * Tuning policy
 ******************************************************************************/

/**
 * Parameterizable tuning policy type for AgentSpmv
 */
template <
    int                             _BLOCK_THREADS,                         ///< Threads per thread block
    int                             _ITEMS_PER_THREAD,                      ///< Items per thread (per tile of input)
    CacheLoadModifier               _ROW_OFFSETS_SEARCH_LOAD_MODIFIER,      ///< Cache load modifier for reading CSR row-offsets during search
    CacheLoadModifier               _ROW_OFFSETS_LOAD_MODIFIER,             ///< Cache load modifier for reading CSR row-offsets
    CacheLoadModifier               _COLUMN_INDICES_LOAD_MODIFIER,          ///< Cache load modifier for reading CSR column-indices
    CacheLoadModifier               _VALUES_LOAD_MODIFIER,                  ///< Cache load modifier for reading CSR values
    CacheLoadModifier               _VECTOR_VALUES_LOAD_MODIFIER,           ///< Cache load modifier for reading vector values
    bool                            _DIRECT_LOAD_NONZEROS,                  ///< Whether to load nonzeros directly from global during sequential merging (vs. pre-staged through shared memory)
    BlockScanAlgorithm              _SCAN_ALGORITHM>                        ///< The BlockScan algorithm to use
struct AgentSpmvPolicy
{
    enum
    {
        BLOCK_THREADS                                                   = _BLOCK_THREADS,                       ///< Threads per thread block
        ITEMS_PER_THREAD                                                = _ITEMS_PER_THREAD,                    ///< Items per thread (per tile of input)
        DIRECT_LOAD_NONZEROS                                            = _DIRECT_LOAD_NONZEROS,                ///< Whether to load nonzeros directly from global during sequential merging (pre-staged through shared memory)
    };

    static const CacheLoadModifier  ROW_OFFSETS_SEARCH_LOAD_MODIFIER    = _ROW_OFFSETS_SEARCH_LOAD_MODIFIER;    ///< Cache load modifier for reading CSR row-offsets
    static const CacheLoadModifier  ROW_OFFSETS_LOAD_MODIFIER           = _ROW_OFFSETS_LOAD_MODIFIER;           ///< Cache load modifier for reading CSR row-offsets
    static const CacheLoadModifier  COLUMN_INDICES_LOAD_MODIFIER        = _COLUMN_INDICES_LOAD_MODIFIER;        ///< Cache load modifier for reading CSR column-indices
    static const CacheLoadModifier  VALUES_LOAD_MODIFIER                = _VALUES_LOAD_MODIFIER;                ///< Cache load modifier for reading CSR values
    static const CacheLoadModifier  VECTOR_VALUES_LOAD_MODIFIER         = _VECTOR_VALUES_LOAD_MODIFIER;         ///< Cache load modifier for reading vector values
    static const BlockScanAlgorithm SCAN_ALGORITHM                      = _SCAN_ALGORITHM;                      ///< The BlockScan algorithm to use

};


/******************************************************************************
 * Thread block abstractions
 ******************************************************************************/

template <
    typename        ValueT,              ///< Matrix and vector value type
    typename        OffsetT>             ///< Signed integer type for sequence offsets
struct SpmvParams
{
    ValueT*         d_values;            ///< Pointer to the array of \p num_nonzeros values of the corresponding nonzero elements of matrix <b>A</b>.
    OffsetT*        d_row_end_offsets;   ///< Pointer to the array of \p m offsets demarcating the end of every row in \p d_column_indices and \p d_values
    OffsetT*        d_column_indices;    ///< Pointer to the array of \p num_nonzeros column-indices of the corresponding nonzero elements of matrix <b>A</b>.  (Indices are zero-valued.)
    ValueT*         d_vector_x;          ///< Pointer to the array of \p num_cols values corresponding to the dense input vector <em>x</em>
    ValueT*         d_vector_y;          ///< Pointer to the array of \p num_rows values corresponding to the dense output vector <em>y</em>
    int             num_rows;            ///< Number of rows of matrix <b>A</b>.
    int             num_cols;            ///< Number of columns of matrix <b>A</b>.
    int             num_nonzeros;        ///< Number of nonzero elements of matrix <b>A</b>.
    ValueT          alpha;               ///< Alpha multiplicand
    ValueT          beta;                ///< Beta addend-multiplicand

    TexRefInputIterator<ValueT, 66778899, OffsetT>  t_vector_x;
};


/**
 * \brief AgentSpmv implements a stateful abstraction of CUDA thread blocks for participating in device-wide SpMV.
 */
template <
    typename    AgentSpmvPolicyT,           ///< Parameterized AgentSpmvPolicy tuning policy type
    typename    ValueT,                     ///< Matrix and vector value type
    typename    OffsetT,                    ///< Signed integer type for sequence offsets
    bool        HAS_ALPHA,                  ///< Whether the input parameter \p alpha is 1
    bool        HAS_BETA,                   ///< Whether the input parameter \p beta is 0
    int         PTX_ARCH = CUB_PTX_ARCH>    ///< PTX compute capability
struct AgentSpmv
{
    //---------------------------------------------------------------------
    // Types and constants
    //---------------------------------------------------------------------

    /// Constants
    enum
    {
        BLOCK_THREADS           = AgentSpmvPolicyT::BLOCK_THREADS,
        ITEMS_PER_THREAD        = AgentSpmvPolicyT::ITEMS_PER_THREAD,
        TILE_ITEMS              = BLOCK_THREADS * ITEMS_PER_THREAD,
    };

    /// 2D merge path coordinate type
    typedef typename CubVector<OffsetT, 2>::Type CoordinateT;

    /// Input iterator wrapper types (for applying cache modifiers)

    typedef CacheModifiedInputIterator<
            AgentSpmvPolicyT::ROW_OFFSETS_SEARCH_LOAD_MODIFIER,
            OffsetT,
            OffsetT>
        RowOffsetsSearchIteratorT;

    typedef CacheModifiedInputIterator<
            AgentSpmvPolicyT::ROW_OFFSETS_LOAD_MODIFIER,
            OffsetT,
            OffsetT>
        RowOffsetsIteratorT;

    typedef CacheModifiedInputIterator<
            AgentSpmvPolicyT::COLUMN_INDICES_LOAD_MODIFIER,
            OffsetT,
            OffsetT>
        ColumnIndicesIteratorT;

    typedef CacheModifiedInputIterator<
            AgentSpmvPolicyT::VALUES_LOAD_MODIFIER,
            ValueT,
            OffsetT>
        ValueIteratorT;

    typedef CacheModifiedInputIterator<
            AgentSpmvPolicyT::VECTOR_VALUES_LOAD_MODIFIER,
            ValueT,
            OffsetT>
        VectorValueIteratorT;

    // Tuple type for scanning (pairs accumulated segment-value with segment-index)
    typedef KeyValuePair<OffsetT, ValueT> KeyValuePairT;

    // Reduce-value-by-segment scan operator
    typedef ReduceByKeyOp<cub::Sum> ReduceBySegmentOpT;

    // BlockReduce specialization
    typedef BlockReduce<
            ValueT,
            BLOCK_THREADS,
            BLOCK_REDUCE_WARP_REDUCTIONS>
        BlockReduceT;

    // BlockScan specialization
    typedef BlockScan<
            KeyValuePairT,
            BLOCK_THREADS,
            AgentSpmvPolicyT::SCAN_ALGORITHM>
        BlockScanT;

    // BlockScan specialization
    typedef BlockScan<
            ValueT,
            BLOCK_THREADS,
            AgentSpmvPolicyT::SCAN_ALGORITHM>
        BlockPrefixSumT;

    // BlockExchange specialization
    typedef BlockExchange<
            ValueT,
            BLOCK_THREADS,
            ITEMS_PER_THREAD>
        BlockExchangeT;

    /// Merge item type (either a non-zero value or a row-end offset)
    union MergeItem
    {
        // Value type to pair with index type OffsetT (NullType if loading values directly during merge)
        typedef typename If<AgentSpmvPolicyT::DIRECT_LOAD_NONZEROS, NullType, ValueT>::Type MergeValueT;

        OffsetT     row_end_offset;
        MergeValueT nonzero;
    };

    /// Shared memory type required by this thread block
    struct _TempStorage
    {
        CoordinateT tile_coords[2];

        union Aliasable
        {
            // Smem needed for tile of merge items
            MergeItem merge_items[ITEMS_PER_THREAD + TILE_ITEMS + 1];

            // Smem needed for block exchange
            typename BlockExchangeT::TempStorage exchange;

            // Smem needed for block-wide reduction
            typename BlockReduceT::TempStorage reduce;

            // Smem needed for tile scanning
            typename BlockScanT::TempStorage scan;

            // Smem needed for tile prefix sum
            typename BlockPrefixSumT::TempStorage prefix_sum;

        } aliasable;
    };

    /// Temporary storage type (unionable)
    struct TempStorage : Uninitialized<_TempStorage> {};


    //---------------------------------------------------------------------
    // Per-thread fields
    //---------------------------------------------------------------------


    _TempStorage&                   temp_storage;         /// Reference to temp_storage

    SpmvParams<ValueT, OffsetT>&    spmv_params;

    ValueIteratorT                  wd_values;            ///< Wrapped pointer to the array of \p num_nonzeros values of the corresponding nonzero elements of matrix <b>A</b>.
    RowOffsetsIteratorT             wd_row_end_offsets;   ///< Wrapped Pointer to the array of \p m offsets demarcating the end of every row in \p d_column_indices and \p d_values
    ColumnIndicesIteratorT          wd_column_indices;    ///< Wrapped Pointer to the array of \p num_nonzeros column-indices of the corresponding nonzero elements of matrix <b>A</b>.  (Indices are zero-valued.)
    VectorValueIteratorT            wd_vector_x;          ///< Wrapped Pointer to the array of \p num_cols values corresponding to the dense input vector <em>x</em>
    VectorValueIteratorT            wd_vector_y;          ///< Wrapped Pointer to the array of \p num_cols values corresponding to the dense input vector <em>x</em>


    //---------------------------------------------------------------------
    // Interface
    //---------------------------------------------------------------------

    /**
     * Constructor
     */
    __device__ __forceinline__ AgentSpmv(
        TempStorage&                    temp_storage,           ///< Reference to temp_storage
        SpmvParams<ValueT, OffsetT>&    spmv_params)            ///< SpMV input parameter bundle
    :
        temp_storage(temp_storage.Alias()),
        spmv_params(spmv_params),
        wd_values(spmv_params.d_values),
        wd_row_end_offsets(spmv_params.d_row_end_offsets),
        wd_column_indices(spmv_params.d_column_indices),
        wd_vector_x(spmv_params.d_vector_x),
        wd_vector_y(spmv_params.d_vector_y)
    {}




    /**
     * Consume a merge tile, specialized for direct-load of nonzeros
     */
    __device__ __forceinline__ KeyValuePairT ConsumeTile(
        int             tile_idx,
        CoordinateT     tile_start_coord,
        CoordinateT     tile_end_coord,
        Int2Type<true>  is_direct_load)     ///< Marker type indicating whether to load nonzeros directly during path-discovery or beforehand in batch
    {
        int         tile_num_rows           = tile_end_coord.x - tile_start_coord.x;
        int         tile_num_nonzeros       = tile_end_coord.y - tile_start_coord.y;
        OffsetT*    s_tile_row_end_offsets  = &temp_storage.aliasable.merge_items[0].row_end_offset;

        // Gather the row end-offsets for the merge tile into shared memory
        for (int item = threadIdx.x; item <= tile_num_rows; item += BLOCK_THREADS)
        {
            s_tile_row_end_offsets[item] = wd_row_end_offsets[tile_start_coord.x + item];
        }

        CTA_SYNC();

        // Search for the thread's starting coordinate within the merge tile
        CountingInputIterator<OffsetT>  tile_nonzero_indices(tile_start_coord.y);
        CoordinateT                     thread_start_coord;

        MergePathSearch(
            OffsetT(threadIdx.x * ITEMS_PER_THREAD),    // Diagonal
            s_tile_row_end_offsets,                     // List A
            tile_nonzero_indices,                       // List B
            tile_num_rows,
            tile_num_nonzeros,
            thread_start_coord);

        CTA_SYNC();            // Perf-sync

        // Compute the thread's merge path segment
        CoordinateT     thread_current_coord = thread_start_coord;
        KeyValuePairT   scan_segment[ITEMS_PER_THREAD];

        ValueT          running_total = 0.0;

        #pragma unroll
        for (int ITEM = 0; ITEM < ITEMS_PER_THREAD; ++ITEM)
        {
            OffsetT nonzero_idx         = CUB_MIN(tile_nonzero_indices[thread_current_coord.y], spmv_params.num_nonzeros - 1);
            OffsetT column_idx          = wd_column_indices[nonzero_idx];
            ValueT  value               = wd_values[nonzero_idx];

            ValueT  vector_value        = spmv_params.t_vector_x[column_idx];
#if (CUB_PTX_ARCH >= 350)
            vector_value                = wd_vector_x[column_idx];
#endif
            ValueT  nonzero             = value * vector_value;

            OffsetT row_end_offset      = s_tile_row_end_offsets[thread_current_coord.x];

            if (tile_nonzero_indices[thread_current_coord.y] < row_end_offset)
            {
                // Move down (accumulate)
                running_total += nonzero;
                scan_segment[ITEM].value    = running_total;
                scan_segment[ITEM].key      = tile_num_rows;
                ++thread_current_coord.y;
            }
            else
            {
                // Move right (reset)
                scan_segment[ITEM].value    = running_total;
                scan_segment[ITEM].key      = thread_current_coord.x;
                running_total               = 0.0;
                ++thread_current_coord.x;
            }
        }

        CTA_SYNC();

        // Block-wide reduce-value-by-segment
        KeyValuePairT       tile_carry;
        ReduceBySegmentOpT  scan_op;
        KeyValuePairT       scan_item;

        scan_item.value = running_total;
        scan_item.key   = thread_current_coord.x;

        BlockScanT(temp_storage.aliasable.scan).ExclusiveScan(scan_item, scan_item, scan_op, tile_carry);

        if (tile_num_rows > 0)
        {
            if (threadIdx.x == 0)
                scan_item.key = -1;

            // Direct scatter
            #pragma unroll
            for (int ITEM = 0; ITEM < ITEMS_PER_THREAD; ++ITEM)
            {
                if (scan_segment[ITEM].key < tile_num_rows)
                {
                    if (scan_item.key == scan_segment[ITEM].key)
                        scan_segment[ITEM].value = scan_item.value + scan_segment[ITEM].value;

                    if (HAS_ALPHA)
                    {
                        scan_segment[ITEM].value *= spmv_params.alpha;
                    }

                    if (HAS_BETA)
                    {
                        // Update the output vector element
                        ValueT addend = spmv_params.beta * wd_vector_y[tile_start_coord.x + scan_segment[ITEM].key];
                        scan_segment[ITEM].value += addend;
                    }

                    // Set the output vector element
                    spmv_params.d_vector_y[tile_start_coord.x + scan_segment[ITEM].key] = scan_segment[ITEM].value;
                }
            }
        }

        // Return the tile's running carry-out
        return tile_carry;
    }



    /**
     * Consume a merge tile, specialized for indirect load of nonzeros
     */
    __device__ __forceinline__ KeyValuePairT ConsumeTile(
        int             tile_idx,
        CoordinateT     tile_start_coord,
        CoordinateT     tile_end_coord,
        Int2Type<false> is_direct_load)     ///< Marker type indicating whether to load nonzeros directly during path-discovery or beforehand in batch
    {
        int         tile_num_rows           = tile_end_coord.x - tile_start_coord.x;
        int         tile_num_nonzeros       = tile_end_coord.y - tile_start_coord.y;

#if (CUB_PTX_ARCH >= 520)

/*
        OffsetT*    s_tile_row_end_offsets  = &temp_storage.merge_items[tile_num_nonzeros].row_end_offset;
        ValueT*     s_tile_nonzeros         = &temp_storage.merge_items[0].nonzero;

        OffsetT col_indices[ITEMS_PER_THREAD];
        ValueT mat_values[ITEMS_PER_THREAD];
        int nonzero_indices[ITEMS_PER_THREAD];

        // Gather the nonzeros for the merge tile into shared memory
        #pragma unroll
        for (int ITEM = 0; ITEM < ITEMS_PER_THREAD; ++ITEM)
        {
            nonzero_indices[ITEM]           = threadIdx.x + (ITEM * BLOCK_THREADS);

            ValueIteratorT a                = wd_values + tile_start_coord.y + nonzero_indices[ITEM];
            ColumnIndicesIteratorT ci       = wd_column_indices + tile_start_coord.y + nonzero_indices[ITEM];

            col_indices[ITEM]               = (nonzero_indices[ITEM] < tile_num_nonzeros) ? *ci : 0;
            mat_values[ITEM]                = (nonzero_indices[ITEM] < tile_num_nonzeros) ? *a : 0.0;
        }

        CTA_SYNC();

        #pragma unroll
        for (int ITEM = 0; ITEM < ITEMS_PER_THREAD; ++ITEM)
        {
            VectorValueIteratorT x = wd_vector_x + col_indices[ITEM];
            mat_values[ITEM] *= *x;
        }

        CTA_SYNC();

        #pragma unroll
        for (int ITEM = 0; ITEM < ITEMS_PER_THREAD; ++ITEM)
        {
            ValueT *s = s_tile_nonzeros + nonzero_indices[ITEM];

            *s = mat_values[ITEM];
        }

        CTA_SYNC();

*/

        OffsetT*    s_tile_row_end_offsets  = &temp_storage.merge_items[0].row_end_offset;
        ValueT*     s_tile_nonzeros         = &temp_storage.merge_items[tile_num_rows + ITEMS_PER_THREAD].nonzero;

        // Gather the nonzeros for the merge tile into shared memory
        #pragma unroll
        for (int ITEM = 0; ITEM < ITEMS_PER_THREAD; ++ITEM)
        {
            int nonzero_idx = threadIdx.x + (ITEM * BLOCK_THREADS);

            ValueIteratorT a                = wd_values + tile_start_coord.y + nonzero_idx;
            ColumnIndicesIteratorT ci       = wd_column_indices + tile_start_coord.y + nonzero_idx;
            ValueT* s                       = s_tile_nonzeros + nonzero_idx;

            if (nonzero_idx < tile_num_nonzeros)
            {

                OffsetT column_idx              = *ci;
                ValueT  value                   = *a;

                ValueT  vector_value            = spmv_params.t_vector_x[column_idx];
                vector_value                    = wd_vector_x[column_idx];

                ValueT  nonzero                 = value * vector_value;

                *s    = nonzero;
            }
        }


#else

        OffsetT*    s_tile_row_end_offsets  = &temp_storage.aliasable.merge_items[0].row_end_offset;
        ValueT*     s_tile_nonzeros         = &temp_storage.aliasable.merge_items[tile_num_rows + ITEMS_PER_THREAD].nonzero;

        // Gather the nonzeros for the merge tile into shared memory
        if (tile_num_nonzeros > 0)
        {
            #pragma unroll
            for (int ITEM = 0; ITEM < ITEMS_PER_THREAD; ++ITEM)
            {
                int     nonzero_idx             = threadIdx.x + (ITEM * BLOCK_THREADS);
                nonzero_idx                     = CUB_MIN(nonzero_idx, tile_num_nonzeros - 1);

                OffsetT column_idx              = wd_column_indices[tile_start_coord.y + nonzero_idx];
                ValueT  value                   = wd_values[tile_start_coord.y + nonzero_idx];

                ValueT  vector_value            = spmv_params.t_vector_x[column_idx];
#if (CUB_PTX_ARCH >= 350)
                vector_value                    = wd_vector_x[column_idx];
#endif
                ValueT  nonzero                 = value * vector_value;

                s_tile_nonzeros[nonzero_idx]    = nonzero;
            }
        }

#endif

        // Gather the row end-offsets for the merge tile into shared memory
        #pragma unroll 1
        for (int item = threadIdx.x; item <= tile_num_rows; item += BLOCK_THREADS)
        {
            s_tile_row_end_offsets[item] = wd_row_end_offsets[tile_start_coord.x + item];
        }

        CTA_SYNC();

        // Search for the thread's starting coordinate within the merge tile
        CountingInputIterator<OffsetT>  tile_nonzero_indices(tile_start_coord.y);
        CoordinateT                     thread_start_coord;

        MergePathSearch(
            OffsetT(threadIdx.x * ITEMS_PER_THREAD),    // Diagonal
            s_tile_row_end_offsets,                     // List A
            tile_nonzero_indices,                       // List B
            tile_num_rows,
            tile_num_nonzeros,
            thread_start_coord);

        CTA_SYNC();            // Perf-sync

        // Compute the thread's merge path segment
        CoordinateT     thread_current_coord = thread_start_coord;
        KeyValuePairT   scan_segment[ITEMS_PER_THREAD];
        ValueT          running_total = 0.0;

        OffsetT row_end_offset  = s_tile_row_end_offsets[thread_current_coord.x];
        ValueT  nonzero         = s_tile_nonzeros[thread_current_coord.y];

        #pragma unroll
        for (int ITEM = 0; ITEM < ITEMS_PER_THREAD; ++ITEM)
        {
            if (tile_nonzero_indices[thread_current_coord.y] < row_end_offset)
            {
                // Move down (accumulate)
                scan_segment[ITEM].value    = nonzero;
                running_total               += nonzero;
                ++thread_current_coord.y;
                nonzero                     = s_tile_nonzeros[thread_current_coord.y];
            }
            else
            {
                // Move right (reset)
                scan_segment[ITEM].value    = 0.0;
                running_total               = 0.0;
                ++thread_current_coord.x;
                row_end_offset              = s_tile_row_end_offsets[thread_current_coord.x];
            }

            scan_segment[ITEM].key = thread_current_coord.x;
        }

        CTA_SYNC();

        // Block-wide reduce-value-by-segment
        KeyValuePairT       tile_carry;
        ReduceBySegmentOpT  scan_op;
        KeyValuePairT       scan_item;

        scan_item.value = running_total;
        scan_item.key = thread_current_coord.x;

        BlockScanT(temp_storage.aliasable.scan).ExclusiveScan(scan_item, scan_item, scan_op, tile_carry);

        if (threadIdx.x == 0)
        {
            scan_item.key = thread_start_coord.x;
            scan_item.value = 0.0;
        }

        if (tile_num_rows > 0)
        {

            CTA_SYNC();

            // Scan downsweep and scatter
            ValueT* s_partials = &temp_storage.aliasable.merge_items[0].nonzero;

            if (scan_item.key != scan_segment[0].key)
            {
                s_partials[scan_item.key] = scan_item.value;
            }
            else
            {
                scan_segment[0].value += scan_item.value;
            }

            #pragma unroll
            for (int ITEM = 1; ITEM < ITEMS_PER_THREAD; ++ITEM)
            {
                if (scan_segment[ITEM - 1].key != scan_segment[ITEM].key)
                {
                    s_partials[scan_segment[ITEM - 1].key] = scan_segment[ITEM - 1].value;
                }
                else
                {
                    scan_segment[ITEM].value += scan_segment[ITEM - 1].value;
                }
            }

            CTA_SYNC();

            #pragma unroll 1
            for (int item = threadIdx.x; item < tile_num_rows; item += BLOCK_THREADS)
            {
                spmv_params.d_vector_y[tile_start_coord.x + item] = s_partials[item];
            }
        }

        // Return the tile's running carry-out
        return tile_carry;
    }







    /**
     * Consume a merge tile, specialized for indirect load of nonzeros
     * /
    template <typename IsDirectLoadT>
    __device__ __forceinline__ KeyValuePairT ConsumeTile1(
        int             tile_idx,
        CoordinateT     tile_start_coord,
        CoordinateT     tile_end_coord,
        IsDirectLoadT   is_direct_load)     ///< Marker type indicating whether to load nonzeros directly during path-discovery or beforehand in batch
    {
        int         tile_num_rows           = tile_end_coord.x - tile_start_coord.x;
        int         tile_num_nonzeros       = tile_end_coord.y - tile_start_coord.y;

        OffsetT*    s_tile_row_end_offsets  = &temp_storage.merge_items[0].row_end_offset;

        int warp_idx                        = threadIdx.x / WARP_THREADS;
        int lane_idx                        = LaneId();

        // Gather the row end-offsets for the merge tile into shared memory
        #pragma unroll 1
        for (int item = threadIdx.x; item <= tile_num_rows; item += BLOCK_THREADS)
        {
            s_tile_row_end_offsets[item] = wd_row_end_offsets[tile_start_coord.x + item];
        }

        CTA_SYNC();

        // Search for warp start/end coords
        if (lane_idx == 0)
        {
            MergePathSearch(
                OffsetT(warp_idx * ITEMS_PER_WARP),                 // Diagonal
                s_tile_row_end_offsets,                             // List A
                CountingInputIterator<OffsetT>(tile_start_coord.y), // List B
                tile_num_rows,
                tile_num_nonzeros,
                temp_storage.warp_coords[warp_idx]);

            CoordinateT last = {tile_num_rows, tile_num_nonzeros};
            temp_storage.warp_coords[WARPS] = last;
        }

        CTA_SYNC();

        CoordinateT     warp_coord          = temp_storage.warp_coords[warp_idx];
        CoordinateT     warp_end_coord      = temp_storage.warp_coords[warp_idx + 1];
        OffsetT         warp_nonzero_idx    = tile_start_coord.y + warp_coord.y;

        // Consume whole rows
        #pragma unroll 1
        for (; warp_coord.x < warp_end_coord.x; ++warp_coord.x)
        {
            ValueT  row_total       = 0.0;
            OffsetT row_end_offset  = s_tile_row_end_offsets[warp_coord.x];

            #pragma unroll 1
            for (OffsetT nonzero_idx = warp_nonzero_idx + lane_idx;
                nonzero_idx < row_end_offset;
                nonzero_idx += WARP_THREADS)
            {
                OffsetT column_idx          = wd_column_indices[nonzero_idx];
                ValueT  value               = wd_values[nonzero_idx];
                ValueT  vector_value        = wd_vector_x[column_idx];
                row_total                   += value * vector_value;
            }

            // Warp reduce
            row_total = WarpReduceT(temp_storage.warp_reduce[warp_idx]).Sum(row_total);

            // Output
            if (lane_idx == 0)
            {
                spmv_params.d_vector_y[tile_start_coord.x + warp_coord.x] = row_total;
            }

            warp_nonzero_idx = row_end_offset;
        }

        // Consume partial portion of thread's last row
        if (warp_nonzero_idx < tile_start_coord.y + warp_end_coord.y)
        {
            ValueT row_total = 0.0;
            for (OffsetT nonzero_idx = warp_nonzero_idx + lane_idx;
                nonzero_idx < tile_start_coord.y + warp_end_coord.y;
                nonzero_idx += WARP_THREADS)
            {

                OffsetT column_idx          = wd_column_indices[nonzero_idx];
                ValueT  value               = wd_values[nonzero_idx];
                ValueT  vector_value        = wd_vector_x[column_idx];
                row_total                   += value * vector_value;
            }

            // Warp reduce
            row_total = WarpReduceT(temp_storage.warp_reduce[warp_idx]).Sum(row_total);

            // Output
            if (lane_idx == 0)
            {
                spmv_params.d_vector_y[tile_start_coord.x + warp_coord.x] = row_total;
            }
        }

        // Return the tile's running carry-out
        KeyValuePairT tile_carry(tile_num_rows, 0.0);
        return tile_carry;
    }
*/







    /**
     * Consume a merge tile, specialized for indirect load of nonzeros
     * /
    __device__ __forceinline__ KeyValuePairT ConsumeTile2(
        int             tile_idx,
        CoordinateT     tile_start_coord,
        CoordinateT     tile_end_coord,
        Int2Type<false> is_direct_load)     ///< Marker type indicating whether to load nonzeros directly during path-discovery or beforehand in batch
    {
        int         tile_num_rows           = tile_end_coord.x - tile_start_coord.x;
        int         tile_num_nonzeros       = tile_end_coord.y - tile_start_coord.y;

        ValueT*     s_tile_nonzeros         = &temp_storage.merge_items[0].nonzero;

        ValueT      nonzeros[ITEMS_PER_THREAD];

        // Gather the nonzeros for the merge tile into shared memory
        #pragma unroll
        for (int ITEM = 0; ITEM < ITEMS_PER_THREAD; ++ITEM)
        {
            int     nonzero_idx         = threadIdx.x + (ITEM * BLOCK_THREADS);
            nonzero_idx                 = CUB_MIN(nonzero_idx, tile_num_nonzeros - 1);

            OffsetT column_idx          = wd_column_indices[tile_start_coord.y + nonzero_idx];
            ValueT  value               = wd_values[tile_start_coord.y + nonzero_idx];

            ValueT  vector_value        = spmv_params.t_vector_x[column_idx];
#if (CUB_PTX_ARCH >= 350)
            vector_value                = wd_vector_x[column_idx];
#endif

            nonzeros[ITEM]              = value * vector_value;
        }

        // Exchange striped->blocked
        BlockExchangeT(temp_storage.exchange).StripedToBlocked(nonzeros);

        CTA_SYNC();

        // Compute an inclusive prefix sum
        BlockPrefixSumT(temp_storage.prefix_sum).InclusiveSum(nonzeros, nonzeros);

        CTA_SYNC();

        if (threadIdx.x == 0)
            s_tile_nonzeros[0] = 0.0;

        // Scatter back to smem
        #pragma unroll
        for (int ITEM = 0; ITEM < ITEMS_PER_THREAD; ++ITEM)
        {
            int item_idx = (threadIdx.x * ITEMS_PER_THREAD) + ITEM + 1;
            s_tile_nonzeros[item_idx] = nonzeros[ITEM];
        }

        CTA_SYNC();

        // Gather the row end-offsets for the merge tile into shared memory
        #pragma unroll 1
        for (int item = threadIdx.x; item < tile_num_rows; item += BLOCK_THREADS)
        {
            OffsetT start = CUB_MAX(wd_row_end_offsets[tile_start_coord.x + item - 1], tile_start_coord.y);
            OffsetT end = wd_row_end_offsets[tile_start_coord.x + item];

            start -= tile_start_coord.y;
            end -= tile_start_coord.y;

            ValueT row_partial = s_tile_nonzeros[end] - s_tile_nonzeros[start];

            spmv_params.d_vector_y[tile_start_coord.x + item] = row_partial;
        }

        // Get the tile's carry-out
        KeyValuePairT tile_carry;
        if (threadIdx.x == 0)
        {
            tile_carry.key = tile_num_rows;

            OffsetT start = CUB_MAX(wd_row_end_offsets[tile_end_coord.x - 1], tile_start_coord.y);
            start -= tile_start_coord.y;
            OffsetT end = tile_num_nonzeros;

            tile_carry.value = s_tile_nonzeros[end] - s_tile_nonzeros[start];
        }

        // Return the tile's running carry-out
        return tile_carry;
    }
*/


    /**
     * Consume input tile
     */
    __device__ __forceinline__ void ConsumeTile(
        CoordinateT*    d_tile_coordinates,     ///< [in] Pointer to the temporary array of tile starting coordinates
        KeyValuePairT*  d_tile_carry_pairs,     ///< [out] Pointer to the temporary array carry-out dot product row-ids, one per block
        int             num_merge_tiles)        ///< [in] Number of merge tiles
    {
        int tile_idx = (blockIdx.x * gridDim.y) + blockIdx.y;    // Current tile index

        if (tile_idx >= num_merge_tiles)
            return;

        // Read our starting coordinates
        if (threadIdx.x < 2)
        {
            if (d_tile_coordinates == NULL)
            {
                // Search our starting coordinates
                OffsetT                         diagonal = (tile_idx + threadIdx.x) * TILE_ITEMS;
                CoordinateT                     tile_coord;
                CountingInputIterator<OffsetT>  nonzero_indices(0);

                // Search the merge path
                MergePathSearch(
                    diagonal,
                    RowOffsetsSearchIteratorT(spmv_params.d_row_end_offsets),
                    nonzero_indices,
                    spmv_params.num_rows,
                    spmv_params.num_nonzeros,
                    tile_coord);

                temp_storage.tile_coords[threadIdx.x] = tile_coord;
            }
            else
            {
                temp_storage.tile_coords[threadIdx.x] = d_tile_coordinates[tile_idx + threadIdx.x];
            }
        }

        CTA_SYNC();

        CoordinateT tile_start_coord     = temp_storage.tile_coords[0];
        CoordinateT tile_end_coord       = temp_storage.tile_coords[1];

        // Consume multi-segment tile
        KeyValuePairT tile_carry = ConsumeTile(
            tile_idx,
            tile_start_coord,
            tile_end_coord,
            Int2Type<AgentSpmvPolicyT::DIRECT_LOAD_NONZEROS>());

        // Output the tile's carry-out
        if (threadIdx.x == 0)
        {
            if (HAS_ALPHA)
                tile_carry.value *= spmv_params.alpha;

            tile_carry.key += tile_start_coord.x;
            d_tile_carry_pairs[tile_idx]    = tile_carry;
        }
    }


};




}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)

