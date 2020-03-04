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
 * The cub::BlockRadixSort class provides [<em>collective</em>](index.html#sec0) methods for radix sorting of items partitioned across a CUDA thread block.
 */


#pragma once

#include "block_exchange.cuh"
#include "block_radix_rank.cuh"
#include "../util_ptx.cuh"
#include "../util_arch.cuh"
#include "../util_type.cuh"
#include "../util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {

/**
 * \brief The BlockRadixSort class provides [<em>collective</em>](index.html#sec0) methods for sorting items partitioned across a CUDA thread block using a radix sorting method.  ![](sorting_logo.png)
 * \ingroup BlockModule
 *
 * \tparam KeyT                 KeyT type
 * \tparam BLOCK_DIM_X          The thread block length in threads along the X dimension
 * \tparam ITEMS_PER_THREAD     The number of items per thread
 * \tparam ValueT               <b>[optional]</b> ValueT type (default: cub::NullType, which indicates a keys-only sort)
 * \tparam RADIX_BITS           <b>[optional]</b> The number of radix bits per digit place (default: 4 bits)
 * \tparam MEMOIZE_OUTER_SCAN   <b>[optional]</b> Whether or not to buffer outer raking scan partials to incur fewer shared memory reads at the expense of higher register pressure (default: true for architectures SM35 and newer, false otherwise).
 * \tparam INNER_SCAN_ALGORITHM <b>[optional]</b> The cub::BlockScanAlgorithm algorithm to use (default: cub::BLOCK_SCAN_WARP_SCANS)
 * \tparam SMEM_CONFIG          <b>[optional]</b> Shared memory bank mode (default: \p cudaSharedMemBankSizeFourByte)
 * \tparam BLOCK_DIM_Y          <b>[optional]</b> The thread block length in threads along the Y dimension (default: 1)
 * \tparam BLOCK_DIM_Z          <b>[optional]</b> The thread block length in threads along the Z dimension (default: 1)
 * \tparam PTX_ARCH             <b>[optional]</b> \ptxversion
 *
 * \par Overview
 * - The [<em>radix sorting method</em>](http://en.wikipedia.org/wiki/Radix_sort) arranges
 *   items into ascending order.  It relies upon a positional representation for
 *   keys, i.e., each key is comprised of an ordered sequence of symbols (e.g., digits,
 *   characters, etc.) specified from least-significant to most-significant.  For a
 *   given input sequence of keys and a set of rules specifying a total ordering
 *   of the symbolic alphabet, the radix sorting method produces a lexicographic
 *   ordering of those keys.
 * - BlockRadixSort can sort all of the built-in C++ numeric primitive types, e.g.:
 *   <tt>unsigned char</tt>, \p int, \p double, etc.  Within each key, the implementation treats fixed-length
 *   bit-sequences of \p RADIX_BITS as radix digit places.  Although the direct radix sorting
 *   method can only be applied to unsigned integral types, BlockRadixSort
 *   is able to sort signed and floating-point types via simple bit-wise transformations
 *   that ensure lexicographic key ordering.
 * - \rowmajor
 *
 * \par Performance Considerations
 * - \granularity
 *
 * \par A Simple Example
 * \blockcollective{BlockRadixSort}
 * \par
 * The code snippet below illustrates a sort of 512 integer keys that
 * are partitioned in a [<em>blocked arrangement</em>](index.html#sec5sec3) across 128 threads
 * where each thread owns 4 consecutive items.
 * \par
 * \code
 * #include <cub/cub.cuh>   // or equivalently <cub/block/block_radix_sort.cuh>
 *
 * __global__ void ExampleKernel(...)
 * {
 *     // Specialize BlockRadixSort for a 1D block of 128 threads owning 4 integer items each
 *     typedef cub::BlockRadixSort<int, 128, 4> BlockRadixSort;
 *
 *     // Allocate shared memory for BlockRadixSort
 *     __shared__ typename BlockRadixSort::TempStorage temp_storage;
 *
 *     // Obtain a segment of consecutive items that are blocked across threads
 *     int thread_keys[4];
 *     ...
 *
 *     // Collectively sort the keys
 *     BlockRadixSort(temp_storage).Sort(thread_keys);
 *
 *     ...
 * \endcode
 * \par
 * Suppose the set of input \p thread_keys across the block of threads is
 * <tt>{ [0,511,1,510], [2,509,3,508], [4,507,5,506], ..., [254,257,255,256] }</tt>.  The
 * corresponding output \p thread_keys in those threads will be
 * <tt>{ [0,1,2,3], [4,5,6,7], [8,9,10,11], ..., [508,509,510,511] }</tt>.
 *
 */
template <
    typename                KeyT,
    int                     BLOCK_DIM_X,
    int                     ITEMS_PER_THREAD,
    typename                ValueT                   = NullType,
    int                     RADIX_BITS              = 4,
    bool                    MEMOIZE_OUTER_SCAN      = (CUB_PTX_ARCH >= 350) ? true : false,
    BlockScanAlgorithm      INNER_SCAN_ALGORITHM    = BLOCK_SCAN_WARP_SCANS,
    cudaSharedMemConfig     SMEM_CONFIG             = cudaSharedMemBankSizeFourByte,
    int                     BLOCK_DIM_Y             = 1,
    int                     BLOCK_DIM_Z             = 1,
    int                     PTX_ARCH                = CUB_PTX_ARCH>
class BlockRadixSort
{
private:

    /******************************************************************************
     * Constants and type definitions
     ******************************************************************************/

    enum
    {
        // The thread block size in threads
        BLOCK_THREADS               = BLOCK_DIM_X * BLOCK_DIM_Y * BLOCK_DIM_Z,

        // Whether or not there are values to be trucked along with keys
        KEYS_ONLY                   = Equals<ValueT, NullType>::VALUE,
    };

    // KeyT traits and unsigned bits type
    typedef Traits<KeyT>                        KeyTraits;
    typedef typename KeyTraits::UnsignedBits    UnsignedBits;

    /// Ascending BlockRadixRank utility type
    typedef BlockRadixRank<
            BLOCK_DIM_X,
            RADIX_BITS,
            false,
            MEMOIZE_OUTER_SCAN,
            INNER_SCAN_ALGORITHM,
            SMEM_CONFIG,
            BLOCK_DIM_Y,
            BLOCK_DIM_Z,
            PTX_ARCH>
        AscendingBlockRadixRank;

    /// Descending BlockRadixRank utility type
    typedef BlockRadixRank<
            BLOCK_DIM_X,
            RADIX_BITS,
            true,
            MEMOIZE_OUTER_SCAN,
            INNER_SCAN_ALGORITHM,
            SMEM_CONFIG,
            BLOCK_DIM_Y,
            BLOCK_DIM_Z,
            PTX_ARCH>
        DescendingBlockRadixRank;

    /// BlockExchange utility type for keys
    typedef BlockExchange<KeyT, BLOCK_DIM_X, ITEMS_PER_THREAD, false, BLOCK_DIM_Y, BLOCK_DIM_Z, PTX_ARCH> BlockExchangeKeys;

    /// BlockExchange utility type for values
    typedef BlockExchange<ValueT, BLOCK_DIM_X, ITEMS_PER_THREAD, false, BLOCK_DIM_Y, BLOCK_DIM_Z, PTX_ARCH> BlockExchangeValues;

    /// Shared memory storage layout type
    union _TempStorage
    {
        typename AscendingBlockRadixRank::TempStorage  asending_ranking_storage;
        typename DescendingBlockRadixRank::TempStorage descending_ranking_storage;
        typename BlockExchangeKeys::TempStorage        exchange_keys;
        typename BlockExchangeValues::TempStorage      exchange_values;
    };


    /******************************************************************************
     * Thread fields
     ******************************************************************************/

    /// Shared storage reference
    _TempStorage &temp_storage;

    /// Linear thread-id
    unsigned int linear_tid;

    /******************************************************************************
     * Utility methods
     ******************************************************************************/

    /// Internal storage allocator
    __device__ __forceinline__ _TempStorage& PrivateStorage()
    {
        __shared__ _TempStorage private_storage;
        return private_storage;
    }

    /// Rank keys (specialized for ascending sort)
    __device__ __forceinline__ void RankKeys(
        UnsignedBits    (&unsigned_keys)[ITEMS_PER_THREAD],
        int             (&ranks)[ITEMS_PER_THREAD],
        int             begin_bit,
        int             pass_bits,
        Int2Type<false> /*is_descending*/)
    {
        AscendingBlockRadixRank(temp_storage.asending_ranking_storage).RankKeys(
            unsigned_keys,
            ranks,
            begin_bit,
            pass_bits);
    }

    /// Rank keys (specialized for descending sort)
    __device__ __forceinline__ void RankKeys(
        UnsignedBits    (&unsigned_keys)[ITEMS_PER_THREAD],
        int             (&ranks)[ITEMS_PER_THREAD],
        int             begin_bit,
        int             pass_bits,
        Int2Type<true>  /*is_descending*/)
    {
        DescendingBlockRadixRank(temp_storage.descending_ranking_storage).RankKeys(
            unsigned_keys,
            ranks,
            begin_bit,
            pass_bits);
    }

    /// ExchangeValues (specialized for key-value sort, to-blocked arrangement)
    __device__ __forceinline__ void ExchangeValues(
        ValueT          (&values)[ITEMS_PER_THREAD],
        int             (&ranks)[ITEMS_PER_THREAD],
        Int2Type<false> /*is_keys_only*/,
        Int2Type<true>  /*is_blocked*/)
    {
        CTA_SYNC();

        // Exchange values through shared memory in blocked arrangement
        BlockExchangeValues(temp_storage.exchange_values).ScatterToBlocked(values, ranks);
    }

    /// ExchangeValues (specialized for key-value sort, to-striped arrangement)
    __device__ __forceinline__ void ExchangeValues(
        ValueT          (&values)[ITEMS_PER_THREAD],
        int             (&ranks)[ITEMS_PER_THREAD],
        Int2Type<false> /*is_keys_only*/,
        Int2Type<false> /*is_blocked*/)
    {
        CTA_SYNC();

        // Exchange values through shared memory in blocked arrangement
        BlockExchangeValues(temp_storage.exchange_values).ScatterToStriped(values, ranks);
    }

    /// ExchangeValues (specialized for keys-only sort)
    template <int IS_BLOCKED>
    __device__ __forceinline__ void ExchangeValues(
        ValueT                  (&/*values*/)[ITEMS_PER_THREAD],
        int                     (&/*ranks*/)[ITEMS_PER_THREAD],
        Int2Type<true>          /*is_keys_only*/,
        Int2Type<IS_BLOCKED>    /*is_blocked*/)
    {}

    /// Sort blocked arrangement
    template <int DESCENDING, int KEYS_ONLY>
    __device__ __forceinline__ void SortBlocked(
        KeyT                    (&keys)[ITEMS_PER_THREAD],          ///< Keys to sort
        ValueT                  (&values)[ITEMS_PER_THREAD],        ///< Values to sort
        int                     begin_bit,                          ///< The beginning (least-significant) bit index needed for key comparison
        int                     end_bit,                            ///< The past-the-end (most-significant) bit index needed for key comparison
        Int2Type<DESCENDING>    is_descending,                      ///< Tag whether is a descending-order sort
        Int2Type<KEYS_ONLY>     is_keys_only)                       ///< Tag whether is keys-only sort
    {
        UnsignedBits (&unsigned_keys)[ITEMS_PER_THREAD] =
            reinterpret_cast<UnsignedBits (&)[ITEMS_PER_THREAD]>(keys);

        // Twiddle bits if necessary
        #pragma unroll
        for (int KEY = 0; KEY < ITEMS_PER_THREAD; KEY++)
        {
            unsigned_keys[KEY] = KeyTraits::TwiddleIn(unsigned_keys[KEY]);
        }

        // Radix sorting passes
        while (true)
        {
            int pass_bits = CUB_MIN(RADIX_BITS, end_bit - begin_bit);

            // Rank the blocked keys
            int ranks[ITEMS_PER_THREAD];
            RankKeys(unsigned_keys, ranks, begin_bit, pass_bits, is_descending);
            begin_bit += RADIX_BITS;

            CTA_SYNC();

            // Exchange keys through shared memory in blocked arrangement
            BlockExchangeKeys(temp_storage.exchange_keys).ScatterToBlocked(keys, ranks);

            // Exchange values through shared memory in blocked arrangement
            ExchangeValues(values, ranks, is_keys_only, Int2Type<true>());

            // Quit if done
            if (begin_bit >= end_bit) break;

            CTA_SYNC();
        }

        // Untwiddle bits if necessary
        #pragma unroll
        for (int KEY = 0; KEY < ITEMS_PER_THREAD; KEY++)
        {
            unsigned_keys[KEY] = KeyTraits::TwiddleOut(unsigned_keys[KEY]);
        }
    }

public:

#ifndef DOXYGEN_SHOULD_SKIP_THIS    // Do not document

    /// Sort blocked -> striped arrangement
    template <int DESCENDING, int KEYS_ONLY>
    __device__ __forceinline__ void SortBlockedToStriped(
        KeyT                    (&keys)[ITEMS_PER_THREAD],          ///< Keys to sort
        ValueT                  (&values)[ITEMS_PER_THREAD],        ///< Values to sort
        int                     begin_bit,                          ///< The beginning (least-significant) bit index needed for key comparison
        int                     end_bit,                            ///< The past-the-end (most-significant) bit index needed for key comparison
        Int2Type<DESCENDING>    is_descending,                      ///< Tag whether is a descending-order sort
        Int2Type<KEYS_ONLY>     is_keys_only)                       ///< Tag whether is keys-only sort
    {
        UnsignedBits (&unsigned_keys)[ITEMS_PER_THREAD] =
            reinterpret_cast<UnsignedBits (&)[ITEMS_PER_THREAD]>(keys);

        // Twiddle bits if necessary
        #pragma unroll
        for (int KEY = 0; KEY < ITEMS_PER_THREAD; KEY++)
        {
            unsigned_keys[KEY] = KeyTraits::TwiddleIn(unsigned_keys[KEY]);
        }

        // Radix sorting passes
        while (true)
        {
            int pass_bits = CUB_MIN(RADIX_BITS, end_bit - begin_bit);

            // Rank the blocked keys
            int ranks[ITEMS_PER_THREAD];
            RankKeys(unsigned_keys, ranks, begin_bit, pass_bits, is_descending);
            begin_bit += RADIX_BITS;

            CTA_SYNC();

            // Check if this is the last pass
            if (begin_bit >= end_bit)
            {
                // Last pass exchanges keys through shared memory in striped arrangement
                BlockExchangeKeys(temp_storage.exchange_keys).ScatterToStriped(keys, ranks);

                // Last pass exchanges through shared memory in striped arrangement
                ExchangeValues(values, ranks, is_keys_only, Int2Type<false>());

                // Quit
                break;
            }

            // Exchange keys through shared memory in blocked arrangement
            BlockExchangeKeys(temp_storage.exchange_keys).ScatterToBlocked(keys, ranks);

            // Exchange values through shared memory in blocked arrangement
            ExchangeValues(values, ranks, is_keys_only, Int2Type<true>());

            CTA_SYNC();
        }

        // Untwiddle bits if necessary
        #pragma unroll
        for (int KEY = 0; KEY < ITEMS_PER_THREAD; KEY++)
        {
            unsigned_keys[KEY] = KeyTraits::TwiddleOut(unsigned_keys[KEY]);
        }
    }

#endif // DOXYGEN_SHOULD_SKIP_THIS

    /// \smemstorage{BlockRadixSort}
    struct TempStorage : Uninitialized<_TempStorage> {};


    /******************************************************************//**
     * \name Collective constructors
     *********************************************************************/
    //@{

    /**
     * \brief Collective constructor using a private static allocation of shared memory as temporary storage.
     */
    __device__ __forceinline__ BlockRadixSort()
    :
        temp_storage(PrivateStorage()),
        linear_tid(RowMajorTid(BLOCK_DIM_X, BLOCK_DIM_Y, BLOCK_DIM_Z))
    {}


    /**
     * \brief Collective constructor using the specified memory allocation as temporary storage.
     */
    __device__ __forceinline__ BlockRadixSort(
        TempStorage &temp_storage)             ///< [in] Reference to memory allocation having layout type TempStorage
    :
        temp_storage(temp_storage.Alias()),
        linear_tid(RowMajorTid(BLOCK_DIM_X, BLOCK_DIM_Y, BLOCK_DIM_Z))
    {}


    //@}  end member group
    /******************************************************************//**
     * \name Sorting (blocked arrangements)
     *********************************************************************/
    //@{

    /**
     * \brief Performs an ascending block-wide radix sort over a [<em>blocked arrangement</em>](index.html#sec5sec3) of keys.
     *
     * \par
     * - \granularity
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates a sort of 512 integer keys that
     * are partitioned in a [<em>blocked arrangement</em>](index.html#sec5sec3) across 128 threads
     * where each thread owns 4 consecutive keys.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_radix_sort.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize BlockRadixSort for a 1D block of 128 threads owning 4 integer keys each
     *     typedef cub::BlockRadixSort<int, 128, 4> BlockRadixSort;
     *
     *     // Allocate shared memory for BlockRadixSort
     *     __shared__ typename BlockRadixSort::TempStorage temp_storage;
     *
     *     // Obtain a segment of consecutive items that are blocked across threads
     *     int thread_keys[4];
     *     ...
     *
     *     // Collectively sort the keys
     *     BlockRadixSort(temp_storage).Sort(thread_keys);
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_keys across the block of threads is
     * <tt>{ [0,511,1,510], [2,509,3,508], [4,507,5,506], ..., [254,257,255,256] }</tt>.
     * The corresponding output \p thread_keys in those threads will be
     * <tt>{ [0,1,2,3], [4,5,6,7], [8,9,10,11], ..., [508,509,510,511] }</tt>.
     */
    __device__ __forceinline__ void Sort(
        KeyT    (&keys)[ITEMS_PER_THREAD],          ///< [in-out] Keys to sort
        int     begin_bit   = 0,                    ///< [in] <b>[optional]</b> The beginning (least-significant) bit index needed for key comparison
        int     end_bit     = sizeof(KeyT) * 8)      ///< [in] <b>[optional]</b> The past-the-end (most-significant) bit index needed for key comparison
    {
        NullType values[ITEMS_PER_THREAD];

        SortBlocked(keys, values, begin_bit, end_bit, Int2Type<false>(), Int2Type<KEYS_ONLY>());
    }


    /**
     * \brief Performs an ascending block-wide radix sort across a [<em>blocked arrangement</em>](index.html#sec5sec3) of keys and values.
     *
     * \par
     * - BlockRadixSort can only accommodate one associated tile of values. To "truck along"
     *   more than one tile of values, simply perform a key-value sort of the keys paired
     *   with a temporary value array that enumerates the key indices.  The reordered indices
     *   can then be used as a gather-vector for exchanging other associated tile data through
     *   shared memory.
     * - \granularity
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates a sort of 512 integer keys and values that
     * are partitioned in a [<em>blocked arrangement</em>](index.html#sec5sec3) across 128 threads
     * where each thread owns 4 consecutive pairs.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_radix_sort.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize BlockRadixSort for a 1D block of 128 threads owning 4 integer keys and values each
     *     typedef cub::BlockRadixSort<int, 128, 4, int> BlockRadixSort;
     *
     *     // Allocate shared memory for BlockRadixSort
     *     __shared__ typename BlockRadixSort::TempStorage temp_storage;
     *
     *     // Obtain a segment of consecutive items that are blocked across threads
     *     int thread_keys[4];
     *     int thread_values[4];
     *     ...
     *
     *     // Collectively sort the keys and values among block threads
     *     BlockRadixSort(temp_storage).Sort(thread_keys, thread_values);
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_keys across the block of threads is
     * <tt>{ [0,511,1,510], [2,509,3,508], [4,507,5,506], ..., [254,257,255,256] }</tt>.  The
     * corresponding output \p thread_keys in those threads will be
     * <tt>{ [0,1,2,3], [4,5,6,7], [8,9,10,11], ..., [508,509,510,511] }</tt>.
     *
     */
    __device__ __forceinline__ void Sort(
        KeyT    (&keys)[ITEMS_PER_THREAD],          ///< [in-out] Keys to sort
        ValueT  (&values)[ITEMS_PER_THREAD],        ///< [in-out] Values to sort
        int     begin_bit   = 0,                    ///< [in] <b>[optional]</b> The beginning (least-significant) bit index needed for key comparison
        int     end_bit     = sizeof(KeyT) * 8)      ///< [in] <b>[optional]</b> The past-the-end (most-significant) bit index needed for key comparison
    {
        SortBlocked(keys, values, begin_bit, end_bit, Int2Type<false>(), Int2Type<KEYS_ONLY>());
    }

    /**
     * \brief Performs a descending block-wide radix sort over a [<em>blocked arrangement</em>](index.html#sec5sec3) of keys.
     *
     * \par
     * - \granularity
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates a sort of 512 integer keys that
     * are partitioned in a [<em>blocked arrangement</em>](index.html#sec5sec3) across 128 threads
     * where each thread owns 4 consecutive keys.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_radix_sort.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize BlockRadixSort for a 1D block of 128 threads owning 4 integer keys each
     *     typedef cub::BlockRadixSort<int, 128, 4> BlockRadixSort;
     *
     *     // Allocate shared memory for BlockRadixSort
     *     __shared__ typename BlockRadixSort::TempStorage temp_storage;
     *
     *     // Obtain a segment of consecutive items that are blocked across threads
     *     int thread_keys[4];
     *     ...
     *
     *     // Collectively sort the keys
     *     BlockRadixSort(temp_storage).Sort(thread_keys);
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_keys across the block of threads is
     * <tt>{ [0,511,1,510], [2,509,3,508], [4,507,5,506], ..., [254,257,255,256] }</tt>.
     * The corresponding output \p thread_keys in those threads will be
     * <tt>{ [511,510,509,508], [11,10,9,8], [7,6,5,4], ..., [3,2,1,0] }</tt>.
     */
    __device__ __forceinline__ void SortDescending(
        KeyT    (&keys)[ITEMS_PER_THREAD],          ///< [in-out] Keys to sort
        int     begin_bit   = 0,                    ///< [in] <b>[optional]</b> The beginning (least-significant) bit index needed for key comparison
        int     end_bit     = sizeof(KeyT) * 8)      ///< [in] <b>[optional]</b> The past-the-end (most-significant) bit index needed for key comparison
    {
        NullType values[ITEMS_PER_THREAD];

        SortBlocked(keys, values, begin_bit, end_bit, Int2Type<true>(), Int2Type<KEYS_ONLY>());
    }


    /**
     * \brief Performs a descending block-wide radix sort across a [<em>blocked arrangement</em>](index.html#sec5sec3) of keys and values.
     *
     * \par
     * - BlockRadixSort can only accommodate one associated tile of values. To "truck along"
     *   more than one tile of values, simply perform a key-value sort of the keys paired
     *   with a temporary value array that enumerates the key indices.  The reordered indices
     *   can then be used as a gather-vector for exchanging other associated tile data through
     *   shared memory.
     * - \granularity
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates a sort of 512 integer keys and values that
     * are partitioned in a [<em>blocked arrangement</em>](index.html#sec5sec3) across 128 threads
     * where each thread owns 4 consecutive pairs.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_radix_sort.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize BlockRadixSort for a 1D block of 128 threads owning 4 integer keys and values each
     *     typedef cub::BlockRadixSort<int, 128, 4, int> BlockRadixSort;
     *
     *     // Allocate shared memory for BlockRadixSort
     *     __shared__ typename BlockRadixSort::TempStorage temp_storage;
     *
     *     // Obtain a segment of consecutive items that are blocked across threads
     *     int thread_keys[4];
     *     int thread_values[4];
     *     ...
     *
     *     // Collectively sort the keys and values among block threads
     *     BlockRadixSort(temp_storage).Sort(thread_keys, thread_values);
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_keys across the block of threads is
     * <tt>{ [0,511,1,510], [2,509,3,508], [4,507,5,506], ..., [254,257,255,256] }</tt>.  The
     * corresponding output \p thread_keys in those threads will be
     * <tt>{ [511,510,509,508], [11,10,9,8], [7,6,5,4], ..., [3,2,1,0] }</tt>.
     *
     */
    __device__ __forceinline__ void SortDescending(
        KeyT    (&keys)[ITEMS_PER_THREAD],          ///< [in-out] Keys to sort
        ValueT  (&values)[ITEMS_PER_THREAD],        ///< [in-out] Values to sort
        int     begin_bit   = 0,                    ///< [in] <b>[optional]</b> The beginning (least-significant) bit index needed for key comparison
        int     end_bit     = sizeof(KeyT) * 8)      ///< [in] <b>[optional]</b> The past-the-end (most-significant) bit index needed for key comparison
    {
        SortBlocked(keys, values, begin_bit, end_bit, Int2Type<true>(), Int2Type<KEYS_ONLY>());
    }


    //@}  end member group
    /******************************************************************//**
     * \name Sorting (blocked arrangement -> striped arrangement)
     *********************************************************************/
    //@{


    /**
     * \brief Performs an ascending radix sort across a [<em>blocked arrangement</em>](index.html#sec5sec3) of keys, leaving them in a [<em>striped arrangement</em>](index.html#sec5sec3).
     *
     * \par
     * - \granularity
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates a sort of 512 integer keys that
     * are initially partitioned in a [<em>blocked arrangement</em>](index.html#sec5sec3) across 128 threads
     * where each thread owns 4 consecutive keys.  The final partitioning is striped.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_radix_sort.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize BlockRadixSort for a 1D block of 128 threads owning 4 integer keys each
     *     typedef cub::BlockRadixSort<int, 128, 4> BlockRadixSort;
     *
     *     // Allocate shared memory for BlockRadixSort
     *     __shared__ typename BlockRadixSort::TempStorage temp_storage;
     *
     *     // Obtain a segment of consecutive items that are blocked across threads
     *     int thread_keys[4];
     *     ...
     *
     *     // Collectively sort the keys
     *     BlockRadixSort(temp_storage).SortBlockedToStriped(thread_keys);
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_keys across the block of threads is
     * <tt>{ [0,511,1,510], [2,509,3,508], [4,507,5,506], ..., [254,257,255,256] }</tt>.  The
     * corresponding output \p thread_keys in those threads will be
     * <tt>{ [0,128,256,384], [1,129,257,385], [2,130,258,386], ..., [127,255,383,511] }</tt>.
     *
     */
    __device__ __forceinline__ void SortBlockedToStriped(
        KeyT    (&keys)[ITEMS_PER_THREAD],          ///< [in-out] Keys to sort
        int     begin_bit   = 0,                    ///< [in] <b>[optional]</b> The beginning (least-significant) bit index needed for key comparison
        int     end_bit     = sizeof(KeyT) * 8)      ///< [in] <b>[optional]</b> The past-the-end (most-significant) bit index needed for key comparison
    {
        NullType values[ITEMS_PER_THREAD];

        SortBlockedToStriped(keys, values, begin_bit, end_bit, Int2Type<false>(), Int2Type<KEYS_ONLY>());
    }


    /**
     * \brief Performs an ascending radix sort across a [<em>blocked arrangement</em>](index.html#sec5sec3) of keys and values, leaving them in a [<em>striped arrangement</em>](index.html#sec5sec3).
     *
     * \par
     * - BlockRadixSort can only accommodate one associated tile of values. To "truck along"
     *   more than one tile of values, simply perform a key-value sort of the keys paired
     *   with a temporary value array that enumerates the key indices.  The reordered indices
     *   can then be used as a gather-vector for exchanging other associated tile data through
     *   shared memory.
     * - \granularity
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates a sort of 512 integer keys and values that
     * are initially partitioned in a [<em>blocked arrangement</em>](index.html#sec5sec3) across 128 threads
     * where each thread owns 4 consecutive pairs.  The final partitioning is striped.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_radix_sort.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize BlockRadixSort for a 1D block of 128 threads owning 4 integer keys and values each
     *     typedef cub::BlockRadixSort<int, 128, 4, int> BlockRadixSort;
     *
     *     // Allocate shared memory for BlockRadixSort
     *     __shared__ typename BlockRadixSort::TempStorage temp_storage;
     *
     *     // Obtain a segment of consecutive items that are blocked across threads
     *     int thread_keys[4];
     *     int thread_values[4];
     *     ...
     *
     *     // Collectively sort the keys and values among block threads
     *     BlockRadixSort(temp_storage).SortBlockedToStriped(thread_keys, thread_values);
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_keys across the block of threads is
     * <tt>{ [0,511,1,510], [2,509,3,508], [4,507,5,506], ..., [254,257,255,256] }</tt>.  The
     * corresponding output \p thread_keys in those threads will be
     * <tt>{ [0,128,256,384], [1,129,257,385], [2,130,258,386], ..., [127,255,383,511] }</tt>.
     *
     */
    __device__ __forceinline__ void SortBlockedToStriped(
        KeyT    (&keys)[ITEMS_PER_THREAD],          ///< [in-out] Keys to sort
        ValueT  (&values)[ITEMS_PER_THREAD],        ///< [in-out] Values to sort
        int     begin_bit   = 0,                    ///< [in] <b>[optional]</b> The beginning (least-significant) bit index needed for key comparison
        int     end_bit     = sizeof(KeyT) * 8)      ///< [in] <b>[optional]</b> The past-the-end (most-significant) bit index needed for key comparison
    {
        SortBlockedToStriped(keys, values, begin_bit, end_bit, Int2Type<false>(), Int2Type<KEYS_ONLY>());
    }


    /**
     * \brief Performs a descending radix sort across a [<em>blocked arrangement</em>](index.html#sec5sec3) of keys, leaving them in a [<em>striped arrangement</em>](index.html#sec5sec3).
     *
     * \par
     * - \granularity
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates a sort of 512 integer keys that
     * are initially partitioned in a [<em>blocked arrangement</em>](index.html#sec5sec3) across 128 threads
     * where each thread owns 4 consecutive keys.  The final partitioning is striped.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_radix_sort.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize BlockRadixSort for a 1D block of 128 threads owning 4 integer keys each
     *     typedef cub::BlockRadixSort<int, 128, 4> BlockRadixSort;
     *
     *     // Allocate shared memory for BlockRadixSort
     *     __shared__ typename BlockRadixSort::TempStorage temp_storage;
     *
     *     // Obtain a segment of consecutive items that are blocked across threads
     *     int thread_keys[4];
     *     ...
     *
     *     // Collectively sort the keys
     *     BlockRadixSort(temp_storage).SortBlockedToStriped(thread_keys);
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_keys across the block of threads is
     * <tt>{ [0,511,1,510], [2,509,3,508], [4,507,5,506], ..., [254,257,255,256] }</tt>.  The
     * corresponding output \p thread_keys in those threads will be
     * <tt>{ [511,383,255,127], [386,258,130,2], [385,257,128,1], ..., [384,256,128,0] }</tt>.
     *
     */
    __device__ __forceinline__ void SortDescendingBlockedToStriped(
        KeyT    (&keys)[ITEMS_PER_THREAD],          ///< [in-out] Keys to sort
        int     begin_bit   = 0,                    ///< [in] <b>[optional]</b> The beginning (least-significant) bit index needed for key comparison
        int     end_bit     = sizeof(KeyT) * 8)      ///< [in] <b>[optional]</b> The past-the-end (most-significant) bit index needed for key comparison
    {
        NullType values[ITEMS_PER_THREAD];

        SortBlockedToStriped(keys, values, begin_bit, end_bit, Int2Type<true>(), Int2Type<KEYS_ONLY>());
    }


    /**
     * \brief Performs a descending radix sort across a [<em>blocked arrangement</em>](index.html#sec5sec3) of keys and values, leaving them in a [<em>striped arrangement</em>](index.html#sec5sec3).
     *
     * \par
     * - BlockRadixSort can only accommodate one associated tile of values. To "truck along"
     *   more than one tile of values, simply perform a key-value sort of the keys paired
     *   with a temporary value array that enumerates the key indices.  The reordered indices
     *   can then be used as a gather-vector for exchanging other associated tile data through
     *   shared memory.
     * - \granularity
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates a sort of 512 integer keys and values that
     * are initially partitioned in a [<em>blocked arrangement</em>](index.html#sec5sec3) across 128 threads
     * where each thread owns 4 consecutive pairs.  The final partitioning is striped.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_radix_sort.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize BlockRadixSort for a 1D block of 128 threads owning 4 integer keys and values each
     *     typedef cub::BlockRadixSort<int, 128, 4, int> BlockRadixSort;
     *
     *     // Allocate shared memory for BlockRadixSort
     *     __shared__ typename BlockRadixSort::TempStorage temp_storage;
     *
     *     // Obtain a segment of consecutive items that are blocked across threads
     *     int thread_keys[4];
     *     int thread_values[4];
     *     ...
     *
     *     // Collectively sort the keys and values among block threads
     *     BlockRadixSort(temp_storage).SortBlockedToStriped(thread_keys, thread_values);
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_keys across the block of threads is
     * <tt>{ [0,511,1,510], [2,509,3,508], [4,507,5,506], ..., [254,257,255,256] }</tt>.  The
     * corresponding output \p thread_keys in those threads will be
     * <tt>{ [511,383,255,127], [386,258,130,2], [385,257,128,1], ..., [384,256,128,0] }</tt>.
     *
     */
    __device__ __forceinline__ void SortDescendingBlockedToStriped(
        KeyT    (&keys)[ITEMS_PER_THREAD],          ///< [in-out] Keys to sort
        ValueT  (&values)[ITEMS_PER_THREAD],        ///< [in-out] Values to sort
        int     begin_bit   = 0,                    ///< [in] <b>[optional]</b> The beginning (least-significant) bit index needed for key comparison
        int     end_bit     = sizeof(KeyT) * 8)      ///< [in] <b>[optional]</b> The past-the-end (most-significant) bit index needed for key comparison
    {
        SortBlockedToStriped(keys, values, begin_bit, end_bit, Int2Type<true>(), Int2Type<KEYS_ONLY>());
    }


    //@}  end member group

};

/**
 * \example example_block_radix_sort.cu
 */

}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)

