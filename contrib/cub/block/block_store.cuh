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
 * Operations for writing linear segments of data from the CUDA thread block
 */

#pragma once

#include <iterator>

#include "block_exchange.cuh"
#include "../util_ptx.cuh"
#include "../util_macro.cuh"
#include "../util_type.cuh"
#include "../util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {

/**
 * \addtogroup UtilIo
 * @{
 */


/******************************************************************//**
 * \name Blocked arrangement I/O (direct)
 *********************************************************************/
//@{

/**
 * \brief Store a blocked arrangement of items across a thread block into a linear segment of items.
 *
 * \blocked
 *
 * \tparam T                    <b>[inferred]</b> The data type to store.
 * \tparam ITEMS_PER_THREAD     <b>[inferred]</b> The number of consecutive items partitioned onto each thread.
 * \tparam OutputIteratorT      <b>[inferred]</b> The random-access iterator type for output \iterator.
 */
template <
    typename            T,
    int                 ITEMS_PER_THREAD,
    typename            OutputIteratorT>
__device__ __forceinline__ void StoreDirectBlocked(
    int                 linear_tid,                 ///< [in] A suitable 1D thread-identifier for the calling thread (e.g., <tt>(threadIdx.y * blockDim.x) + linear_tid</tt> for 2D thread blocks)
    OutputIteratorT     block_itr,                  ///< [in] The thread block's base output iterator for storing to
    T                   (&items)[ITEMS_PER_THREAD]) ///< [in] Data to store
{
    OutputIteratorT thread_itr = block_itr + (linear_tid * ITEMS_PER_THREAD);

    // Store directly in thread-blocked order
    #pragma unroll
    for (int ITEM = 0; ITEM < ITEMS_PER_THREAD; ITEM++)
    {
        thread_itr[ITEM] = items[ITEM];
    }
}


/**
 * \brief Store a blocked arrangement of items across a thread block into a linear segment of items, guarded by range
 *
 * \blocked
 *
 * \tparam T                    <b>[inferred]</b> The data type to store.
 * \tparam ITEMS_PER_THREAD     <b>[inferred]</b> The number of consecutive items partitioned onto each thread.
 * \tparam OutputIteratorT      <b>[inferred]</b> The random-access iterator type for output \iterator.
 */
template <
    typename            T,
    int                 ITEMS_PER_THREAD,
    typename            OutputIteratorT>
__device__ __forceinline__ void StoreDirectBlocked(
    int                 linear_tid,                 ///< [in] A suitable 1D thread-identifier for the calling thread (e.g., <tt>(threadIdx.y * blockDim.x) + linear_tid</tt> for 2D thread blocks)
    OutputIteratorT     block_itr,                  ///< [in] The thread block's base output iterator for storing to
    T                   (&items)[ITEMS_PER_THREAD], ///< [in] Data to store
    int                 valid_items)                ///< [in] Number of valid items to write
{
    OutputIteratorT thread_itr = block_itr + (linear_tid * ITEMS_PER_THREAD);

    // Store directly in thread-blocked order
    #pragma unroll
    for (int ITEM = 0; ITEM < ITEMS_PER_THREAD; ITEM++)
    {
        if (ITEM + (linear_tid * ITEMS_PER_THREAD) < valid_items)
        {
            thread_itr[ITEM] = items[ITEM];
        }
    }
}


/**
 * \brief Store a blocked arrangement of items across a thread block into a linear segment of items.
 *
 * \blocked
 *
 * The output offset (\p block_ptr + \p block_offset) must be quad-item aligned,
 * which is the default starting offset returned by \p cudaMalloc()
 *
 * \par
 * The following conditions will prevent vectorization and storing will fall back to cub::BLOCK_STORE_DIRECT:
 *   - \p ITEMS_PER_THREAD is odd
 *   - The data type \p T is not a built-in primitive or CUDA vector type (e.g., \p short, \p int2, \p double, \p float2, etc.)
 *
 * \tparam T                    <b>[inferred]</b> The data type to store.
 * \tparam ITEMS_PER_THREAD     <b>[inferred]</b> The number of consecutive items partitioned onto each thread.
 *
 */
template <
    typename            T,
    int                 ITEMS_PER_THREAD>
__device__ __forceinline__ void StoreDirectBlockedVectorized(
    int                 linear_tid,                 ///< [in] A suitable 1D thread-identifier for the calling thread (e.g., <tt>(threadIdx.y * blockDim.x) + linear_tid</tt> for 2D thread blocks)
    T                   *block_ptr,                 ///< [in] Input pointer for storing from
    T                   (&items)[ITEMS_PER_THREAD]) ///< [in] Data to store
{
    enum
    {
        // Maximum CUDA vector size is 4 elements
        MAX_VEC_SIZE = CUB_MIN(4, ITEMS_PER_THREAD),

        // Vector size must be a power of two and an even divisor of the items per thread
        VEC_SIZE = ((((MAX_VEC_SIZE - 1) & MAX_VEC_SIZE) == 0) && ((ITEMS_PER_THREAD % MAX_VEC_SIZE) == 0)) ?
            MAX_VEC_SIZE :
            1,

        VECTORS_PER_THREAD = ITEMS_PER_THREAD / VEC_SIZE,
    };

    // Vector type
    typedef typename CubVector<T, VEC_SIZE>::Type Vector;

    // Alias global pointer
    Vector *block_ptr_vectors = reinterpret_cast<Vector*>(const_cast<T*>(block_ptr));

    // Alias pointers (use "raw" array here which should get optimized away to prevent conservative PTXAS lmem spilling)
    Vector raw_vector[VECTORS_PER_THREAD];
    T *raw_items = reinterpret_cast<T*>(raw_vector);

    // Copy
    #pragma unroll
    for (int ITEM = 0; ITEM < ITEMS_PER_THREAD; ITEM++)
    {
        raw_items[ITEM] = items[ITEM];
    }

    // Direct-store using vector types
    StoreDirectBlocked(linear_tid, block_ptr_vectors, raw_vector);
}



//@}  end member group
/******************************************************************//**
 * \name Striped arrangement I/O (direct)
 *********************************************************************/
//@{


/**
 * \brief Store a striped arrangement of data across the thread block into a linear segment of items.
 *
 * \striped
 *
 * \tparam BLOCK_THREADS        The thread block size in threads
 * \tparam T                    <b>[inferred]</b> The data type to store.
 * \tparam ITEMS_PER_THREAD     <b>[inferred]</b> The number of consecutive items partitioned onto each thread.
 * \tparam OutputIteratorT      <b>[inferred]</b> The random-access iterator type for output \iterator.
 */
template <
    int                 BLOCK_THREADS,
    typename            T,
    int                 ITEMS_PER_THREAD,
    typename            OutputIteratorT>
__device__ __forceinline__ void StoreDirectStriped(
    int                 linear_tid,                 ///< [in] A suitable 1D thread-identifier for the calling thread (e.g., <tt>(threadIdx.y * blockDim.x) + linear_tid</tt> for 2D thread blocks)
    OutputIteratorT     block_itr,                  ///< [in] The thread block's base output iterator for storing to
    T                   (&items)[ITEMS_PER_THREAD]) ///< [in] Data to store
{
    OutputIteratorT thread_itr = block_itr + linear_tid;

    // Store directly in striped order
    #pragma unroll
    for (int ITEM = 0; ITEM < ITEMS_PER_THREAD; ITEM++)
    {
        thread_itr[(ITEM * BLOCK_THREADS)] = items[ITEM];
    }
}


/**
 * \brief Store a striped arrangement of data across the thread block into a linear segment of items, guarded by range
 *
 * \striped
 *
 * \tparam BLOCK_THREADS        The thread block size in threads
 * \tparam T                    <b>[inferred]</b> The data type to store.
 * \tparam ITEMS_PER_THREAD     <b>[inferred]</b> The number of consecutive items partitioned onto each thread.
 * \tparam OutputIteratorT      <b>[inferred]</b> The random-access iterator type for output \iterator.
 */
template <
    int                 BLOCK_THREADS,
    typename            T,
    int                 ITEMS_PER_THREAD,
    typename            OutputIteratorT>
__device__ __forceinline__ void StoreDirectStriped(
    int                 linear_tid,                 ///< [in] A suitable 1D thread-identifier for the calling thread (e.g., <tt>(threadIdx.y * blockDim.x) + linear_tid</tt> for 2D thread blocks)
    OutputIteratorT     block_itr,                  ///< [in] The thread block's base output iterator for storing to
    T                   (&items)[ITEMS_PER_THREAD], ///< [in] Data to store
    int                 valid_items)                ///< [in] Number of valid items to write
{
    OutputIteratorT thread_itr = block_itr + linear_tid;

    // Store directly in striped order
    #pragma unroll
    for (int ITEM = 0; ITEM < ITEMS_PER_THREAD; ITEM++)
    {
        if ((ITEM * BLOCK_THREADS) + linear_tid < valid_items)
        {
            thread_itr[(ITEM * BLOCK_THREADS)] = items[ITEM];
        }
    }
}



//@}  end member group
/******************************************************************//**
 * \name Warp-striped arrangement I/O (direct)
 *********************************************************************/
//@{


/**
 * \brief Store a warp-striped arrangement of data across the thread block into a linear segment of items.
 *
 * \warpstriped
 *
 * \par Usage Considerations
 * The number of threads in the thread block must be a multiple of the architecture's warp size.
 *
 * \tparam T                    <b>[inferred]</b> The data type to store.
 * \tparam ITEMS_PER_THREAD     <b>[inferred]</b> The number of consecutive items partitioned onto each thread.
 * \tparam OutputIteratorT      <b>[inferred]</b> The random-access iterator type for output \iterator.
 */
template <
    typename            T,
    int                 ITEMS_PER_THREAD,
    typename            OutputIteratorT>
__device__ __forceinline__ void StoreDirectWarpStriped(
    int                 linear_tid,                 ///< [in] A suitable 1D thread-identifier for the calling thread (e.g., <tt>(threadIdx.y * blockDim.x) + linear_tid</tt> for 2D thread blocks)
    OutputIteratorT     block_itr,                  ///< [in] The thread block's base output iterator for storing to
    T                   (&items)[ITEMS_PER_THREAD]) ///< [out] Data to load
{
    int tid         = linear_tid & (CUB_PTX_WARP_THREADS - 1);
    int wid         = linear_tid >> CUB_PTX_LOG_WARP_THREADS;
    int warp_offset = wid * CUB_PTX_WARP_THREADS * ITEMS_PER_THREAD;

    OutputIteratorT thread_itr = block_itr + warp_offset + tid;

    // Store directly in warp-striped order
    #pragma unroll
    for (int ITEM = 0; ITEM < ITEMS_PER_THREAD; ITEM++)
    {
        thread_itr[(ITEM * CUB_PTX_WARP_THREADS)] = items[ITEM];
    }
}


/**
 * \brief Store a warp-striped arrangement of data across the thread block into a linear segment of items, guarded by range
 *
 * \warpstriped
 *
 * \par Usage Considerations
 * The number of threads in the thread block must be a multiple of the architecture's warp size.
 *
 * \tparam T                    <b>[inferred]</b> The data type to store.
 * \tparam ITEMS_PER_THREAD     <b>[inferred]</b> The number of consecutive items partitioned onto each thread.
 * \tparam OutputIteratorT      <b>[inferred]</b> The random-access iterator type for output \iterator.
 */
template <
    typename            T,
    int                 ITEMS_PER_THREAD,
    typename            OutputIteratorT>
__device__ __forceinline__ void StoreDirectWarpStriped(
    int                 linear_tid,                 ///< [in] A suitable 1D thread-identifier for the calling thread (e.g., <tt>(threadIdx.y * blockDim.x) + linear_tid</tt> for 2D thread blocks)
    OutputIteratorT     block_itr,                  ///< [in] The thread block's base output iterator for storing to
    T                   (&items)[ITEMS_PER_THREAD], ///< [in] Data to store
    int                 valid_items)                ///< [in] Number of valid items to write
{
    int tid         = linear_tid & (CUB_PTX_WARP_THREADS - 1);
    int wid         = linear_tid >> CUB_PTX_LOG_WARP_THREADS;
    int warp_offset = wid * CUB_PTX_WARP_THREADS * ITEMS_PER_THREAD;

    OutputIteratorT thread_itr = block_itr + warp_offset + tid;

    // Store directly in warp-striped order
    #pragma unroll
    for (int ITEM = 0; ITEM < ITEMS_PER_THREAD; ITEM++)
    {
        if (warp_offset + tid + (ITEM * CUB_PTX_WARP_THREADS) < valid_items)
        {
            thread_itr[(ITEM * CUB_PTX_WARP_THREADS)] = items[ITEM];
        }
    }
}


//@}  end member group


/** @} */       // end group UtilIo


//-----------------------------------------------------------------------------
// Generic BlockStore abstraction
//-----------------------------------------------------------------------------

/**
 * \brief cub::BlockStoreAlgorithm enumerates alternative algorithms for cub::BlockStore to write a blocked arrangement of items across a CUDA thread block to a linear segment of memory.
 */
enum BlockStoreAlgorithm
{
    /**
     * \par Overview
     *
     * A [<em>blocked arrangement</em>](index.html#sec5sec3) of data is written
     * directly to memory.
     *
     * \par Performance Considerations
     * - The utilization of memory transactions (coalescing) decreases as the
     *   access stride between threads increases (i.e., the number items per thread).
     */
    BLOCK_STORE_DIRECT,

    /**
     * \par Overview
     *
     * A [<em>blocked arrangement</em>](index.html#sec5sec3) of data is written directly
     * to memory using CUDA's built-in vectorized stores as a coalescing optimization.
     * For example, <tt>st.global.v4.s32</tt> instructions will be generated
     * when \p T = \p int and \p ITEMS_PER_THREAD % 4 == 0.
     *
     * \par Performance Considerations
     * - The utilization of memory transactions (coalescing) remains high until the the
     *   access stride between threads (i.e., the number items per thread) exceeds the
     *   maximum vector store width (typically 4 items or 64B, whichever is lower).
     * - The following conditions will prevent vectorization and writing will fall back to cub::BLOCK_STORE_DIRECT:
     *   - \p ITEMS_PER_THREAD is odd
     *   - The \p OutputIteratorT is not a simple pointer type
     *   - The block output offset is not quadword-aligned
     *   - The data type \p T is not a built-in primitive or CUDA vector type (e.g., \p short, \p int2, \p double, \p float2, etc.)
     */
    BLOCK_STORE_VECTORIZE,

    /**
     * \par Overview
     * A [<em>blocked arrangement</em>](index.html#sec5sec3) is locally
     * transposed and then efficiently written to memory as a [<em>striped arrangement</em>](index.html#sec5sec3).
     *
     * \par Performance Considerations
     * - The utilization of memory transactions (coalescing) remains high regardless
     *   of items written per thread.
     * - The local reordering incurs slightly longer latencies and throughput than the
     *   direct cub::BLOCK_STORE_DIRECT and cub::BLOCK_STORE_VECTORIZE alternatives.
     */
    BLOCK_STORE_TRANSPOSE,

    /**
     * \par Overview
     * A [<em>blocked arrangement</em>](index.html#sec5sec3) is locally
     * transposed and then efficiently written to memory as a
     * [<em>warp-striped arrangement</em>](index.html#sec5sec3)
     *
     * \par Usage Considerations
     * - BLOCK_THREADS must be a multiple of WARP_THREADS
     *
     * \par Performance Considerations
     * - The utilization of memory transactions (coalescing) remains high regardless
     *   of items written per thread.
     * - The local reordering incurs slightly longer latencies and throughput than the
     *   direct cub::BLOCK_STORE_DIRECT and cub::BLOCK_STORE_VECTORIZE alternatives.
     */
    BLOCK_STORE_WARP_TRANSPOSE,

    /**
     * \par Overview
     * A [<em>blocked arrangement</em>](index.html#sec5sec3) is locally
     * transposed and then efficiently written to memory as a
     * [<em>warp-striped arrangement</em>](index.html#sec5sec3)
     * To reduce the shared memory requirement, only one warp's worth of shared
     * memory is provisioned and is subsequently time-sliced among warps.
     *
     * \par Usage Considerations
     * - BLOCK_THREADS must be a multiple of WARP_THREADS
     *
     * \par Performance Considerations
     * - The utilization of memory transactions (coalescing) remains high regardless
     *   of items written per thread.
     * - Provisions less shared memory temporary storage, but incurs larger
     *   latencies than the BLOCK_STORE_WARP_TRANSPOSE alternative.
     */
    BLOCK_STORE_WARP_TRANSPOSE_TIMESLICED,

};


/**
 * \brief The BlockStore class provides [<em>collective</em>](index.html#sec0) data movement methods for writing a [<em>blocked arrangement</em>](index.html#sec5sec3) of items partitioned across a CUDA thread block to a linear segment of memory.  ![](block_store_logo.png)
 * \ingroup BlockModule
 * \ingroup UtilIo
 *
 * \tparam T                    The type of data to be written.
 * \tparam BLOCK_DIM_X          The thread block length in threads along the X dimension
 * \tparam ITEMS_PER_THREAD     The number of consecutive items partitioned onto each thread.
 * \tparam ALGORITHM            <b>[optional]</b> cub::BlockStoreAlgorithm tuning policy enumeration.  default: cub::BLOCK_STORE_DIRECT.
 * \tparam WARP_TIME_SLICING    <b>[optional]</b> Whether or not only one warp's worth of shared memory should be allocated and time-sliced among block-warps during any load-related data transpositions (versus each warp having its own storage). (default: false)
 * \tparam BLOCK_DIM_Y          <b>[optional]</b> The thread block length in threads along the Y dimension (default: 1)
 * \tparam BLOCK_DIM_Z          <b>[optional]</b> The thread block length in threads along the Z dimension (default: 1)
 * \tparam PTX_ARCH             <b>[optional]</b> \ptxversion
 *
 * \par Overview
 * - The BlockStore class provides a single data movement abstraction that can be specialized
 *   to implement different cub::BlockStoreAlgorithm strategies.  This facilitates different
 *   performance policies for different architectures, data types, granularity sizes, etc.
 * - BlockStore can be optionally specialized by different data movement strategies:
 *   -# <b>cub::BLOCK_STORE_DIRECT</b>.  A [<em>blocked arrangement</em>](index.html#sec5sec3) of data is written
 *      directly to memory. [More...](\ref cub::BlockStoreAlgorithm)
 *   -# <b>cub::BLOCK_STORE_VECTORIZE</b>.  A [<em>blocked arrangement</em>](index.html#sec5sec3)
 *      of data is written directly to memory using CUDA's built-in vectorized stores as a
 *      coalescing optimization.  [More...](\ref cub::BlockStoreAlgorithm)
 *   -# <b>cub::BLOCK_STORE_TRANSPOSE</b>.  A [<em>blocked arrangement</em>](index.html#sec5sec3)
 *      is locally transposed into a [<em>striped arrangement</em>](index.html#sec5sec3) which is
 *      then written to memory.  [More...](\ref cub::BlockStoreAlgorithm)
 *   -# <b>cub::BLOCK_STORE_WARP_TRANSPOSE</b>.  A [<em>blocked arrangement</em>](index.html#sec5sec3)
 *      is locally transposed into a [<em>warp-striped arrangement</em>](index.html#sec5sec3) which is
 *      then written to memory.  [More...](\ref cub::BlockStoreAlgorithm)
 * - \rowmajor
 *
 * \par A Simple Example
 * \blockcollective{BlockStore}
 * \par
 * The code snippet below illustrates the storing of a "blocked" arrangement
 * of 512 integers across 128 threads (where each thread owns 4 consecutive items)
 * into a linear segment of memory.  The store is specialized for \p BLOCK_STORE_WARP_TRANSPOSE,
 * meaning items are locally reordered among threads so that memory references will be
 * efficiently coalesced using a warp-striped access pattern.
 * \par
 * \code
 * #include <cub/cub.cuh>   // or equivalently <cub/block/block_store.cuh>
 *
 * __global__ void ExampleKernel(int *d_data, ...)
 * {
 *     // Specialize BlockStore for a 1D block of 128 threads owning 4 integer items each
 *     typedef cub::BlockStore<int, 128, 4, BLOCK_STORE_WARP_TRANSPOSE> BlockStore;
 *
 *     // Allocate shared memory for BlockStore
 *     __shared__ typename BlockStore::TempStorage temp_storage;
 *
 *     // Obtain a segment of consecutive items that are blocked across threads
 *     int thread_data[4];
 *     ...
 *
 *     // Store items to linear memory
 *     int thread_data[4];
 *     BlockStore(temp_storage).Store(d_data, thread_data);
 *
 * \endcode
 * \par
 * Suppose the set of \p thread_data across the block of threads is
 * <tt>{ [0,1,2,3], [4,5,6,7], ..., [508,509,510,511] }</tt>.
 * The output \p d_data will be <tt>0, 1, 2, 3, 4, 5, ...</tt>.
 *
 */
template <
    typename                T,
    int                     BLOCK_DIM_X,
    int                     ITEMS_PER_THREAD,
    BlockStoreAlgorithm     ALGORITHM           = BLOCK_STORE_DIRECT,
    int                     BLOCK_DIM_Y         = 1,
    int                     BLOCK_DIM_Z         = 1,
    int                     PTX_ARCH            = CUB_PTX_ARCH>
class BlockStore
{
private:
    /******************************************************************************
     * Constants and typed definitions
     ******************************************************************************/

    /// Constants
    enum
    {
        /// The thread block size in threads
        BLOCK_THREADS = BLOCK_DIM_X * BLOCK_DIM_Y * BLOCK_DIM_Z,
    };


    /******************************************************************************
     * Algorithmic variants
     ******************************************************************************/

    /// Store helper
    template <BlockStoreAlgorithm _POLICY, int DUMMY>
    struct StoreInternal;


    /**
     * BLOCK_STORE_DIRECT specialization of store helper
     */
    template <int DUMMY>
    struct StoreInternal<BLOCK_STORE_DIRECT, DUMMY>
    {
        /// Shared memory storage layout type
        typedef NullType TempStorage;

        /// Linear thread-id
        int linear_tid;

        /// Constructor
        __device__ __forceinline__ StoreInternal(
            TempStorage &/*temp_storage*/,
            int linear_tid)
        :
            linear_tid(linear_tid)
        {}

        /// Store items into a linear segment of memory
        template <typename OutputIteratorT>
        __device__ __forceinline__ void Store(
            OutputIteratorT     block_itr,                  ///< [in] The thread block's base output iterator for storing to
            T                   (&items)[ITEMS_PER_THREAD]) ///< [in] Data to store
        {
            StoreDirectBlocked(linear_tid, block_itr, items);
        }

        /// Store items into a linear segment of memory, guarded by range
        template <typename OutputIteratorT>
        __device__ __forceinline__ void Store(
            OutputIteratorT     block_itr,                  ///< [in] The thread block's base output iterator for storing to
            T                   (&items)[ITEMS_PER_THREAD], ///< [in] Data to store
            int                 valid_items)                ///< [in] Number of valid items to write
        {
            StoreDirectBlocked(linear_tid, block_itr, items, valid_items);
        }
    };


    /**
     * BLOCK_STORE_VECTORIZE specialization of store helper
     */
    template <int DUMMY>
    struct StoreInternal<BLOCK_STORE_VECTORIZE, DUMMY>
    {
        /// Shared memory storage layout type
        typedef NullType TempStorage;

        /// Linear thread-id
        int linear_tid;

        /// Constructor
        __device__ __forceinline__ StoreInternal(
            TempStorage &/*temp_storage*/,
            int linear_tid)
        :
            linear_tid(linear_tid)
        {}

        /// Store items into a linear segment of memory, specialized for native pointer types (attempts vectorization)
        __device__ __forceinline__ void Store(
            T                   *block_ptr,                 ///< [in] The thread block's base output iterator for storing to
            T                   (&items)[ITEMS_PER_THREAD]) ///< [in] Data to store
        {
            StoreDirectBlockedVectorized(linear_tid, block_ptr, items);
        }

        /// Store items into a linear segment of memory, specialized for opaque input iterators (skips vectorization)
        template <typename OutputIteratorT>
        __device__ __forceinline__ void Store(
            OutputIteratorT    block_itr,                  ///< [in] The thread block's base output iterator for storing to
            T                   (&items)[ITEMS_PER_THREAD]) ///< [in] Data to store
        {
            StoreDirectBlocked(linear_tid, block_itr, items);
        }

        /// Store items into a linear segment of memory, guarded by range
        template <typename OutputIteratorT>
        __device__ __forceinline__ void Store(
            OutputIteratorT     block_itr,                  ///< [in] The thread block's base output iterator for storing to
            T                   (&items)[ITEMS_PER_THREAD], ///< [in] Data to store
            int                 valid_items)                ///< [in] Number of valid items to write
        {
            StoreDirectBlocked(linear_tid, block_itr, items, valid_items);
        }
    };


    /**
     * BLOCK_STORE_TRANSPOSE specialization of store helper
     */
    template <int DUMMY>
    struct StoreInternal<BLOCK_STORE_TRANSPOSE, DUMMY>
    {
        // BlockExchange utility type for keys
        typedef BlockExchange<T, BLOCK_DIM_X, ITEMS_PER_THREAD, false, BLOCK_DIM_Y, BLOCK_DIM_Z, PTX_ARCH> BlockExchange;

        /// Shared memory storage layout type
        struct _TempStorage : BlockExchange::TempStorage
        {
            /// Temporary storage for partially-full block guard
            volatile int valid_items;
        };

        /// Alias wrapper allowing storage to be unioned
        struct TempStorage : Uninitialized<_TempStorage> {};

        /// Thread reference to shared storage
        _TempStorage &temp_storage;

        /// Linear thread-id
        int linear_tid;

        /// Constructor
        __device__ __forceinline__ StoreInternal(
            TempStorage &temp_storage,
            int linear_tid)
        :
            temp_storage(temp_storage.Alias()),
            linear_tid(linear_tid)
        {}

        /// Store items into a linear segment of memory
        template <typename OutputIteratorT>
        __device__ __forceinline__ void Store(
            OutputIteratorT     block_itr,                  ///< [in] The thread block's base output iterator for storing to
            T                   (&items)[ITEMS_PER_THREAD]) ///< [in] Data to store
        {
            BlockExchange(temp_storage).BlockedToStriped(items);
            StoreDirectStriped<BLOCK_THREADS>(linear_tid, block_itr, items);
        }

        /// Store items into a linear segment of memory, guarded by range
        template <typename OutputIteratorT>
        __device__ __forceinline__ void Store(
            OutputIteratorT   block_itr,                  ///< [in] The thread block's base output iterator for storing to
            T                   (&items)[ITEMS_PER_THREAD], ///< [in] Data to store
            int                 valid_items)                ///< [in] Number of valid items to write
        {
            BlockExchange(temp_storage).BlockedToStriped(items);
            if (linear_tid == 0)
                temp_storage.valid_items = valid_items;     // Move through volatile smem as a workaround to prevent RF spilling on subsequent loads
            CTA_SYNC();
            StoreDirectStriped<BLOCK_THREADS>(linear_tid, block_itr, items, temp_storage.valid_items);
        }
    };


    /**
     * BLOCK_STORE_WARP_TRANSPOSE specialization of store helper
     */
    template <int DUMMY>
    struct StoreInternal<BLOCK_STORE_WARP_TRANSPOSE, DUMMY>
    {
        enum
        {
            WARP_THREADS = CUB_WARP_THREADS(PTX_ARCH)
        };

        // Assert BLOCK_THREADS must be a multiple of WARP_THREADS
        CUB_STATIC_ASSERT((BLOCK_THREADS % WARP_THREADS == 0), "BLOCK_THREADS must be a multiple of WARP_THREADS");

        // BlockExchange utility type for keys
        typedef BlockExchange<T, BLOCK_DIM_X, ITEMS_PER_THREAD, false, BLOCK_DIM_Y, BLOCK_DIM_Z, PTX_ARCH> BlockExchange;

        /// Shared memory storage layout type
        struct _TempStorage : BlockExchange::TempStorage
        {
            /// Temporary storage for partially-full block guard
            volatile int valid_items;
        };

        /// Alias wrapper allowing storage to be unioned
        struct TempStorage : Uninitialized<_TempStorage> {};

        /// Thread reference to shared storage
        _TempStorage &temp_storage;

        /// Linear thread-id
        int linear_tid;

        /// Constructor
        __device__ __forceinline__ StoreInternal(
            TempStorage &temp_storage,
            int linear_tid)
        :
            temp_storage(temp_storage.Alias()),
            linear_tid(linear_tid)
        {}

        /// Store items into a linear segment of memory
        template <typename OutputIteratorT>
        __device__ __forceinline__ void Store(
            OutputIteratorT   block_itr,                    ///< [in] The thread block's base output iterator for storing to
            T                 (&items)[ITEMS_PER_THREAD])   ///< [in] Data to store
        {
            BlockExchange(temp_storage).BlockedToWarpStriped(items);
            StoreDirectWarpStriped(linear_tid, block_itr, items);
        }

        /// Store items into a linear segment of memory, guarded by range
        template <typename OutputIteratorT>
        __device__ __forceinline__ void Store(
            OutputIteratorT   block_itr,                    ///< [in] The thread block's base output iterator for storing to
            T                 (&items)[ITEMS_PER_THREAD],   ///< [in] Data to store
            int               valid_items)                  ///< [in] Number of valid items to write
        {
            BlockExchange(temp_storage).BlockedToWarpStriped(items);
            if (linear_tid == 0)
                temp_storage.valid_items = valid_items;     // Move through volatile smem as a workaround to prevent RF spilling on subsequent loads
            CTA_SYNC();
            StoreDirectWarpStriped(linear_tid, block_itr, items, temp_storage.valid_items);
        }
    };


    /**
     * BLOCK_STORE_WARP_TRANSPOSE_TIMESLICED specialization of store helper
     */
    template <int DUMMY>
    struct StoreInternal<BLOCK_STORE_WARP_TRANSPOSE_TIMESLICED, DUMMY>
    {
        enum
        {
            WARP_THREADS = CUB_WARP_THREADS(PTX_ARCH)
        };

        // Assert BLOCK_THREADS must be a multiple of WARP_THREADS
        CUB_STATIC_ASSERT((BLOCK_THREADS % WARP_THREADS == 0), "BLOCK_THREADS must be a multiple of WARP_THREADS");

        // BlockExchange utility type for keys
        typedef BlockExchange<T, BLOCK_DIM_X, ITEMS_PER_THREAD, true, BLOCK_DIM_Y, BLOCK_DIM_Z, PTX_ARCH> BlockExchange;

        /// Shared memory storage layout type
        struct _TempStorage : BlockExchange::TempStorage
        {
            /// Temporary storage for partially-full block guard
            volatile int valid_items;
        };

        /// Alias wrapper allowing storage to be unioned
        struct TempStorage : Uninitialized<_TempStorage> {};

        /// Thread reference to shared storage
        _TempStorage &temp_storage;

        /// Linear thread-id
        int linear_tid;

        /// Constructor
        __device__ __forceinline__ StoreInternal(
            TempStorage &temp_storage,
            int linear_tid)
        :
            temp_storage(temp_storage.Alias()),
            linear_tid(linear_tid)
        {}

        /// Store items into a linear segment of memory
        template <typename OutputIteratorT>
        __device__ __forceinline__ void Store(
            OutputIteratorT     block_itr,                  ///< [in] The thread block's base output iterator for storing to
            T                   (&items)[ITEMS_PER_THREAD]) ///< [in] Data to store
        {
            BlockExchange(temp_storage).BlockedToWarpStriped(items);
            StoreDirectWarpStriped(linear_tid, block_itr, items);
        }

        /// Store items into a linear segment of memory, guarded by range
        template <typename OutputIteratorT>
        __device__ __forceinline__ void Store(
            OutputIteratorT   block_itr,                  ///< [in] The thread block's base output iterator for storing to
            T                   (&items)[ITEMS_PER_THREAD], ///< [in] Data to store
            int                 valid_items)                ///< [in] Number of valid items to write
        {
            BlockExchange(temp_storage).BlockedToWarpStriped(items);
            if (linear_tid == 0)
                temp_storage.valid_items = valid_items;     // Move through volatile smem as a workaround to prevent RF spilling on subsequent loads
            CTA_SYNC();
            StoreDirectWarpStriped(linear_tid, block_itr, items, temp_storage.valid_items);
        }
    };

    /******************************************************************************
     * Type definitions
     ******************************************************************************/

    /// Internal load implementation to use
    typedef StoreInternal<ALGORITHM, 0> InternalStore;


    /// Shared memory storage layout type
    typedef typename InternalStore::TempStorage _TempStorage;


    /******************************************************************************
     * Utility methods
     ******************************************************************************/

    /// Internal storage allocator
    __device__ __forceinline__ _TempStorage& PrivateStorage()
    {
        __shared__ _TempStorage private_storage;
        return private_storage;
    }


    /******************************************************************************
     * Thread fields
     ******************************************************************************/

    /// Thread reference to shared storage
    _TempStorage &temp_storage;

    /// Linear thread-id
    int linear_tid;

public:


    /// \smemstorage{BlockStore}
    struct TempStorage : Uninitialized<_TempStorage> {};


    /******************************************************************//**
     * \name Collective constructors
     *********************************************************************/
    //@{

    /**
     * \brief Collective constructor using a private static allocation of shared memory as temporary storage.
     */
    __device__ __forceinline__ BlockStore()
    :
        temp_storage(PrivateStorage()),
        linear_tid(RowMajorTid(BLOCK_DIM_X, BLOCK_DIM_Y, BLOCK_DIM_Z))
    {}


    /**
     * \brief Collective constructor using the specified memory allocation as temporary storage.
     */
    __device__ __forceinline__ BlockStore(
        TempStorage &temp_storage)             ///< [in] Reference to memory allocation having layout type TempStorage
    :
        temp_storage(temp_storage.Alias()),
        linear_tid(RowMajorTid(BLOCK_DIM_X, BLOCK_DIM_Y, BLOCK_DIM_Z))
    {}


    //@}  end member group
    /******************************************************************//**
     * \name Data movement
     *********************************************************************/
    //@{


    /**
     * \brief Store items into a linear segment of memory.
     *
     * \par
     * - \blocked
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates the storing of a "blocked" arrangement
     * of 512 integers across 128 threads (where each thread owns 4 consecutive items)
     * into a linear segment of memory.  The store is specialized for \p BLOCK_STORE_WARP_TRANSPOSE,
     * meaning items are locally reordered among threads so that memory references will be
     * efficiently coalesced using a warp-striped access pattern.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_store.cuh>
     *
     * __global__ void ExampleKernel(int *d_data, ...)
     * {
     *     // Specialize BlockStore for a 1D block of 128 threads owning 4 integer items each
     *     typedef cub::BlockStore<int, 128, 4, BLOCK_STORE_WARP_TRANSPOSE> BlockStore;
     *
     *     // Allocate shared memory for BlockStore
     *     __shared__ typename BlockStore::TempStorage temp_storage;
     *
     *     // Obtain a segment of consecutive items that are blocked across threads
     *     int thread_data[4];
     *     ...
     *
     *     // Store items to linear memory
     *     int thread_data[4];
     *     BlockStore(temp_storage).Store(d_data, thread_data);
     *
     * \endcode
     * \par
     * Suppose the set of \p thread_data across the block of threads is
     * <tt>{ [0,1,2,3], [4,5,6,7], ..., [508,509,510,511] }</tt>.
     * The output \p d_data will be <tt>0, 1, 2, 3, 4, 5, ...</tt>.
     *
     */
    template <typename OutputIteratorT>
    __device__ __forceinline__ void Store(
        OutputIteratorT     block_itr,                  ///< [in] The thread block's base output iterator for storing to
        T                   (&items)[ITEMS_PER_THREAD]) ///< [in] Data to store
    {
        InternalStore(temp_storage, linear_tid).Store(block_itr, items);
    }

    /**
     * \brief Store items into a linear segment of memory, guarded by range.
     *
     * \par
     * - \blocked
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates the guarded storing of a "blocked" arrangement
     * of 512 integers across 128 threads (where each thread owns 4 consecutive items)
     * into a linear segment of memory.  The store is specialized for \p BLOCK_STORE_WARP_TRANSPOSE,
     * meaning items are locally reordered among threads so that memory references will be
     * efficiently coalesced using a warp-striped access pattern.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_store.cuh>
     *
     * __global__ void ExampleKernel(int *d_data, int valid_items, ...)
     * {
     *     // Specialize BlockStore for a 1D block of 128 threads owning 4 integer items each
     *     typedef cub::BlockStore<int, 128, 4, BLOCK_STORE_WARP_TRANSPOSE> BlockStore;
     *
     *     // Allocate shared memory for BlockStore
     *     __shared__ typename BlockStore::TempStorage temp_storage;
     *
     *     // Obtain a segment of consecutive items that are blocked across threads
     *     int thread_data[4];
     *     ...
     *
     *     // Store items to linear memory
     *     int thread_data[4];
     *     BlockStore(temp_storage).Store(d_data, thread_data, valid_items);
     *
     * \endcode
     * \par
     * Suppose the set of \p thread_data across the block of threads is
     * <tt>{ [0,1,2,3], [4,5,6,7], ..., [508,509,510,511] }</tt> and \p valid_items is \p 5.
     * The output \p d_data will be <tt>0, 1, 2, 3, 4, ?, ?, ?, ...</tt>, with
     * only the first two threads being unmasked to store portions of valid data.
     *
     */
    template <typename OutputIteratorT>
    __device__ __forceinline__ void Store(
        OutputIteratorT     block_itr,                  ///< [in] The thread block's base output iterator for storing to
        T                   (&items)[ITEMS_PER_THREAD], ///< [in] Data to store
        int                 valid_items)                ///< [in] Number of valid items to write
    {
        InternalStore(temp_storage, linear_tid).Store(block_itr, items, valid_items);
    }
};


}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)

