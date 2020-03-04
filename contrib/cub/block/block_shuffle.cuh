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
 * The cub::BlockShuffle class provides [<em>collective</em>](index.html#sec0) methods for shuffling data partitioned across a CUDA thread block.
 */

#pragma once

#include "../util_arch.cuh"
#include "../util_ptx.cuh"
#include "../util_macro.cuh"
#include "../util_type.cuh"
#include "../util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {

/**
 * \brief The BlockShuffle class provides [<em>collective</em>](index.html#sec0) methods for shuffling data partitioned across a CUDA thread block.
 * \ingroup BlockModule
 *
 * \tparam T                    The data type to be exchanged.
 * \tparam BLOCK_DIM_X          The thread block length in threads along the X dimension
 * \tparam BLOCK_DIM_Y          <b>[optional]</b> The thread block length in threads along the Y dimension (default: 1)
 * \tparam BLOCK_DIM_Z          <b>[optional]</b> The thread block length in threads along the Z dimension (default: 1)
 * \tparam PTX_ARCH             <b>[optional]</b> \ptxversion
 *
 * \par Overview
 * It is commonplace for blocks of threads to rearrange data items between
 * threads.  The BlockShuffle abstraction allows threads to efficiently shift items
 * either (a) up to their successor or (b) down to their predecessor.
 *
 */
template <
    typename            T,
    int                 BLOCK_DIM_X,
    int                 BLOCK_DIM_Y         = 1,
    int                 BLOCK_DIM_Z         = 1,
    int                 PTX_ARCH            = CUB_PTX_ARCH>
class BlockShuffle
{
private:

    /******************************************************************************
     * Constants
     ******************************************************************************/

    enum
    {
        BLOCK_THREADS               = BLOCK_DIM_X * BLOCK_DIM_Y * BLOCK_DIM_Z,

        LOG_WARP_THREADS            = CUB_LOG_WARP_THREADS(PTX_ARCH),
        WARP_THREADS                = 1 << LOG_WARP_THREADS,
        WARPS                       = (BLOCK_THREADS + WARP_THREADS - 1) / WARP_THREADS,
    };

    /******************************************************************************
     * Type definitions
     ******************************************************************************/

    /// Shared memory storage layout type (last element from each thread's input)
    struct _TempStorage
    {
        T prev[BLOCK_THREADS];
        T next[BLOCK_THREADS];
    };


public:

    /// \smemstorage{BlockShuffle}
    struct TempStorage : Uninitialized<_TempStorage> {};

private:


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


public:

    /******************************************************************//**
     * \name Collective constructors
     *********************************************************************/
    //@{

    /**
     * \brief Collective constructor using a private static allocation of shared memory as temporary storage.
     */
    __device__ __forceinline__ BlockShuffle()
    :
        temp_storage(PrivateStorage()),
        linear_tid(RowMajorTid(BLOCK_DIM_X, BLOCK_DIM_Y, BLOCK_DIM_Z))
    {}


    /**
     * \brief Collective constructor using the specified memory allocation as temporary storage.
     */
    __device__ __forceinline__ BlockShuffle(
        TempStorage &temp_storage)             ///< [in] Reference to memory allocation having layout type TempStorage
    :
        temp_storage(temp_storage.Alias()),
        linear_tid(RowMajorTid(BLOCK_DIM_X, BLOCK_DIM_Y, BLOCK_DIM_Z))
    {}


    //@}  end member group
    /******************************************************************//**
     * \name Shuffle movement
     *********************************************************************/
    //@{


    /**
     * \brief Each <em>thread<sub>i</sub></em> obtains the \p input provided by <em>thread</em><sub><em>i</em>+<tt>distance</tt></sub>. The offset \p distance may be negative.
     *
     * \par
     * - \smemreuse
     */
    __device__ __forceinline__ void Offset(
        T   input,                  ///< [in] The input item from the calling thread (<em>thread<sub>i</sub></em>)
        T&  output,                 ///< [out] The \p input item from the successor (or predecessor) thread <em>thread</em><sub><em>i</em>+<tt>distance</tt></sub> (may be aliased to \p input).  This value is only updated for for <em>thread<sub>i</sub></em> when 0 <= (<em>i</em> + \p distance) < <tt>BLOCK_THREADS-1</tt>
        int distance = 1)           ///< [in] Offset distance (may be negative)
    {
        temp_storage[linear_tid].prev = input;

        CTA_SYNC();

        if ((linear_tid + distance >= 0) && (linear_tid + distance < BLOCK_THREADS))
            output = temp_storage[linear_tid + distance].prev;
    }


    /**
     * \brief Each <em>thread<sub>i</sub></em> obtains the \p input provided by <em>thread</em><sub><em>i</em>+<tt>distance</tt></sub>.
     *
     * \par
     * - \smemreuse
     */
    __device__ __forceinline__ void Rotate(
        T   input,                  ///< [in] The calling thread's input item
        T&  output,                 ///< [out] The \p input item from thread <em>thread</em><sub>(<em>i</em>+<tt>distance></tt>)%<tt><BLOCK_THREADS></tt></sub> (may be aliased to \p input).  This value is not updated for <em>thread</em><sub>BLOCK_THREADS-1</sub>
        unsigned int distance = 1)  ///< [in] Offset distance (0 < \p distance < <tt>BLOCK_THREADS</tt>)
    {
        temp_storage[linear_tid].prev = input;

        CTA_SYNC();

        unsigned int offset = threadIdx.x + distance;
        if (offset >= BLOCK_THREADS)
            offset -= BLOCK_THREADS;

        output = temp_storage[offset].prev;
    }


    /**
     * \brief The thread block rotates its [<em>blocked arrangement</em>](index.html#sec5sec3) of \p input items, shifting it up by one item
     *
     * \par
     * - \blocked
     * - \granularity
     * - \smemreuse
     */
    template <int ITEMS_PER_THREAD>
    __device__ __forceinline__ void Up(
        T (&input)[ITEMS_PER_THREAD],   ///< [in] The calling thread's input items
        T (&prev)[ITEMS_PER_THREAD])    ///< [out] The corresponding predecessor items (may be aliased to \p input).  The item \p prev[0] is not updated for <em>thread</em><sub>0</sub>.
    {
        temp_storage[linear_tid].prev = input[ITEMS_PER_THREAD - 1];

        CTA_SYNC();

        #pragma unroll
        for (int ITEM = ITEMS_PER_THREAD - 1; ITEM > 0; --ITEM)
            prev[ITEM] = input[ITEM - 1];


        if (linear_tid > 0)
            prev[0] = temp_storage[linear_tid - 1].prev;
    }


    /**
     * \brief The thread block rotates its [<em>blocked arrangement</em>](index.html#sec5sec3) of \p input items, shifting it up by one item.  All threads receive the \p input provided by <em>thread</em><sub><tt>BLOCK_THREADS-1</tt></sub>.
     *
     * \par
     * - \blocked
     * - \granularity
     * - \smemreuse
     */
    template <int ITEMS_PER_THREAD>
    __device__ __forceinline__ void Up(
        T (&input)[ITEMS_PER_THREAD],   ///< [in] The calling thread's input items
        T (&prev)[ITEMS_PER_THREAD],    ///< [out] The corresponding predecessor items (may be aliased to \p input).  The item \p prev[0] is not updated for <em>thread</em><sub>0</sub>.
        T &block_suffix)                ///< [out] The item \p input[ITEMS_PER_THREAD-1] from <em>thread</em><sub><tt>BLOCK_THREADS-1</tt></sub>, provided to all threads
    {
        Up(input, prev);
        block_suffix = temp_storage[BLOCK_THREADS - 1].prev;
    }


    /**
     * \brief The thread block rotates its [<em>blocked arrangement</em>](index.html#sec5sec3) of \p input items, shifting it down by one item
     *
     * \par
     * - \blocked
     * - \granularity
     * - \smemreuse
     */
    template <int ITEMS_PER_THREAD>
    __device__ __forceinline__ void Down(
        T (&input)[ITEMS_PER_THREAD],   ///< [in] The calling thread's input items
        T (&prev)[ITEMS_PER_THREAD])    ///< [out] The corresponding predecessor items (may be aliased to \p input).  The value \p prev[0] is not updated for <em>thread</em><sub>BLOCK_THREADS-1</sub>.
    {
        temp_storage[linear_tid].prev = input[ITEMS_PER_THREAD - 1];

        CTA_SYNC();

        #pragma unroll
        for (int ITEM = ITEMS_PER_THREAD - 1; ITEM > 0; --ITEM)
            prev[ITEM] = input[ITEM - 1];

        if (linear_tid > 0)
            prev[0] = temp_storage[linear_tid - 1].prev;
    }


    /**
     * \brief The thread block rotates its [<em>blocked arrangement</em>](index.html#sec5sec3) of input items, shifting it down by one item.  All threads receive \p input[0] provided by <em>thread</em><sub><tt>0</tt></sub>.
     *
     * \par
     * - \blocked
     * - \granularity
     * - \smemreuse
     */
    template <int ITEMS_PER_THREAD>
    __device__ __forceinline__ void Down(
        T (&input)[ITEMS_PER_THREAD],   ///< [in] The calling thread's input items
        T (&prev)[ITEMS_PER_THREAD],    ///< [out] The corresponding predecessor items (may be aliased to \p input).  The value \p prev[0] is not updated for <em>thread</em><sub>BLOCK_THREADS-1</sub>.
        T &block_prefix)                ///< [out] The item \p input[0] from <em>thread</em><sub><tt>0</tt></sub>, provided to all threads
    {
        Up(input, prev);
        block_prefix = temp_storage[BLOCK_THREADS - 1].prev;
    }

    //@}  end member group


};

}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)

