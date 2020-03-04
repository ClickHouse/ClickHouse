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
 * The cub::BlockDiscontinuity class provides [<em>collective</em>](index.html#sec0) methods for flagging discontinuities within an ordered set of items partitioned across a CUDA thread block.
 */

#pragma once

#include "../util_type.cuh"
#include "../util_ptx.cuh"
#include "../util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {

template <
    typename    T,
    int         BLOCK_DIM_X,
    int         BLOCK_DIM_Y     = 1,
    int         BLOCK_DIM_Z     = 1,
    int         PTX_ARCH        = CUB_PTX_ARCH>
class BlockAdjacentDifference
{
private:

    /******************************************************************************
     * Constants and type definitions
     ******************************************************************************/

    /// Constants
    enum
    {
        /// The thread block size in threads
        BLOCK_THREADS = BLOCK_DIM_X * BLOCK_DIM_Y * BLOCK_DIM_Z,
    };


    /// Shared memory storage layout type (last element from each thread's input)
    struct _TempStorage
    {
        T first_items[BLOCK_THREADS];
        T last_items[BLOCK_THREADS];
    };


    /******************************************************************************
     * Utility methods
     ******************************************************************************/

    /// Internal storage allocator
    __device__ __forceinline__ _TempStorage& PrivateStorage()
    {
        __shared__ _TempStorage private_storage;
        return private_storage;
    }


    /// Specialization for when FlagOp has third index param
    template <typename FlagOp, bool HAS_PARAM = BinaryOpHasIdxParam<T, FlagOp>::HAS_PARAM>
    struct ApplyOp
    {
        // Apply flag operator
        static __device__ __forceinline__ T FlagT(FlagOp flag_op, const T &a, const T &b, int idx)
        {
            return flag_op(b, a, idx);
        }
    };

    /// Specialization for when FlagOp does not have a third index param
    template <typename FlagOp>
    struct ApplyOp<FlagOp, false>
    {
        // Apply flag operator
        static __device__ __forceinline__ T FlagT(FlagOp flag_op, const T &a, const T &b, int /*idx*/)
        {
            return flag_op(b, a);
        }
    };

    /// Templated unrolling of item comparison (inductive case)
    template <int ITERATION, int MAX_ITERATIONS>
    struct Iterate
    {
        // Head flags
        template <
            int             ITEMS_PER_THREAD,
            typename        FlagT,
            typename        FlagOp>
        static __device__ __forceinline__ void FlagHeads(
            int                     linear_tid,
            FlagT                   (&flags)[ITEMS_PER_THREAD],         ///< [out] Calling thread's discontinuity head_flags
            T                       (&input)[ITEMS_PER_THREAD],         ///< [in] Calling thread's input items
            T                       (&preds)[ITEMS_PER_THREAD],         ///< [out] Calling thread's predecessor items
            FlagOp                  flag_op)                            ///< [in] Binary boolean flag predicate
        {
            preds[ITERATION] = input[ITERATION - 1];

            flags[ITERATION] = ApplyOp<FlagOp>::FlagT(
                flag_op,
                preds[ITERATION],
                input[ITERATION],
                (linear_tid * ITEMS_PER_THREAD) + ITERATION);

            Iterate<ITERATION + 1, MAX_ITERATIONS>::FlagHeads(linear_tid, flags, input, preds, flag_op);
        }

        // Tail flags
        template <
            int             ITEMS_PER_THREAD,
            typename        FlagT,
            typename        FlagOp>
        static __device__ __forceinline__ void FlagTails(
            int                     linear_tid,
            FlagT                   (&flags)[ITEMS_PER_THREAD],         ///< [out] Calling thread's discontinuity head_flags
            T                       (&input)[ITEMS_PER_THREAD],         ///< [in] Calling thread's input items
            FlagOp                  flag_op)                            ///< [in] Binary boolean flag predicate
        {
            flags[ITERATION] = ApplyOp<FlagOp>::FlagT(
                flag_op,
                input[ITERATION],
                input[ITERATION + 1],
                (linear_tid * ITEMS_PER_THREAD) + ITERATION + 1);

            Iterate<ITERATION + 1, MAX_ITERATIONS>::FlagTails(linear_tid, flags, input, flag_op);
        }

    };

    /// Templated unrolling of item comparison (termination case)
    template <int MAX_ITERATIONS>
    struct Iterate<MAX_ITERATIONS, MAX_ITERATIONS>
    {
        // Head flags
        template <
            int             ITEMS_PER_THREAD,
            typename        FlagT,
            typename        FlagOp>
        static __device__ __forceinline__ void FlagHeads(
            int                     /*linear_tid*/,
            FlagT                   (&/*flags*/)[ITEMS_PER_THREAD],         ///< [out] Calling thread's discontinuity head_flags
            T                       (&/*input*/)[ITEMS_PER_THREAD],         ///< [in] Calling thread's input items
            T                       (&/*preds*/)[ITEMS_PER_THREAD],         ///< [out] Calling thread's predecessor items
            FlagOp                  /*flag_op*/)                            ///< [in] Binary boolean flag predicate
        {}

        // Tail flags
        template <
            int             ITEMS_PER_THREAD,
            typename        FlagT,
            typename        FlagOp>
        static __device__ __forceinline__ void FlagTails(
            int                     /*linear_tid*/,
            FlagT                   (&/*flags*/)[ITEMS_PER_THREAD],         ///< [out] Calling thread's discontinuity head_flags
            T                       (&/*input*/)[ITEMS_PER_THREAD],         ///< [in] Calling thread's input items
            FlagOp                  /*flag_op*/)                            ///< [in] Binary boolean flag predicate
        {}
    };


    /******************************************************************************
     * Thread fields
     ******************************************************************************/

    /// Shared storage reference
    _TempStorage &temp_storage;

    /// Linear thread-id
    unsigned int linear_tid;


public:

    /// \smemstorage{BlockDiscontinuity}
    struct TempStorage : Uninitialized<_TempStorage> {};


    /******************************************************************//**
     * \name Collective constructors
     *********************************************************************/
    //@{

    /**
     * \brief Collective constructor using a private static allocation of shared memory as temporary storage.
     */
    __device__ __forceinline__ BlockAdjacentDifference()
    :
        temp_storage(PrivateStorage()),
        linear_tid(RowMajorTid(BLOCK_DIM_X, BLOCK_DIM_Y, BLOCK_DIM_Z))
    {}


    /**
     * \brief Collective constructor using the specified memory allocation as temporary storage.
     */
    __device__ __forceinline__ BlockAdjacentDifference(
        TempStorage &temp_storage)  ///< [in] Reference to memory allocation having layout type TempStorage
    :
        temp_storage(temp_storage.Alias()),
        linear_tid(RowMajorTid(BLOCK_DIM_X, BLOCK_DIM_Y, BLOCK_DIM_Z))
    {}


    //@}  end member group
    /******************************************************************//**
     * \name Head flag operations
     *********************************************************************/
    //@{


#ifndef DOXYGEN_SHOULD_SKIP_THIS    // Do not document

    template <
        int             ITEMS_PER_THREAD,
        typename        FlagT,
        typename        FlagOp>
    __device__ __forceinline__ void FlagHeads(
        FlagT           (&head_flags)[ITEMS_PER_THREAD],    ///< [out] Calling thread's discontinuity head_flags
        T               (&input)[ITEMS_PER_THREAD],         ///< [in] Calling thread's input items
        T               (&preds)[ITEMS_PER_THREAD],         ///< [out] Calling thread's predecessor items
        FlagOp          flag_op)                            ///< [in] Binary boolean flag predicate
    {
        // Share last item
        temp_storage.last_items[linear_tid] = input[ITEMS_PER_THREAD - 1];

        CTA_SYNC();

        if (linear_tid == 0)
        {
            // Set flag for first thread-item (preds[0] is undefined)
            head_flags[0] = 1;
        }
        else
        {
            preds[0] = temp_storage.last_items[linear_tid - 1];
            head_flags[0] = ApplyOp<FlagOp>::FlagT(flag_op, preds[0], input[0], linear_tid * ITEMS_PER_THREAD);
        }

        // Set head_flags for remaining items
        Iterate<1, ITEMS_PER_THREAD>::FlagHeads(linear_tid, head_flags, input, preds, flag_op);
    }

    template <
        int             ITEMS_PER_THREAD,
        typename        FlagT,
        typename        FlagOp>
    __device__ __forceinline__ void FlagHeads(
        FlagT           (&head_flags)[ITEMS_PER_THREAD],    ///< [out] Calling thread's discontinuity head_flags
        T               (&input)[ITEMS_PER_THREAD],         ///< [in] Calling thread's input items
        T               (&preds)[ITEMS_PER_THREAD],         ///< [out] Calling thread's predecessor items
        FlagOp          flag_op,                            ///< [in] Binary boolean flag predicate
        T               tile_predecessor_item)              ///< [in] <b>[<em>thread</em><sub>0</sub> only]</b> Item with which to compare the first tile item (<tt>input<sub>0</sub></tt> from <em>thread</em><sub>0</sub>).
    {
        // Share last item
        temp_storage.last_items[linear_tid] = input[ITEMS_PER_THREAD - 1];

        CTA_SYNC();

        // Set flag for first thread-item
        preds[0] = (linear_tid == 0) ?
            tile_predecessor_item :              // First thread
            temp_storage.last_items[linear_tid - 1];

        head_flags[0] = ApplyOp<FlagOp>::FlagT(flag_op, preds[0], input[0], linear_tid * ITEMS_PER_THREAD);

        // Set head_flags for remaining items
        Iterate<1, ITEMS_PER_THREAD>::FlagHeads(linear_tid, head_flags, input, preds, flag_op);
    }

#endif // DOXYGEN_SHOULD_SKIP_THIS


    template <
        int             ITEMS_PER_THREAD,
        typename        FlagT,
        typename        FlagOp>
    __device__ __forceinline__ void FlagHeads(
        FlagT           (&head_flags)[ITEMS_PER_THREAD],    ///< [out] Calling thread's discontinuity head_flags
        T               (&input)[ITEMS_PER_THREAD],         ///< [in] Calling thread's input items
        FlagOp          flag_op)                            ///< [in] Binary boolean flag predicate
    {
        T preds[ITEMS_PER_THREAD];
        FlagHeads(head_flags, input, preds, flag_op);
    }


    template <
        int             ITEMS_PER_THREAD,
        typename        FlagT,
        typename        FlagOp>
    __device__ __forceinline__ void FlagHeads(
        FlagT           (&head_flags)[ITEMS_PER_THREAD],    ///< [out] Calling thread's discontinuity head_flags
        T               (&input)[ITEMS_PER_THREAD],         ///< [in] Calling thread's input items
        FlagOp          flag_op,                            ///< [in] Binary boolean flag predicate
        T               tile_predecessor_item)              ///< [in] <b>[<em>thread</em><sub>0</sub> only]</b> Item with which to compare the first tile item (<tt>input<sub>0</sub></tt> from <em>thread</em><sub>0</sub>).
    {
        T preds[ITEMS_PER_THREAD];
        FlagHeads(head_flags, input, preds, flag_op, tile_predecessor_item);
    }



    template <
        int             ITEMS_PER_THREAD,
        typename        FlagT,
        typename        FlagOp>
    __device__ __forceinline__ void FlagTails(
        FlagT           (&tail_flags)[ITEMS_PER_THREAD],    ///< [out] Calling thread's discontinuity tail_flags
        T               (&input)[ITEMS_PER_THREAD],         ///< [in] Calling thread's input items
        FlagOp          flag_op)                            ///< [in] Binary boolean flag predicate
    {
        // Share first item
        temp_storage.first_items[linear_tid] = input[0];

        CTA_SYNC();

        // Set flag for last thread-item
        tail_flags[ITEMS_PER_THREAD - 1] = (linear_tid == BLOCK_THREADS - 1) ?
            1 :                             // Last thread
            ApplyOp<FlagOp>::FlagT(
                flag_op,
                input[ITEMS_PER_THREAD - 1],
                temp_storage.first_items[linear_tid + 1],
                (linear_tid * ITEMS_PER_THREAD) + ITEMS_PER_THREAD);

        // Set tail_flags for remaining items
        Iterate<0, ITEMS_PER_THREAD - 1>::FlagTails(linear_tid, tail_flags, input, flag_op);
    }


    template <
        int             ITEMS_PER_THREAD,
        typename        FlagT,
        typename        FlagOp>
    __device__ __forceinline__ void FlagTails(
        FlagT           (&tail_flags)[ITEMS_PER_THREAD],    ///< [out] Calling thread's discontinuity tail_flags
        T               (&input)[ITEMS_PER_THREAD],         ///< [in] Calling thread's input items
        FlagOp          flag_op,                            ///< [in] Binary boolean flag predicate
        T               tile_successor_item)                ///< [in] <b>[<em>thread</em><sub><tt>BLOCK_THREADS</tt>-1</sub> only]</b> Item with which to compare the last tile item (<tt>input</tt><sub><em>ITEMS_PER_THREAD</em>-1</sub> from <em>thread</em><sub><em>BLOCK_THREADS</em>-1</sub>).
    {
        // Share first item
        temp_storage.first_items[linear_tid] = input[0];

        CTA_SYNC();

        // Set flag for last thread-item
        T successor_item = (linear_tid == BLOCK_THREADS - 1) ?
            tile_successor_item :              // Last thread
            temp_storage.first_items[linear_tid + 1];

        tail_flags[ITEMS_PER_THREAD - 1] = ApplyOp<FlagOp>::FlagT(
            flag_op,
            input[ITEMS_PER_THREAD - 1],
            successor_item,
            (linear_tid * ITEMS_PER_THREAD) + ITEMS_PER_THREAD);

        // Set tail_flags for remaining items
        Iterate<0, ITEMS_PER_THREAD - 1>::FlagTails(linear_tid, tail_flags, input, flag_op);
    }


    template <
        int             ITEMS_PER_THREAD,
        typename        FlagT,
        typename        FlagOp>
    __device__ __forceinline__ void FlagHeadsAndTails(
        FlagT           (&head_flags)[ITEMS_PER_THREAD],    ///< [out] Calling thread's discontinuity head_flags
        FlagT           (&tail_flags)[ITEMS_PER_THREAD],    ///< [out] Calling thread's discontinuity tail_flags
        T               (&input)[ITEMS_PER_THREAD],         ///< [in] Calling thread's input items
        FlagOp          flag_op)                            ///< [in] Binary boolean flag predicate
    {
        // Share first and last items
        temp_storage.first_items[linear_tid] = input[0];
        temp_storage.last_items[linear_tid] = input[ITEMS_PER_THREAD - 1];

        CTA_SYNC();

        T preds[ITEMS_PER_THREAD];

        // Set flag for first thread-item
        preds[0] = temp_storage.last_items[linear_tid - 1];
        if (linear_tid == 0)
        {
            head_flags[0] = 1;
        }
        else
        {
            head_flags[0] = ApplyOp<FlagOp>::FlagT(
                flag_op,
                preds[0],
                input[0],
                linear_tid * ITEMS_PER_THREAD);
        }


        // Set flag for last thread-item
        tail_flags[ITEMS_PER_THREAD - 1] = (linear_tid == BLOCK_THREADS - 1) ?
            1 :                             // Last thread
            ApplyOp<FlagOp>::FlagT(
                flag_op,
                input[ITEMS_PER_THREAD - 1],
                temp_storage.first_items[linear_tid + 1],
                (linear_tid * ITEMS_PER_THREAD) + ITEMS_PER_THREAD);

        // Set head_flags for remaining items
        Iterate<1, ITEMS_PER_THREAD>::FlagHeads(linear_tid, head_flags, input, preds, flag_op);

        // Set tail_flags for remaining items
        Iterate<0, ITEMS_PER_THREAD - 1>::FlagTails(linear_tid, tail_flags, input, flag_op);
    }


    template <
        int             ITEMS_PER_THREAD,
        typename        FlagT,
        typename        FlagOp>
    __device__ __forceinline__ void FlagHeadsAndTails(
        FlagT           (&head_flags)[ITEMS_PER_THREAD],    ///< [out] Calling thread's discontinuity head_flags
        FlagT           (&tail_flags)[ITEMS_PER_THREAD],    ///< [out] Calling thread's discontinuity tail_flags
        T               tile_successor_item,                ///< [in] <b>[<em>thread</em><sub><tt>BLOCK_THREADS</tt>-1</sub> only]</b> Item with which to compare the last tile item (<tt>input</tt><sub><em>ITEMS_PER_THREAD</em>-1</sub> from <em>thread</em><sub><em>BLOCK_THREADS</em>-1</sub>).
        T               (&input)[ITEMS_PER_THREAD],         ///< [in] Calling thread's input items
        FlagOp          flag_op)                            ///< [in] Binary boolean flag predicate
    {
        // Share first and last items
        temp_storage.first_items[linear_tid] = input[0];
        temp_storage.last_items[linear_tid] = input[ITEMS_PER_THREAD - 1];

        CTA_SYNC();

        T preds[ITEMS_PER_THREAD];

        // Set flag for first thread-item
        if (linear_tid == 0)
        {
            head_flags[0] = 1;
        }
        else
        {
            preds[0] = temp_storage.last_items[linear_tid - 1];
            head_flags[0] = ApplyOp<FlagOp>::FlagT(
                flag_op,
                preds[0],
                input[0],
                linear_tid * ITEMS_PER_THREAD);
        }

        // Set flag for last thread-item
        T successor_item = (linear_tid == BLOCK_THREADS - 1) ?
            tile_successor_item :              // Last thread
            temp_storage.first_items[linear_tid + 1];

        tail_flags[ITEMS_PER_THREAD - 1] = ApplyOp<FlagOp>::FlagT(
            flag_op,
            input[ITEMS_PER_THREAD - 1],
            successor_item,
            (linear_tid * ITEMS_PER_THREAD) + ITEMS_PER_THREAD);

        // Set head_flags for remaining items
        Iterate<1, ITEMS_PER_THREAD>::FlagHeads(linear_tid, head_flags, input, preds, flag_op);

        // Set tail_flags for remaining items
        Iterate<0, ITEMS_PER_THREAD - 1>::FlagTails(linear_tid, tail_flags, input, flag_op);
    }

    template <
        int             ITEMS_PER_THREAD,
        typename        FlagT,
        typename        FlagOp>
    __device__ __forceinline__ void FlagHeadsAndTails(
        FlagT           (&head_flags)[ITEMS_PER_THREAD],    ///< [out] Calling thread's discontinuity head_flags
        T               tile_predecessor_item,              ///< [in] <b>[<em>thread</em><sub>0</sub> only]</b> Item with which to compare the first tile item (<tt>input<sub>0</sub></tt> from <em>thread</em><sub>0</sub>).
        FlagT           (&tail_flags)[ITEMS_PER_THREAD],    ///< [out] Calling thread's discontinuity tail_flags
        T               (&input)[ITEMS_PER_THREAD],         ///< [in] Calling thread's input items
        FlagOp          flag_op)                            ///< [in] Binary boolean flag predicate
    {
        // Share first and last items
        temp_storage.first_items[linear_tid] = input[0];
        temp_storage.last_items[linear_tid] = input[ITEMS_PER_THREAD - 1];

        CTA_SYNC();

        T preds[ITEMS_PER_THREAD];

        // Set flag for first thread-item
        preds[0] = (linear_tid == 0) ?
            tile_predecessor_item :              // First thread
            temp_storage.last_items[linear_tid - 1];

        head_flags[0] = ApplyOp<FlagOp>::FlagT(
            flag_op,
            preds[0],
            input[0],
            linear_tid * ITEMS_PER_THREAD);

        // Set flag for last thread-item
        tail_flags[ITEMS_PER_THREAD - 1] = (linear_tid == BLOCK_THREADS - 1) ?
            1 :                             // Last thread
            ApplyOp<FlagOp>::FlagT(
                flag_op,
                input[ITEMS_PER_THREAD - 1],
                temp_storage.first_items[linear_tid + 1],
                (linear_tid * ITEMS_PER_THREAD) + ITEMS_PER_THREAD);

        // Set head_flags for remaining items
        Iterate<1, ITEMS_PER_THREAD>::FlagHeads(linear_tid, head_flags, input, preds, flag_op);

        // Set tail_flags for remaining items
        Iterate<0, ITEMS_PER_THREAD - 1>::FlagTails(linear_tid, tail_flags, input, flag_op);
    }


    template <
        int             ITEMS_PER_THREAD,
        typename        FlagT,
        typename        FlagOp>
    __device__ __forceinline__ void FlagHeadsAndTails(
        FlagT           (&head_flags)[ITEMS_PER_THREAD],    ///< [out] Calling thread's discontinuity head_flags
        T               tile_predecessor_item,              ///< [in] <b>[<em>thread</em><sub>0</sub> only]</b> Item with which to compare the first tile item (<tt>input<sub>0</sub></tt> from <em>thread</em><sub>0</sub>).
        FlagT           (&tail_flags)[ITEMS_PER_THREAD],    ///< [out] Calling thread's discontinuity tail_flags
        T               tile_successor_item,                ///< [in] <b>[<em>thread</em><sub><tt>BLOCK_THREADS</tt>-1</sub> only]</b> Item with which to compare the last tile item (<tt>input</tt><sub><em>ITEMS_PER_THREAD</em>-1</sub> from <em>thread</em><sub><em>BLOCK_THREADS</em>-1</sub>).
        T               (&input)[ITEMS_PER_THREAD],         ///< [in] Calling thread's input items
        FlagOp          flag_op)                            ///< [in] Binary boolean flag predicate
    {
        // Share first and last items
        temp_storage.first_items[linear_tid] = input[0];
        temp_storage.last_items[linear_tid] = input[ITEMS_PER_THREAD - 1];

        CTA_SYNC();

        T preds[ITEMS_PER_THREAD];

        // Set flag for first thread-item
        preds[0] = (linear_tid == 0) ?
            tile_predecessor_item :              // First thread
            temp_storage.last_items[linear_tid - 1];

        head_flags[0] = ApplyOp<FlagOp>::FlagT(
            flag_op,
            preds[0],
            input[0],
            linear_tid * ITEMS_PER_THREAD);

        // Set flag for last thread-item
        T successor_item = (linear_tid == BLOCK_THREADS - 1) ?
            tile_successor_item :              // Last thread
            temp_storage.first_items[linear_tid + 1];

        tail_flags[ITEMS_PER_THREAD - 1] = ApplyOp<FlagOp>::FlagT(
            flag_op,
            input[ITEMS_PER_THREAD - 1],
            successor_item,
            (linear_tid * ITEMS_PER_THREAD) + ITEMS_PER_THREAD);

        // Set head_flags for remaining items
        Iterate<1, ITEMS_PER_THREAD>::FlagHeads(linear_tid, head_flags, input, preds, flag_op);

        // Set tail_flags for remaining items
        Iterate<0, ITEMS_PER_THREAD - 1>::FlagTails(linear_tid, tail_flags, input, flag_op);
    }



};


}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)
