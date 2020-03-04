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
 * cub::WarpScanSmem provides smem-based variants of parallel prefix scan of items partitioned across a CUDA thread warp.
 */

#pragma once

#include "../../thread/thread_operators.cuh"
#include "../../thread/thread_load.cuh"
#include "../../thread/thread_store.cuh"
#include "../../util_type.cuh"
#include "../../util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {

/**
 * \brief WarpScanSmem provides smem-based variants of parallel prefix scan of items partitioned across a CUDA thread warp.
 */
template <
    typename    T,                      ///< Data type being scanned
    int         LOGICAL_WARP_THREADS,   ///< Number of threads per logical warp
    int         PTX_ARCH>               ///< The PTX compute capability for which to to specialize this collective
struct WarpScanSmem
{
    /******************************************************************************
     * Constants and type definitions
     ******************************************************************************/

    enum
    {
        /// Whether the logical warp size and the PTX warp size coincide
        IS_ARCH_WARP = (LOGICAL_WARP_THREADS == CUB_WARP_THREADS(PTX_ARCH)),

        /// Whether the logical warp size is a power-of-two
        IS_POW_OF_TWO = PowerOfTwo<LOGICAL_WARP_THREADS>::VALUE,

        /// The number of warp scan steps
        STEPS = Log2<LOGICAL_WARP_THREADS>::VALUE,

        /// The number of threads in half a warp
        HALF_WARP_THREADS = 1 << (STEPS - 1),

        /// The number of shared memory elements per warp
        WARP_SMEM_ELEMENTS =  LOGICAL_WARP_THREADS + HALF_WARP_THREADS,
    };

    /// Storage cell type (workaround for SM1x compiler bugs with custom-ops like Max() on signed chars)
    typedef typename If<((Equals<T, char>::VALUE || Equals<T, signed char>::VALUE) && (PTX_ARCH < 200)), int, T>::Type CellT;

    /// Shared memory storage layout type (1.5 warps-worth of elements for each warp)
    typedef CellT _TempStorage[WARP_SMEM_ELEMENTS];

    // Alias wrapper allowing storage to be unioned
    struct TempStorage : Uninitialized<_TempStorage> {};


    /******************************************************************************
     * Thread fields
     ******************************************************************************/

    _TempStorage    &temp_storage;
    unsigned int    lane_id;
    unsigned int    member_mask;


    /******************************************************************************
     * Construction
     ******************************************************************************/

    /// Constructor
    __device__ __forceinline__ WarpScanSmem(
        TempStorage     &temp_storage)
    :
        temp_storage(temp_storage.Alias()),

        lane_id(IS_ARCH_WARP ?
            LaneId() :
            LaneId() % LOGICAL_WARP_THREADS),

        member_mask((0xffffffff >> (32 - LOGICAL_WARP_THREADS)) << ((IS_ARCH_WARP || !IS_POW_OF_TWO ) ?
            0 : // arch-width and non-power-of-two subwarps cannot be tiled with the arch-warp
            ((LaneId() / LOGICAL_WARP_THREADS) * LOGICAL_WARP_THREADS)))
    {}


    /******************************************************************************
     * Utility methods
     ******************************************************************************/

    /// Basic inclusive scan iteration (template unrolled, inductive-case specialization)
    template <
        bool        HAS_IDENTITY,
        int         STEP,
        typename    ScanOp>
    __device__ __forceinline__ void ScanStep(
        T                       &partial,
        ScanOp                  scan_op,
        Int2Type<STEP>          /*step*/)
    {
        const int OFFSET = 1 << STEP;

        // Share partial into buffer
        ThreadStore<STORE_VOLATILE>(&temp_storage[HALF_WARP_THREADS + lane_id], (CellT) partial);

        WARP_SYNC(member_mask);

        // Update partial if addend is in range
        if (HAS_IDENTITY || (lane_id >= OFFSET))
        {
            T addend = (T) ThreadLoad<LOAD_VOLATILE>(&temp_storage[HALF_WARP_THREADS + lane_id - OFFSET]);
            partial = scan_op(addend, partial);
        }
        WARP_SYNC(member_mask);

        ScanStep<HAS_IDENTITY>(partial, scan_op, Int2Type<STEP + 1>());
    }


    /// Basic inclusive scan iteration(template unrolled, base-case specialization)
    template <
        bool        HAS_IDENTITY,
        typename    ScanOp>
    __device__ __forceinline__ void ScanStep(
        T                       &/*partial*/,
        ScanOp                  /*scan_op*/,
        Int2Type<STEPS>         /*step*/)
    {}


    /// Inclusive prefix scan (specialized for summation across primitive types)
    __device__ __forceinline__ void InclusiveScan(
        T                       input,              ///< [in] Calling thread's input item.
        T                       &output,            ///< [out] Calling thread's output item.  May be aliased with \p input.
        Sum                     scan_op,            ///< [in] Binary scan operator
        Int2Type<true>          /*is_primitive*/)   ///< [in] Marker type indicating whether T is primitive type
    {
        T identity = 0;
        ThreadStore<STORE_VOLATILE>(&temp_storage[lane_id], (CellT) identity);

        WARP_SYNC(member_mask);

        // Iterate scan steps
        output = input;
        ScanStep<true>(output, scan_op, Int2Type<0>());
    }


    /// Inclusive prefix scan
    template <typename ScanOp, int IS_PRIMITIVE>
    __device__ __forceinline__ void InclusiveScan(
        T                       input,              ///< [in] Calling thread's input item.
        T                       &output,            ///< [out] Calling thread's output item.  May be aliased with \p input.
        ScanOp                  scan_op,            ///< [in] Binary scan operator
        Int2Type<IS_PRIMITIVE>  /*is_primitive*/)   ///< [in] Marker type indicating whether T is primitive type
    {
        // Iterate scan steps
        output = input;
        ScanStep<false>(output, scan_op, Int2Type<0>());
    }


    /******************************************************************************
     * Interface
     ******************************************************************************/

    //---------------------------------------------------------------------
    // Broadcast
    //---------------------------------------------------------------------

    /// Broadcast
    __device__ __forceinline__ T Broadcast(
        T               input,              ///< [in] The value to broadcast
        unsigned int    src_lane)           ///< [in] Which warp lane is to do the broadcasting
    {
        if (lane_id == src_lane)
        {
            ThreadStore<STORE_VOLATILE>(temp_storage, (CellT) input);
        }

        WARP_SYNC(member_mask);

        return (T)ThreadLoad<LOAD_VOLATILE>(temp_storage);
    }


    //---------------------------------------------------------------------
    // Inclusive operations
    //---------------------------------------------------------------------

    /// Inclusive scan
    template <typename ScanOp>
    __device__ __forceinline__ void InclusiveScan(
        T               input,              ///< [in] Calling thread's input item.
        T               &inclusive_output,  ///< [out] Calling thread's output item.  May be aliased with \p input.
        ScanOp          scan_op)            ///< [in] Binary scan operator
    {
        InclusiveScan(input, inclusive_output, scan_op, Int2Type<Traits<T>::PRIMITIVE>());
    }


    /// Inclusive scan with aggregate
    template <typename ScanOp>
    __device__ __forceinline__ void InclusiveScan(
        T               input,              ///< [in] Calling thread's input item.
        T               &inclusive_output,  ///< [out] Calling thread's output item.  May be aliased with \p input.
        ScanOp          scan_op,            ///< [in] Binary scan operator
        T               &warp_aggregate)    ///< [out] Warp-wide aggregate reduction of input items.
    {
        InclusiveScan(input, inclusive_output, scan_op);

        // Retrieve aggregate
        ThreadStore<STORE_VOLATILE>(&temp_storage[HALF_WARP_THREADS + lane_id], (CellT) inclusive_output);

        WARP_SYNC(member_mask);

        warp_aggregate = (T) ThreadLoad<LOAD_VOLATILE>(&temp_storage[WARP_SMEM_ELEMENTS - 1]);

        WARP_SYNC(member_mask);
    }


    //---------------------------------------------------------------------
    // Get exclusive from inclusive
    //---------------------------------------------------------------------

    /// Update inclusive and exclusive using input and inclusive
    template <typename ScanOpT, typename IsIntegerT>
    __device__ __forceinline__ void Update(
        T                       /*input*/,      ///< [in]
        T                       &inclusive,     ///< [in, out]
        T                       &exclusive,     ///< [out]
        ScanOpT                 /*scan_op*/,    ///< [in]
        IsIntegerT              /*is_integer*/) ///< [in]
    {
        // initial value unknown
        ThreadStore<STORE_VOLATILE>(&temp_storage[HALF_WARP_THREADS + lane_id], (CellT) inclusive);

        WARP_SYNC(member_mask);

        exclusive = (T) ThreadLoad<LOAD_VOLATILE>(&temp_storage[HALF_WARP_THREADS + lane_id - 1]);
    }

    /// Update inclusive and exclusive using input and inclusive (specialized for summation of integer types)
    __device__ __forceinline__ void Update(
        T                       input,
        T                       &inclusive,
        T                       &exclusive,
        cub::Sum                /*scan_op*/,
        Int2Type<true>          /*is_integer*/)
    {
        // initial value presumed 0
        exclusive = inclusive - input;
    }

    /// Update inclusive and exclusive using initial value using input, inclusive, and initial value
    template <typename ScanOpT, typename IsIntegerT>
    __device__ __forceinline__ void Update (
        T                       /*input*/,
        T                       &inclusive,
        T                       &exclusive,
        ScanOpT                 scan_op,
        T                       initial_value,
        IsIntegerT              /*is_integer*/)
    {
        inclusive = scan_op(initial_value, inclusive);
        ThreadStore<STORE_VOLATILE>(&temp_storage[HALF_WARP_THREADS + lane_id], (CellT) inclusive);

        WARP_SYNC(member_mask);

        exclusive = (T) ThreadLoad<LOAD_VOLATILE>(&temp_storage[HALF_WARP_THREADS + lane_id - 1]);
        if (lane_id == 0)
            exclusive = initial_value;
    }

    /// Update inclusive and exclusive using initial value using input and inclusive (specialized for summation of integer types)
    __device__ __forceinline__ void Update (
        T                       input,
        T                       &inclusive,
        T                       &exclusive,
        cub::Sum                scan_op,
        T                       initial_value,
        Int2Type<true>          /*is_integer*/)
    {
        inclusive = scan_op(initial_value, inclusive);
        exclusive = inclusive - input;
    }


    /// Update inclusive, exclusive, and warp aggregate using input and inclusive
    template <typename ScanOpT, typename IsIntegerT>
    __device__ __forceinline__ void Update (
        T                       /*input*/,
        T                       &inclusive,
        T                       &exclusive,
        T                       &warp_aggregate,
        ScanOpT                 /*scan_op*/,
        IsIntegerT              /*is_integer*/)
    {
        // Initial value presumed to be unknown or identity (either way our padding is correct)
        ThreadStore<STORE_VOLATILE>(&temp_storage[HALF_WARP_THREADS + lane_id], (CellT) inclusive);

        WARP_SYNC(member_mask);

        exclusive = (T) ThreadLoad<LOAD_VOLATILE>(&temp_storage[HALF_WARP_THREADS + lane_id - 1]);
        warp_aggregate = (T) ThreadLoad<LOAD_VOLATILE>(&temp_storage[WARP_SMEM_ELEMENTS - 1]);
    }

    /// Update inclusive, exclusive, and warp aggregate using input and inclusive (specialized for summation of integer types)
    __device__ __forceinline__ void Update (
        T                       input,
        T                       &inclusive,
        T                       &exclusive,
        T                       &warp_aggregate,
        cub::Sum                /*scan_o*/,
        Int2Type<true>          /*is_integer*/)
    {
        // Initial value presumed to be unknown or identity (either way our padding is correct)
        ThreadStore<STORE_VOLATILE>(&temp_storage[HALF_WARP_THREADS + lane_id], (CellT) inclusive);

        WARP_SYNC(member_mask);

        warp_aggregate = (T) ThreadLoad<LOAD_VOLATILE>(&temp_storage[WARP_SMEM_ELEMENTS - 1]);
        exclusive = inclusive - input;
    }

    /// Update inclusive, exclusive, and warp aggregate using input, inclusive, and initial value
    template <typename ScanOpT, typename IsIntegerT>
    __device__ __forceinline__ void Update (
        T                       /*input*/,
        T                       &inclusive,
        T                       &exclusive,
        T                       &warp_aggregate,
        ScanOpT                 scan_op,
        T                       initial_value,
        IsIntegerT              /*is_integer*/)
    {
        // Broadcast warp aggregate
        ThreadStore<STORE_VOLATILE>(&temp_storage[HALF_WARP_THREADS + lane_id], (CellT) inclusive);

        WARP_SYNC(member_mask);

        warp_aggregate = (T) ThreadLoad<LOAD_VOLATILE>(&temp_storage[WARP_SMEM_ELEMENTS - 1]);

        WARP_SYNC(member_mask);

        // Update inclusive with initial value
        inclusive = scan_op(initial_value, inclusive);

        // Get exclusive from exclusive
        ThreadStore<STORE_VOLATILE>(&temp_storage[HALF_WARP_THREADS + lane_id - 1], (CellT) inclusive);

        WARP_SYNC(member_mask);

        exclusive = (T) ThreadLoad<LOAD_VOLATILE>(&temp_storage[HALF_WARP_THREADS + lane_id - 2]);

        if (lane_id == 0)
            exclusive = initial_value;
    }


};


}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)
