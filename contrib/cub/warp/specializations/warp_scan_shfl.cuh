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
 * cub::WarpScanShfl provides SHFL-based variants of parallel prefix scan of items partitioned across a CUDA thread warp.
 */

#pragma once

#include "../../thread/thread_operators.cuh"
#include "../../util_type.cuh"
#include "../../util_ptx.cuh"
#include "../../util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {

/**
 * \brief WarpScanShfl provides SHFL-based variants of parallel prefix scan of items partitioned across a CUDA thread warp.
 *
 * LOGICAL_WARP_THREADS must be a power-of-two
 */
template <
    typename    T,                      ///< Data type being scanned
    int         LOGICAL_WARP_THREADS,   ///< Number of threads per logical warp
    int         PTX_ARCH>               ///< The PTX compute capability for which to to specialize this collective
struct WarpScanShfl
{
    //---------------------------------------------------------------------
    // Constants and type definitions
    //---------------------------------------------------------------------

    enum
    {
        /// Whether the logical warp size and the PTX warp size coincide
        IS_ARCH_WARP = (LOGICAL_WARP_THREADS == CUB_WARP_THREADS(PTX_ARCH)),

        /// The number of warp scan steps
        STEPS = Log2<LOGICAL_WARP_THREADS>::VALUE,

        /// The 5-bit SHFL mask for logically splitting warps into sub-segments starts 8-bits up
        SHFL_C = ((0xFFFFFFFFU << STEPS) & 31) << 8,
    };

    template <typename S>
    struct IntegerTraits
    {
        enum {
            ///Whether the data type is a small (32b or less) integer for which we can use a single SFHL instruction per exchange
            IS_SMALL_UNSIGNED = (Traits<S>::CATEGORY == UNSIGNED_INTEGER) && (sizeof(S) <= sizeof(unsigned int))
        };
    };

    /// Shared memory storage layout type
    struct TempStorage {};


    //---------------------------------------------------------------------
    // Thread fields
    //---------------------------------------------------------------------

    unsigned int lane_id;

    unsigned int member_mask;

    //---------------------------------------------------------------------
    // Construction
    //---------------------------------------------------------------------

    /// Constructor
    __device__ __forceinline__ WarpScanShfl(
        TempStorage &/*temp_storage*/)
    :
        lane_id(LaneId()),

        member_mask((0xffffffff >> (32 - LOGICAL_WARP_THREADS)) << ((IS_ARCH_WARP) ?
            0 : // arch-width subwarps need not be tiled within the arch-warp
            ((lane_id / LOGICAL_WARP_THREADS) * LOGICAL_WARP_THREADS)))
    {}


    //---------------------------------------------------------------------
    // Inclusive scan steps
    //---------------------------------------------------------------------

    /// Inclusive prefix scan step (specialized for summation across int32 types)
    __device__ __forceinline__ int InclusiveScanStep(
        int             input,              ///< [in] Calling thread's input item.
        cub::Sum        /*scan_op*/,        ///< [in] Binary scan operator
        int             first_lane,         ///< [in] Index of first lane in segment
        int             offset)             ///< [in] Up-offset to pull from
    {
        int output;
        int shfl_c = first_lane | SHFL_C;   // Shuffle control (mask and first-lane)

        // Use predicate set from SHFL to guard against invalid peers
#ifdef CUB_USE_COOPERATIVE_GROUPS
        asm volatile(
            "{"
            "  .reg .s32 r0;"
            "  .reg .pred p;"
            "  shfl.sync.up.b32 r0|p, %1, %2, %3, %5;"
            "  @p add.s32 r0, r0, %4;"
            "  mov.s32 %0, r0;"
            "}"
            : "=r"(output) : "r"(input), "r"(offset), "r"(shfl_c), "r"(input), "r"(member_mask));
#else
        asm volatile(
            "{"
            "  .reg .s32 r0;"
            "  .reg .pred p;"
            "  shfl.up.b32 r0|p, %1, %2, %3;"
            "  @p add.s32 r0, r0, %4;"
            "  mov.s32 %0, r0;"
            "}"
            : "=r"(output) : "r"(input), "r"(offset), "r"(shfl_c), "r"(input));
#endif

        return output;
    }

    /// Inclusive prefix scan step (specialized for summation across uint32 types)
    __device__ __forceinline__ unsigned int InclusiveScanStep(
        unsigned int    input,              ///< [in] Calling thread's input item.
        cub::Sum        /*scan_op*/,        ///< [in] Binary scan operator
        int             first_lane,         ///< [in] Index of first lane in segment
        int             offset)             ///< [in] Up-offset to pull from
    {
        unsigned int output;
        int shfl_c = first_lane | SHFL_C;   // Shuffle control (mask and first-lane)

        // Use predicate set from SHFL to guard against invalid peers
#ifdef CUB_USE_COOPERATIVE_GROUPS
        asm volatile(
            "{"
            "  .reg .u32 r0;"
            "  .reg .pred p;"
            "  shfl.sync.up.b32 r0|p, %1, %2, %3, %5;"
            "  @p add.u32 r0, r0, %4;"
            "  mov.u32 %0, r0;"
            "}"
            : "=r"(output) : "r"(input), "r"(offset), "r"(shfl_c), "r"(input), "r"(member_mask));
#else
        asm volatile(
            "{"
            "  .reg .u32 r0;"
            "  .reg .pred p;"
            "  shfl.up.b32 r0|p, %1, %2, %3;"
            "  @p add.u32 r0, r0, %4;"
            "  mov.u32 %0, r0;"
            "}"
            : "=r"(output) : "r"(input), "r"(offset), "r"(shfl_c), "r"(input));
#endif

        return output;
    }


    /// Inclusive prefix scan step (specialized for summation across fp32 types)
    __device__ __forceinline__ float InclusiveScanStep(
        float           input,              ///< [in] Calling thread's input item.
        cub::Sum        /*scan_op*/,        ///< [in] Binary scan operator
        int             first_lane,         ///< [in] Index of first lane in segment
        int             offset)             ///< [in] Up-offset to pull from
    {
        float output;
        int shfl_c = first_lane | SHFL_C;   // Shuffle control (mask and first-lane)

        // Use predicate set from SHFL to guard against invalid peers
#ifdef CUB_USE_COOPERATIVE_GROUPS
        asm volatile(
            "{"
            "  .reg .f32 r0;"
            "  .reg .pred p;"
            "  shfl.sync.up.b32 r0|p, %1, %2, %3, %5;"
            "  @p add.f32 r0, r0, %4;"
            "  mov.f32 %0, r0;"
            "}"
            : "=f"(output) : "f"(input), "r"(offset), "r"(shfl_c), "f"(input), "r"(member_mask));
#else
        asm volatile(
            "{"
            "  .reg .f32 r0;"
            "  .reg .pred p;"
            "  shfl.up.b32 r0|p, %1, %2, %3;"
            "  @p add.f32 r0, r0, %4;"
            "  mov.f32 %0, r0;"
            "}"
            : "=f"(output) : "f"(input), "r"(offset), "r"(shfl_c), "f"(input));
#endif

        return output;
    }


    /// Inclusive prefix scan step (specialized for summation across unsigned long long types)
    __device__ __forceinline__ unsigned long long InclusiveScanStep(
        unsigned long long  input,              ///< [in] Calling thread's input item.
        cub::Sum            /*scan_op*/,        ///< [in] Binary scan operator
        int             first_lane,         ///< [in] Index of first lane in segment
        int             offset)             ///< [in] Up-offset to pull from
    {
        unsigned long long output;
        int shfl_c = first_lane | SHFL_C;   // Shuffle control (mask and first-lane)

        // Use predicate set from SHFL to guard against invalid peers
#ifdef CUB_USE_COOPERATIVE_GROUPS
        asm volatile(
            "{"
            "  .reg .u64 r0;"
            "  .reg .u32 lo;"
            "  .reg .u32 hi;"
            "  .reg .pred p;"
            "  mov.b64 {lo, hi}, %1;"
            "  shfl.sync.up.b32 lo|p, lo, %2, %3, %5;"
            "  shfl.sync.up.b32 hi|p, hi, %2, %3, %5;"
            "  mov.b64 r0, {lo, hi};"
            "  @p add.u64 r0, r0, %4;"
            "  mov.u64 %0, r0;"
            "}"
            : "=l"(output) : "l"(input), "r"(offset), "r"(shfl_c), "l"(input), "r"(member_mask));
#else
        asm volatile(
            "{"
            "  .reg .u64 r0;"
            "  .reg .u32 lo;"
            "  .reg .u32 hi;"
            "  .reg .pred p;"
            "  mov.b64 {lo, hi}, %1;"
            "  shfl.up.b32 lo|p, lo, %2, %3;"
            "  shfl.up.b32 hi|p, hi, %2, %3;"
            "  mov.b64 r0, {lo, hi};"
            "  @p add.u64 r0, r0, %4;"
            "  mov.u64 %0, r0;"
            "}"
            : "=l"(output) : "l"(input), "r"(offset), "r"(shfl_c), "l"(input));
#endif

        return output;
    }


    /// Inclusive prefix scan step (specialized for summation across long long types)
    __device__ __forceinline__ long long InclusiveScanStep(
        long long       input,              ///< [in] Calling thread's input item.
        cub::Sum        /*scan_op*/,        ///< [in] Binary scan operator
        int             first_lane,         ///< [in] Index of first lane in segment
        int             offset)             ///< [in] Up-offset to pull from
    {
        long long output;
        int shfl_c = first_lane | SHFL_C;   // Shuffle control (mask and first-lane)

        // Use predicate set from SHFL to guard against invalid peers
#ifdef CUB_USE_COOPERATIVE_GROUPS
        asm volatile(
            "{"
            "  .reg .s64 r0;"
            "  .reg .u32 lo;"
            "  .reg .u32 hi;"
            "  .reg .pred p;"
            "  mov.b64 {lo, hi}, %1;"
            "  shfl.sync.up.b32 lo|p, lo, %2, %3, %5;"
            "  shfl.sync.up.b32 hi|p, hi, %2, %3, %5;"
            "  mov.b64 r0, {lo, hi};"
            "  @p add.s64 r0, r0, %4;"
            "  mov.s64 %0, r0;"
            "}"
            : "=l"(output) : "l"(input), "r"(offset), "r"(shfl_c), "l"(input), "r"(member_mask));
#else
        asm volatile(
            "{"
            "  .reg .s64 r0;"
            "  .reg .u32 lo;"
            "  .reg .u32 hi;"
            "  .reg .pred p;"
            "  mov.b64 {lo, hi}, %1;"
            "  shfl.up.b32 lo|p, lo, %2, %3;"
            "  shfl.up.b32 hi|p, hi, %2, %3;"
            "  mov.b64 r0, {lo, hi};"
            "  @p add.s64 r0, r0, %4;"
            "  mov.s64 %0, r0;"
            "}"
            : "=l"(output) : "l"(input), "r"(offset), "r"(shfl_c), "l"(input));
#endif

        return output;
    }


    /// Inclusive prefix scan step (specialized for summation across fp64 types)
    __device__ __forceinline__ double InclusiveScanStep(
        double          input,              ///< [in] Calling thread's input item.
        cub::Sum        /*scan_op*/,        ///< [in] Binary scan operator
        int             first_lane,         ///< [in] Index of first lane in segment
        int             offset)             ///< [in] Up-offset to pull from
    {
        double output;
        int shfl_c = first_lane | SHFL_C;   // Shuffle control (mask and first-lane)

        // Use predicate set from SHFL to guard against invalid peers
#ifdef CUB_USE_COOPERATIVE_GROUPS
        asm volatile(
            "{"
            "  .reg .u32 lo;"
            "  .reg .u32 hi;"
            "  .reg .pred p;"
            "  .reg .f64 r0;"
            "  mov.b64 %0, %1;"
            "  mov.b64 {lo, hi}, %1;"
            "  shfl.sync.up.b32 lo|p, lo, %2, %3, %4;"
            "  shfl.sync.up.b32 hi|p, hi, %2, %3, %4;"
            "  mov.b64 r0, {lo, hi};"
            "  @p add.f64 %0, %0, r0;"
            "}"
            : "=d"(output) : "d"(input), "r"(offset), "r"(shfl_c), "r"(member_mask));
#else
        asm volatile(
            "{"
            "  .reg .u32 lo;"
            "  .reg .u32 hi;"
            "  .reg .pred p;"
            "  .reg .f64 r0;"
            "  mov.b64 %0, %1;"
            "  mov.b64 {lo, hi}, %1;"
            "  shfl.up.b32 lo|p, lo, %2, %3;"
            "  shfl.up.b32 hi|p, hi, %2, %3;"
            "  mov.b64 r0, {lo, hi};"
            "  @p add.f64 %0, %0, r0;"
            "}"
            : "=d"(output) : "d"(input), "r"(offset), "r"(shfl_c));
#endif

        return output;
    }


/*
    /// Inclusive prefix scan (specialized for ReduceBySegmentOp<cub::Sum> across KeyValuePair<OffsetT, Value> types)
    template <typename Value, typename OffsetT>
    __device__ __forceinline__ KeyValuePair<OffsetT, Value>InclusiveScanStep(
        KeyValuePair<OffsetT, Value>    input,              ///< [in] Calling thread's input item.
        ReduceBySegmentOp<cub::Sum>     scan_op,            ///< [in] Binary scan operator
        int                             first_lane,         ///< [in] Index of first lane in segment
        int                             offset)             ///< [in] Up-offset to pull from
    {
        KeyValuePair<OffsetT, Value> output;

        output.value = InclusiveScanStep(input.value, cub::Sum(), first_lane, offset, Int2Type<IntegerTraits<Value>::IS_SMALL_UNSIGNED>());
        output.key = InclusiveScanStep(input.key, cub::Sum(), first_lane, offset, Int2Type<IntegerTraits<OffsetT>::IS_SMALL_UNSIGNED>());

        if (input.key > 0)
            output.value = input.value;

        return output;
    }
*/

    /// Inclusive prefix scan step (generic)
    template <typename _T, typename ScanOpT>
    __device__ __forceinline__ _T InclusiveScanStep(
        _T              input,              ///< [in] Calling thread's input item.
        ScanOpT          scan_op,            ///< [in] Binary scan operator
        int             first_lane,         ///< [in] Index of first lane in segment
        int             offset)             ///< [in] Up-offset to pull from
    {
        _T temp = ShuffleUp(input, offset, first_lane, member_mask);

        // Perform scan op if from a valid peer
        _T output = scan_op(temp, input);
        if (static_cast<int>(lane_id) < first_lane + offset)
            output = input;

        return output;
    }


    /// Inclusive prefix scan step (specialized for small integers size 32b or less)
    template <typename _T, typename ScanOpT>
    __device__ __forceinline__ _T InclusiveScanStep(
        _T              input,              ///< [in] Calling thread's input item.
        ScanOpT          scan_op,            ///< [in] Binary scan operator
        int             first_lane,         ///< [in] Index of first lane in segment
        int             offset,             ///< [in] Up-offset to pull from
        Int2Type<true>  /*is_small_unsigned*/)  ///< [in] Marker type indicating whether T is a small integer
    {
        return InclusiveScanStep(input, scan_op, first_lane, offset);
    }


    /// Inclusive prefix scan step (specialized for types other than small integers size 32b or less)
    template <typename _T, typename ScanOpT>
    __device__ __forceinline__ _T InclusiveScanStep(
        _T              input,              ///< [in] Calling thread's input item.
        ScanOpT          scan_op,            ///< [in] Binary scan operator
        int             first_lane,         ///< [in] Index of first lane in segment
        int             offset,             ///< [in] Up-offset to pull from
        Int2Type<false> /*is_small_unsigned*/)  ///< [in] Marker type indicating whether T is a small integer
    {
        return InclusiveScanStep(input, scan_op, first_lane, offset);
    }

    //---------------------------------------------------------------------
    // Templated inclusive scan iteration
    //---------------------------------------------------------------------

    template <typename _T, typename ScanOp, int STEP>
    __device__ __forceinline__ void InclusiveScanStep(
        _T&             input,              ///< [in] Calling thread's input item.
        ScanOp          scan_op,            ///< [in] Binary scan operator
        int             first_lane,         ///< [in] Index of first lane in segment
        Int2Type<STEP>  /*step*/)               ///< [in] Marker type indicating scan step
    {
        input = InclusiveScanStep(input, scan_op, first_lane, 1 << STEP, Int2Type<IntegerTraits<T>::IS_SMALL_UNSIGNED>());

        InclusiveScanStep(input, scan_op, first_lane, Int2Type<STEP + 1>());
    }

    template <typename _T, typename ScanOp>
    __device__ __forceinline__ void InclusiveScanStep(
        _T&             /*input*/,              ///< [in] Calling thread's input item.
        ScanOp          /*scan_op*/,            ///< [in] Binary scan operator
        int             /*first_lane*/,         ///< [in] Index of first lane in segment
        Int2Type<STEPS> /*step*/)               ///< [in] Marker type indicating scan step
    {}


    /******************************************************************************
     * Interface
     ******************************************************************************/

    //---------------------------------------------------------------------
    // Broadcast
    //---------------------------------------------------------------------

    /// Broadcast
    __device__ __forceinline__ T Broadcast(
        T               input,              ///< [in] The value to broadcast
        int             src_lane)           ///< [in] Which warp lane is to do the broadcasting
    {
        return ShuffleIndex(input, src_lane, LOGICAL_WARP_THREADS, member_mask);
    }


    //---------------------------------------------------------------------
    // Inclusive operations
    //---------------------------------------------------------------------

    /// Inclusive scan
    template <typename _T, typename ScanOpT>
    __device__ __forceinline__ void InclusiveScan(
        _T              input,              ///< [in] Calling thread's input item.
        _T              &inclusive_output,  ///< [out] Calling thread's output item.  May be aliased with \p input.
        ScanOpT         scan_op)            ///< [in] Binary scan operator
    {
        inclusive_output = input;

        // Iterate scan steps
        int segment_first_lane = 0;

        // Iterate scan steps
//        InclusiveScanStep(inclusive_output, scan_op, segment_first_lane, Int2Type<0>());

        // Iterate scan steps
        #pragma unroll
        for (int STEP = 0; STEP < STEPS; STEP++)
        {
            inclusive_output = InclusiveScanStep(
                inclusive_output,
                scan_op,
                segment_first_lane,
                (1 << STEP),
                Int2Type<IntegerTraits<T>::IS_SMALL_UNSIGNED>());
        }

    }

    /// Inclusive scan, specialized for reduce-value-by-key
    template <typename KeyT, typename ValueT, typename ReductionOpT>
    __device__ __forceinline__ void InclusiveScan(
        KeyValuePair<KeyT, ValueT>      input,              ///< [in] Calling thread's input item.
        KeyValuePair<KeyT, ValueT>      &inclusive_output,  ///< [out] Calling thread's output item.  May be aliased with \p input.
        ReduceByKeyOp<ReductionOpT >    scan_op)            ///< [in] Binary scan operator
    {
        inclusive_output = input;

        KeyT pred_key = ShuffleUp(inclusive_output.key, 1, 0, member_mask);

        unsigned int ballot = WARP_BALLOT((pred_key != inclusive_output.key), member_mask);

        // Mask away all lanes greater than ours
        ballot = ballot & LaneMaskLe();

        // Find index of first set bit
        int segment_first_lane = CUB_MAX(0, 31 - __clz(ballot));

        // Iterate scan steps
//        InclusiveScanStep(inclusive_output.value, scan_op.op, segment_first_lane, Int2Type<0>());

        // Iterate scan steps
        #pragma unroll
        for (int STEP = 0; STEP < STEPS; STEP++)
        {
            inclusive_output.value = InclusiveScanStep(
                inclusive_output.value,
                scan_op.op,
                segment_first_lane,
                (1 << STEP),
                Int2Type<IntegerTraits<T>::IS_SMALL_UNSIGNED>());
        }
    }


    /// Inclusive scan with aggregate
    template <typename ScanOpT>
    __device__ __forceinline__ void InclusiveScan(
        T               input,              ///< [in] Calling thread's input item.
        T               &inclusive_output,  ///< [out] Calling thread's output item.  May be aliased with \p input.
        ScanOpT         scan_op,            ///< [in] Binary scan operator
        T               &warp_aggregate)    ///< [out] Warp-wide aggregate reduction of input items.
    {
        InclusiveScan(input, inclusive_output, scan_op);

        // Grab aggregate from last warp lane
        warp_aggregate = ShuffleIndex(inclusive_output, LOGICAL_WARP_THREADS - 1, LOGICAL_WARP_THREADS, member_mask);
    }


    //---------------------------------------------------------------------
    // Get exclusive from inclusive
    //---------------------------------------------------------------------

    /// Update inclusive and exclusive using input and inclusive
    template <typename ScanOpT, typename IsIntegerT>
    __device__ __forceinline__ void Update(
        T                       /*input*/,          ///< [in]
        T                       &inclusive,         ///< [in, out]
        T                       &exclusive,         ///< [out]
        ScanOpT                 /*scan_op*/,        ///< [in]
        IsIntegerT              /*is_integer*/)     ///< [in]
    {
        // initial value unknown
        exclusive = ShuffleUp(inclusive, 1, 0, member_mask);
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
        exclusive = ShuffleUp(inclusive, 1, 0, member_mask);

        unsigned int segment_id = (IS_ARCH_WARP) ?
            lane_id :
            lane_id % LOGICAL_WARP_THREADS;

        if (segment_id == 0)
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
        T                       input,
        T                       &inclusive,
        T                       &exclusive,
        T                       &warp_aggregate,
        ScanOpT                 scan_op,
        IsIntegerT              is_integer)
    {
        warp_aggregate = ShuffleIndex(inclusive, LOGICAL_WARP_THREADS - 1, LOGICAL_WARP_THREADS, member_mask);
        Update(input, inclusive, exclusive, scan_op, is_integer);
    }

    /// Update inclusive, exclusive, and warp aggregate using input, inclusive, and initial value
    template <typename ScanOpT, typename IsIntegerT>
    __device__ __forceinline__ void Update (
        T                       input,
        T                       &inclusive,
        T                       &exclusive,
        T                       &warp_aggregate,
        ScanOpT                 scan_op,
        T                       initial_value,
        IsIntegerT              is_integer)
    {
        warp_aggregate = ShuffleIndex(inclusive, LOGICAL_WARP_THREADS - 1, LOGICAL_WARP_THREADS, member_mask);
        Update(input, inclusive, exclusive, scan_op, initial_value, is_integer);
    }



};


}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)
