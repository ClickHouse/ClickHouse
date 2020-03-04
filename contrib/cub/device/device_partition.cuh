
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
 * cub::DevicePartition provides device-wide, parallel operations for partitioning sequences of data items residing within device-accessible memory.
 */

#pragma once

#include <stdio.h>
#include <iterator>

#include "dispatch/dispatch_select_if.cuh"
#include "../util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {


/**
 * \brief DevicePartition provides device-wide, parallel operations for partitioning sequences of data items residing within device-accessible memory. ![](partition_logo.png)
 * \ingroup SingleModule
 *
 * \par Overview
 * These operations apply a selection criterion to construct a partitioned output sequence from items selected/unselected from
 * a specified input sequence.
 *
 * \par Usage Considerations
 * \cdp_class{DevicePartition}
 *
 * \par Performance
 * \linear_performance{partition}
 *
 * \par
 * The following chart illustrates DevicePartition::If
 * performance across different CUDA architectures for \p int32 items,
 * where 50% of the items are randomly selected for the first partition.
 * \plots_below
 *
 * \image html partition_if_int32_50_percent.png
 *
 */
struct DevicePartition
{
    /**
     * \brief Uses the \p d_flags sequence to split the corresponding items from \p d_in into a partitioned sequence \p d_out.  The total number of items copied into the first partition is written to \p d_num_selected_out. ![](partition_flags_logo.png)
     *
     * \par
     * - The value type of \p d_flags must be castable to \p bool (e.g., \p bool, \p char, \p int, etc.).
     * - Copies of the selected items are compacted into \p d_out and maintain their original
     *   relative ordering, however copies of the unselected items are compacted into the
     *   rear of \p d_out in reverse order.
     * - \devicestorage
     *
     * \par Snippet
     * The code snippet below illustrates the compaction of items selected from an \p int device vector.
     * \par
     * \code
     * #include <cub/cub.cuh>       // or equivalently <cub/device/device_partition.cuh>
     *
     * // Declare, allocate, and initialize device-accessible pointers for input, flags, and output
     * int  num_items;              // e.g., 8
     * int  *d_in;                  // e.g., [1, 2, 3, 4, 5, 6, 7, 8]
     * char *d_flags;               // e.g., [1, 0, 0, 1, 0, 1, 1, 0]
     * int  *d_out;                 // e.g., [ ,  ,  ,  ,  ,  ,  ,  ]
     * int  *d_num_selected_out;    // e.g., [ ]
     * ...
     *
     * // Determine temporary device storage requirements
     * void     *d_temp_storage = NULL;
     * size_t   temp_storage_bytes = 0;
     * cub::DevicePartition::Flagged(d_temp_storage, temp_storage_bytes, d_in, d_flags, d_out, d_num_selected_out, num_items);
     *
     * // Allocate temporary storage
     * cudaMalloc(&d_temp_storage, temp_storage_bytes);
     *
     * // Run selection
     * cub::DevicePartition::Flagged(d_temp_storage, temp_storage_bytes, d_in, d_flags, d_out, d_num_selected_out, num_items);
     *
     * // d_out                 <-- [1, 4, 6, 7, 8, 5, 3, 2]
     * // d_num_selected_out    <-- [4]
     *
     * \endcode
     *
     * \tparam InputIteratorT       <b>[inferred]</b> Random-access input iterator type for reading input items \iterator
     * \tparam FlagIterator         <b>[inferred]</b> Random-access input iterator type for reading selection flags \iterator
     * \tparam OutputIteratorT      <b>[inferred]</b> Random-access output iterator type for writing output items \iterator
     * \tparam NumSelectedIteratorT  <b>[inferred]</b> Output iterator type for recording the number of items selected \iterator
     */
    template <
        typename                    InputIteratorT,
        typename                    FlagIterator,
        typename                    OutputIteratorT,
        typename                    NumSelectedIteratorT>
    CUB_RUNTIME_FUNCTION __forceinline__
    static cudaError_t Flagged(
        void*               d_temp_storage,                ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
        size_t                      &temp_storage_bytes,            ///< [in,out] Reference to size in bytes of \p d_temp_storage allocation
        InputIteratorT              d_in,                           ///< [in] Pointer to the input sequence of data items
        FlagIterator                d_flags,                        ///< [in] Pointer to the input sequence of selection flags
        OutputIteratorT             d_out,                          ///< [out] Pointer to the output sequence of partitioned data items
        NumSelectedIteratorT        d_num_selected_out,             ///< [out] Pointer to the output total number of items selected (i.e., the offset of the unselected partition)
        int                         num_items,                      ///< [in] Total number of items to select from
        cudaStream_t                stream             = 0,         ///< [in] <b>[optional]</b> CUDA stream to launch kernels within.  Default is stream<sub>0</sub>.
        bool                        debug_synchronous  = false)     ///< [in] <b>[optional]</b> Whether or not to synchronize the stream after every kernel launch to check for errors.  May cause significant slowdown.  Default is \p false.
    {
        typedef int                     OffsetT;         // Signed integer type for global offsets
        typedef NullType                SelectOp;       // Selection op (not used)
        typedef NullType                EqualityOp;     // Equality operator (not used)

        return DispatchSelectIf<InputIteratorT, FlagIterator, OutputIteratorT, NumSelectedIteratorT, SelectOp, EqualityOp, OffsetT, true>::Dispatch(
            d_temp_storage,
            temp_storage_bytes,
            d_in,
            d_flags,
            d_out,
            d_num_selected_out,
            SelectOp(),
            EqualityOp(),
            num_items,
            stream,
            debug_synchronous);
    }


    /**
     * \brief Uses the \p select_op functor to split the corresponding items from \p d_in into a partitioned sequence \p d_out.  The total number of items copied into the first partition is written to \p d_num_selected_out. ![](partition_logo.png)
     *
     * \par
     * - Copies of the selected items are compacted into \p d_out and maintain their original
     *   relative ordering, however copies of the unselected items are compacted into the
     *   rear of \p d_out in reverse order.
     * - \devicestorage
     *
     * \par Performance
     * The following charts illustrate saturated partition-if performance across different
     * CUDA architectures for \p int32 and \p int64 items, respectively.  Items are
     * selected for the first partition with 50% probability.
     *
     * \image html partition_if_int32_50_percent.png
     * \image html partition_if_int64_50_percent.png
     *
     * \par
     * The following charts are similar, but 5% selection probability for the first partition:
     *
     * \image html partition_if_int32_5_percent.png
     * \image html partition_if_int64_5_percent.png
     *
     * \par Snippet
     * The code snippet below illustrates the compaction of items selected from an \p int device vector.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/device/device_partition.cuh>
     *
     * // Functor type for selecting values less than some criteria
     * struct LessThan
     * {
     *     int compare;
     *
     *     CUB_RUNTIME_FUNCTION __forceinline__
     *     LessThan(int compare) : compare(compare) {}
     *
     *     CUB_RUNTIME_FUNCTION __forceinline__
     *     bool operator()(const int &a) const {
     *         return (a < compare);
     *     }
     * };
     *
     * // Declare, allocate, and initialize device-accessible pointers for input and output
     * int      num_items;              // e.g., 8
     * int      *d_in;                  // e.g., [0, 2, 3, 9, 5, 2, 81, 8]
     * int      *d_out;                 // e.g., [ ,  ,  ,  ,  ,  ,  ,  ]
     * int      *d_num_selected_out;    // e.g., [ ]
     * LessThan select_op(7);
     * ...
     *
     * // Determine temporary device storage requirements
     * void     *d_temp_storage = NULL;
     * size_t   temp_storage_bytes = 0;
     * cub::DeviceSelect::If(d_temp_storage, temp_storage_bytes, d_in, d_out, d_num_selected_out, num_items, select_op);
     *
     * // Allocate temporary storage
     * cudaMalloc(&d_temp_storage, temp_storage_bytes);
     *
     * // Run selection
     * cub::DeviceSelect::If(d_temp_storage, temp_storage_bytes, d_in, d_out, d_num_selected_out, num_items, select_op);
     *
     * // d_out                 <-- [0, 2, 3, 5, 2, 8, 81, 9]
     * // d_num_selected_out    <-- [5]
     *
     * \endcode
     *
     * \tparam InputIteratorT       <b>[inferred]</b> Random-access input iterator type for reading input items \iterator
     * \tparam OutputIteratorT      <b>[inferred]</b> Random-access output iterator type for writing output items \iterator
     * \tparam NumSelectedIteratorT  <b>[inferred]</b> Output iterator type for recording the number of items selected \iterator
     * \tparam SelectOp             <b>[inferred]</b> Selection functor type having member <tt>bool operator()(const T &a)</tt>
     */
    template <
        typename                    InputIteratorT,
        typename                    OutputIteratorT,
        typename                    NumSelectedIteratorT,
        typename                    SelectOp>
    CUB_RUNTIME_FUNCTION __forceinline__
    static cudaError_t If(
        void*               d_temp_storage,                ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
        size_t                      &temp_storage_bytes,            ///< [in,out] Reference to size in bytes of \p d_temp_storage allocation
        InputIteratorT              d_in,                           ///< [in] Pointer to the input sequence of data items
        OutputIteratorT             d_out,                          ///< [out] Pointer to the output sequence of partitioned data items
        NumSelectedIteratorT        d_num_selected_out,             ///< [out] Pointer to the output total number of items selected (i.e., the offset of the unselected partition)
        int                         num_items,                      ///< [in] Total number of items to select from
        SelectOp                    select_op,                      ///< [in] Unary selection operator
        cudaStream_t                stream             = 0,         ///< [in] <b>[optional]</b> CUDA stream to launch kernels within.  Default is stream<sub>0</sub>.
        bool                        debug_synchronous  = false)     ///< [in] <b>[optional]</b> Whether or not to synchronize the stream after every kernel launch to check for errors.  May cause significant slowdown.  Default is \p false.
    {
        typedef int                     OffsetT;         // Signed integer type for global offsets
        typedef NullType*               FlagIterator;   // FlagT iterator type (not used)
        typedef NullType                EqualityOp;     // Equality operator (not used)

        return DispatchSelectIf<InputIteratorT, FlagIterator, OutputIteratorT, NumSelectedIteratorT, SelectOp, EqualityOp, OffsetT, true>::Dispatch(
            d_temp_storage,
            temp_storage_bytes,
            d_in,
            NULL,
            d_out,
            d_num_selected_out,
            select_op,
            EqualityOp(),
            num_items,
            stream,
            debug_synchronous);
    }

};

/**
 * \example example_device_partition_flagged.cu
 * \example example_device_partition_if.cu
 */

}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)


