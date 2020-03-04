
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
 * cub::DeviceRadixSort provides device-wide, parallel operations for computing a radix sort across a sequence of data items residing within device-accessible memory.
 */

#pragma once

#include <stdio.h>
#include <iterator>

#include "dispatch/dispatch_radix_sort.cuh"
#include "../util_arch.cuh"
#include "../util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {


/**
 * \brief DeviceRadixSort provides device-wide, parallel operations for computing a radix sort across a sequence of data items residing within device-accessible memory. ![](sorting_logo.png)
 * \ingroup SingleModule
 *
 * \par Overview
 * The [<em>radix sorting method</em>](http://en.wikipedia.org/wiki/Radix_sort) arranges
 * items into ascending (or descending) order.  The algorithm relies upon a positional representation for
 * keys, i.e., each key is comprised of an ordered sequence of symbols (e.g., digits,
 * characters, etc.) specified from least-significant to most-significant.  For a
 * given input sequence of keys and a set of rules specifying a total ordering
 * of the symbolic alphabet, the radix sorting method produces a lexicographic
 * ordering of those keys.
 *
 * \par
 * DeviceRadixSort can sort all of the built-in C++ numeric primitive types, e.g.:
 * <tt>unsigned char</tt>, \p int, \p double, etc.  Although the direct radix sorting
 * method can only be applied to unsigned integral types, DeviceRadixSort
 * is able to sort signed and floating-point types via simple bit-wise transformations
 * that ensure lexicographic key ordering.
 *
 * \par Usage Considerations
 * \cdp_class{DeviceRadixSort}
 *
 * \par Performance
 * \linear_performance{radix sort} The following chart illustrates DeviceRadixSort::SortKeys
 * performance across different CUDA architectures for uniform-random \p uint32 keys.
 * \plots_below
 *
 * \image html lsb_radix_sort_int32_keys.png
 *
 */
struct DeviceRadixSort
{

    /******************************************************************//**
     * \name KeyT-value pairs
     *********************************************************************/
    //@{

    /**
     * \brief Sorts key-value pairs into ascending order. (~<em>2N </em>auxiliary storage required)
     *
     * \par
     * - The contents of the input data are not altered by the sorting operation
     * - An optional bit subrange <tt>[begin_bit, end_bit)</tt> of differentiating key bits can be specified.  This can reduce overall sorting overhead and yield a corresponding performance improvement.
     * - \devicestorageNP  For sorting using only <em>O</em>(<tt>P</tt>) temporary storage, see the sorting interface using DoubleBuffer wrappers below.
     * - \devicestorage
     *
     * \par Performance
     * The following charts illustrate saturated sorting performance across different
     * CUDA architectures for uniform-random <tt>uint32,uint32</tt> and
     * <tt>uint64,uint64</tt> pairs, respectively.
     *
     * \image html lsb_radix_sort_int32_pairs.png
     * \image html lsb_radix_sort_int64_pairs.png
     *
     * \par Snippet
     * The code snippet below illustrates the sorting of a device vector of \p int keys
     * with associated vector of \p int values.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/device/device_radix_sort.cuh>
     *
     * // Declare, allocate, and initialize device-accessible pointers for sorting data
     * int  num_items;          // e.g., 7
     * int  *d_keys_in;         // e.g., [8, 6, 7, 5, 3, 0, 9]
     * int  *d_keys_out;        // e.g., [        ...        ]
     * int  *d_values_in;       // e.g., [0, 1, 2, 3, 4, 5, 6]
     * int  *d_values_out;      // e.g., [        ...        ]
     * ...
     *
     * // Determine temporary device storage requirements
     * void     *d_temp_storage = NULL;
     * size_t   temp_storage_bytes = 0;
     * cub::DeviceRadixSort::SortPairs(d_temp_storage, temp_storage_bytes,
     *     d_keys_in, d_keys_out, d_values_in, d_values_out, num_items);
     *
     * // Allocate temporary storage
     * cudaMalloc(&d_temp_storage, temp_storage_bytes);
     *
     * // Run sorting operation
     * cub::DeviceRadixSort::SortPairs(d_temp_storage, temp_storage_bytes,
     *     d_keys_in, d_keys_out, d_values_in, d_values_out, num_items);
     *
     * // d_keys_out            <-- [0, 3, 5, 6, 7, 8, 9]
     * // d_values_out          <-- [5, 4, 3, 1, 2, 0, 6]
     *
     * \endcode
     *
     * \tparam KeyT      <b>[inferred]</b> KeyT type
     * \tparam ValueT    <b>[inferred]</b> ValueT type
     */
    template <
        typename            KeyT,
        typename            ValueT>
    CUB_RUNTIME_FUNCTION
    static cudaError_t SortPairs(
        void                *d_temp_storage,                        ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
        size_t              &temp_storage_bytes,                    ///< [in,out] Reference to size in bytes of \p d_temp_storage allocation
        const KeyT          *d_keys_in,                             ///< [in] Pointer to the input data of key data to sort
        KeyT                *d_keys_out,                            ///< [out] Pointer to the sorted output sequence of key data
        const ValueT        *d_values_in,                           ///< [in] Pointer to the corresponding input sequence of associated value items
        ValueT              *d_values_out,                          ///< [out] Pointer to the correspondingly-reordered output sequence of associated value items
        int                 num_items,                              ///< [in] Number of items to sort
        int                 begin_bit           = 0,                ///< [in] <b>[optional]</b> The least-significant bit index (inclusive)  needed for key comparison
        int                 end_bit             = sizeof(KeyT) * 8, ///< [in] <b>[optional]</b> The most-significant bit index (exclusive) needed for key comparison (e.g., sizeof(unsigned int) * 8)
        cudaStream_t        stream              = 0,                ///< [in] <b>[optional]</b> CUDA stream to launch kernels within.  Default is stream<sub>0</sub>.
        bool                debug_synchronous   = false)            ///< [in] <b>[optional]</b> Whether or not to synchronize the stream after every kernel launch to check for errors.  Also causes launch configurations to be printed to the console.  Default is \p false.
    {
        // Signed integer type for global offsets
        typedef int OffsetT;

        DoubleBuffer<KeyT>       d_keys(const_cast<KeyT*>(d_keys_in), d_keys_out);
        DoubleBuffer<ValueT>     d_values(const_cast<ValueT*>(d_values_in), d_values_out);

        return DispatchRadixSort<false, KeyT, ValueT, OffsetT>::Dispatch(
            d_temp_storage,
            temp_storage_bytes,
            d_keys,
            d_values,
            num_items,
            begin_bit,
            end_bit,
            false,
            stream,
            debug_synchronous);
    }


    /**
     * \brief Sorts key-value pairs into ascending order. (~<em>N </em>auxiliary storage required)
     *
     * \par
     * - The sorting operation is given a pair of key buffers and a corresponding
     *   pair of associated value buffers.  Each pair is managed by a DoubleBuffer
     *   structure that indicates which of the two buffers is "current" (and thus
     *   contains the input data to be sorted).
     * - The contents of both buffers within each pair may be altered by the sorting
     *   operation.
     * - Upon completion, the sorting operation will update the "current" indicator
     *   within each DoubleBuffer wrapper to reference which of the two buffers
     *   now contains the sorted output sequence (a function of the number of key bits
     *   specified and the targeted device architecture).
     * - An optional bit subrange <tt>[begin_bit, end_bit)</tt> of differentiating key bits can be specified.  This can reduce overall sorting overhead and yield a corresponding performance improvement.
     * - \devicestorageP
     * - \devicestorage
     *
     * \par Performance
     * The following charts illustrate saturated sorting performance across different
     * CUDA architectures for uniform-random <tt>uint32,uint32</tt> and
     * <tt>uint64,uint64</tt> pairs, respectively.
     *
     * \image html lsb_radix_sort_int32_pairs.png
     * \image html lsb_radix_sort_int64_pairs.png
     *
     * \par Snippet
     * The code snippet below illustrates the sorting of a device vector of \p int keys
     * with associated vector of \p int values.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/device/device_radix_sort.cuh>
     *
     * // Declare, allocate, and initialize device-accessible pointers for sorting data
     * int  num_items;          // e.g., 7
     * int  *d_key_buf;         // e.g., [8, 6, 7, 5, 3, 0, 9]
     * int  *d_key_alt_buf;     // e.g., [        ...        ]
     * int  *d_value_buf;       // e.g., [0, 1, 2, 3, 4, 5, 6]
     * int  *d_value_alt_buf;   // e.g., [        ...        ]
     * ...
     *
     * // Create a set of DoubleBuffers to wrap pairs of device pointers
     * cub::DoubleBuffer<int> d_keys(d_key_buf, d_key_alt_buf);
     * cub::DoubleBuffer<int> d_values(d_value_buf, d_value_alt_buf);
     *
     * // Determine temporary device storage requirements
     * void     *d_temp_storage = NULL;
     * size_t   temp_storage_bytes = 0;
     * cub::DeviceRadixSort::SortPairs(d_temp_storage, temp_storage_bytes, d_keys, d_values, num_items);
     *
     * // Allocate temporary storage
     * cudaMalloc(&d_temp_storage, temp_storage_bytes);
     *
     * // Run sorting operation
     * cub::DeviceRadixSort::SortPairs(d_temp_storage, temp_storage_bytes, d_keys, d_values, num_items);
     *
     * // d_keys.Current()      <-- [0, 3, 5, 6, 7, 8, 9]
     * // d_values.Current()    <-- [5, 4, 3, 1, 2, 0, 6]
     *
     * \endcode
     *
     * \tparam KeyT      <b>[inferred]</b> KeyT type
     * \tparam ValueT    <b>[inferred]</b> ValueT type
     */
    template <
        typename            KeyT,
        typename            ValueT>
    CUB_RUNTIME_FUNCTION
    static cudaError_t SortPairs(
        void                    *d_temp_storage,                        ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
        size_t                  &temp_storage_bytes,                    ///< [in,out] Reference to size in bytes of \p d_temp_storage allocation
        DoubleBuffer<KeyT>      &d_keys,                                ///< [in,out] Reference to the double-buffer of keys whose "current" device-accessible buffer contains the unsorted input keys and, upon return, is updated to point to the sorted output keys
        DoubleBuffer<ValueT>    &d_values,                              ///< [in,out] Double-buffer of values whose "current" device-accessible buffer contains the unsorted input values and, upon return, is updated to point to the sorted output values
        int                     num_items,                              ///< [in] Number of items to sort
        int                     begin_bit           = 0,                ///< [in] <b>[optional]</b> The least-significant bit index (inclusive)  needed for key comparison
        int                     end_bit             = sizeof(KeyT) * 8, ///< [in] <b>[optional]</b> The most-significant bit index (exclusive) needed for key comparison (e.g., sizeof(unsigned int) * 8)
        cudaStream_t            stream              = 0,                ///< [in] <b>[optional]</b> CUDA stream to launch kernels within.  Default is stream<sub>0</sub>.
        bool                    debug_synchronous   = false)            ///< [in] <b>[optional]</b> Whether or not to synchronize the stream after every kernel launch to check for errors.  Also causes launch configurations to be printed to the console.  Default is \p false.
    {
        // Signed integer type for global offsets
        typedef int OffsetT;

        return DispatchRadixSort<false, KeyT, ValueT, OffsetT>::Dispatch(
            d_temp_storage,
            temp_storage_bytes,
            d_keys,
            d_values,
            num_items,
            begin_bit,
            end_bit,
            true,
            stream,
            debug_synchronous);
    }


    /**
     * \brief Sorts key-value pairs into descending order. (~<em>2N</em> auxiliary storage required).
     *
     * \par
     * - The contents of the input data are not altered by the sorting operation
     * - An optional bit subrange <tt>[begin_bit, end_bit)</tt> of differentiating key bits can be specified.  This can reduce overall sorting overhead and yield a corresponding performance improvement.
     * - \devicestorageNP  For sorting using only <em>O</em>(<tt>P</tt>) temporary storage, see the sorting interface using DoubleBuffer wrappers below.
     * - \devicestorage
     *
     * \par Performance
     * Performance is similar to DeviceRadixSort::SortPairs.
     *
     * \par Snippet
     * The code snippet below illustrates the sorting of a device vector of \p int keys
     * with associated vector of \p int values.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/device/device_radix_sort.cuh>
     *
     * // Declare, allocate, and initialize device-accessible pointers for sorting data
     * int  num_items;          // e.g., 7
     * int  *d_keys_in;         // e.g., [8, 6, 7, 5, 3, 0, 9]
     * int  *d_keys_out;        // e.g., [        ...        ]
     * int  *d_values_in;       // e.g., [0, 1, 2, 3, 4, 5, 6]
     * int  *d_values_out;      // e.g., [        ...        ]
     * ...
     *
     * // Determine temporary device storage requirements
     * void     *d_temp_storage = NULL;
     * size_t   temp_storage_bytes = 0;
     * cub::DeviceRadixSort::SortPairsDescending(d_temp_storage, temp_storage_bytes,
     *     d_keys_in, d_keys_out, d_values_in, d_values_out, num_items);
     *
     * // Allocate temporary storage
     * cudaMalloc(&d_temp_storage, temp_storage_bytes);
     *
     * // Run sorting operation
     * cub::DeviceRadixSort::SortPairsDescending(d_temp_storage, temp_storage_bytes,
     *     d_keys_in, d_keys_out, d_values_in, d_values_out, num_items);
     *
     * // d_keys_out            <-- [9, 8, 7, 6, 5, 3, 0]
     * // d_values_out          <-- [6, 0, 2, 1, 3, 4, 5]
     *
     * \endcode
     *
     * \tparam KeyT      <b>[inferred]</b> KeyT type
     * \tparam ValueT    <b>[inferred]</b> ValueT type
     */
    template <
        typename            KeyT,
        typename            ValueT>
    CUB_RUNTIME_FUNCTION
    static cudaError_t SortPairsDescending(
        void                *d_temp_storage,                        ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
        size_t              &temp_storage_bytes,                    ///< [in,out] Reference to size in bytes of \p d_temp_storage allocation
        const KeyT          *d_keys_in,                             ///< [in] Pointer to the input data of key data to sort
        KeyT                *d_keys_out,                            ///< [out] Pointer to the sorted output sequence of key data
        const ValueT        *d_values_in,                           ///< [in] Pointer to the corresponding input sequence of associated value items
        ValueT              *d_values_out,                          ///< [out] Pointer to the correspondingly-reordered output sequence of associated value items
        int                 num_items,                              ///< [in] Number of items to sort
        int                 begin_bit           = 0,                ///< [in] <b>[optional]</b> The least-significant bit index (inclusive)  needed for key comparison
        int                 end_bit             = sizeof(KeyT) * 8, ///< [in] <b>[optional]</b> The most-significant bit index (exclusive) needed for key comparison (e.g., sizeof(unsigned int) * 8)
        cudaStream_t        stream              = 0,                ///< [in] <b>[optional]</b> CUDA stream to launch kernels within.  Default is stream<sub>0</sub>.
        bool                debug_synchronous   = false)            ///< [in] <b>[optional]</b> Whether or not to synchronize the stream after every kernel launch to check for errors.  Also causes launch configurations to be printed to the console.  Default is \p false.
    {
        // Signed integer type for global offsets
        typedef int OffsetT;

        DoubleBuffer<KeyT>       d_keys(const_cast<KeyT*>(d_keys_in), d_keys_out);
        DoubleBuffer<ValueT>     d_values(const_cast<ValueT*>(d_values_in), d_values_out);

        return DispatchRadixSort<true, KeyT, ValueT, OffsetT>::Dispatch(
            d_temp_storage,
            temp_storage_bytes,
            d_keys,
            d_values,
            num_items,
            begin_bit,
            end_bit,
            false,
            stream,
            debug_synchronous);
    }


    /**
     * \brief Sorts key-value pairs into descending order. (~<em>N </em>auxiliary storage required).
     *
     * \par
     * - The sorting operation is given a pair of key buffers and a corresponding
     *   pair of associated value buffers.  Each pair is managed by a DoubleBuffer
     *   structure that indicates which of the two buffers is "current" (and thus
     *   contains the input data to be sorted).
     * - The contents of both buffers within each pair may be altered by the sorting
     *   operation.
     * - Upon completion, the sorting operation will update the "current" indicator
     *   within each DoubleBuffer wrapper to reference which of the two buffers
     *   now contains the sorted output sequence (a function of the number of key bits
     *   specified and the targeted device architecture).
     * - An optional bit subrange <tt>[begin_bit, end_bit)</tt> of differentiating key bits can be specified.  This can reduce overall sorting overhead and yield a corresponding performance improvement.
     * - \devicestorageP
     * - \devicestorage
     *
     * \par Performance
     * Performance is similar to DeviceRadixSort::SortPairs.
     *
     * \par Snippet
     * The code snippet below illustrates the sorting of a device vector of \p int keys
     * with associated vector of \p int values.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/device/device_radix_sort.cuh>
     *
     * // Declare, allocate, and initialize device-accessible pointers for sorting data
     * int  num_items;          // e.g., 7
     * int  *d_key_buf;         // e.g., [8, 6, 7, 5, 3, 0, 9]
     * int  *d_key_alt_buf;     // e.g., [        ...        ]
     * int  *d_value_buf;       // e.g., [0, 1, 2, 3, 4, 5, 6]
     * int  *d_value_alt_buf;   // e.g., [        ...        ]
     * ...
     *
     * // Create a set of DoubleBuffers to wrap pairs of device pointers
     * cub::DoubleBuffer<int> d_keys(d_key_buf, d_key_alt_buf);
     * cub::DoubleBuffer<int> d_values(d_value_buf, d_value_alt_buf);
     *
     * // Determine temporary device storage requirements
     * void     *d_temp_storage = NULL;
     * size_t   temp_storage_bytes = 0;
     * cub::DeviceRadixSort::SortPairsDescending(d_temp_storage, temp_storage_bytes, d_keys, d_values, num_items);
     *
     * // Allocate temporary storage
     * cudaMalloc(&d_temp_storage, temp_storage_bytes);
     *
     * // Run sorting operation
     * cub::DeviceRadixSort::SortPairsDescending(d_temp_storage, temp_storage_bytes, d_keys, d_values, num_items);
     *
     * // d_keys.Current()      <-- [9, 8, 7, 6, 5, 3, 0]
     * // d_values.Current()    <-- [6, 0, 2, 1, 3, 4, 5]
     *
     * \endcode
     *
     * \tparam KeyT      <b>[inferred]</b> KeyT type
     * \tparam ValueT    <b>[inferred]</b> ValueT type
     */
    template <
        typename            KeyT,
        typename            ValueT>
    CUB_RUNTIME_FUNCTION
    static cudaError_t SortPairsDescending(
        void                    *d_temp_storage,                        ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
        size_t                  &temp_storage_bytes,                    ///< [in,out] Reference to size in bytes of \p d_temp_storage allocation
        DoubleBuffer<KeyT>      &d_keys,                                ///< [in,out] Reference to the double-buffer of keys whose "current" device-accessible buffer contains the unsorted input keys and, upon return, is updated to point to the sorted output keys
        DoubleBuffer<ValueT>    &d_values,                              ///< [in,out] Double-buffer of values whose "current" device-accessible buffer contains the unsorted input values and, upon return, is updated to point to the sorted output values
        int                     num_items,                              ///< [in] Number of items to sort
        int                     begin_bit           = 0,                ///< [in] <b>[optional]</b> The least-significant bit index (inclusive)  needed for key comparison
        int                     end_bit             = sizeof(KeyT) * 8, ///< [in] <b>[optional]</b> The most-significant bit index (exclusive) needed for key comparison (e.g., sizeof(unsigned int) * 8)
        cudaStream_t            stream              = 0,                ///< [in] <b>[optional]</b> CUDA stream to launch kernels within.  Default is stream<sub>0</sub>.
        bool                    debug_synchronous   = false)            ///< [in] <b>[optional]</b> Whether or not to synchronize the stream after every kernel launch to check for errors.  Also causes launch configurations to be printed to the console.  Default is \p false.
    {
        // Signed integer type for global offsets
        typedef int OffsetT;

        return DispatchRadixSort<true, KeyT, ValueT, OffsetT>::Dispatch(
            d_temp_storage,
            temp_storage_bytes,
            d_keys,
            d_values,
            num_items,
            begin_bit,
            end_bit,
            true,
            stream,
            debug_synchronous);
    }


    //@}  end member group
    /******************************************************************//**
     * \name Keys-only
     *********************************************************************/
    //@{


    /**
     * \brief Sorts keys into ascending order. (~<em>2N </em>auxiliary storage required)
     *
     * \par
     * - The contents of the input data are not altered by the sorting operation
     * - An optional bit subrange <tt>[begin_bit, end_bit)</tt> of differentiating key bits can be specified.  This can reduce overall sorting overhead and yield a corresponding performance improvement.
     * - \devicestorageNP  For sorting using only <em>O</em>(<tt>P</tt>) temporary storage, see the sorting interface using DoubleBuffer wrappers below.
     * - \devicestorage
     *
     * \par Performance
     * The following charts illustrate saturated sorting performance across different
     * CUDA architectures for uniform-random \p uint32 and \p uint64 keys, respectively.
     *
     * \image html lsb_radix_sort_int32_keys.png
     * \image html lsb_radix_sort_int64_keys.png
     *
     * \par Snippet
     * The code snippet below illustrates the sorting of a device vector of \p int keys.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/device/device_radix_sort.cuh>
     *
     * // Declare, allocate, and initialize device-accessible pointers for sorting data
     * int  num_items;          // e.g., 7
     * int  *d_keys_in;         // e.g., [8, 6, 7, 5, 3, 0, 9]
     * int  *d_keys_out;        // e.g., [        ...        ]
     * ...
     *
     * // Determine temporary device storage requirements
     * void     *d_temp_storage = NULL;
     * size_t   temp_storage_bytes = 0;
     * cub::DeviceRadixSort::SortKeys(d_temp_storage, temp_storage_bytes, d_keys_in, d_keys_out, num_items);
     *
     * // Allocate temporary storage
     * cudaMalloc(&d_temp_storage, temp_storage_bytes);
     *
     * // Run sorting operation
     * cub::DeviceRadixSort::SortKeys(d_temp_storage, temp_storage_bytes, d_keys_in, d_keys_out, num_items);
     *
     * // d_keys_out            <-- [0, 3, 5, 6, 7, 8, 9]
     *
     * \endcode
     *
     * \tparam KeyT      <b>[inferred]</b> KeyT type
     */
    template <typename KeyT>
    CUB_RUNTIME_FUNCTION
    static cudaError_t SortKeys(
        void                *d_temp_storage,                        ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
        size_t              &temp_storage_bytes,                    ///< [in,out] Reference to size in bytes of \p d_temp_storage allocation
        const KeyT          *d_keys_in,                             ///< [in] Pointer to the input data of key data to sort
        KeyT                *d_keys_out,                            ///< [out] Pointer to the sorted output sequence of key data
        int                 num_items,                              ///< [in] Number of items to sort
        int                 begin_bit           = 0,                ///< [in] <b>[optional]</b> The least-significant bit index (inclusive)  needed for key comparison
        int                 end_bit             = sizeof(KeyT) * 8, ///< [in] <b>[optional]</b> The most-significant bit index (exclusive) needed for key comparison (e.g., sizeof(unsigned int) * 8)
        cudaStream_t        stream              = 0,                ///< [in] <b>[optional]</b> CUDA stream to launch kernels within.  Default is stream<sub>0</sub>.
        bool                debug_synchronous   = false)            ///< [in] <b>[optional]</b> Whether or not to synchronize the stream after every kernel launch to check for errors.  Also causes launch configurations to be printed to the console.  Default is \p false.
    {
        // Signed integer type for global offsets
        typedef int OffsetT;

        // Null value type
        DoubleBuffer<KeyT>      d_keys(const_cast<KeyT*>(d_keys_in), d_keys_out);
        DoubleBuffer<NullType>  d_values;

        return DispatchRadixSort<false, KeyT, NullType, OffsetT>::Dispatch(
            d_temp_storage,
            temp_storage_bytes,
            d_keys,
            d_values,
            num_items,
            begin_bit,
            end_bit,
            false,
            stream,
            debug_synchronous);
    }


    /**
     * \brief Sorts keys into ascending order. (~<em>N </em>auxiliary storage required).
     *
     * \par
     * - The sorting operation is given a pair of key buffers managed by a
     *   DoubleBuffer structure that indicates which of the two buffers is
     *   "current" (and thus contains the input data to be sorted).
     * - The contents of both buffers may be altered by the sorting operation.
     * - Upon completion, the sorting operation will update the "current" indicator
     *   within the DoubleBuffer wrapper to reference which of the two buffers
     *   now contains the sorted output sequence (a function of the number of key bits
     *   specified and the targeted device architecture).
     * - An optional bit subrange <tt>[begin_bit, end_bit)</tt> of differentiating key bits can be specified.  This can reduce overall sorting overhead and yield a corresponding performance improvement.
     * - \devicestorageP
     * - \devicestorage
     *
     * \par Performance
     * The following charts illustrate saturated sorting performance across different
     * CUDA architectures for uniform-random \p uint32 and \p uint64 keys, respectively.
     *
     * \image html lsb_radix_sort_int32_keys.png
     * \image html lsb_radix_sort_int64_keys.png
     *
     * \par Snippet
     * The code snippet below illustrates the sorting of a device vector of \p int keys.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/device/device_radix_sort.cuh>
     *
     * // Declare, allocate, and initialize device-accessible pointers for sorting data
     * int  num_items;          // e.g., 7
     * int  *d_key_buf;         // e.g., [8, 6, 7, 5, 3, 0, 9]
     * int  *d_key_alt_buf;     // e.g., [        ...        ]
     * ...
     *
     * // Create a DoubleBuffer to wrap the pair of device pointers
     * cub::DoubleBuffer<int> d_keys(d_key_buf, d_key_alt_buf);
     *
     * // Determine temporary device storage requirements
     * void     *d_temp_storage = NULL;
     * size_t   temp_storage_bytes = 0;
     * cub::DeviceRadixSort::SortKeys(d_temp_storage, temp_storage_bytes, d_keys, num_items);
     *
     * // Allocate temporary storage
     * cudaMalloc(&d_temp_storage, temp_storage_bytes);
     *
     * // Run sorting operation
     * cub::DeviceRadixSort::SortKeys(d_temp_storage, temp_storage_bytes, d_keys, num_items);
     *
     * // d_keys.Current()      <-- [0, 3, 5, 6, 7, 8, 9]
     *
     * \endcode
     *
     * \tparam KeyT      <b>[inferred]</b> KeyT type
     */
    template <typename KeyT>
    CUB_RUNTIME_FUNCTION
    static cudaError_t SortKeys(
        void                *d_temp_storage,                        ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
        size_t              &temp_storage_bytes,                    ///< [in,out] Reference to size in bytes of \p d_temp_storage allocation
        DoubleBuffer<KeyT>  &d_keys,                                ///< [in,out] Reference to the double-buffer of keys whose "current" device-accessible buffer contains the unsorted input keys and, upon return, is updated to point to the sorted output keys
        int                 num_items,                              ///< [in] Number of items to sort
        int                 begin_bit           = 0,                ///< [in] <b>[optional]</b> The least-significant bit index (inclusive)  needed for key comparison
        int                 end_bit             = sizeof(KeyT) * 8, ///< [in] <b>[optional]</b> The most-significant bit index (exclusive) needed for key comparison (e.g., sizeof(unsigned int) * 8)
        cudaStream_t        stream              = 0,                ///< [in] <b>[optional]</b> CUDA stream to launch kernels within.  Default is stream<sub>0</sub>.
        bool                debug_synchronous   = false)            ///< [in] <b>[optional]</b> Whether or not to synchronize the stream after every kernel launch to check for errors.  Also causes launch configurations to be printed to the console.  Default is \p false.
    {
        // Signed integer type for global offsets
        typedef int OffsetT;

        // Null value type
        DoubleBuffer<NullType> d_values;

        return DispatchRadixSort<false, KeyT, NullType, OffsetT>::Dispatch(
            d_temp_storage,
            temp_storage_bytes,
            d_keys,
            d_values,
            num_items,
            begin_bit,
            end_bit,
            true,
            stream,
            debug_synchronous);
    }

    /**
     * \brief Sorts keys into descending order. (~<em>2N</em> auxiliary storage required).
     *
     * \par
     * - The contents of the input data are not altered by the sorting operation
     * - An optional bit subrange <tt>[begin_bit, end_bit)</tt> of differentiating key bits can be specified.  This can reduce overall sorting overhead and yield a corresponding performance improvement.
     * - \devicestorageNP  For sorting using only <em>O</em>(<tt>P</tt>) temporary storage, see the sorting interface using DoubleBuffer wrappers below.
     * - \devicestorage
     *
     * \par Performance
     * Performance is similar to DeviceRadixSort::SortKeys.
     *
     * \par Snippet
     * The code snippet below illustrates the sorting of a device vector of \p int keys.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/device/device_radix_sort.cuh>
     *
     * // Declare, allocate, and initialize device-accessible pointers for sorting data
     * int  num_items;          // e.g., 7
     * int  *d_keys_in;         // e.g., [8, 6, 7, 5, 3, 0, 9]
     * int  *d_keys_out;        // e.g., [        ...        ]
     * ...
     *
     * // Create a DoubleBuffer to wrap the pair of device pointers
     * cub::DoubleBuffer<int> d_keys(d_key_buf, d_key_alt_buf);
     *
     * // Determine temporary device storage requirements
     * void     *d_temp_storage = NULL;
     * size_t   temp_storage_bytes = 0;
     * cub::DeviceRadixSort::SortKeysDescending(d_temp_storage, temp_storage_bytes, d_keys_in, d_keys_out, num_items);
     *
     * // Allocate temporary storage
     * cudaMalloc(&d_temp_storage, temp_storage_bytes);
     *
     * // Run sorting operation
     * cub::DeviceRadixSort::SortKeysDescending(d_temp_storage, temp_storage_bytes, d_keys_in, d_keys_out, num_items);
     *
     * // d_keys_out            <-- [9, 8, 7, 6, 5, 3, 0]s
     *
     * \endcode
     *
     * \tparam KeyT      <b>[inferred]</b> KeyT type
     */
    template <typename KeyT>
    CUB_RUNTIME_FUNCTION
    static cudaError_t SortKeysDescending(
        void                *d_temp_storage,                        ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
        size_t              &temp_storage_bytes,                    ///< [in,out] Reference to size in bytes of \p d_temp_storage allocation
        const KeyT          *d_keys_in,                             ///< [in] Pointer to the input data of key data to sort
        KeyT                *d_keys_out,                            ///< [out] Pointer to the sorted output sequence of key data
        int                 num_items,                              ///< [in] Number of items to sort
        int                 begin_bit           = 0,                ///< [in] <b>[optional]</b> The least-significant bit index (inclusive)  needed for key comparison
        int                 end_bit             = sizeof(KeyT) * 8, ///< [in] <b>[optional]</b> The most-significant bit index (exclusive) needed for key comparison (e.g., sizeof(unsigned int) * 8)
        cudaStream_t        stream              = 0,                ///< [in] <b>[optional]</b> CUDA stream to launch kernels within.  Default is stream<sub>0</sub>.
        bool                debug_synchronous   = false)            ///< [in] <b>[optional]</b> Whether or not to synchronize the stream after every kernel launch to check for errors.  Also causes launch configurations to be printed to the console.  Default is \p false.
    {
        // Signed integer type for global offsets
        typedef int OffsetT;

        DoubleBuffer<KeyT>      d_keys(const_cast<KeyT*>(d_keys_in), d_keys_out);
        DoubleBuffer<NullType>  d_values;

        return DispatchRadixSort<true, KeyT, NullType, OffsetT>::Dispatch(
            d_temp_storage,
            temp_storage_bytes,
            d_keys,
            d_values,
            num_items,
            begin_bit,
            end_bit,
            false,
            stream,
            debug_synchronous);
    }


    /**
     * \brief Sorts keys into descending order. (~<em>N </em>auxiliary storage required).
     *
     * \par
     * - The sorting operation is given a pair of key buffers managed by a
     *   DoubleBuffer structure that indicates which of the two buffers is
     *   "current" (and thus contains the input data to be sorted).
     * - The contents of both buffers may be altered by the sorting operation.
     * - Upon completion, the sorting operation will update the "current" indicator
     *   within the DoubleBuffer wrapper to reference which of the two buffers
     *   now contains the sorted output sequence (a function of the number of key bits
     *   specified and the targeted device architecture).
     * - An optional bit subrange <tt>[begin_bit, end_bit)</tt> of differentiating key bits can be specified.  This can reduce overall sorting overhead and yield a corresponding performance improvement.
     * - \devicestorageP
     * - \devicestorage
     *
     * \par Performance
     * Performance is similar to DeviceRadixSort::SortKeys.
     *
     * \par Snippet
     * The code snippet below illustrates the sorting of a device vector of \p int keys.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/device/device_radix_sort.cuh>
     *
     * // Declare, allocate, and initialize device-accessible pointers for sorting data
     * int  num_items;          // e.g., 7
     * int  *d_key_buf;         // e.g., [8, 6, 7, 5, 3, 0, 9]
     * int  *d_key_alt_buf;     // e.g., [        ...        ]
     * ...
     *
     * // Create a DoubleBuffer to wrap the pair of device pointers
     * cub::DoubleBuffer<int> d_keys(d_key_buf, d_key_alt_buf);
     *
     * // Determine temporary device storage requirements
     * void     *d_temp_storage = NULL;
     * size_t   temp_storage_bytes = 0;
     * cub::DeviceRadixSort::SortKeysDescending(d_temp_storage, temp_storage_bytes, d_keys, num_items);
     *
     * // Allocate temporary storage
     * cudaMalloc(&d_temp_storage, temp_storage_bytes);
     *
     * // Run sorting operation
     * cub::DeviceRadixSort::SortKeysDescending(d_temp_storage, temp_storage_bytes, d_keys, num_items);
     *
     * // d_keys.Current()      <-- [9, 8, 7, 6, 5, 3, 0]
     *
     * \endcode
     *
     * \tparam KeyT      <b>[inferred]</b> KeyT type
     */
    template <typename KeyT>
    CUB_RUNTIME_FUNCTION
    static cudaError_t SortKeysDescending(
        void                *d_temp_storage,                        ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
        size_t              &temp_storage_bytes,                    ///< [in,out] Reference to size in bytes of \p d_temp_storage allocation
        DoubleBuffer<KeyT>  &d_keys,                                ///< [in,out] Reference to the double-buffer of keys whose "current" device-accessible buffer contains the unsorted input keys and, upon return, is updated to point to the sorted output keys
        int                 num_items,                              ///< [in] Number of items to sort
        int                 begin_bit           = 0,                ///< [in] <b>[optional]</b> The least-significant bit index (inclusive)  needed for key comparison
        int                 end_bit             = sizeof(KeyT) * 8, ///< [in] <b>[optional]</b> The most-significant bit index (exclusive) needed for key comparison (e.g., sizeof(unsigned int) * 8)
        cudaStream_t        stream              = 0,                ///< [in] <b>[optional]</b> CUDA stream to launch kernels within.  Default is stream<sub>0</sub>.
        bool                debug_synchronous   = false)            ///< [in] <b>[optional]</b> Whether or not to synchronize the stream after every kernel launch to check for errors.  Also causes launch configurations to be printed to the console.  Default is \p false.
    {
        // Signed integer type for global offsets
        typedef int OffsetT;

        // Null value type
        DoubleBuffer<NullType> d_values;

        return DispatchRadixSort<true, KeyT, NullType, OffsetT>::Dispatch(
            d_temp_storage,
            temp_storage_bytes,
            d_keys,
            d_values,
            num_items,
            begin_bit,
            end_bit,
            true,
            stream,
            debug_synchronous);
    }


    //@}  end member group


};

/**
 * \example example_device_radix_sort.cu
 */

}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)


