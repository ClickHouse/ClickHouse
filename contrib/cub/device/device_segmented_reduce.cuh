
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
 * cub::DeviceSegmentedReduce provides device-wide, parallel operations for computing a batched reduction across multiple sequences of data items residing within device-accessible memory.
 */

#pragma once

#include <stdio.h>
#include <iterator>

#include "../iterator/arg_index_input_iterator.cuh"
#include "dispatch/dispatch_reduce.cuh"
#include "dispatch/dispatch_reduce_by_key.cuh"
#include "../util_type.cuh"
#include "../util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {


/**
 * \brief DeviceSegmentedReduce provides device-wide, parallel operations for computing a reduction across multiple sequences of data items residing within device-accessible memory. ![](reduce_logo.png)
 * \ingroup SegmentedModule
 *
 * \par Overview
 * A <a href="http://en.wikipedia.org/wiki/Reduce_(higher-order_function)"><em>reduction</em></a> (or <em>fold</em>)
 * uses a binary combining operator to compute a single aggregate from a sequence of input elements.
 *
 * \par Usage Considerations
 * \cdp_class{DeviceSegmentedReduce}
 *
 */
struct DeviceSegmentedReduce
{
    /**
     * \brief Computes a device-wide segmented reduction using the specified binary \p reduction_op functor.
     *
     * \par
     * - Does not support binary reduction operators that are non-commutative.
     * - When input a contiguous sequence of segments, a single sequence
     *   \p segment_offsets (of length <tt>num_segments+1</tt>) can be aliased
     *   for both the \p d_begin_offsets and \p d_end_offsets parameters (where
     *   the latter is specified as <tt>segment_offsets+1</tt>).
     * - \devicestorage
     *
     * \par Snippet
     * The code snippet below illustrates a custom min-reduction of a device vector of \p int data elements.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/device/device_radix_sort.cuh>
     *
     * // CustomMin functor
     * struct CustomMin
     * {
     *     template <typename T>
     *     CUB_RUNTIME_FUNCTION __forceinline__
     *     T operator()(const T &a, const T &b) const {
     *         return (b < a) ? b : a;
     *     }
     * };
     *
     * // Declare, allocate, and initialize device-accessible pointers for input and output
     * int          num_segments;   // e.g., 3
     * int          *d_offsets;     // e.g., [0, 3, 3, 7]
     * int          *d_in;          // e.g., [8, 6, 7, 5, 3, 0, 9]
     * int          *d_out;         // e.g., [-, -, -]
     * CustomMin    min_op;
     * int          initial_value;           // e.g., INT_MAX
     * ...
     *
     * // Determine temporary device storage requirements
     * void     *d_temp_storage = NULL;
     * size_t   temp_storage_bytes = 0;
     * cub::DeviceSegmentedReduce::Reduce(d_temp_storage, temp_storage_bytes, d_in, d_out,
     *     num_segments, d_offsets, d_offsets + 1, min_op, initial_value);
     *
     * // Allocate temporary storage
     * cudaMalloc(&d_temp_storage, temp_storage_bytes);
     *
     * // Run reduction
     * cub::DeviceSegmentedReduce::Reduce(d_temp_storage, temp_storage_bytes, d_in, d_out,
     *     num_segments, d_offsets, d_offsets + 1, min_op, initial_value);
     *
     * // d_out <-- [6, INT_MAX, 0]
     *
     * \endcode
     *
     * \tparam InputIteratorT       <b>[inferred]</b> Random-access input iterator type for reading input items \iterator
     * \tparam OutputIteratorT      <b>[inferred]</b> Output iterator type for recording the reduced aggregate \iterator
     * \tparam OffsetIteratorT      <b>[inferred]</b> Random-access input iterator type for reading segment offsets \iterator
     * \tparam ReductionOp          <b>[inferred]</b> Binary reduction functor type having member <tt>T operator()(const T &a, const T &b)</tt>
     * \tparam T                    <b>[inferred]</b> Data element type that is convertible to the \p value type of \p InputIteratorT
     */
    template <
        typename            InputIteratorT,
        typename            OutputIteratorT,
        typename            OffsetIteratorT,
        typename            ReductionOp,
        typename            T>
    CUB_RUNTIME_FUNCTION
    static cudaError_t Reduce(
        void                *d_temp_storage,                    ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
        size_t              &temp_storage_bytes,                ///< [in,out] Reference to size in bytes of \p d_temp_storage allocation
        InputIteratorT      d_in,                               ///< [in] Pointer to the input sequence of data items
        OutputIteratorT     d_out,                              ///< [out] Pointer to the output aggregate
        int                 num_segments,                       ///< [in] The number of segments that comprise the sorting data
        OffsetIteratorT     d_begin_offsets,                    ///< [in] Pointer to the sequence of beginning offsets of length \p num_segments, such that <tt>d_begin_offsets[i]</tt> is the first element of the <em>i</em><sup>th</sup> data segment in <tt>d_keys_*</tt> and <tt>d_values_*</tt>
        OffsetIteratorT     d_end_offsets,                      ///< [in] Pointer to the sequence of ending offsets of length \p num_segments, such that <tt>d_end_offsets[i]-1</tt> is the last element of the <em>i</em><sup>th</sup> data segment in <tt>d_keys_*</tt> and <tt>d_values_*</tt>.  If <tt>d_end_offsets[i]-1</tt> <= <tt>d_begin_offsets[i]</tt>, the <em>i</em><sup>th</sup> is considered empty.
        ReductionOp         reduction_op,                       ///< [in] Binary reduction functor 
        T                   initial_value,                      ///< [in] Initial value of the reduction for each segment
        cudaStream_t        stream              = 0,            ///< [in] <b>[optional]</b> CUDA stream to launch kernels within.  Default is stream<sub>0</sub>.
        bool                debug_synchronous   = false)        ///< [in] <b>[optional]</b> Whether or not to synchronize the stream after every kernel launch to check for errors.  Also causes launch configurations to be printed to the console.  Default is \p false.
    {
        // Signed integer type for global offsets
        typedef int OffsetT;

        return DispatchSegmentedReduce<InputIteratorT, OutputIteratorT, OffsetIteratorT, OffsetT, ReductionOp>::Dispatch(
            d_temp_storage,
            temp_storage_bytes,
            d_in,
            d_out,
            num_segments,
            d_begin_offsets,
            d_end_offsets,
            reduction_op,
            initial_value,
            stream,
            debug_synchronous);
    }


    /**
     * \brief Computes a device-wide segmented sum using the addition ('+') operator.
     *
     * \par
     * - Uses \p 0 as the initial value of the reduction for each segment.
     * - When input a contiguous sequence of segments, a single sequence
     *   \p segment_offsets (of length <tt>num_segments+1</tt>) can be aliased
     *   for both the \p d_begin_offsets and \p d_end_offsets parameters (where
     *   the latter is specified as <tt>segment_offsets+1</tt>).
     * - Does not support \p + operators that are non-commutative..
     * - \devicestorage
     *
     * \par Snippet
     * The code snippet below illustrates the sum reduction of a device vector of \p int data elements.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/device/device_radix_sort.cuh>
     *
     * // Declare, allocate, and initialize device-accessible pointers for input and output
     * int num_segments;   // e.g., 3
     * int *d_offsets;     // e.g., [0, 3, 3, 7]
     * int *d_in;          // e.g., [8, 6, 7, 5, 3, 0, 9]
     * int *d_out;         // e.g., [-, -, -]
     * ...
     *
     * // Determine temporary device storage requirements
     * void     *d_temp_storage = NULL;
     * size_t   temp_storage_bytes = 0;
     * cub::DeviceSegmentedReduce::Sum(d_temp_storage, temp_storage_bytes, d_in, d_out,
     *     num_segments, d_offsets, d_offsets + 1);
     *
     * // Allocate temporary storage
     * cudaMalloc(&d_temp_storage, temp_storage_bytes);
     *
     * // Run sum-reduction
     * cub::DeviceSegmentedReduce::Sum(d_temp_storage, temp_storage_bytes, d_in, d_out,
     *     num_segments, d_offsets, d_offsets + 1);
     *
     * // d_out <-- [21, 0, 17]
     *
     * \endcode
     *
     * \tparam InputIteratorT     <b>[inferred]</b> Random-access input iterator type for reading input items \iterator
     * \tparam OutputIteratorT    <b>[inferred]</b> Output iterator type for recording the reduced aggregate \iterator
     * \tparam OffsetIteratorT      <b>[inferred]</b> Random-access input iterator type for reading segment offsets \iterator
     */
    template <
        typename            InputIteratorT,
        typename            OutputIteratorT,
        typename            OffsetIteratorT>
    CUB_RUNTIME_FUNCTION
    static cudaError_t Sum(
        void                *d_temp_storage,                    ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
        size_t              &temp_storage_bytes,                ///< [in,out] Reference to size in bytes of \p d_temp_storage allocation
        InputIteratorT      d_in,                               ///< [in] Pointer to the input sequence of data items
        OutputIteratorT     d_out,                              ///< [out] Pointer to the output aggregate
        int                 num_segments,                       ///< [in] The number of segments that comprise the sorting data
        OffsetIteratorT     d_begin_offsets,                    ///< [in] Pointer to the sequence of beginning offsets of length \p num_segments, such that <tt>d_begin_offsets[i]</tt> is the first element of the <em>i</em><sup>th</sup> data segment in <tt>d_keys_*</tt> and <tt>d_values_*</tt>
        OffsetIteratorT     d_end_offsets,                      ///< [in] Pointer to the sequence of ending offsets of length \p num_segments, such that <tt>d_end_offsets[i]-1</tt> is the last element of the <em>i</em><sup>th</sup> data segment in <tt>d_keys_*</tt> and <tt>d_values_*</tt>.  If <tt>d_end_offsets[i]-1</tt> <= <tt>d_begin_offsets[i]</tt>, the <em>i</em><sup>th</sup> is considered empty.
        cudaStream_t        stream              = 0,            ///< [in] <b>[optional]</b> CUDA stream to launch kernels within.  Default is stream<sub>0</sub>.
        bool                debug_synchronous   = false)        ///< [in] <b>[optional]</b> Whether or not to synchronize the stream after every kernel launch to check for errors.  Also causes launch configurations to be printed to the console.  Default is \p false.
    {
        // Signed integer type for global offsets
        typedef int OffsetT;

        // The output value type
        typedef typename If<(Equals<typename std::iterator_traits<OutputIteratorT>::value_type, void>::VALUE),  // OutputT =  (if output iterator's value type is void) ?
            typename std::iterator_traits<InputIteratorT>::value_type,                                          // ... then the input iterator's value type,
            typename std::iterator_traits<OutputIteratorT>::value_type>::Type OutputT;                          // ... else the output iterator's value type

        return DispatchSegmentedReduce<InputIteratorT,  OutputIteratorT, OffsetIteratorT, OffsetT, cub::Sum>::Dispatch(
            d_temp_storage,
            temp_storage_bytes,
            d_in,
            d_out,
            num_segments,
            d_begin_offsets,
            d_end_offsets,
            cub::Sum(),
            OutputT(),            // zero-initialize
            stream,
            debug_synchronous);
    }


    /**
     * \brief Computes a device-wide segmented minimum using the less-than ('<') operator.
     *
     * \par
     * - Uses <tt>std::numeric_limits<T>::max()</tt> as the initial value of the reduction for each segment.
     * - When input a contiguous sequence of segments, a single sequence
     *   \p segment_offsets (of length <tt>num_segments+1</tt>) can be aliased
     *   for both the \p d_begin_offsets and \p d_end_offsets parameters (where
     *   the latter is specified as <tt>segment_offsets+1</tt>).
     * - Does not support \p < operators that are non-commutative.
     * - \devicestorage
     *
     * \par Snippet
     * The code snippet below illustrates the min-reduction of a device vector of \p int data elements.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/device/device_radix_sort.cuh>
     *
     * // Declare, allocate, and initialize device-accessible pointers for input and output
     * int num_segments;   // e.g., 3
     * int *d_offsets;     // e.g., [0, 3, 3, 7]
     * int *d_in;          // e.g., [8, 6, 7, 5, 3, 0, 9]
     * int *d_out;         // e.g., [-, -, -]
     * ...
     *
     * // Determine temporary device storage requirements
     * void     *d_temp_storage = NULL;
     * size_t   temp_storage_bytes = 0;
     * cub::DeviceSegmentedReduce::Min(d_temp_storage, temp_storage_bytes, d_in, d_out,
     *     num_segments, d_offsets, d_offsets + 1);
     *
     * // Allocate temporary storage
     * cudaMalloc(&d_temp_storage, temp_storage_bytes);
     *
     * // Run min-reduction
     * cub::DeviceSegmentedReduce::Min(d_temp_storage, temp_storage_bytes, d_in, d_out,
     *     num_segments, d_offsets, d_offsets + 1);
     *
     * // d_out <-- [6, INT_MAX, 0]
     *
     * \endcode
     *
     * \tparam InputIteratorT     <b>[inferred]</b> Random-access input iterator type for reading input items \iterator
     * \tparam OutputIteratorT    <b>[inferred]</b> Output iterator type for recording the reduced aggregate \iterator
     * \tparam OffsetIteratorT      <b>[inferred]</b> Random-access input iterator type for reading segment offsets \iterator
     */
    template <
        typename            InputIteratorT,
        typename            OutputIteratorT,
        typename            OffsetIteratorT>
    CUB_RUNTIME_FUNCTION
    static cudaError_t Min(
        void                *d_temp_storage,                    ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
        size_t              &temp_storage_bytes,                ///< [in,out] Reference to size in bytes of \p d_temp_storage allocation
        InputIteratorT      d_in,                               ///< [in] Pointer to the input sequence of data items
        OutputIteratorT     d_out,                              ///< [out] Pointer to the output aggregate
        int                 num_segments,                       ///< [in] The number of segments that comprise the sorting data
        OffsetIteratorT     d_begin_offsets,                    ///< [in] Pointer to the sequence of beginning offsets of length \p num_segments, such that <tt>d_begin_offsets[i]</tt> is the first element of the <em>i</em><sup>th</sup> data segment in <tt>d_keys_*</tt> and <tt>d_values_*</tt>
        OffsetIteratorT     d_end_offsets,                      ///< [in] Pointer to the sequence of ending offsets of length \p num_segments, such that <tt>d_end_offsets[i]-1</tt> is the last element of the <em>i</em><sup>th</sup> data segment in <tt>d_keys_*</tt> and <tt>d_values_*</tt>.  If <tt>d_end_offsets[i]-1</tt> <= <tt>d_begin_offsets[i]</tt>, the <em>i</em><sup>th</sup> is considered empty.
        cudaStream_t        stream              = 0,            ///< [in] <b>[optional]</b> CUDA stream to launch kernels within.  Default is stream<sub>0</sub>.
        bool                debug_synchronous   = false)        ///< [in] <b>[optional]</b> Whether or not to synchronize the stream after every kernel launch to check for errors.  Also causes launch configurations to be printed to the console.  Default is \p false.
    {
        // Signed integer type for global offsets
        typedef int OffsetT;

        // The input value type
        typedef typename std::iterator_traits<InputIteratorT>::value_type InputT;

        return DispatchSegmentedReduce<InputIteratorT,  OutputIteratorT, OffsetIteratorT, OffsetT, cub::Min>::Dispatch(
            d_temp_storage,
            temp_storage_bytes,
            d_in,
            d_out,
            num_segments,
            d_begin_offsets,
            d_end_offsets,
            cub::Min(),
            Traits<InputT>::Max(),    // replace with std::numeric_limits<T>::max() when C++11 support is more prevalent
            stream,
            debug_synchronous);
    }


    /**
     * \brief Finds the first device-wide minimum in each segment using the less-than ('<') operator, also returning the in-segment index of that item.
     *
     * \par
     * - The output value type of \p d_out is cub::KeyValuePair <tt><int, T></tt> (assuming the value type of \p d_in is \p T)
     *   - The minimum of the <em>i</em><sup>th</sup> segment is written to <tt>d_out[i].value</tt> and its offset in that segment is written to <tt>d_out[i].key</tt>.
     *   - The <tt>{1, std::numeric_limits<T>::max()}</tt> tuple is produced for zero-length inputs
     * - When input a contiguous sequence of segments, a single sequence
     *   \p segment_offsets (of length <tt>num_segments+1</tt>) can be aliased
     *   for both the \p d_begin_offsets and \p d_end_offsets parameters (where
     *   the latter is specified as <tt>segment_offsets+1</tt>).
     * - Does not support \p < operators that are non-commutative.
     * - \devicestorage
     *
     * \par Snippet
     * The code snippet below illustrates the argmin-reduction of a device vector of \p int data elements.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/device/device_radix_sort.cuh>
     *
     * // Declare, allocate, and initialize device-accessible pointers for input and output
     * int                      num_segments;   // e.g., 3
     * int                      *d_offsets;     // e.g., [0, 3, 3, 7]
     * int                      *d_in;          // e.g., [8, 6, 7, 5, 3, 0, 9]
     * KeyValuePair<int, int>   *d_out;         // e.g., [{-,-}, {-,-}, {-,-}]
     * ...
     *
     * // Determine temporary device storage requirements
     * void     *d_temp_storage = NULL;
     * size_t   temp_storage_bytes = 0;
     * cub::DeviceSegmentedReduce::ArgMin(d_temp_storage, temp_storage_bytes, d_in, d_out,
     *     num_segments, d_offsets, d_offsets + 1);
     *
     * // Allocate temporary storage
     * cudaMalloc(&d_temp_storage, temp_storage_bytes);
     *
     * // Run argmin-reduction
     * cub::DeviceSegmentedReduce::ArgMin(d_temp_storage, temp_storage_bytes, d_in, d_out,
     *     num_segments, d_offsets, d_offsets + 1);
     *
     * // d_out <-- [{1,6}, {1,INT_MAX}, {2,0}]
     *
     * \endcode
     *
     * \tparam InputIteratorT     <b>[inferred]</b> Random-access input iterator type for reading input items (of some type \p T) \iterator
     * \tparam OutputIteratorT    <b>[inferred]</b> Output iterator type for recording the reduced aggregate (having value type <tt>KeyValuePair<int, T></tt>) \iterator
     * \tparam OffsetIteratorT      <b>[inferred]</b> Random-access input iterator type for reading segment offsets \iterator
     */
    template <
        typename            InputIteratorT,
        typename            OutputIteratorT,
        typename            OffsetIteratorT>
    CUB_RUNTIME_FUNCTION
    static cudaError_t ArgMin(
        void                *d_temp_storage,                    ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
        size_t              &temp_storage_bytes,                ///< [in,out] Reference to size in bytes of \p d_temp_storage allocation
        InputIteratorT      d_in,                               ///< [in] Pointer to the input sequence of data items
        OutputIteratorT     d_out,                              ///< [out] Pointer to the output aggregate
        int                 num_segments,                       ///< [in] The number of segments that comprise the sorting data
        OffsetIteratorT     d_begin_offsets,                    ///< [in] Pointer to the sequence of beginning offsets of length \p num_segments, such that <tt>d_begin_offsets[i]</tt> is the first element of the <em>i</em><sup>th</sup> data segment in <tt>d_keys_*</tt> and <tt>d_values_*</tt>
        OffsetIteratorT     d_end_offsets,                      ///< [in] Pointer to the sequence of ending offsets of length \p num_segments, such that <tt>d_end_offsets[i]-1</tt> is the last element of the <em>i</em><sup>th</sup> data segment in <tt>d_keys_*</tt> and <tt>d_values_*</tt>.  If <tt>d_end_offsets[i]-1</tt> <= <tt>d_begin_offsets[i]</tt>, the <em>i</em><sup>th</sup> is considered empty.
        cudaStream_t        stream              = 0,            ///< [in] <b>[optional]</b> CUDA stream to launch kernels within.  Default is stream<sub>0</sub>.
        bool                debug_synchronous   = false)        ///< [in] <b>[optional]</b> Whether or not to synchronize the stream after every kernel launch to check for errors.  Also causes launch configurations to be printed to the console.  Default is \p false.
    {
        // Signed integer type for global offsets
        typedef int OffsetT;

        // The input type
        typedef typename std::iterator_traits<InputIteratorT>::value_type InputValueT;

        // The output tuple type
        typedef typename If<(Equals<typename std::iterator_traits<OutputIteratorT>::value_type, void>::VALUE),  // OutputT =  (if output iterator's value type is void) ?
            KeyValuePair<OffsetT, InputValueT>,                                                                 // ... then the key value pair OffsetT + InputValueT
            typename std::iterator_traits<OutputIteratorT>::value_type>::Type OutputTupleT;                     // ... else the output iterator's value type

        // The output value type
        typedef typename OutputTupleT::Value OutputValueT;

        // Wrapped input iterator to produce index-value <OffsetT, InputT> tuples
        typedef ArgIndexInputIterator<InputIteratorT, OffsetT, OutputValueT> ArgIndexInputIteratorT;
        ArgIndexInputIteratorT d_indexed_in(d_in);

        // Initial value
        OutputTupleT initial_value(1, Traits<InputValueT>::Max());   // replace with std::numeric_limits<T>::max() when C++11 support is more prevalent

        return DispatchSegmentedReduce<ArgIndexInputIteratorT,  OutputIteratorT, OffsetIteratorT, OffsetT, cub::ArgMin>::Dispatch(
            d_temp_storage,
            temp_storage_bytes,
            d_indexed_in,
            d_out,
            num_segments,
            d_begin_offsets,
            d_end_offsets,
            cub::ArgMin(),
            initial_value,
            stream,
            debug_synchronous);
    }


    /**
     * \brief Computes a device-wide segmented maximum using the greater-than ('>') operator.
     *
     * \par
     * - Uses <tt>std::numeric_limits<T>::lowest()</tt> as the initial value of the reduction.
     * - When input a contiguous sequence of segments, a single sequence
     *   \p segment_offsets (of length <tt>num_segments+1</tt>) can be aliased
     *   for both the \p d_begin_offsets and \p d_end_offsets parameters (where
     *   the latter is specified as <tt>segment_offsets+1</tt>).
     * - Does not support \p > operators that are non-commutative.
     * - \devicestorage
     *
     * \par Snippet
     * The code snippet below illustrates the max-reduction of a device vector of \p int data elements.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/device/device_radix_sort.cuh>
     *
     * // Declare, allocate, and initialize device-accessible pointers for input and output
     * int num_segments;   // e.g., 3
     * int *d_offsets;     // e.g., [0, 3, 3, 7]
     * int *d_in;          // e.g., [8, 6, 7, 5, 3, 0, 9]
     * int *d_out;         // e.g., [-, -, -]
     * ...
     *
     * // Determine temporary device storage requirements
     * void     *d_temp_storage = NULL;
     * size_t   temp_storage_bytes = 0;
     * cub::DeviceSegmentedReduce::Max(d_temp_storage, temp_storage_bytes, d_in, d_out,
     *     num_segments, d_offsets, d_offsets + 1);
     *
     * // Allocate temporary storage
     * cudaMalloc(&d_temp_storage, temp_storage_bytes);
     *
     * // Run max-reduction
     * cub::DeviceSegmentedReduce::Max(d_temp_storage, temp_storage_bytes, d_in, d_out,
     *     num_segments, d_offsets, d_offsets + 1);
     *
     * // d_out <-- [8, INT_MIN, 9]
     *
     * \endcode
     *
     * \tparam InputIteratorT     <b>[inferred]</b> Random-access input iterator type for reading input items \iterator
     * \tparam OutputIteratorT    <b>[inferred]</b> Output iterator type for recording the reduced aggregate \iterator
     * \tparam OffsetIteratorT      <b>[inferred]</b> Random-access input iterator type for reading segment offsets \iterator
     */
    template <
        typename            InputIteratorT,
        typename            OutputIteratorT,
        typename            OffsetIteratorT>
    CUB_RUNTIME_FUNCTION
    static cudaError_t Max(
        void                *d_temp_storage,                    ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
        size_t              &temp_storage_bytes,                ///< [in,out] Reference to size in bytes of \p d_temp_storage allocation
        InputIteratorT      d_in,                               ///< [in] Pointer to the input sequence of data items
        OutputIteratorT     d_out,                              ///< [out] Pointer to the output aggregate
        int                 num_segments,                       ///< [in] The number of segments that comprise the sorting data
        OffsetIteratorT     d_begin_offsets,                    ///< [in] Pointer to the sequence of beginning offsets of length \p num_segments, such that <tt>d_begin_offsets[i]</tt> is the first element of the <em>i</em><sup>th</sup> data segment in <tt>d_keys_*</tt> and <tt>d_values_*</tt>
        OffsetIteratorT     d_end_offsets,                      ///< [in] Pointer to the sequence of ending offsets of length \p num_segments, such that <tt>d_end_offsets[i]-1</tt> is the last element of the <em>i</em><sup>th</sup> data segment in <tt>d_keys_*</tt> and <tt>d_values_*</tt>.  If <tt>d_end_offsets[i]-1</tt> <= <tt>d_begin_offsets[i]</tt>, the <em>i</em><sup>th</sup> is considered empty.
        cudaStream_t        stream              = 0,            ///< [in] <b>[optional]</b> CUDA stream to launch kernels within.  Default is stream<sub>0</sub>.
        bool                debug_synchronous   = false)        ///< [in] <b>[optional]</b> Whether or not to synchronize the stream after every kernel launch to check for errors.  Also causes launch configurations to be printed to the console.  Default is \p false.
    {
        // Signed integer type for global offsets
        typedef int OffsetT;

        // The input value type
        typedef typename std::iterator_traits<InputIteratorT>::value_type InputT;

        return DispatchSegmentedReduce<InputIteratorT,  OutputIteratorT, OffsetIteratorT, OffsetT, cub::Max>::Dispatch(
            d_temp_storage,
            temp_storage_bytes,
            d_in,
            d_out,
            num_segments,
            d_begin_offsets,
            d_end_offsets,
            cub::Max(),
            Traits<InputT>::Lowest(),    // replace with std::numeric_limits<T>::lowest() when C++11 support is more prevalent
            stream,
            debug_synchronous);
    }


    /**
     * \brief Finds the first device-wide maximum in each segment using the greater-than ('>') operator, also returning the in-segment index of that item
     *
     * \par
     * - The output value type of \p d_out is cub::KeyValuePair <tt><int, T></tt> (assuming the value type of \p d_in is \p T)
     *   - The maximum of the <em>i</em><sup>th</sup> segment is written to <tt>d_out[i].value</tt> and its offset in that segment is written to <tt>d_out[i].key</tt>.
     *   - The <tt>{1, std::numeric_limits<T>::lowest()}</tt> tuple is produced for zero-length inputs
     * - When input a contiguous sequence of segments, a single sequence
     *   \p segment_offsets (of length <tt>num_segments+1</tt>) can be aliased
     *   for both the \p d_begin_offsets and \p d_end_offsets parameters (where
     *   the latter is specified as <tt>segment_offsets+1</tt>).
     * - Does not support \p > operators that are non-commutative.
     * - \devicestorage
     *
     * \par Snippet
     * The code snippet below illustrates the argmax-reduction of a device vector of \p int data elements.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/device/device_reduce.cuh>
     *
     * // Declare, allocate, and initialize device-accessible pointers for input and output
     * int                      num_segments;   // e.g., 3
     * int                      *d_offsets;     // e.g., [0, 3, 3, 7]
     * int                      *d_in;          // e.g., [8, 6, 7, 5, 3, 0, 9]
     * KeyValuePair<int, int>   *d_out;         // e.g., [{-,-}, {-,-}, {-,-}]
     * ...
     *
     * // Determine temporary device storage requirements
     * void     *d_temp_storage = NULL;
     * size_t   temp_storage_bytes = 0;
     * cub::DeviceSegmentedReduce::ArgMax(d_temp_storage, temp_storage_bytes, d_in, d_out,
     *     num_segments, d_offsets, d_offsets + 1);
     *
     * // Allocate temporary storage
     * cudaMalloc(&d_temp_storage, temp_storage_bytes);
     *
     * // Run argmax-reduction
     * cub::DeviceSegmentedReduce::ArgMax(d_temp_storage, temp_storage_bytes, d_in, d_out,
     *     num_segments, d_offsets, d_offsets + 1);
     *
     * // d_out <-- [{0,8}, {1,INT_MIN}, {3,9}]
     *
     * \endcode
     *
     * \tparam InputIteratorT     <b>[inferred]</b> Random-access input iterator type for reading input items (of some type \p T) \iterator
     * \tparam OutputIteratorT    <b>[inferred]</b> Output iterator type for recording the reduced aggregate (having value type <tt>KeyValuePair<int, T></tt>) \iterator
     * \tparam OffsetIteratorT    <b>[inferred]</b> Random-access input iterator type for reading segment offsets \iterator
     */
    template <
        typename            InputIteratorT,
        typename            OutputIteratorT,
        typename            OffsetIteratorT>
    CUB_RUNTIME_FUNCTION
    static cudaError_t ArgMax(
        void                *d_temp_storage,                    ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
        size_t              &temp_storage_bytes,                ///< [in,out] Reference to size in bytes of \p d_temp_storage allocation
        InputIteratorT      d_in,                               ///< [in] Pointer to the input sequence of data items
        OutputIteratorT     d_out,                              ///< [out] Pointer to the output aggregate
        int                 num_segments,                       ///< [in] The number of segments that comprise the sorting data
        OffsetIteratorT     d_begin_offsets,                    ///< [in] Pointer to the sequence of beginning offsets of length \p num_segments, such that <tt>d_begin_offsets[i]</tt> is the first element of the <em>i</em><sup>th</sup> data segment in <tt>d_keys_*</tt> and <tt>d_values_*</tt>
        OffsetIteratorT     d_end_offsets,                      ///< [in] Pointer to the sequence of ending offsets of length \p num_segments, such that <tt>d_end_offsets[i]-1</tt> is the last element of the <em>i</em><sup>th</sup> data segment in <tt>d_keys_*</tt> and <tt>d_values_*</tt>.  If <tt>d_end_offsets[i]-1</tt> <= <tt>d_begin_offsets[i]</tt>, the <em>i</em><sup>th</sup> is considered empty.
        cudaStream_t        stream              = 0,            ///< [in] <b>[optional]</b> CUDA stream to launch kernels within.  Default is stream<sub>0</sub>.
        bool                debug_synchronous   = false)        ///< [in] <b>[optional]</b> Whether or not to synchronize the stream after every kernel launch to check for errors.  Also causes launch configurations to be printed to the console.  Default is \p false.
    {
        // Signed integer type for global offsets
        typedef int OffsetT;

        // The input type
        typedef typename std::iterator_traits<InputIteratorT>::value_type InputValueT;

        // The output tuple type
        typedef typename If<(Equals<typename std::iterator_traits<OutputIteratorT>::value_type, void>::VALUE),  // OutputT =  (if output iterator's value type is void) ?
            KeyValuePair<OffsetT, InputValueT>,                                                                 // ... then the key value pair OffsetT + InputValueT
            typename std::iterator_traits<OutputIteratorT>::value_type>::Type OutputTupleT;                     // ... else the output iterator's value type

        // The output value type
        typedef typename OutputTupleT::Value OutputValueT;

        // Wrapped input iterator to produce index-value <OffsetT, InputT> tuples
        typedef ArgIndexInputIterator<InputIteratorT, OffsetT, OutputValueT> ArgIndexInputIteratorT;
        ArgIndexInputIteratorT d_indexed_in(d_in);

        // Initial value
        OutputTupleT initial_value(1, Traits<InputValueT>::Lowest());     // replace with std::numeric_limits<T>::lowest() when C++11 support is more prevalent

        return DispatchSegmentedReduce<ArgIndexInputIteratorT, OutputIteratorT, OffsetIteratorT, OffsetT, cub::ArgMax>::Dispatch(
            d_temp_storage,
            temp_storage_bytes,
            d_indexed_in,
            d_out,
            num_segments,
            d_begin_offsets,
            d_end_offsets,
            cub::ArgMax(),
            initial_value,
            stream,
            debug_synchronous);
    }

};

}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)


