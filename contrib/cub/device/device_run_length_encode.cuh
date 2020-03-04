
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
 * cub::DeviceRunLengthEncode provides device-wide, parallel operations for computing a run-length encoding across a sequence of data items residing within device-accessible memory.
 */

#pragma once

#include <stdio.h>
#include <iterator>

#include "dispatch/dispatch_rle.cuh"
#include "dispatch/dispatch_reduce_by_key.cuh"
#include "../util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {


/**
 * \brief DeviceRunLengthEncode provides device-wide, parallel operations for demarcating "runs" of same-valued items within a sequence residing within device-accessible memory. ![](run_length_encode_logo.png)
 * \ingroup SingleModule
 *
 * \par Overview
 * A <a href="http://en.wikipedia.org/wiki/Run-length_encoding"><em>run-length encoding</em></a>
 * computes a simple compressed representation of a sequence of input elements such that each
 * maximal "run" of consecutive same-valued data items is encoded as a single data value along with a
 * count of the elements in that run.
 *
 * \par Usage Considerations
 * \cdp_class{DeviceRunLengthEncode}
 *
 * \par Performance
 * \linear_performance{run-length encode}
 *
 * \par
 * The following chart illustrates DeviceRunLengthEncode::RunLengthEncode performance across
 * different CUDA architectures for \p int32 items.
 * Segments have lengths uniformly sampled from [1,1000].
 *
 * \image html rle_int32_len_500.png
 *
 * \par
 * \plots_below
 *
 */
struct DeviceRunLengthEncode
{

    /**
     * \brief Computes a run-length encoding of the sequence \p d_in.
     *
     * \par
     * - For the <em>i</em><sup>th</sup> run encountered, the first key of the run and its length are written to
     *   <tt>d_unique_out[<em>i</em>]</tt> and <tt>d_counts_out[<em>i</em>]</tt>,
     *   respectively.
     * - The total number of runs encountered is written to \p d_num_runs_out.
     * - The <tt>==</tt> equality operator is used to determine whether values are equivalent
     * - \devicestorage
     *
     * \par Performance
     * The following charts illustrate saturated encode performance across different
     * CUDA architectures for \p int32 and \p int64 items, respectively.  Segments have
     * lengths uniformly sampled from [1,1000].
     *
     * \image html rle_int32_len_500.png
     * \image html rle_int64_len_500.png
     *
     * \par
     * The following charts are similar, but with segment lengths uniformly sampled from [1,10]:
     *
     * \image html rle_int32_len_5.png
     * \image html rle_int64_len_5.png
     *
     * \par Snippet
     * The code snippet below illustrates the run-length encoding of a sequence of \p int values.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/device/device_run_length_encode.cuh>
     *
     * // Declare, allocate, and initialize device-accessible pointers for input and output
     * int          num_items;          // e.g., 8
     * int          *d_in;              // e.g., [0, 2, 2, 9, 5, 5, 5, 8]
     * int          *d_unique_out;      // e.g., [ ,  ,  ,  ,  ,  ,  ,  ]
     * int          *d_counts_out;      // e.g., [ ,  ,  ,  ,  ,  ,  ,  ]
     * int          *d_num_runs_out;    // e.g., [ ]
     * ...
     *
     * // Determine temporary device storage requirements
     * void     *d_temp_storage = NULL;
     * size_t   temp_storage_bytes = 0;
     * cub::DeviceRunLengthEncode::Encode(d_temp_storage, temp_storage_bytes, d_in, d_unique_out, d_counts_out, d_num_runs_out, num_items);
     *
     * // Allocate temporary storage
     * cudaMalloc(&d_temp_storage, temp_storage_bytes);
     *
     * // Run encoding
     * cub::DeviceRunLengthEncode::Encode(d_temp_storage, temp_storage_bytes, d_in, d_unique_out, d_counts_out, d_num_runs_out, num_items);
     *
     * // d_unique_out      <-- [0, 2, 9, 5, 8]
     * // d_counts_out      <-- [1, 2, 1, 3, 1]
     * // d_num_runs_out    <-- [5]
     *
     * \endcode
     *
     * \tparam InputIteratorT           <b>[inferred]</b> Random-access input iterator type for reading input items \iterator
     * \tparam UniqueOutputIteratorT    <b>[inferred]</b> Random-access output iterator type for writing unique output items \iterator
     * \tparam LengthsOutputIteratorT   <b>[inferred]</b> Random-access output iterator type for writing output counts \iterator
     * \tparam NumRunsOutputIteratorT   <b>[inferred]</b> Output iterator type for recording the number of runs encountered \iterator
     */
    template <
        typename                    InputIteratorT,
        typename                    UniqueOutputIteratorT,
        typename                    LengthsOutputIteratorT,
        typename                    NumRunsOutputIteratorT>
    CUB_RUNTIME_FUNCTION __forceinline__
    static cudaError_t Encode(
        void*                       d_temp_storage,                ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
        size_t                      &temp_storage_bytes,            ///< [in,out] Reference to size in bytes of \p d_temp_storage allocation
        InputIteratorT              d_in,                           ///< [in] Pointer to the input sequence of keys
        UniqueOutputIteratorT       d_unique_out,                   ///< [out] Pointer to the output sequence of unique keys (one key per run)
        LengthsOutputIteratorT      d_counts_out,                   ///< [out] Pointer to the output sequence of run-lengths (one count per run)
        NumRunsOutputIteratorT      d_num_runs_out,                     ///< [out] Pointer to total number of runs
        int                         num_items,                      ///< [in] Total number of associated key+value pairs (i.e., the length of \p d_in_keys and \p d_in_values)
        cudaStream_t                stream             = 0,         ///< [in] <b>[optional]</b> CUDA stream to launch kernels within.  Default is stream<sub>0</sub>.
        bool                        debug_synchronous  = false)     ///< [in] <b>[optional]</b> Whether or not to synchronize the stream after every kernel launch to check for errors.  May cause significant slowdown.  Default is \p false.
    {
        typedef int         OffsetT;                    // Signed integer type for global offsets
        typedef NullType*   FlagIterator;               // FlagT iterator type (not used)
        typedef NullType    SelectOp;                   // Selection op (not used)
        typedef Equality    EqualityOp;                 // Default == operator
        typedef cub::Sum    ReductionOp;                // Value reduction operator

        // The lengths output value type
        typedef typename If<(Equals<typename std::iterator_traits<LengthsOutputIteratorT>::value_type, void>::VALUE),   // LengthT =  (if output iterator's value type is void) ?
            OffsetT,                                                                                                    // ... then the OffsetT type,
            typename std::iterator_traits<LengthsOutputIteratorT>::value_type>::Type LengthT;                           // ... else the output iterator's value type

        // Generator type for providing 1s values for run-length reduction
        typedef ConstantInputIterator<LengthT, OffsetT> LengthsInputIteratorT;

        return DispatchReduceByKey<InputIteratorT, UniqueOutputIteratorT, LengthsInputIteratorT, LengthsOutputIteratorT, NumRunsOutputIteratorT, EqualityOp, ReductionOp, OffsetT>::Dispatch(
            d_temp_storage,
            temp_storage_bytes,
            d_in,
            d_unique_out,
            LengthsInputIteratorT((LengthT) 1),
            d_counts_out,
            d_num_runs_out,
            EqualityOp(),
            ReductionOp(),
            num_items,
            stream,
            debug_synchronous);
    }


    /**
     * \brief Enumerates the starting offsets and lengths of all non-trivial runs (of length > 1) of same-valued keys in the sequence \p d_in.
     *
     * \par
     * - For the <em>i</em><sup>th</sup> non-trivial run, the run's starting offset
     *   and its length are written to <tt>d_offsets_out[<em>i</em>]</tt> and
     *   <tt>d_lengths_out[<em>i</em>]</tt>, respectively.
     * - The total number of runs encountered is written to \p d_num_runs_out.
     * - The <tt>==</tt> equality operator is used to determine whether values are equivalent
     * - \devicestorage
     *
     * \par Performance
     *
     * \par Snippet
     * The code snippet below illustrates the identification of non-trivial runs within a sequence of \p int values.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/device/device_run_length_encode.cuh>
     *
     * // Declare, allocate, and initialize device-accessible pointers for input and output
     * int          num_items;          // e.g., 8
     * int          *d_in;              // e.g., [0, 2, 2, 9, 5, 5, 5, 8]
     * int          *d_offsets_out;     // e.g., [ ,  ,  ,  ,  ,  ,  ,  ]
     * int          *d_lengths_out;     // e.g., [ ,  ,  ,  ,  ,  ,  ,  ]
     * int          *d_num_runs_out;    // e.g., [ ]
     * ...
     *
     * // Determine temporary device storage requirements
     * void     *d_temp_storage = NULL;
     * size_t   temp_storage_bytes = 0;
     * cub::DeviceRunLengthEncode::NonTrivialRuns(d_temp_storage, temp_storage_bytes, d_in, d_offsets_out, d_lengths_out, d_num_runs_out, num_items);
     *
     * // Allocate temporary storage
     * cudaMalloc(&d_temp_storage, temp_storage_bytes);
     *
     * // Run encoding
     * cub::DeviceRunLengthEncode::NonTrivialRuns(d_temp_storage, temp_storage_bytes, d_in, d_offsets_out, d_lengths_out, d_num_runs_out, num_items);
     *
     * // d_offsets_out         <-- [1, 4]
     * // d_lengths_out         <-- [2, 3]
     * // d_num_runs_out        <-- [2]
     *
     * \endcode
     *
     * \tparam InputIteratorT           <b>[inferred]</b> Random-access input iterator type for reading input items \iterator
     * \tparam OffsetsOutputIteratorT   <b>[inferred]</b> Random-access output iterator type for writing run-offset values \iterator
     * \tparam LengthsOutputIteratorT   <b>[inferred]</b> Random-access output iterator type for writing run-length values \iterator
     * \tparam NumRunsOutputIteratorT   <b>[inferred]</b> Output iterator type for recording the number of runs encountered \iterator
     */
    template <
        typename                InputIteratorT,
        typename                OffsetsOutputIteratorT,
        typename                LengthsOutputIteratorT,
        typename                NumRunsOutputIteratorT>
    CUB_RUNTIME_FUNCTION __forceinline__
    static cudaError_t NonTrivialRuns(
        void*               d_temp_storage,                ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
        size_t                  &temp_storage_bytes,            ///< [in,out] Reference to size in bytes of \p d_temp_storage allocation
        InputIteratorT          d_in,                           ///< [in] Pointer to input sequence of data items
        OffsetsOutputIteratorT  d_offsets_out,                  ///< [out] Pointer to output sequence of run-offsets (one offset per non-trivial run)
        LengthsOutputIteratorT  d_lengths_out,                  ///< [out] Pointer to output sequence of run-lengths (one count per non-trivial run)
        NumRunsOutputIteratorT  d_num_runs_out,                 ///< [out] Pointer to total number of runs (i.e., length of \p d_offsets_out)
        int                     num_items,                      ///< [in] Total number of associated key+value pairs (i.e., the length of \p d_in_keys and \p d_in_values)
        cudaStream_t            stream             = 0,         ///< [in] <b>[optional]</b> CUDA stream to launch kernels within.  Default is stream<sub>0</sub>.
        bool                    debug_synchronous  = false)     ///< [in] <b>[optional]</b> Whether or not to synchronize the stream after every kernel launch to check for errors.  May cause significant slowdown.  Default is \p false.
    {
        typedef int         OffsetT;                    // Signed integer type for global offsets
        typedef Equality    EqualityOp;                 // Default == operator

        return DeviceRleDispatch<InputIteratorT, OffsetsOutputIteratorT, LengthsOutputIteratorT, NumRunsOutputIteratorT, EqualityOp, OffsetT>::Dispatch(
            d_temp_storage,
            temp_storage_bytes,
            d_in,
            d_offsets_out,
            d_lengths_out,
            d_num_runs_out,
            EqualityOp(),
            num_items,
            stream,
            debug_synchronous);
    }


};


}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)


