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
 * Thread utilities for sequential search
 */

#pragma once

#include "../util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {


/**
 * Computes the begin offsets into A and B for the specific diagonal
 */
template <
    typename AIteratorT,
    typename BIteratorT,
    typename OffsetT,
    typename CoordinateT>
__host__ __device__ __forceinline__ void MergePathSearch(
    OffsetT         diagonal,
    AIteratorT      a,
    BIteratorT      b,
    OffsetT         a_len,
    OffsetT         b_len,
    CoordinateT&    path_coordinate)
{
    /// The value type of the input iterator
    typedef typename std::iterator_traits<AIteratorT>::value_type T;

    OffsetT split_min = CUB_MAX(diagonal - b_len, 0);
    OffsetT split_max = CUB_MIN(diagonal, a_len);

    while (split_min < split_max)
    {
        OffsetT split_pivot = (split_min + split_max) >> 1;
        if (a[split_pivot] <= b[diagonal - split_pivot - 1])
        {
            // Move candidate split range up A, down B
            split_min = split_pivot + 1;
        }
        else
        {
            // Move candidate split range up B, down A
            split_max = split_pivot;
        }
    }

    path_coordinate.x = CUB_MIN(split_min, a_len);
    path_coordinate.y = diagonal - split_min;
}



/**
 * \brief Returns the offset of the first value within \p input which does not compare less than \p val
 */
template <
    typename InputIteratorT,
    typename OffsetT,
    typename T>
__device__ __forceinline__ OffsetT LowerBound(
    InputIteratorT      input,              ///< [in] Input sequence
    OffsetT             num_items,          ///< [in] Input sequence length
    T                   val)                ///< [in] Search key
{
    OffsetT retval = 0;
    while (num_items > 0)
    {
        OffsetT half = num_items >> 1;
        if (input[retval + half] < val)
        {
            retval = retval + (half + 1);
            num_items = num_items - (half + 1);
        }
        else
        {
            num_items = half;
        }
    }

    return retval;
}


/**
 * \brief Returns the offset of the first value within \p input which compares greater than \p val
 */
template <
    typename InputIteratorT,
    typename OffsetT,
    typename T>
__device__ __forceinline__ OffsetT UpperBound(
    InputIteratorT      input,              ///< [in] Input sequence
    OffsetT             num_items,          ///< [in] Input sequence length
    T                   val)                ///< [in] Search key
{
    OffsetT retval = 0;
    while (num_items > 0)
    {
        OffsetT half = num_items >> 1;
        if (val < input[retval + half])
        {
            num_items = half;
        }
        else
        {
            retval = retval + (half + 1);
            num_items = num_items - (half + 1);
        }
    }

    return retval;
}





}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)
