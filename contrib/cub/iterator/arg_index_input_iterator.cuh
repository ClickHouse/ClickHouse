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
 * Random-access iterator types
 */

#pragma once

#include <iterator>
#include <iostream>

#include "../thread/thread_load.cuh"
#include "../thread/thread_store.cuh"
#include "../util_device.cuh"
#include "../util_namespace.cuh"

#include <thrust/version.h>

#if (THRUST_VERSION >= 100700)
    // This iterator is compatible with Thrust API 1.7 and newer
    #include <thrust/iterator/iterator_facade.h>
    #include <thrust/iterator/iterator_traits.h>
#endif // THRUST_VERSION

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {

/**
 * \addtogroup UtilIterator
 * @{
 */


/**
 * \brief A random-access input wrapper for pairing dereferenced values with their corresponding indices (forming \p KeyValuePair tuples).
 *
 * \par Overview
 * - ArgIndexInputIteratorTwraps a random access input iterator \p itr of type \p InputIteratorT.
 *   Dereferencing an ArgIndexInputIteratorTat offset \p i produces a \p KeyValuePair value whose
 *   \p key field is \p i and whose \p value field is <tt>itr[i]</tt>.
 * - Can be used with any data type.
 * - Can be constructed, manipulated, and exchanged within and between host and device
 *   functions.  Wrapped host memory can only be dereferenced on the host, and wrapped
 *   device memory can only be dereferenced on the device.
 * - Compatible with Thrust API v1.7 or newer.
 *
 * \par Snippet
 * The code snippet below illustrates the use of \p ArgIndexInputIteratorTto
 * dereference an array of doubles
 * \par
 * \code
 * #include <cub/cub.cuh>   // or equivalently <cub/iterator/arg_index_input_iterator.cuh>
 *
 * // Declare, allocate, and initialize a device array
 * double *d_in;         // e.g., [8.0, 6.0, 7.0, 5.0, 3.0, 0.0, 9.0]
 *
 * // Create an iterator wrapper
 * cub::ArgIndexInputIterator<double*> itr(d_in);
 *
 * // Within device code:
 * typedef typename cub::ArgIndexInputIterator<double*>::value_type Tuple;
 * Tuple item_offset_pair.key = *itr;
 * printf("%f @ %d\n",
 *   item_offset_pair.value,
 *   item_offset_pair.key);   // 8.0 @ 0
 *
 * itr = itr + 6;
 * item_offset_pair.key = *itr;
 * printf("%f @ %d\n",
 *   item_offset_pair.value,
 *   item_offset_pair.key);   // 9.0 @ 6
 *
 * \endcode
 *
 * \tparam InputIteratorT       The value type of the wrapped input iterator
 * \tparam OffsetT              The difference type of this iterator (Default: \p ptrdiff_t)
 * \tparam OutputValueT         The paired value type of the <offset,value> tuple (Default: value type of input iterator)
 */
template <
    typename    InputIteratorT,
    typename    OffsetT             = ptrdiff_t,
    typename    OutputValueT        = typename std::iterator_traits<InputIteratorT>::value_type>
class ArgIndexInputIterator
{
public:

    // Required iterator traits
    typedef ArgIndexInputIterator                       self_type;              ///< My own type
    typedef OffsetT                                     difference_type;        ///< Type to express the result of subtracting one iterator from another
    typedef KeyValuePair<difference_type, OutputValueT> value_type;             ///< The type of the element the iterator can point to
    typedef value_type*                                 pointer;                ///< The type of a pointer to an element the iterator can point to
    typedef value_type                                  reference;              ///< The type of a reference to an element the iterator can point to

#if (THRUST_VERSION >= 100700)
    // Use Thrust's iterator categories so we can use these iterators in Thrust 1.7 (or newer) methods
    typedef typename thrust::detail::iterator_facade_category<
        thrust::any_system_tag,
        thrust::random_access_traversal_tag,
        value_type,
        reference
      >::type iterator_category;                                        ///< The iterator category
#else
    typedef std::random_access_iterator_tag     iterator_category;      ///< The iterator category
#endif  // THRUST_VERSION

private:

    InputIteratorT  itr;
    difference_type offset;

public:

    /// Constructor
    __host__ __device__ __forceinline__ ArgIndexInputIterator(
        InputIteratorT  itr,            ///< Input iterator to wrap
        difference_type offset = 0)     ///< OffsetT (in items) from \p itr denoting the position of the iterator
    :
        itr(itr),
        offset(offset)
    {}

    /// Postfix increment
    __host__ __device__ __forceinline__ self_type operator++(int)
    {
        self_type retval = *this;
        offset++;
        return retval;
    }

    /// Prefix increment
    __host__ __device__ __forceinline__ self_type operator++()
    {
        offset++;
        return *this;
    }

    /// Indirection
    __host__ __device__ __forceinline__ reference operator*() const
    {
        value_type retval;
        retval.value = itr[offset];
        retval.key = offset;
        return retval;
    }

    /// Addition
    template <typename Distance>
    __host__ __device__ __forceinline__ self_type operator+(Distance n) const
    {
        self_type retval(itr, offset + n);
        return retval;
    }

    /// Addition assignment
    template <typename Distance>
    __host__ __device__ __forceinline__ self_type& operator+=(Distance n)
    {
        offset += n;
        return *this;
    }

    /// Subtraction
    template <typename Distance>
    __host__ __device__ __forceinline__ self_type operator-(Distance n) const
    {
        self_type retval(itr, offset - n);
        return retval;
    }

    /// Subtraction assignment
    template <typename Distance>
    __host__ __device__ __forceinline__ self_type& operator-=(Distance n)
    {
        offset -= n;
        return *this;
    }

    /// Distance
    __host__ __device__ __forceinline__ difference_type operator-(self_type other) const
    {
        return offset - other.offset;
    }

    /// Array subscript
    template <typename Distance>
    __host__ __device__ __forceinline__ reference operator[](Distance n) const
    {
        self_type offset = (*this) + n;
        return *offset;
    }

    /// Structure dereference
    __host__ __device__ __forceinline__ pointer operator->()
    {
        return &(*(*this));
    }

    /// Equal to
    __host__ __device__ __forceinline__ bool operator==(const self_type& rhs)
    {
        return ((itr == rhs.itr) && (offset == rhs.offset));
    }

    /// Not equal to
    __host__ __device__ __forceinline__ bool operator!=(const self_type& rhs)
    {
        return ((itr != rhs.itr) || (offset != rhs.offset));
    }

    /// Normalize
    __host__ __device__ __forceinline__ void normalize()
    {
        itr += offset;
        offset = 0;
    }

    /// ostream operator
    friend std::ostream& operator<<(std::ostream& os, const self_type& /*itr*/)
    {
        return os;
    }
};



/** @} */       // end group UtilIterator

}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)
