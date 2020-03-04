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
 * \brief A random-access input wrapper for transforming dereferenced values.
 *
 * \par Overview
 * - TransformInputIteratorTwraps a unary conversion functor of type \p
 *   ConversionOp and a random-access input iterator of type <tt>InputIteratorT</tt>,
 *   using the former to produce references of type \p ValueType from the latter.
 * - Can be used with any data type.
 * - Can be constructed, manipulated, and exchanged within and between host and device
 *   functions.  Wrapped host memory can only be dereferenced on the host, and wrapped
 *   device memory can only be dereferenced on the device.
 * - Compatible with Thrust API v1.7 or newer.
 *
 * \par Snippet
 * The code snippet below illustrates the use of \p TransformInputIteratorTto
 * dereference an array of integers, tripling the values and converting them to doubles.
 * \par
 * \code
 * #include <cub/cub.cuh>   // or equivalently <cub/iterator/transform_input_iterator.cuh>
 *
 * // Functor for tripling integer values and converting to doubles
 * struct TripleDoubler
 * {
 *     __host__ __device__ __forceinline__
 *     double operator()(const int &a) const {
 *         return double(a * 3);
 *     }
 * };
 *
 * // Declare, allocate, and initialize a device array
 * int *d_in;                   // e.g., [8, 6, 7, 5, 3, 0, 9]
 * TripleDoubler conversion_op;
 *
 * // Create an iterator wrapper
 * cub::TransformInputIterator<double, TripleDoubler, int*> itr(d_in, conversion_op);
 *
 * // Within device code:
 * printf("%f\n", itr[0]);  // 24.0
 * printf("%f\n", itr[1]);  // 18.0
 * printf("%f\n", itr[6]);  // 27.0
 *
 * \endcode
 *
 * \tparam ValueType            The value type of this iterator
 * \tparam ConversionOp         Unary functor type for mapping objects of type \p InputType to type \p ValueType.  Must have member <tt>ValueType operator()(const InputType &datum)</tt>.
 * \tparam InputIteratorT       The type of the wrapped input iterator
 * \tparam OffsetT              The difference type of this iterator (Default: \p ptrdiff_t)
 *
 */
template <
    typename ValueType,
    typename ConversionOp,
    typename InputIteratorT,
    typename OffsetT = ptrdiff_t>
class TransformInputIterator
{
public:

    // Required iterator traits
    typedef TransformInputIterator              self_type;              ///< My own type
    typedef OffsetT                             difference_type;        ///< Type to express the result of subtracting one iterator from another
    typedef ValueType                           value_type;             ///< The type of the element the iterator can point to
    typedef ValueType*                          pointer;                ///< The type of a pointer to an element the iterator can point to
    typedef ValueType                           reference;              ///< The type of a reference to an element the iterator can point to

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

    ConversionOp    conversion_op;
    InputIteratorT  input_itr;

public:

    /// Constructor
    __host__ __device__ __forceinline__ TransformInputIterator(
        InputIteratorT      input_itr,          ///< Input iterator to wrap
        ConversionOp        conversion_op)      ///< Conversion functor to wrap
    :
        conversion_op(conversion_op),
        input_itr(input_itr)
    {}

    /// Postfix increment
    __host__ __device__ __forceinline__ self_type operator++(int)
    {
        self_type retval = *this;
        input_itr++;
        return retval;
    }

    /// Prefix increment
    __host__ __device__ __forceinline__ self_type operator++()
    {
        input_itr++;
        return *this;
    }

    /// Indirection
    __host__ __device__ __forceinline__ reference operator*() const
    {
        return conversion_op(*input_itr);
    }

    /// Addition
    template <typename Distance>
    __host__ __device__ __forceinline__ self_type operator+(Distance n) const
    {
        self_type retval(input_itr + n, conversion_op);
        return retval;
    }

    /// Addition assignment
    template <typename Distance>
    __host__ __device__ __forceinline__ self_type& operator+=(Distance n)
    {
        input_itr += n;
        return *this;
    }

    /// Subtraction
    template <typename Distance>
    __host__ __device__ __forceinline__ self_type operator-(Distance n) const
    {
        self_type retval(input_itr - n, conversion_op);
        return retval;
    }

    /// Subtraction assignment
    template <typename Distance>
    __host__ __device__ __forceinline__ self_type& operator-=(Distance n)
    {
        input_itr -= n;
        return *this;
    }

    /// Distance
    __host__ __device__ __forceinline__ difference_type operator-(self_type other) const
    {
        return input_itr - other.input_itr;
    }

    /// Array subscript
    template <typename Distance>
    __host__ __device__ __forceinline__ reference operator[](Distance n) const
    {
        return conversion_op(input_itr[n]);
    }

    /// Structure dereference
    __host__ __device__ __forceinline__ pointer operator->()
    {
        return &conversion_op(*input_itr);
    }

    /// Equal to
    __host__ __device__ __forceinline__ bool operator==(const self_type& rhs)
    {
        return (input_itr == rhs.input_itr);
    }

    /// Not equal to
    __host__ __device__ __forceinline__ bool operator!=(const self_type& rhs)
    {
        return (input_itr != rhs.input_itr);
    }

    /// ostream operator
    friend std::ostream& operator<<(std::ostream& os, const self_type& itr)
    {
        return os;
    }
};



/** @} */       // end group UtilIterator

}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)
