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
 * \brief A random-access input wrapper for dereferencing array values using a PTX cache load modifier.
 *
 * \par Overview
 * - CacheModifiedInputIteratorTis a random-access input iterator that wraps a native
 *   device pointer of type <tt>ValueType*</tt>. \p ValueType references are
 *   made by reading \p ValueType values through loads modified by \p MODIFIER.
 * - Can be used to load any data type from memory using PTX cache load modifiers (e.g., "LOAD_LDG",
 *   "LOAD_CG", "LOAD_CA", "LOAD_CS", "LOAD_CV", etc.).
 * - Can be constructed, manipulated, and exchanged within and between host and device
 *   functions, but can only be dereferenced within device functions.
 * - Compatible with Thrust API v1.7 or newer.
 *
 * \par Snippet
 * The code snippet below illustrates the use of \p CacheModifiedInputIteratorTto
 * dereference a device array of double using the "ldg" PTX load modifier
 * (i.e., load values through texture cache).
 * \par
 * \code
 * #include <cub/cub.cuh>   // or equivalently <cub/iterator/cache_modified_input_iterator.cuh>
 *
 * // Declare, allocate, and initialize a device array
 * double *d_in;            // e.g., [8.0, 6.0, 7.0, 5.0, 3.0, 0.0, 9.0]
 *
 * // Create an iterator wrapper
 * cub::CacheModifiedInputIterator<cub::LOAD_LDG, double> itr(d_in);
 *
 * // Within device code:
 * printf("%f\n", itr[0]);  // 8.0
 * printf("%f\n", itr[1]);  // 6.0
 * printf("%f\n", itr[6]);  // 9.0
 *
 * \endcode
 *
 * \tparam CacheLoadModifier    The cub::CacheLoadModifier to use when accessing data
 * \tparam ValueType            The value type of this iterator
 * \tparam OffsetT              The difference type of this iterator (Default: \p ptrdiff_t)
 */
template <
    CacheLoadModifier   MODIFIER,
    typename            ValueType,
    typename            OffsetT = ptrdiff_t>
class CacheModifiedInputIterator
{
public:

    // Required iterator traits
    typedef CacheModifiedInputIterator          self_type;              ///< My own type
    typedef OffsetT                             difference_type;        ///< Type to express the result of subtracting one iterator from another
    typedef ValueType                           value_type;             ///< The type of the element the iterator can point to
    typedef ValueType*                          pointer;                ///< The type of a pointer to an element the iterator can point to
    typedef ValueType                           reference;              ///< The type of a reference to an element the iterator can point to

#if (THRUST_VERSION >= 100700)
    // Use Thrust's iterator categories so we can use these iterators in Thrust 1.7 (or newer) methods
    typedef typename thrust::detail::iterator_facade_category<
        thrust::device_system_tag,
        thrust::random_access_traversal_tag,
        value_type,
        reference
      >::type iterator_category;                                        ///< The iterator category
#else
    typedef std::random_access_iterator_tag     iterator_category;      ///< The iterator category
#endif  // THRUST_VERSION


public:

    /// Wrapped native pointer
    ValueType* ptr;

    /// Constructor
    template <typename QualifiedValueType>
    __host__ __device__ __forceinline__ CacheModifiedInputIterator(
        QualifiedValueType* ptr)     ///< Native pointer to wrap
    :
        ptr(const_cast<typename RemoveQualifiers<QualifiedValueType>::Type *>(ptr))
    {}

    /// Postfix increment
    __host__ __device__ __forceinline__ self_type operator++(int)
    {
        self_type retval = *this;
        ptr++;
        return retval;
    }

    /// Prefix increment
    __host__ __device__ __forceinline__ self_type operator++()
    {
        ptr++;
        return *this;
    }

    /// Indirection
    __device__ __forceinline__ reference operator*() const
    {
        return ThreadLoad<MODIFIER>(ptr);
    }

    /// Addition
    template <typename Distance>
    __host__ __device__ __forceinline__ self_type operator+(Distance n) const
    {
        self_type retval(ptr + n);
        return retval;
    }

    /// Addition assignment
    template <typename Distance>
    __host__ __device__ __forceinline__ self_type& operator+=(Distance n)
    {
        ptr += n;
        return *this;
    }

    /// Subtraction
    template <typename Distance>
    __host__ __device__ __forceinline__ self_type operator-(Distance n) const
    {
        self_type retval(ptr - n);
        return retval;
    }

    /// Subtraction assignment
    template <typename Distance>
    __host__ __device__ __forceinline__ self_type& operator-=(Distance n)
    {
        ptr -= n;
        return *this;
    }

    /// Distance
    __host__ __device__ __forceinline__ difference_type operator-(self_type other) const
    {
        return ptr - other.ptr;
    }

    /// Array subscript
    template <typename Distance>
    __device__ __forceinline__ reference operator[](Distance n) const
    {
        return ThreadLoad<MODIFIER>(ptr + n);
    }

    /// Structure dereference
    __device__ __forceinline__ pointer operator->()
    {
        return &ThreadLoad<MODIFIER>(ptr);
    }

    /// Equal to
    __host__ __device__ __forceinline__ bool operator==(const self_type& rhs)
    {
        return (ptr == rhs.ptr);
    }

    /// Not equal to
    __host__ __device__ __forceinline__ bool operator!=(const self_type& rhs)
    {
        return (ptr != rhs.ptr);
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
