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
 * \brief A random-access output wrapper for storing array values using a PTX cache-modifier.
 *
 * \par Overview
 * - CacheModifiedOutputIterator is a random-access output iterator that wraps a native
 *   device pointer of type <tt>ValueType*</tt>. \p ValueType references are
 *   made by writing \p ValueType values through stores modified by \p MODIFIER.
 * - Can be used to store any data type to memory using PTX cache store modifiers (e.g., "STORE_WB",
 *   "STORE_CG", "STORE_CS", "STORE_WT", etc.).
 * - Can be constructed, manipulated, and exchanged within and between host and device
 *   functions, but can only be dereferenced within device functions.
 * - Compatible with Thrust API v1.7 or newer.
 *
 * \par Snippet
 * The code snippet below illustrates the use of \p CacheModifiedOutputIterator to
 * dereference a device array of doubles using the "wt" PTX load modifier
 * (i.e., write-through to system memory).
 * \par
 * \code
 * #include <cub/cub.cuh>   // or equivalently <cub/iterator/cache_modified_output_iterator.cuh>
 *
 * // Declare, allocate, and initialize a device array
 * double *d_out;              // e.g., [, , , , , , ]
 *
 * // Create an iterator wrapper
 * cub::CacheModifiedOutputIterator<cub::STORE_WT, double> itr(d_out);
 *
 * // Within device code:
 * itr[0]  = 8.0;
 * itr[1]  = 66.0;
 * itr[55] = 24.0;
 *
 * \endcode
 *
 * \par Usage Considerations
 * - Can only be dereferenced within device code
 *
 * \tparam CacheStoreModifier     The cub::CacheStoreModifier to use when accessing data
 * \tparam ValueType            The value type of this iterator
 * \tparam OffsetT              The difference type of this iterator (Default: \p ptrdiff_t)
 */
template <
    CacheStoreModifier  MODIFIER,
    typename            ValueType,
    typename            OffsetT = ptrdiff_t>
class CacheModifiedOutputIterator
{
private:

    // Proxy object
    struct Reference
    {
        ValueType* ptr;

        /// Constructor
        __host__ __device__ __forceinline__ Reference(ValueType* ptr) : ptr(ptr) {}

        /// Assignment
        __device__ __forceinline__ ValueType operator =(ValueType val)
        {
            ThreadStore<MODIFIER>(ptr, val);
            return val;
        }
    };

public:

    // Required iterator traits
    typedef CacheModifiedOutputIterator         self_type;              ///< My own type
    typedef OffsetT                             difference_type;        ///< Type to express the result of subtracting one iterator from another
    typedef void                                value_type;             ///< The type of the element the iterator can point to
    typedef void                                pointer;                ///< The type of a pointer to an element the iterator can point to
    typedef Reference                           reference;              ///< The type of a reference to an element the iterator can point to

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

private:

    ValueType* ptr;

public:

    /// Constructor
    template <typename QualifiedValueType>
    __host__ __device__ __forceinline__ CacheModifiedOutputIterator(
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
    __host__ __device__ __forceinline__ reference operator*() const
    {
        return Reference(ptr);
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
    __host__ __device__ __forceinline__ reference operator[](Distance n) const
    {
        return Reference(ptr + n);
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
    friend std::ostream& operator<<(std::ostream& os, const self_type& itr)
    {
        return os;
    }
};


/** @} */       // end group UtilIterator

}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)
