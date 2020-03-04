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
#include "../util_debug.cuh"
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
 * \brief A random-access input wrapper for dereferencing array values through texture cache.  Uses newer Kepler-style texture objects.
 *
 * \par Overview
 * - TexObjInputIteratorTwraps a native device pointer of type <tt>ValueType*</tt>. References
 *   to elements are to be loaded through texture cache.
 * - Can be used to load any data type from memory through texture cache.
 * - Can be manipulated and exchanged within and between host and device
 *   functions, can only be constructed within host functions, and can only be
 *   dereferenced within device functions.
 * - With regard to nested/dynamic parallelism, TexObjInputIteratorTiterators may only be
 *   created by the host thread, but can be used by any descendant kernel.
 * - Compatible with Thrust API v1.7 or newer.
 *
 * \par Snippet
 * The code snippet below illustrates the use of \p TexRefInputIteratorTto
 * dereference a device array of doubles through texture cache.
 * \par
 * \code
 * #include <cub/cub.cuh>   // or equivalently <cub/iterator/tex_obj_input_iterator.cuh>
 *
 * // Declare, allocate, and initialize a device array
 * int num_items;   // e.g., 7
 * double *d_in;    // e.g., [8.0, 6.0, 7.0, 5.0, 3.0, 0.0, 9.0]
 *
 * // Create an iterator wrapper
 * cub::TexObjInputIterator<double> itr;
 * itr.BindTexture(d_in, sizeof(double) * num_items);
 * ...
 *
 * // Within device code:
 * printf("%f\n", itr[0]);      // 8.0
 * printf("%f\n", itr[1]);      // 6.0
 * printf("%f\n", itr[6]);      // 9.0
 *
 * ...
 * itr.UnbindTexture();
 *
 * \endcode
 *
 * \tparam T                    The value type of this iterator
 * \tparam OffsetT              The difference type of this iterator (Default: \p ptrdiff_t)
 */
template <
    typename    T,
    typename    OffsetT = ptrdiff_t>
class TexObjInputIterator
{
public:

    // Required iterator traits
    typedef TexObjInputIterator                 self_type;              ///< My own type
    typedef OffsetT                             difference_type;        ///< Type to express the result of subtracting one iterator from another
    typedef T                                   value_type;             ///< The type of the element the iterator can point to
    typedef T*                                  pointer;                ///< The type of a pointer to an element the iterator can point to
    typedef T                                   reference;              ///< The type of a reference to an element the iterator can point to

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

    // Largest texture word we can use in device
    typedef typename UnitWord<T>::TextureWord TextureWord;

    // Number of texture words per T
    enum {
        TEXTURE_MULTIPLE = sizeof(T) / sizeof(TextureWord)
    };

private:

    T*                  ptr;
    difference_type     tex_offset;
    cudaTextureObject_t tex_obj;

public:

    /// Constructor
    __host__ __device__ __forceinline__ TexObjInputIterator()
    :
        ptr(NULL),
        tex_offset(0),
        tex_obj(0)
    {}

    /// Use this iterator to bind \p ptr with a texture reference
    template <typename QualifiedT>
    cudaError_t BindTexture(
        QualifiedT      *ptr,               ///< Native pointer to wrap that is aligned to cudaDeviceProp::textureAlignment
        size_t          bytes = size_t(-1),         ///< Number of bytes in the range
        size_t          tex_offset = 0)     ///< OffsetT (in items) from \p ptr denoting the position of the iterator
    {
        this->ptr = const_cast<typename RemoveQualifiers<QualifiedT>::Type *>(ptr);
        this->tex_offset = tex_offset;

        cudaChannelFormatDesc   channel_desc = cudaCreateChannelDesc<TextureWord>();
        cudaResourceDesc        res_desc;
        cudaTextureDesc         tex_desc;
        memset(&res_desc, 0, sizeof(cudaResourceDesc));
        memset(&tex_desc, 0, sizeof(cudaTextureDesc));
        res_desc.resType                = cudaResourceTypeLinear;
        res_desc.res.linear.devPtr      = this->ptr;
        res_desc.res.linear.desc        = channel_desc;
        res_desc.res.linear.sizeInBytes = bytes;
        tex_desc.readMode               = cudaReadModeElementType;
        return cudaCreateTextureObject(&tex_obj, &res_desc, &tex_desc, NULL);
    }

    /// Unbind this iterator from its texture reference
    cudaError_t UnbindTexture()
    {
        return cudaDestroyTextureObject(tex_obj);
    }

    /// Postfix increment
    __host__ __device__ __forceinline__ self_type operator++(int)
    {
        self_type retval = *this;
        tex_offset++;
        return retval;
    }

    /// Prefix increment
    __host__ __device__ __forceinline__ self_type operator++()
    {
        tex_offset++;
        return *this;
    }

    /// Indirection
    __host__ __device__ __forceinline__ reference operator*() const
    {
#if (CUB_PTX_ARCH == 0)
        // Simply dereference the pointer on the host
        return ptr[tex_offset];
#else
        // Move array of uninitialized words, then alias and assign to return value
        TextureWord words[TEXTURE_MULTIPLE];

        #pragma unroll
        for (int i = 0; i < TEXTURE_MULTIPLE; ++i)
        {
            words[i] = tex1Dfetch<TextureWord>(
                tex_obj,
                (tex_offset * TEXTURE_MULTIPLE) + i);
        }

        // Load from words
        return *reinterpret_cast<T*>(words);
#endif
    }

    /// Addition
    template <typename Distance>
    __host__ __device__ __forceinline__ self_type operator+(Distance n) const
    {
        self_type retval;
        retval.ptr          = ptr;
        retval.tex_obj      = tex_obj;
        retval.tex_offset   = tex_offset + n;
        return retval;
    }

    /// Addition assignment
    template <typename Distance>
    __host__ __device__ __forceinline__ self_type& operator+=(Distance n)
    {
        tex_offset += n;
        return *this;
    }

    /// Subtraction
    template <typename Distance>
    __host__ __device__ __forceinline__ self_type operator-(Distance n) const
    {
        self_type retval;
        retval.ptr          = ptr;
        retval.tex_obj      = tex_obj;
        retval.tex_offset   = tex_offset - n;
        return retval;
    }

    /// Subtraction assignment
    template <typename Distance>
    __host__ __device__ __forceinline__ self_type& operator-=(Distance n)
    {
        tex_offset -= n;
        return *this;
    }

    /// Distance
    __host__ __device__ __forceinline__ difference_type operator-(self_type other) const
    {
        return tex_offset - other.tex_offset;
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
        return ((ptr == rhs.ptr) && (tex_offset == rhs.tex_offset) && (tex_obj == rhs.tex_obj));
    }

    /// Not equal to
    __host__ __device__ __forceinline__ bool operator!=(const self_type& rhs)
    {
        return ((ptr != rhs.ptr) || (tex_offset != rhs.tex_offset) || (tex_obj != rhs.tex_obj));
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
