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
 * Simple binary operator functor types
 */

/******************************************************************************
 * Simple functor operators
 ******************************************************************************/

#pragma once

#include "../util_macro.cuh"
#include "../util_type.cuh"
#include "../util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {


/**
 * \addtogroup UtilModule
 * @{
 */

/**
 * \brief Default equality functor
 */
struct Equality
{
    /// Boolean equality operator, returns <tt>(a == b)</tt>
    template <typename T>
    __host__ __device__ __forceinline__ bool operator()(const T &a, const T &b) const
    {
        return a == b;
    }
};


/**
 * \brief Default inequality functor
 */
struct Inequality
{
    /// Boolean inequality operator, returns <tt>(a != b)</tt>
    template <typename T>
    __host__ __device__ __forceinline__ bool operator()(const T &a, const T &b) const
    {
        return a != b;
    }
};


/**
 * \brief Inequality functor (wraps equality functor)
 */
template <typename EqualityOp>
struct InequalityWrapper
{
    /// Wrapped equality operator
    EqualityOp op;

    /// Constructor
    __host__ __device__ __forceinline__
    InequalityWrapper(EqualityOp op) : op(op) {}

    /// Boolean inequality operator, returns <tt>(a != b)</tt>
    template <typename T>
    __host__ __device__ __forceinline__ bool operator()(const T &a, const T &b)
    {
        return !op(a, b);
    }
};


/**
 * \brief Default sum functor
 */
struct Sum
{
    /// Boolean sum operator, returns <tt>a + b</tt>
    template <typename T>
    __host__ __device__ __forceinline__ T operator()(const T &a, const T &b) const
    {
        return a + b;
    }
};


/**
 * \brief Default max functor
 */
struct Max
{
    /// Boolean max operator, returns <tt>(a > b) ? a : b</tt>
    template <typename T>
    __host__ __device__ __forceinline__ T operator()(const T &a, const T &b) const
    {
        return CUB_MAX(a, b);
    }
};


/**
 * \brief Arg max functor (keeps the value and offset of the first occurrence of the larger item)
 */
struct ArgMax
{
    /// Boolean max operator, preferring the item having the smaller offset in case of ties
    template <typename T, typename OffsetT>
    __host__ __device__ __forceinline__ KeyValuePair<OffsetT, T> operator()(
        const KeyValuePair<OffsetT, T> &a,
        const KeyValuePair<OffsetT, T> &b) const
    {
// Mooch BUG (device reduce argmax gk110 3.2 million random fp32)
//        return ((b.value > a.value) || ((a.value == b.value) && (b.key < a.key))) ? b : a;

        if ((b.value > a.value) || ((a.value == b.value) && (b.key < a.key)))
            return b;
        return a;
    }
};


/**
 * \brief Default min functor
 */
struct Min
{
    /// Boolean min operator, returns <tt>(a < b) ? a : b</tt>
    template <typename T>
    __host__ __device__ __forceinline__ T operator()(const T &a, const T &b) const
    {
        return CUB_MIN(a, b);
    }
};


/**
 * \brief Arg min functor (keeps the value and offset of the first occurrence of the smallest item)
 */
struct ArgMin
{
    /// Boolean min operator, preferring the item having the smaller offset in case of ties
    template <typename T, typename OffsetT>
    __host__ __device__ __forceinline__ KeyValuePair<OffsetT, T> operator()(
        const KeyValuePair<OffsetT, T> &a,
        const KeyValuePair<OffsetT, T> &b) const
    {
// Mooch BUG (device reduce argmax gk110 3.2 million random fp32)
//        return ((b.value < a.value) || ((a.value == b.value) && (b.key < a.key))) ? b : a;

        if ((b.value < a.value) || ((a.value == b.value) && (b.key < a.key)))
            return b;
        return a;
    }
};


/**
 * \brief Default cast functor
 */
template <typename B>
struct CastOp
{
    /// Cast operator, returns <tt>(B) a</tt>
    template <typename A>
    __host__ __device__ __forceinline__ B operator()(const A &a) const
    {
        return (B) a;
    }
};


/**
 * \brief Binary operator wrapper for switching non-commutative scan arguments
 */
template <typename ScanOp>
class SwizzleScanOp
{
private:

    /// Wrapped scan operator
    ScanOp scan_op;

public:

    /// Constructor
    __host__ __device__ __forceinline__
    SwizzleScanOp(ScanOp scan_op) : scan_op(scan_op) {}

    /// Switch the scan arguments
    template <typename T>
    __host__ __device__ __forceinline__
    T operator()(const T &a, const T &b)
    {
      T _a(a);
      T _b(b);

      return scan_op(_b, _a);
    }
};


/**
 * \brief Reduce-by-segment functor.
 *
 * Given two cub::KeyValuePair inputs \p a and \p b and a
 * binary associative combining operator \p <tt>f(const T &x, const T &y)</tt>,
 * an instance of this functor returns a cub::KeyValuePair whose \p key
 * field is <tt>a.key</tt> + <tt>b.key</tt>, and whose \p value field
 * is either b.value if b.key is non-zero, or f(a.value, b.value) otherwise.
 *
 * ReduceBySegmentOp is an associative, non-commutative binary combining operator
 * for input sequences of cub::KeyValuePair pairings.  Such
 * sequences are typically used to represent a segmented set of values to be reduced
 * and a corresponding set of {0,1}-valued integer "head flags" demarcating the
 * first value of each segment.
 *
 */
template <typename ReductionOpT>    ///< Binary reduction operator to apply to values
struct ReduceBySegmentOp
{
    /// Wrapped reduction operator
    ReductionOpT op;

    /// Constructor
    __host__ __device__ __forceinline__ ReduceBySegmentOp() {}

    /// Constructor
    __host__ __device__ __forceinline__ ReduceBySegmentOp(ReductionOpT op) : op(op) {}

    /// Scan operator
    template <typename KeyValuePairT>       ///< KeyValuePair pairing of T (value) and OffsetT (head flag)
    __host__ __device__ __forceinline__ KeyValuePairT operator()(
        const KeyValuePairT &first,         ///< First partial reduction
        const KeyValuePairT &second)        ///< Second partial reduction
    {
        KeyValuePairT retval;
        retval.key = first.key + second.key;
        retval.value = (second.key) ?
                second.value :                          // The second partial reduction spans a segment reset, so it's value aggregate becomes the running aggregate
                op(first.value, second.value);          // The second partial reduction does not span a reset, so accumulate both into the running aggregate
        return retval;
    }
};



template <typename ReductionOpT>    ///< Binary reduction operator to apply to values
struct ReduceByKeyOp
{
    /// Wrapped reduction operator
    ReductionOpT op;

    /// Constructor
    __host__ __device__ __forceinline__ ReduceByKeyOp() {}

    /// Constructor
    __host__ __device__ __forceinline__ ReduceByKeyOp(ReductionOpT op) : op(op) {}

    /// Scan operator
    template <typename KeyValuePairT>
    __host__ __device__ __forceinline__ KeyValuePairT operator()(
        const KeyValuePairT &first,       ///< First partial reduction
        const KeyValuePairT &second)      ///< Second partial reduction
    {
        KeyValuePairT retval = second;

        if (first.key == second.key)
            retval.value = op(first.value, retval.value);

        return retval;
    }
};







/** @} */       // end group UtilModule


}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)
