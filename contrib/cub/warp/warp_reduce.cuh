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
 * The cub::WarpReduce class provides [<em>collective</em>](index.html#sec0) methods for computing a parallel reduction of items partitioned across a CUDA thread warp.
 */

#pragma once

#include "specializations/warp_reduce_shfl.cuh"
#include "specializations/warp_reduce_smem.cuh"
#include "../thread/thread_operators.cuh"
#include "../util_arch.cuh"
#include "../util_type.cuh"
#include "../util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {


/**
 * \addtogroup WarpModule
 * @{
 */

/**
 * \brief The WarpReduce class provides [<em>collective</em>](index.html#sec0) methods for computing a parallel reduction of items partitioned across a CUDA thread warp. ![](warp_reduce_logo.png)
 *
 * \tparam T                        The reduction input/output element type
 * \tparam LOGICAL_WARP_THREADS     <b>[optional]</b> The number of threads per "logical" warp (may be less than the number of hardware warp threads).  Default is the warp size of the targeted CUDA compute-capability (e.g., 32 threads for SM20).
 * \tparam PTX_ARCH                 <b>[optional]</b> \ptxversion
 *
 * \par Overview
 * - A <a href="http://en.wikipedia.org/wiki/Reduce_(higher-order_function)"><em>reduction</em></a> (or <em>fold</em>)
 *   uses a binary combining operator to compute a single aggregate from a list of input elements.
 * - Supports "logical" warps smaller than the physical warp size (e.g., logical warps of 8 threads)
 * - The number of entrant threads must be an multiple of \p LOGICAL_WARP_THREADS
 *
 * \par Performance Considerations
 * - Uses special instructions when applicable (e.g., warp \p SHFL instructions)
 * - Uses synchronization-free communication between warp lanes when applicable
 * - Incurs zero bank conflicts for most types
 * - Computation is slightly more efficient (i.e., having lower instruction overhead) for:
 *     - Summation (<b><em>vs.</em></b> generic reduction)
 *     - The architecture's warp size is a whole multiple of \p LOGICAL_WARP_THREADS
 *
 * \par Simple Examples
 * \warpcollective{WarpReduce}
 * \par
 * The code snippet below illustrates four concurrent warp sum reductions within a block of
 * 128 threads (one per each of the 32-thread warps).
 * \par
 * \code
 * #include <cub/cub.cuh>
 *
 * __global__ void ExampleKernel(...)
 * {
 *     // Specialize WarpReduce for type int
 *     typedef cub::WarpReduce<int> WarpReduce;
 *
 *     // Allocate WarpReduce shared memory for 4 warps
 *     __shared__ typename WarpReduce::TempStorage temp_storage[4];
 *
 *     // Obtain one input item per thread
 *     int thread_data = ...
 *
 *     // Return the warp-wide sums to each lane0 (threads 0, 32, 64, and 96)
 *     int warp_id = threadIdx.x / 32;
 *     int aggregate = WarpReduce(temp_storage[warp_id]).Sum(thread_data);
 *
 * \endcode
 * \par
 * Suppose the set of input \p thread_data across the block of threads is <tt>{0, 1, 2, 3, ..., 127}</tt>.
 * The corresponding output \p aggregate in threads 0, 32, 64, and 96 will \p 496, \p 1520,
 * \p 2544, and \p 3568, respectively (and is undefined in other threads).
 *
 * \par
 * The code snippet below illustrates a single warp sum reduction within a block of
 * 128 threads.
 * \par
 * \code
 * #include <cub/cub.cuh>
 *
 * __global__ void ExampleKernel(...)
 * {
 *     // Specialize WarpReduce for type int
 *     typedef cub::WarpReduce<int> WarpReduce;
 *
 *     // Allocate WarpReduce shared memory for one warp
 *     __shared__ typename WarpReduce::TempStorage temp_storage;
 *     ...
 *
 *     // Only the first warp performs a reduction
 *     if (threadIdx.x < 32)
 *     {
 *         // Obtain one input item per thread
 *         int thread_data = ...
 *
 *         // Return the warp-wide sum to lane0
 *         int aggregate = WarpReduce(temp_storage).Sum(thread_data);
 *
 * \endcode
 * \par
 * Suppose the set of input \p thread_data across the warp of threads is <tt>{0, 1, 2, 3, ..., 31}</tt>.
 * The corresponding output \p aggregate in thread0 will be \p 496 (and is undefined in other threads).
 *
 */
template <
    typename    T,
    int         LOGICAL_WARP_THREADS    = CUB_PTX_WARP_THREADS,
    int         PTX_ARCH                = CUB_PTX_ARCH>
class WarpReduce
{
private:

    /******************************************************************************
     * Constants and type definitions
     ******************************************************************************/

    enum
    {
        /// Whether the logical warp size and the PTX warp size coincide
        IS_ARCH_WARP = (LOGICAL_WARP_THREADS == CUB_WARP_THREADS(PTX_ARCH)),

        /// Whether the logical warp size is a power-of-two
        IS_POW_OF_TWO = PowerOfTwo<LOGICAL_WARP_THREADS>::VALUE,
    };

public:

    #ifndef DOXYGEN_SHOULD_SKIP_THIS    // Do not document

    /// Internal specialization.  Use SHFL-based reduction if (architecture is >= SM30) and (LOGICAL_WARP_THREADS is a power-of-two)
    typedef typename If<(PTX_ARCH >= 300) && (IS_POW_OF_TWO),
        WarpReduceShfl<T, LOGICAL_WARP_THREADS, PTX_ARCH>,
        WarpReduceSmem<T, LOGICAL_WARP_THREADS, PTX_ARCH> >::Type InternalWarpReduce;

    #endif // DOXYGEN_SHOULD_SKIP_THIS


private:

    /// Shared memory storage layout type for WarpReduce
    typedef typename InternalWarpReduce::TempStorage _TempStorage;


    /******************************************************************************
     * Thread fields
     ******************************************************************************/

    /// Shared storage reference
    _TempStorage &temp_storage;


    /******************************************************************************
     * Utility methods
     ******************************************************************************/

public:

    /// \smemstorage{WarpReduce}
    struct TempStorage : Uninitialized<_TempStorage> {};


    /******************************************************************//**
     * \name Collective constructors
     *********************************************************************/
    //@{


    /**
     * \brief Collective constructor using the specified memory allocation as temporary storage.  Logical warp and lane identifiers are constructed from <tt>threadIdx.x</tt>.
     */
    __device__ __forceinline__ WarpReduce(
        TempStorage &temp_storage)             ///< [in] Reference to memory allocation having layout type TempStorage
    :
        temp_storage(temp_storage.Alias())
    {}


    //@}  end member group
    /******************************************************************//**
     * \name Summation reductions
     *********************************************************************/
    //@{


    /**
     * \brief Computes a warp-wide sum in the calling warp.  The output is valid in warp <em>lane</em><sub>0</sub>.
     *
     * \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates four concurrent warp sum reductions within a block of
     * 128 threads (one per each of the 32-thread warps).
     * \par
     * \code
     * #include <cub/cub.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize WarpReduce for type int
     *     typedef cub::WarpReduce<int> WarpReduce;
     *
     *     // Allocate WarpReduce shared memory for 4 warps
     *     __shared__ typename WarpReduce::TempStorage temp_storage[4];
     *
     *     // Obtain one input item per thread
     *     int thread_data = ...
     *
     *     // Return the warp-wide sums to each lane0
     *     int warp_id = threadIdx.x / 32;
     *     int aggregate = WarpReduce(temp_storage[warp_id]).Sum(thread_data);
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data across the block of threads is <tt>{0, 1, 2, 3, ..., 127}</tt>.
     * The corresponding output \p aggregate in threads 0, 32, 64, and 96 will \p 496, \p 1520,
     * \p 2544, and \p 3568, respectively (and is undefined in other threads).
     *
     */
    __device__ __forceinline__ T Sum(
        T                   input)              ///< [in] Calling thread's input
    {
        return InternalWarpReduce(temp_storage).template Reduce<true, 1>(input, LOGICAL_WARP_THREADS, cub::Sum());
    }

    /**
     * \brief Computes a partially-full warp-wide sum in the calling warp.  The output is valid in warp <em>lane</em><sub>0</sub>.
     *
     * All threads across the calling warp must agree on the same value for \p valid_items.  Otherwise the result is undefined.
     *
     * \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates a sum reduction within a single, partially-full
     * block of 32 threads (one warp).
     * \par
     * \code
     * #include <cub/cub.cuh>
     *
     * __global__ void ExampleKernel(int *d_data, int valid_items)
     * {
     *     // Specialize WarpReduce for type int
     *     typedef cub::WarpReduce<int> WarpReduce;
     *
     *     // Allocate WarpReduce shared memory for one warp
     *     __shared__ typename WarpReduce::TempStorage temp_storage;
     *
     *     // Obtain one input item per thread if in range
     *     int thread_data;
     *     if (threadIdx.x < valid_items)
     *         thread_data = d_data[threadIdx.x];
     *
     *     // Return the warp-wide sums to each lane0
     *     int aggregate = WarpReduce(temp_storage).Sum(
     *         thread_data, valid_items);
     *
     * \endcode
     * \par
     * Suppose the input \p d_data is <tt>{0, 1, 2, 3, 4, ...</tt> and \p valid_items
     * is \p 4.  The corresponding output \p aggregate in thread0 is \p 6 (and is
     * undefined in other threads).
     *
     */
    __device__ __forceinline__ T Sum(
        T                   input,              ///< [in] Calling thread's input
        int                 valid_items)        ///< [in] Total number of valid items in the calling thread's logical warp (may be less than \p LOGICAL_WARP_THREADS)
    {
        // Determine if we don't need bounds checking
        return InternalWarpReduce(temp_storage).template Reduce<false, 1>(input, valid_items, cub::Sum());
    }


    /**
     * \brief Computes a segmented sum in the calling warp where segments are defined by head-flags.  The sum of each segment is returned to the first lane in that segment (which always includes <em>lane</em><sub>0</sub>).
     *
     * \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates a head-segmented warp sum
     * reduction within a block of 32 threads (one warp).
     * \par
     * \code
     * #include <cub/cub.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize WarpReduce for type int
     *     typedef cub::WarpReduce<int> WarpReduce;
     *
     *     // Allocate WarpReduce shared memory for one warp
     *     __shared__ typename WarpReduce::TempStorage temp_storage;
     *
     *     // Obtain one input item and flag per thread
     *     int thread_data = ...
     *     int head_flag = ...
     *
     *     // Return the warp-wide sums to each lane0
     *     int aggregate = WarpReduce(temp_storage).HeadSegmentedSum(
     *         thread_data, head_flag);
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data and \p head_flag across the block of threads
     * is <tt>{0, 1, 2, 3, ..., 31</tt> and is <tt>{1, 0, 0, 0, 1, 0, 0, 0, ..., 1, 0, 0, 0</tt>,
     * respectively.  The corresponding output \p aggregate in threads 0, 4, 8, etc. will be
     * \p 6, \p 22, \p 38, etc. (and is undefined in other threads).
     *
     * \tparam ReductionOp     <b>[inferred]</b> Binary reduction operator type having member <tt>T operator()(const T &a, const T &b)</tt>
     *
     */
    template <
        typename            FlagT>
    __device__ __forceinline__ T HeadSegmentedSum(
        T                   input,              ///< [in] Calling thread's input
        FlagT                head_flag)          ///< [in] Head flag denoting whether or not \p input is the start of a new segment
    {
        return HeadSegmentedReduce(input, head_flag, cub::Sum());
    }


    /**
     * \brief Computes a segmented sum in the calling warp where segments are defined by tail-flags.  The sum of each segment is returned to the first lane in that segment (which always includes <em>lane</em><sub>0</sub>).
     *
     * \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates a tail-segmented warp sum
     * reduction within a block of 32 threads (one warp).
     * \par
     * \code
     * #include <cub/cub.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize WarpReduce for type int
     *     typedef cub::WarpReduce<int> WarpReduce;
     *
     *     // Allocate WarpReduce shared memory for one warp
     *     __shared__ typename WarpReduce::TempStorage temp_storage;
     *
     *     // Obtain one input item and flag per thread
     *     int thread_data = ...
     *     int tail_flag = ...
     *
     *     // Return the warp-wide sums to each lane0
     *     int aggregate = WarpReduce(temp_storage).TailSegmentedSum(
     *         thread_data, tail_flag);
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data and \p tail_flag across the block of threads
     * is <tt>{0, 1, 2, 3, ..., 31</tt> and is <tt>{0, 0, 0, 1, 0, 0, 0, 1, ..., 0, 0, 0, 1</tt>,
     * respectively.  The corresponding output \p aggregate in threads 0, 4, 8, etc. will be
     * \p 6, \p 22, \p 38, etc. (and is undefined in other threads).
     *
     * \tparam ReductionOp     <b>[inferred]</b> Binary reduction operator type having member <tt>T operator()(const T &a, const T &b)</tt>
     */
    template <
        typename            FlagT>
    __device__ __forceinline__ T TailSegmentedSum(
        T                   input,              ///< [in] Calling thread's input
        FlagT                tail_flag)          ///< [in] Head flag denoting whether or not \p input is the start of a new segment
    {
        return TailSegmentedReduce(input, tail_flag, cub::Sum());
    }



    //@}  end member group
    /******************************************************************//**
     * \name Generic reductions
     *********************************************************************/
    //@{

    /**
     * \brief Computes a warp-wide reduction in the calling warp using the specified binary reduction functor.  The output is valid in warp <em>lane</em><sub>0</sub>.
     *
     * Supports non-commutative reduction operators
     *
     * \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates four concurrent warp max reductions within a block of
     * 128 threads (one per each of the 32-thread warps).
     * \par
     * \code
     * #include <cub/cub.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize WarpReduce for type int
     *     typedef cub::WarpReduce<int> WarpReduce;
     *
     *     // Allocate WarpReduce shared memory for 4 warps
     *     __shared__ typename WarpReduce::TempStorage temp_storage[4];
     *
     *     // Obtain one input item per thread
     *     int thread_data = ...
     *
     *     // Return the warp-wide reductions to each lane0
     *     int warp_id = threadIdx.x / 32;
     *     int aggregate = WarpReduce(temp_storage[warp_id]).Reduce(
     *         thread_data, cub::Max());
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data across the block of threads is <tt>{0, 1, 2, 3, ..., 127}</tt>.
     * The corresponding output \p aggregate in threads 0, 32, 64, and 96 will \p 31, \p 63,
     * \p 95, and \p 127, respectively  (and is undefined in other threads).
     *
     * \tparam ReductionOp     <b>[inferred]</b> Binary reduction operator type having member <tt>T operator()(const T &a, const T &b)</tt>
     */
    template <typename ReductionOp>
    __device__ __forceinline__ T Reduce(
        T                   input,              ///< [in] Calling thread's input
        ReductionOp         reduction_op)       ///< [in] Binary reduction operator
    {
        return InternalWarpReduce(temp_storage).template Reduce<true, 1>(input, LOGICAL_WARP_THREADS, reduction_op);
    }

    /**
     * \brief Computes a partially-full warp-wide reduction in the calling warp using the specified binary reduction functor.  The output is valid in warp <em>lane</em><sub>0</sub>.
     *
     * All threads across the calling warp must agree on the same value for \p valid_items.  Otherwise the result is undefined.
     *
     * Supports non-commutative reduction operators
     *
     * \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates a max reduction within a single, partially-full
     * block of 32 threads (one warp).
     * \par
     * \code
     * #include <cub/cub.cuh>
     *
     * __global__ void ExampleKernel(int *d_data, int valid_items)
     * {
     *     // Specialize WarpReduce for type int
     *     typedef cub::WarpReduce<int> WarpReduce;
     *
     *     // Allocate WarpReduce shared memory for one warp
     *     __shared__ typename WarpReduce::TempStorage temp_storage;
     *
     *     // Obtain one input item per thread if in range
     *     int thread_data;
     *     if (threadIdx.x < valid_items)
     *         thread_data = d_data[threadIdx.x];
     *
     *     // Return the warp-wide reductions to each lane0
     *     int aggregate = WarpReduce(temp_storage).Reduce(
     *         thread_data, cub::Max(), valid_items);
     *
     * \endcode
     * \par
     * Suppose the input \p d_data is <tt>{0, 1, 2, 3, 4, ...</tt> and \p valid_items
     * is \p 4.  The corresponding output \p aggregate in thread0 is \p 3 (and is
     * undefined in other threads).
     *
     * \tparam ReductionOp     <b>[inferred]</b> Binary reduction operator type having member <tt>T operator()(const T &a, const T &b)</tt>
     */
    template <typename ReductionOp>
    __device__ __forceinline__ T Reduce(
        T                   input,              ///< [in] Calling thread's input
        ReductionOp         reduction_op,       ///< [in] Binary reduction operator
        int                 valid_items)        ///< [in] Total number of valid items in the calling thread's logical warp (may be less than \p LOGICAL_WARP_THREADS)
    {
        return InternalWarpReduce(temp_storage).template Reduce<false, 1>(input, valid_items, reduction_op);
    }


    /**
     * \brief Computes a segmented reduction in the calling warp where segments are defined by head-flags.  The reduction of each segment is returned to the first lane in that segment (which always includes <em>lane</em><sub>0</sub>).
     *
     * Supports non-commutative reduction operators
     *
     * \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates a head-segmented warp max
     * reduction within a block of 32 threads (one warp).
     * \par
     * \code
     * #include <cub/cub.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize WarpReduce for type int
     *     typedef cub::WarpReduce<int> WarpReduce;
     *
     *     // Allocate WarpReduce shared memory for one warp
     *     __shared__ typename WarpReduce::TempStorage temp_storage;
     *
     *     // Obtain one input item and flag per thread
     *     int thread_data = ...
     *     int head_flag = ...
     *
     *     // Return the warp-wide reductions to each lane0
     *     int aggregate = WarpReduce(temp_storage).HeadSegmentedReduce(
     *         thread_data, head_flag, cub::Max());
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data and \p head_flag across the block of threads
     * is <tt>{0, 1, 2, 3, ..., 31</tt> and is <tt>{1, 0, 0, 0, 1, 0, 0, 0, ..., 1, 0, 0, 0</tt>,
     * respectively.  The corresponding output \p aggregate in threads 0, 4, 8, etc. will be
     * \p 3, \p 7, \p 11, etc. (and is undefined in other threads).
     *
     * \tparam ReductionOp     <b>[inferred]</b> Binary reduction operator type having member <tt>T operator()(const T &a, const T &b)</tt>
     */
    template <
        typename            ReductionOp,
        typename            FlagT>
    __device__ __forceinline__ T HeadSegmentedReduce(
        T                   input,              ///< [in] Calling thread's input
        FlagT                head_flag,          ///< [in] Head flag denoting whether or not \p input is the start of a new segment
        ReductionOp         reduction_op)       ///< [in] Reduction operator
    {
        return InternalWarpReduce(temp_storage).template SegmentedReduce<true>(input, head_flag, reduction_op);
    }


    /**
     * \brief Computes a segmented reduction in the calling warp where segments are defined by tail-flags.  The reduction of each segment is returned to the first lane in that segment (which always includes <em>lane</em><sub>0</sub>).
     *
     * Supports non-commutative reduction operators
     *
     * \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates a tail-segmented warp max
     * reduction within a block of 32 threads (one warp).
     * \par
     * \code
     * #include <cub/cub.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize WarpReduce for type int
     *     typedef cub::WarpReduce<int> WarpReduce;
     *
     *     // Allocate WarpReduce shared memory for one warp
     *     __shared__ typename WarpReduce::TempStorage temp_storage;
     *
     *     // Obtain one input item and flag per thread
     *     int thread_data = ...
     *     int tail_flag = ...
     *
     *     // Return the warp-wide reductions to each lane0
     *     int aggregate = WarpReduce(temp_storage).TailSegmentedReduce(
     *         thread_data, tail_flag, cub::Max());
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data and \p tail_flag across the block of threads
     * is <tt>{0, 1, 2, 3, ..., 31</tt> and is <tt>{0, 0, 0, 1, 0, 0, 0, 1, ..., 0, 0, 0, 1</tt>,
     * respectively.  The corresponding output \p aggregate in threads 0, 4, 8, etc. will be
     * \p 3, \p 7, \p 11, etc. (and is undefined in other threads).
     *
     * \tparam ReductionOp     <b>[inferred]</b> Binary reduction operator type having member <tt>T operator()(const T &a, const T &b)</tt>
     */
    template <
        typename            ReductionOp,
        typename            FlagT>
    __device__ __forceinline__ T TailSegmentedReduce(
        T                   input,              ///< [in] Calling thread's input
        FlagT                tail_flag,          ///< [in] Tail flag denoting whether or not \p input is the end of the current segment
        ReductionOp         reduction_op)       ///< [in] Reduction operator
    {
        return InternalWarpReduce(temp_storage).template SegmentedReduce<false>(input, tail_flag, reduction_op);
    }



    //@}  end member group
};

/** @} */       // end group WarpModule

}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)
