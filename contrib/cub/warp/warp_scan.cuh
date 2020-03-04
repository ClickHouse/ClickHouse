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
 * The cub::WarpScan class provides [<em>collective</em>](index.html#sec0) methods for computing a parallel prefix scan of items partitioned across a CUDA thread warp.
 */

#pragma once

#include "specializations/warp_scan_shfl.cuh"
#include "specializations/warp_scan_smem.cuh"
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
 * \brief The WarpScan class provides [<em>collective</em>](index.html#sec0) methods for computing a parallel prefix scan of items partitioned across a CUDA thread warp.  ![](warp_scan_logo.png)
 *
 * \tparam T                        The scan input/output element type
 * \tparam LOGICAL_WARP_THREADS     <b>[optional]</b> The number of threads per "logical" warp (may be less than the number of hardware warp threads).  Default is the warp size associated with the CUDA Compute Capability targeted by the compiler (e.g., 32 threads for SM20).
 * \tparam PTX_ARCH                 <b>[optional]</b> \ptxversion
 *
 * \par Overview
 * - Given a list of input elements and a binary reduction operator, a [<em>prefix scan</em>](http://en.wikipedia.org/wiki/Prefix_sum)
 *   produces an output list where each element is computed to be the reduction
 *   of the elements occurring earlier in the input list.  <em>Prefix sum</em>
 *   connotes a prefix scan with the addition operator. The term \em inclusive indicates
 *   that the <em>i</em><sup>th</sup> output reduction incorporates the <em>i</em><sup>th</sup> input.
 *   The term \em exclusive indicates the <em>i</em><sup>th</sup> input is not incorporated into
 *   the <em>i</em><sup>th</sup> output reduction.
 * - Supports non-commutative scan operators
 * - Supports "logical" warps smaller than the physical warp size (e.g., a logical warp of 8 threads)
 * - The number of entrant threads must be an multiple of \p LOGICAL_WARP_THREADS
 *
 * \par Performance Considerations
 * - Uses special instructions when applicable (e.g., warp \p SHFL)
 * - Uses synchronization-free communication between warp lanes when applicable
 * - Incurs zero bank conflicts for most types
 * - Computation is slightly more efficient (i.e., having lower instruction overhead) for:
 *     - Summation (<b><em>vs.</em></b> generic scan)
 *     - The architecture's warp size is a whole multiple of \p LOGICAL_WARP_THREADS
 *
 * \par Simple Examples
 * \warpcollective{WarpScan}
 * \par
 * The code snippet below illustrates four concurrent warp prefix sums within a block of
 * 128 threads (one per each of the 32-thread warps).
 * \par
 * \code
 * #include <cub/cub.cuh>
 *
 * __global__ void ExampleKernel(...)
 * {
 *     // Specialize WarpScan for type int
 *     typedef cub::WarpScan<int> WarpScan;
 *
 *     // Allocate WarpScan shared memory for 4 warps
 *     __shared__ typename WarpScan::TempStorage temp_storage[4];
 *
 *     // Obtain one input item per thread
 *     int thread_data = ...
 *
 *     // Compute warp-wide prefix sums
 *     int warp_id = threadIdx.x / 32;
 *     WarpScan(temp_storage[warp_id]).ExclusiveSum(thread_data, thread_data);
 *
 * \endcode
 * \par
 * Suppose the set of input \p thread_data across the block of threads is <tt>{1, 1, 1, 1, ...}</tt>.
 * The corresponding output \p thread_data in each of the four warps of threads will be
 * <tt>0, 1, 2, 3, ..., 31}</tt>.
 *
 * \par
 * The code snippet below illustrates a single warp prefix sum within a block of
 * 128 threads.
 * \par
 * \code
 * #include <cub/cub.cuh>
 *
 * __global__ void ExampleKernel(...)
 * {
 *     // Specialize WarpScan for type int
 *     typedef cub::WarpScan<int> WarpScan;
 *
 *     // Allocate WarpScan shared memory for one warp
 *     __shared__ typename WarpScan::TempStorage temp_storage;
 *     ...
 *
 *     // Only the first warp performs a prefix sum
 *     if (threadIdx.x < 32)
 *     {
 *         // Obtain one input item per thread
 *         int thread_data = ...
 *
 *         // Compute warp-wide prefix sums
 *         WarpScan(temp_storage).ExclusiveSum(thread_data, thread_data);
 *
 * \endcode
 * \par
 * Suppose the set of input \p thread_data across the warp of threads is <tt>{1, 1, 1, 1, ...}</tt>.
 * The corresponding output \p thread_data will be <tt>{0, 1, 2, 3, ..., 31}</tt>.
 *
 */
template <
    typename    T,
    int         LOGICAL_WARP_THREADS    = CUB_PTX_WARP_THREADS,
    int         PTX_ARCH                = CUB_PTX_ARCH>
class WarpScan
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
        IS_POW_OF_TWO = ((LOGICAL_WARP_THREADS & (LOGICAL_WARP_THREADS - 1)) == 0),

        /// Whether the data type is an integer (which has fully-associative addition)
        IS_INTEGER = ((Traits<T>::CATEGORY == SIGNED_INTEGER) || (Traits<T>::CATEGORY == UNSIGNED_INTEGER))
    };

    /// Internal specialization.  Use SHFL-based scan if (architecture is >= SM30) and (LOGICAL_WARP_THREADS is a power-of-two)
    typedef typename If<(PTX_ARCH >= 300) && (IS_POW_OF_TWO),
        WarpScanShfl<T, LOGICAL_WARP_THREADS, PTX_ARCH>,
        WarpScanSmem<T, LOGICAL_WARP_THREADS, PTX_ARCH> >::Type InternalWarpScan;

    /// Shared memory storage layout type for WarpScan
    typedef typename InternalWarpScan::TempStorage _TempStorage;


    /******************************************************************************
     * Thread fields
     ******************************************************************************/

    /// Shared storage reference
    _TempStorage    &temp_storage;
    unsigned int    lane_id;



    /******************************************************************************
     * Public types
     ******************************************************************************/

public:

    /// \smemstorage{WarpScan}
    struct TempStorage : Uninitialized<_TempStorage> {};


    /******************************************************************//**
     * \name Collective constructors
     *********************************************************************/
    //@{

    /**
     * \brief Collective constructor using the specified memory allocation as temporary storage.  Logical warp and lane identifiers are constructed from <tt>threadIdx.x</tt>.
     */
    __device__ __forceinline__ WarpScan(
        TempStorage &temp_storage)             ///< [in] Reference to memory allocation having layout type TempStorage
    :
        temp_storage(temp_storage.Alias()),
        lane_id(IS_ARCH_WARP ?
            LaneId() :
            LaneId() % LOGICAL_WARP_THREADS)
    {}


    //@}  end member group
    /******************************************************************//**
     * \name Inclusive prefix sums
     *********************************************************************/
    //@{


    /**
     * \brief Computes an inclusive prefix sum across the calling warp.
     *
     * \par
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates four concurrent warp-wide inclusive prefix sums within a block of
     * 128 threads (one per each of the 32-thread warps).
     * \par
     * \code
     * #include <cub/cub.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize WarpScan for type int
     *     typedef cub::WarpScan<int> WarpScan;
     *
     *     // Allocate WarpScan shared memory for 4 warps
     *     __shared__ typename WarpScan::TempStorage temp_storage[4];
     *
     *     // Obtain one input item per thread
     *     int thread_data = ...
     *
     *     // Compute inclusive warp-wide prefix sums
     *     int warp_id = threadIdx.x / 32;
     *     WarpScan(temp_storage[warp_id]).InclusiveSum(thread_data, thread_data);
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data across the block of threads is <tt>{1, 1, 1, 1, ...}</tt>.
     * The corresponding output \p thread_data in each of the four warps of threads will be
     * <tt>1, 2, 3, ..., 32}</tt>.
     */
    __device__ __forceinline__ void InclusiveSum(
        T               input,              ///< [in] Calling thread's input item.
        T               &inclusive_output)  ///< [out] Calling thread's output item.  May be aliased with \p input.
    {
        InclusiveScan(input, inclusive_output, cub::Sum());
    }


    /**
     * \brief Computes an inclusive prefix sum across the calling warp.  Also provides every thread with the warp-wide \p warp_aggregate of all inputs.
     *
     * \par
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates four concurrent warp-wide inclusive prefix sums within a block of
     * 128 threads (one per each of the 32-thread warps).
     * \par
     * \code
     * #include <cub/cub.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize WarpScan for type int
     *     typedef cub::WarpScan<int> WarpScan;
     *
     *     // Allocate WarpScan shared memory for 4 warps
     *     __shared__ typename WarpScan::TempStorage temp_storage[4];
     *
     *     // Obtain one input item per thread
     *     int thread_data = ...
     *
     *     // Compute inclusive warp-wide prefix sums
     *     int warp_aggregate;
     *     int warp_id = threadIdx.x / 32;
     *     WarpScan(temp_storage[warp_id]).InclusiveSum(thread_data, thread_data, warp_aggregate);
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data across the block of threads is <tt>{1, 1, 1, 1, ...}</tt>.
     * The corresponding output \p thread_data in each of the four warps of threads will be
     * <tt>1, 2, 3, ..., 32}</tt>.  Furthermore, \p warp_aggregate for all threads in all warps will be \p 32.
     */
    __device__ __forceinline__ void InclusiveSum(
        T               input,              ///< [in] Calling thread's input item.
        T               &inclusive_output,  ///< [out] Calling thread's output item.  May be aliased with \p input.
        T               &warp_aggregate)    ///< [out] Warp-wide aggregate reduction of input items.
    {
        InclusiveScan(input, inclusive_output, cub::Sum(), warp_aggregate);
    }


    //@}  end member group
    /******************************************************************//**
     * \name Exclusive prefix sums
     *********************************************************************/
    //@{


    /**
     * \brief Computes an exclusive prefix sum across the calling warp.  The value of 0 is applied as the initial value, and is assigned to \p exclusive_output in <em>thread</em><sub>0</sub>.
     *
     * \par
     *  - \identityzero
     *  - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates four concurrent warp-wide exclusive prefix sums within a block of
     * 128 threads (one per each of the 32-thread warps).
     * \par
     * \code
     * #include <cub/cub.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize WarpScan for type int
     *     typedef cub::WarpScan<int> WarpScan;
     *
     *     // Allocate WarpScan shared memory for 4 warps
     *     __shared__ typename WarpScan::TempStorage temp_storage[4];
     *
     *     // Obtain one input item per thread
     *     int thread_data = ...
     *
     *     // Compute exclusive warp-wide prefix sums
     *     int warp_id = threadIdx.x / 32;
     *     WarpScan(temp_storage[warp_id]).ExclusiveSum(thread_data, thread_data);
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data across the block of threads is <tt>{1, 1, 1, 1, ...}</tt>.
     * The corresponding output \p thread_data in each of the four warps of threads will be
     * <tt>0, 1, 2, ..., 31}</tt>.
     *
     */
    __device__ __forceinline__ void ExclusiveSum(
        T               input,              ///< [in] Calling thread's input item.
        T               &exclusive_output)  ///< [out] Calling thread's output item.  May be aliased with \p input.
    {
        T initial_value = 0;
        ExclusiveScan(input, exclusive_output, initial_value, cub::Sum());
    }


    /**
     * \brief Computes an exclusive prefix sum across the calling warp.  The value of 0 is applied as the initial value, and is assigned to \p exclusive_output in <em>thread</em><sub>0</sub>.  Also provides every thread with the warp-wide \p warp_aggregate of all inputs.
     *
     * \par
     *  - \identityzero
     *  - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates four concurrent warp-wide exclusive prefix sums within a block of
     * 128 threads (one per each of the 32-thread warps).
     * \par
     * \code
     * #include <cub/cub.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize WarpScan for type int
     *     typedef cub::WarpScan<int> WarpScan;
     *
     *     // Allocate WarpScan shared memory for 4 warps
     *     __shared__ typename WarpScan::TempStorage temp_storage[4];
     *
     *     // Obtain one input item per thread
     *     int thread_data = ...
     *
     *     // Compute exclusive warp-wide prefix sums
     *     int warp_aggregate;
     *     int warp_id = threadIdx.x / 32;
     *     WarpScan(temp_storage[warp_id]).ExclusiveSum(thread_data, thread_data, warp_aggregate);
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data across the block of threads is <tt>{1, 1, 1, 1, ...}</tt>.
     * The corresponding output \p thread_data in each of the four warps of threads will be
     * <tt>0, 1, 2, ..., 31}</tt>.  Furthermore, \p warp_aggregate for all threads in all warps will be \p 32.
     */
    __device__ __forceinline__ void ExclusiveSum(
        T               input,              ///< [in] Calling thread's input item.
        T               &exclusive_output,  ///< [out] Calling thread's output item.  May be aliased with \p input.
        T               &warp_aggregate)    ///< [out] Warp-wide aggregate reduction of input items.
    {
        T initial_value = 0;
        ExclusiveScan(input, exclusive_output, initial_value, cub::Sum(), warp_aggregate);
    }


    //@}  end member group
    /******************************************************************//**
     * \name Inclusive prefix scans
     *********************************************************************/
    //@{

    /**
     * \brief Computes an inclusive prefix scan using the specified binary scan functor across the calling warp.
     *
     * \par
     *  - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates four concurrent warp-wide inclusive prefix max scans within a block of
     * 128 threads (one per each of the 32-thread warps).
     * \par
     * \code
     * #include <cub/cub.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize WarpScan for type int
     *     typedef cub::WarpScan<int> WarpScan;
     *
     *     // Allocate WarpScan shared memory for 4 warps
     *     __shared__ typename WarpScan::TempStorage temp_storage[4];
     *
     *     // Obtain one input item per thread
     *     int thread_data = ...
     *
     *     // Compute inclusive warp-wide prefix max scans
     *     int warp_id = threadIdx.x / 32;
     *     WarpScan(temp_storage[warp_id]).InclusiveScan(thread_data, thread_data, cub::Max());
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data across the block of threads is <tt>{0, -1, 2, -3, ..., 126, -127}</tt>.
     * The corresponding output \p thread_data in the first warp would be
     * <tt>0, 0, 2, 2, ..., 30, 30</tt>, the output for the second warp would be <tt>32, 32, 34, 34, ..., 62, 62</tt>, etc.
     *
     * \tparam ScanOp     <b>[inferred]</b> Binary scan operator type having member <tt>T operator()(const T &a, const T &b)</tt>
     */
    template <typename ScanOp>
    __device__ __forceinline__ void InclusiveScan(
        T               input,              ///< [in] Calling thread's input item.
        T               &inclusive_output,  ///< [out] Calling thread's output item.  May be aliased with \p input.
        ScanOp          scan_op)            ///< [in] Binary scan operator
    {
        InternalWarpScan(temp_storage).InclusiveScan(input, inclusive_output, scan_op);
    }


    /**
     * \brief Computes an inclusive prefix scan using the specified binary scan functor across the calling warp.  Also provides every thread with the warp-wide \p warp_aggregate of all inputs.
     *
     * \par
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates four concurrent warp-wide inclusive prefix max scans within a block of
     * 128 threads (one per each of the 32-thread warps).
     * \par
     * \code
     * #include <cub/cub.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize WarpScan for type int
     *     typedef cub::WarpScan<int> WarpScan;
     *
     *     // Allocate WarpScan shared memory for 4 warps
     *     __shared__ typename WarpScan::TempStorage temp_storage[4];
     *
     *     // Obtain one input item per thread
     *     int thread_data = ...
     *
     *     // Compute inclusive warp-wide prefix max scans
     *     int warp_aggregate;
     *     int warp_id = threadIdx.x / 32;
     *     WarpScan(temp_storage[warp_id]).InclusiveScan(
     *         thread_data, thread_data, cub::Max(), warp_aggregate);
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data across the block of threads is <tt>{0, -1, 2, -3, ..., 126, -127}</tt>.
     * The corresponding output \p thread_data in the first warp would be
     * <tt>0, 0, 2, 2, ..., 30, 30</tt>, the output for the second warp would be <tt>32, 32, 34, 34, ..., 62, 62</tt>, etc.
     * Furthermore, \p warp_aggregate would be assigned \p 30 for threads in the first warp, \p 62 for threads
     * in the second warp, etc.
     *
     * \tparam ScanOp     <b>[inferred]</b> Binary scan operator type having member <tt>T operator()(const T &a, const T &b)</tt>
     */
    template <typename ScanOp>
    __device__ __forceinline__ void InclusiveScan(
        T               input,              ///< [in] Calling thread's input item.
        T               &inclusive_output,  ///< [out] Calling thread's output item.  May be aliased with \p input.
        ScanOp          scan_op,            ///< [in] Binary scan operator
        T               &warp_aggregate)    ///< [out] Warp-wide aggregate reduction of input items.
    {
        InternalWarpScan(temp_storage).InclusiveScan(input, inclusive_output, scan_op, warp_aggregate);
    }


    //@}  end member group
    /******************************************************************//**
     * \name Exclusive prefix scans
     *********************************************************************/
    //@{

    /**
     * \brief Computes an exclusive prefix scan using the specified binary scan functor across the calling warp.  Because no initial value is supplied, the \p output computed for <em>warp-lane</em><sub>0</sub> is undefined.
     *
     * \par
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates four concurrent warp-wide exclusive prefix max scans within a block of
     * 128 threads (one per each of the 32-thread warps).
     * \par
     * \code
     * #include <cub/cub.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize WarpScan for type int
     *     typedef cub::WarpScan<int> WarpScan;
     *
     *     // Allocate WarpScan shared memory for 4 warps
     *     __shared__ typename WarpScan::TempStorage temp_storage[4];
     *
     *     // Obtain one input item per thread
     *     int thread_data = ...
     *
     *     // Compute exclusive warp-wide prefix max scans
     *     int warp_id = threadIdx.x / 32;
     *     WarpScan(temp_storage[warp_id]).ExclusiveScan(thread_data, thread_data, cub::Max());
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data across the block of threads is <tt>{0, -1, 2, -3, ..., 126, -127}</tt>.
     * The corresponding output \p thread_data in the first warp would be
     * <tt>?, 0, 0, 2, ..., 28, 30</tt>, the output for the second warp would be <tt>?, 32, 32, 34, ..., 60, 62</tt>, etc.
     * (The output \p thread_data in warp lane<sub>0</sub> is undefined.)
     *
     * \tparam ScanOp     <b>[inferred]</b> Binary scan operator type having member <tt>T operator()(const T &a, const T &b)</tt>
     */
    template <typename ScanOp>
    __device__ __forceinline__ void ExclusiveScan(
        T               input,              ///< [in] Calling thread's input item.
        T               &exclusive_output,  ///< [out] Calling thread's output item.  May be aliased with \p input.
        ScanOp          scan_op)            ///< [in] Binary scan operator
    {
        InternalWarpScan internal(temp_storage);

        T inclusive_output;
        internal.InclusiveScan(input, inclusive_output, scan_op);

        internal.Update(
            input,
            inclusive_output,
            exclusive_output,
            scan_op,
            Int2Type<IS_INTEGER>());
    }


    /**
     * \brief Computes an exclusive prefix scan using the specified binary scan functor across the calling warp.
     *
     * \par
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates four concurrent warp-wide exclusive prefix max scans within a block of
     * 128 threads (one per each of the 32-thread warps).
     * \par
     * \code
     * #include <cub/cub.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize WarpScan for type int
     *     typedef cub::WarpScan<int> WarpScan;
     *
     *     // Allocate WarpScan shared memory for 4 warps
     *     __shared__ typename WarpScan::TempStorage temp_storage[4];
     *
     *     // Obtain one input item per thread
     *     int thread_data = ...
     *
     *     // Compute exclusive warp-wide prefix max scans
     *     int warp_id = threadIdx.x / 32;
     *     WarpScan(temp_storage[warp_id]).ExclusiveScan(thread_data, thread_data, INT_MIN, cub::Max());
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data across the block of threads is <tt>{0, -1, 2, -3, ..., 126, -127}</tt>.
     * The corresponding output \p thread_data in the first warp would be
     * <tt>INT_MIN, 0, 0, 2, ..., 28, 30</tt>, the output for the second warp would be <tt>30, 32, 32, 34, ..., 60, 62</tt>, etc.
     *
     * \tparam ScanOp     <b>[inferred]</b> Binary scan operator type having member <tt>T operator()(const T &a, const T &b)</tt>
     */
    template <typename ScanOp>
    __device__ __forceinline__ void ExclusiveScan(
        T               input,              ///< [in] Calling thread's input item.
        T               &exclusive_output,  ///< [out] Calling thread's output item.  May be aliased with \p input.
        T               initial_value,      ///< [in] Initial value to seed the exclusive scan
        ScanOp          scan_op)            ///< [in] Binary scan operator
    {
        InternalWarpScan internal(temp_storage);

        T inclusive_output;
        internal.InclusiveScan(input, inclusive_output, scan_op);

        internal.Update(
            input,
            inclusive_output,
            exclusive_output,
            scan_op,
            initial_value,
            Int2Type<IS_INTEGER>());
    }


    /**
     * \brief Computes an exclusive prefix scan using the specified binary scan functor across the calling warp.  Because no initial value is supplied, the \p output computed for <em>warp-lane</em><sub>0</sub> is undefined.  Also provides every thread with the warp-wide \p warp_aggregate of all inputs.
     *
     * \par
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates four concurrent warp-wide exclusive prefix max scans within a block of
     * 128 threads (one per each of the 32-thread warps).
     * \par
     * \code
     * #include <cub/cub.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize WarpScan for type int
     *     typedef cub::WarpScan<int> WarpScan;
     *
     *     // Allocate WarpScan shared memory for 4 warps
     *     __shared__ typename WarpScan::TempStorage temp_storage[4];
     *
     *     // Obtain one input item per thread
     *     int thread_data = ...
     *
     *     // Compute exclusive warp-wide prefix max scans
     *     int warp_aggregate;
     *     int warp_id = threadIdx.x / 32;
     *     WarpScan(temp_storage[warp_id]).ExclusiveScan(thread_data, thread_data, cub::Max(), warp_aggregate);
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data across the block of threads is <tt>{0, -1, 2, -3, ..., 126, -127}</tt>.
     * The corresponding output \p thread_data in the first warp would be
     * <tt>?, 0, 0, 2, ..., 28, 30</tt>, the output for the second warp would be <tt>?, 32, 32, 34, ..., 60, 62</tt>, etc.
     * (The output \p thread_data in warp lane<sub>0</sub> is undefined.)  Furthermore, \p warp_aggregate would be assigned \p 30 for threads in the first warp, \p 62 for threads
     * in the second warp, etc.
     *
     * \tparam ScanOp     <b>[inferred]</b> Binary scan operator type having member <tt>T operator()(const T &a, const T &b)</tt>
     */
    template <typename ScanOp>
    __device__ __forceinline__ void ExclusiveScan(
        T               input,              ///< [in] Calling thread's input item.
        T               &exclusive_output,   ///< [out] Calling thread's output item.  May be aliased with \p input.
        ScanOp          scan_op,            ///< [in] Binary scan operator
        T               &warp_aggregate)    ///< [out] Warp-wide aggregate reduction of input items.
    {
        InternalWarpScan internal(temp_storage);

        T inclusive_output;
        internal.InclusiveScan(input, inclusive_output, scan_op);

        internal.Update(
            input,
            inclusive_output,
            exclusive_output,
            warp_aggregate,
            scan_op,
            Int2Type<IS_INTEGER>());
    }


    /**
     * \brief Computes an exclusive prefix scan using the specified binary scan functor across the calling warp.  Also provides every thread with the warp-wide \p warp_aggregate of all inputs.
     *
     * \par
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates four concurrent warp-wide exclusive prefix max scans within a block of
     * 128 threads (one per each of the 32-thread warps).
     * \par
     * \code
     * #include <cub/cub.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize WarpScan for type int
     *     typedef cub::WarpScan<int> WarpScan;
     *
     *     // Allocate WarpScan shared memory for 4 warps
     *     __shared__ typename WarpScan::TempStorage temp_storage[4];
     *
     *     // Obtain one input item per thread
     *     int thread_data = ...
     *
     *     // Compute exclusive warp-wide prefix max scans
     *     int warp_aggregate;
     *     int warp_id = threadIdx.x / 32;
     *     WarpScan(temp_storage[warp_id]).ExclusiveScan(thread_data, thread_data, INT_MIN, cub::Max(), warp_aggregate);
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data across the block of threads is <tt>{0, -1, 2, -3, ..., 126, -127}</tt>.
     * The corresponding output \p thread_data in the first warp would be
     * <tt>INT_MIN, 0, 0, 2, ..., 28, 30</tt>, the output for the second warp would be <tt>30, 32, 32, 34, ..., 60, 62</tt>, etc.
     * Furthermore, \p warp_aggregate would be assigned \p 30 for threads in the first warp, \p 62 for threads
     * in the second warp, etc.
     *
     * \tparam ScanOp     <b>[inferred]</b> Binary scan operator type having member <tt>T operator()(const T &a, const T &b)</tt>
     */
    template <typename ScanOp>
    __device__ __forceinline__ void ExclusiveScan(
        T               input,              ///< [in] Calling thread's input item.
        T               &exclusive_output,  ///< [out] Calling thread's output item.  May be aliased with \p input.
        T               initial_value,      ///< [in] Initial value to seed the exclusive scan
        ScanOp          scan_op,            ///< [in] Binary scan operator
        T               &warp_aggregate)    ///< [out] Warp-wide aggregate reduction of input items.
    {
        InternalWarpScan internal(temp_storage);

        T inclusive_output;
        internal.InclusiveScan(input, inclusive_output, scan_op);

        internal.Update(
            input,
            inclusive_output,
            exclusive_output,
            warp_aggregate,
            scan_op,
            initial_value,
            Int2Type<IS_INTEGER>());
    }


    //@}  end member group
    /******************************************************************//**
     * \name Combination (inclusive & exclusive) prefix scans
     *********************************************************************/
    //@{


    /**
     * \brief Computes both inclusive and exclusive prefix scans using the specified binary scan functor across the calling warp.  Because no initial value is supplied, the \p exclusive_output computed for <em>warp-lane</em><sub>0</sub> is undefined.
     *
     * \par
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates four concurrent warp-wide exclusive prefix max scans within a block of
     * 128 threads (one per each of the 32-thread warps).
     * \par
     * \code
     * #include <cub/cub.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize WarpScan for type int
     *     typedef cub::WarpScan<int> WarpScan;
     *
     *     // Allocate WarpScan shared memory for 4 warps
     *     __shared__ typename WarpScan::TempStorage temp_storage[4];
     *
     *     // Obtain one input item per thread
     *     int thread_data = ...
     *
     *     // Compute exclusive warp-wide prefix max scans
     *     int inclusive_partial, exclusive_partial;
     *     WarpScan(temp_storage[warp_id]).Scan(thread_data, inclusive_partial, exclusive_partial, cub::Max());
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data across the block of threads is <tt>{0, -1, 2, -3, ..., 126, -127}</tt>.
     * The corresponding output \p inclusive_partial in the first warp would be
     * <tt>0, 0, 2, 2, ..., 30, 30</tt>, the output for the second warp would be <tt>32, 32, 34, 34, ..., 62, 62</tt>, etc.
     * The corresponding output \p exclusive_partial in the first warp would be
     * <tt>?, 0, 0, 2, ..., 28, 30</tt>, the output for the second warp would be <tt>?, 32, 32, 34, ..., 60, 62</tt>, etc.
     * (The output \p thread_data in warp lane<sub>0</sub> is undefined.)
     *
     * \tparam ScanOp     <b>[inferred]</b> Binary scan operator type having member <tt>T operator()(const T &a, const T &b)</tt>
     */
    template <typename ScanOp>
    __device__ __forceinline__ void Scan(
        T               input,              ///< [in] Calling thread's input item.
        T               &inclusive_output,  ///< [out] Calling thread's inclusive-scan output item.
        T               &exclusive_output,  ///< [out] Calling thread's exclusive-scan output item.
        ScanOp          scan_op)            ///< [in] Binary scan operator
    {
        InternalWarpScan internal(temp_storage);

        internal.InclusiveScan(input, inclusive_output, scan_op);

        internal.Update(
            input,
            inclusive_output,
            exclusive_output,
            scan_op,
            Int2Type<IS_INTEGER>());
    }


    /**
     * \brief Computes both inclusive and exclusive prefix scans using the specified binary scan functor across the calling warp.
     *
     * \par
     *  - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates four concurrent warp-wide prefix max scans within a block of
     * 128 threads (one per each of the 32-thread warps).
     * \par
     * \code
     * #include <cub/cub.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize WarpScan for type int
     *     typedef cub::WarpScan<int> WarpScan;
     *
     *     // Allocate WarpScan shared memory for 4 warps
     *     __shared__ typename WarpScan::TempStorage temp_storage[4];
     *
     *     // Obtain one input item per thread
     *     int thread_data = ...
     *
     *     // Compute inclusive warp-wide prefix max scans
     *     int warp_id = threadIdx.x / 32;
     *     int inclusive_partial, exclusive_partial;
     *     WarpScan(temp_storage[warp_id]).Scan(thread_data, inclusive_partial, exclusive_partial, INT_MIN, cub::Max());
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data across the block of threads is <tt>{0, -1, 2, -3, ..., 126, -127}</tt>.
     * The corresponding output \p inclusive_partial in the first warp would be
     * <tt>0, 0, 2, 2, ..., 30, 30</tt>, the output for the second warp would be <tt>32, 32, 34, 34, ..., 62, 62</tt>, etc.
     * The corresponding output \p exclusive_partial in the first warp would be
     * <tt>INT_MIN, 0, 0, 2, ..., 28, 30</tt>, the output for the second warp would be <tt>30, 32, 32, 34, ..., 60, 62</tt>, etc.
     *
     * \tparam ScanOp     <b>[inferred]</b> Binary scan operator type having member <tt>T operator()(const T &a, const T &b)</tt>
     */
    template <typename ScanOp>
    __device__ __forceinline__ void Scan(
        T               input,              ///< [in] Calling thread's input item.
        T               &inclusive_output,  ///< [out] Calling thread's inclusive-scan output item.
        T               &exclusive_output,  ///< [out] Calling thread's exclusive-scan output item.
        T               initial_value,      ///< [in] Initial value to seed the exclusive scan
        ScanOp          scan_op)            ///< [in] Binary scan operator
    {
        InternalWarpScan internal(temp_storage);

        internal.InclusiveScan(input, inclusive_output, scan_op);

        internal.Update(
            input,
            inclusive_output,
            exclusive_output,
            scan_op,
            initial_value,
            Int2Type<IS_INTEGER>());
    }



    //@}  end member group
    /******************************************************************//**
     * \name Data exchange
     *********************************************************************/
    //@{

    /**
     * \brief Broadcast the value \p input from <em>warp-lane</em><sub><tt>src_lane</tt></sub> to all lanes in the warp
     *
     * \par
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates the warp-wide broadcasts of values from
     * lanes<sub>0</sub> in each of four warps to all other threads in those warps.
     * \par
     * \code
     * #include <cub/cub.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize WarpScan for type int
     *     typedef cub::WarpScan<int> WarpScan;
     *
     *     // Allocate WarpScan shared memory for 4 warps
     *     __shared__ typename WarpScan::TempStorage temp_storage[4];
     *
     *     // Obtain one input item per thread
     *     int thread_data = ...
     *
     *     // Broadcast from lane0 in each warp to all other threads in the warp
     *     int warp_id = threadIdx.x / 32;
     *     thread_data = WarpScan(temp_storage[warp_id]).Broadcast(thread_data, 0);
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data across the block of threads is <tt>{0, 1, 2, 3, ..., 127}</tt>.
     * The corresponding output \p thread_data will be
     * <tt>{0, 0, ..., 0}</tt> in warp<sub>0</sub>,
     * <tt>{32, 32, ..., 32}</tt> in warp<sub>1</sub>,
     * <tt>{64, 64, ..., 64}</tt> in warp<sub>2</sub>, etc.
     */
    __device__ __forceinline__ T Broadcast(
        T               input,              ///< [in] The value to broadcast
        unsigned int    src_lane)           ///< [in] Which warp lane is to do the broadcasting
    {
        return InternalWarpScan(temp_storage).Broadcast(input, src_lane);
    }

    //@}  end member group

};

/** @} */       // end group WarpModule

}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)
