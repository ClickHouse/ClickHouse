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
 * The cub::BlockScan class provides [<em>collective</em>](index.html#sec0) methods for computing a parallel prefix sum/scan of items partitioned across a CUDA thread block.
 */

#pragma once

#include "specializations/block_scan_raking.cuh"
#include "specializations/block_scan_warp_scans.cuh"
#include "../util_arch.cuh"
#include "../util_type.cuh"
#include "../util_ptx.cuh"
#include "../util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {


/******************************************************************************
 * Algorithmic variants
 ******************************************************************************/

/**
 * \brief BlockScanAlgorithm enumerates alternative algorithms for cub::BlockScan to compute a parallel prefix scan across a CUDA thread block.
 */
enum BlockScanAlgorithm
{

    /**
     * \par Overview
     * An efficient "raking reduce-then-scan" prefix scan algorithm.  Execution is comprised of five phases:
     * -# Upsweep sequential reduction in registers (if threads contribute more than one input each).  Each thread then places the partial reduction of its item(s) into shared memory.
     * -# Upsweep sequential reduction in shared memory.  Threads within a single warp rake across segments of shared partial reductions.
     * -# A warp-synchronous Kogge-Stone style exclusive scan within the raking warp.
     * -# Downsweep sequential exclusive scan in shared memory.  Threads within a single warp rake across segments of shared partial reductions, seeded with the warp-scan output.
     * -# Downsweep sequential scan in registers (if threads contribute more than one input), seeded with the raking scan output.
     *
     * \par
     * \image html block_scan_raking.png
     * <div class="centercaption">\p BLOCK_SCAN_RAKING data flow for a hypothetical 16-thread thread block and 4-thread raking warp.</div>
     *
     * \par Performance Considerations
     * - Although this variant may suffer longer turnaround latencies when the
     *   GPU is under-occupied, it can often provide higher overall throughput
     *   across the GPU when suitably occupied.
     */
    BLOCK_SCAN_RAKING,


    /**
     * \par Overview
     * Similar to cub::BLOCK_SCAN_RAKING, but with fewer shared memory reads at
     * the expense of higher register pressure.  Raking threads preserve their
     * "upsweep" segment of values in registers while performing warp-synchronous
     * scan, allowing the "downsweep" not to re-read them from shared memory.
     */
    BLOCK_SCAN_RAKING_MEMOIZE,


    /**
     * \par Overview
     * A quick "tiled warpscans" prefix scan algorithm.  Execution is comprised of four phases:
     * -# Upsweep sequential reduction in registers (if threads contribute more than one input each).  Each thread then places the partial reduction of its item(s) into shared memory.
     * -# Compute a shallow, but inefficient warp-synchronous Kogge-Stone style scan within each warp.
     * -# A propagation phase where the warp scan outputs in each warp are updated with the aggregate from each preceding warp.
     * -# Downsweep sequential scan in registers (if threads contribute more than one input), seeded with the raking scan output.
     *
     * \par
     * \image html block_scan_warpscans.png
     * <div class="centercaption">\p BLOCK_SCAN_WARP_SCANS data flow for a hypothetical 16-thread thread block and 4-thread raking warp.</div>
     *
     * \par Performance Considerations
     * - Although this variant may suffer lower overall throughput across the
     *   GPU because due to a heavy reliance on inefficient warpscans, it can
     *   often provide lower turnaround latencies when the GPU is under-occupied.
     */
    BLOCK_SCAN_WARP_SCANS,
};


/******************************************************************************
 * Block scan
 ******************************************************************************/

/**
 * \brief The BlockScan class provides [<em>collective</em>](index.html#sec0) methods for computing a parallel prefix sum/scan of items partitioned across a CUDA thread block. ![](block_scan_logo.png)
 * \ingroup BlockModule
 *
 * \tparam T                Data type being scanned
 * \tparam BLOCK_DIM_X      The thread block length in threads along the X dimension
 * \tparam ALGORITHM        <b>[optional]</b> cub::BlockScanAlgorithm enumerator specifying the underlying algorithm to use (default: cub::BLOCK_SCAN_RAKING)
 * \tparam BLOCK_DIM_Y      <b>[optional]</b> The thread block length in threads along the Y dimension (default: 1)
 * \tparam BLOCK_DIM_Z      <b>[optional]</b> The thread block length in threads along the Z dimension (default: 1)
 * \tparam PTX_ARCH         <b>[optional]</b> \ptxversion
 *
 * \par Overview
 * - Given a list of input elements and a binary reduction operator, a [<em>prefix scan</em>](http://en.wikipedia.org/wiki/Prefix_sum)
 *   produces an output list where each element is computed to be the reduction
 *   of the elements occurring earlier in the input list.  <em>Prefix sum</em>
 *   connotes a prefix scan with the addition operator. The term \em inclusive indicates
 *   that the <em>i</em><sup>th</sup> output reduction incorporates the <em>i</em><sup>th</sup> input.
 *   The term \em exclusive indicates the <em>i</em><sup>th</sup> input is not incorporated into
 *   the <em>i</em><sup>th</sup> output reduction.
 * - \rowmajor
 * - BlockScan can be optionally specialized by algorithm to accommodate different workload profiles:
 *   -# <b>cub::BLOCK_SCAN_RAKING</b>.  An efficient (high throughput) "raking reduce-then-scan" prefix scan algorithm. [More...](\ref cub::BlockScanAlgorithm)
 *   -# <b>cub::BLOCK_SCAN_RAKING_MEMOIZE</b>.  Similar to cub::BLOCK_SCAN_RAKING, but having higher throughput at the expense of additional register pressure for intermediate storage. [More...](\ref cub::BlockScanAlgorithm)
 *   -# <b>cub::BLOCK_SCAN_WARP_SCANS</b>.  A quick (low latency) "tiled warpscans" prefix scan algorithm. [More...](\ref cub::BlockScanAlgorithm)
 *
 * \par Performance Considerations
 * - \granularity
 * - Uses special instructions when applicable (e.g., warp \p SHFL)
 * - Uses synchronization-free communication between warp lanes when applicable
 * - Invokes a minimal number of minimal block-wide synchronization barriers (only
 *   one or two depending on algorithm selection)
 * - Incurs zero bank conflicts for most types
 * - Computation is slightly more efficient (i.e., having lower instruction overhead) for:
 *   - Prefix sum variants (<b><em>vs.</em></b> generic scan)
 *   - \blocksize
 * - See cub::BlockScanAlgorithm for performance details regarding algorithmic alternatives
 *
 * \par A Simple Example
 * \blockcollective{BlockScan}
 * \par
 * The code snippet below illustrates an exclusive prefix sum of 512 integer items that
 * are partitioned in a [<em>blocked arrangement</em>](index.html#sec5sec3) across 128 threads
 * where each thread owns 4 consecutive items.
 * \par
 * \code
 * #include <cub/cub.cuh>   // or equivalently <cub/block/block_scan.cuh>
 *
 * __global__ void ExampleKernel(...)
 * {
 *     // Specialize BlockScan for a 1D block of 128 threads on type int
 *     typedef cub::BlockScan<int, 128> BlockScan;
 *
 *     // Allocate shared memory for BlockScan
 *     __shared__ typename BlockScan::TempStorage temp_storage;
 *
 *     // Obtain a segment of consecutive items that are blocked across threads
 *     int thread_data[4];
 *     ...
 *
 *     // Collectively compute the block-wide exclusive prefix sum
 *     BlockScan(temp_storage).ExclusiveSum(thread_data, thread_data);
 *
 * \endcode
 * \par
 * Suppose the set of input \p thread_data across the block of threads is
 * <tt>{[1,1,1,1], [1,1,1,1], ..., [1,1,1,1]}</tt>.
 * The corresponding output \p thread_data in those threads will be
 * <tt>{[0,1,2,3], [4,5,6,7], ..., [508,509,510,511]}</tt>.
 *
 */
template <
    typename            T,
    int                 BLOCK_DIM_X,
    BlockScanAlgorithm  ALGORITHM       = BLOCK_SCAN_RAKING,
    int                 BLOCK_DIM_Y     = 1,
    int                 BLOCK_DIM_Z     = 1,
    int                 PTX_ARCH        = CUB_PTX_ARCH>
class BlockScan
{
private:

    /******************************************************************************
     * Constants and type definitions
     ******************************************************************************/

    /// Constants
    enum
    {
        /// The thread block size in threads
        BLOCK_THREADS = BLOCK_DIM_X * BLOCK_DIM_Y * BLOCK_DIM_Z,
    };

    /**
     * Ensure the template parameterization meets the requirements of the
     * specified algorithm. Currently, the BLOCK_SCAN_WARP_SCANS policy
     * cannot be used with thread block sizes not a multiple of the
     * architectural warp size.
     */
    static const BlockScanAlgorithm SAFE_ALGORITHM =
        ((ALGORITHM == BLOCK_SCAN_WARP_SCANS) && (BLOCK_THREADS % CUB_WARP_THREADS(PTX_ARCH) != 0)) ?
            BLOCK_SCAN_RAKING :
            ALGORITHM;

    typedef BlockScanWarpScans<T, BLOCK_DIM_X, BLOCK_DIM_Y, BLOCK_DIM_Z, PTX_ARCH> WarpScans;
    typedef BlockScanRaking<T, BLOCK_DIM_X, BLOCK_DIM_Y, BLOCK_DIM_Z, (SAFE_ALGORITHM == BLOCK_SCAN_RAKING_MEMOIZE), PTX_ARCH> Raking;

    /// Define the delegate type for the desired algorithm
    typedef typename If<(SAFE_ALGORITHM == BLOCK_SCAN_WARP_SCANS),
        WarpScans,
        Raking>::Type InternalBlockScan;

    /// Shared memory storage layout type for BlockScan
    typedef typename InternalBlockScan::TempStorage _TempStorage;


    /******************************************************************************
     * Thread fields
     ******************************************************************************/

    /// Shared storage reference
    _TempStorage &temp_storage;

    /// Linear thread-id
    unsigned int linear_tid;


    /******************************************************************************
     * Utility methods
     ******************************************************************************/

    /// Internal storage allocator
    __device__ __forceinline__ _TempStorage& PrivateStorage()
    {
        __shared__ _TempStorage private_storage;
        return private_storage;
    }


    /******************************************************************************
     * Public types
     ******************************************************************************/
public:

    /// \smemstorage{BlockScan}
    struct TempStorage : Uninitialized<_TempStorage> {};


    /******************************************************************//**
     * \name Collective constructors
     *********************************************************************/
    //@{

    /**
     * \brief Collective constructor using a private static allocation of shared memory as temporary storage.
     */
    __device__ __forceinline__ BlockScan()
    :
        temp_storage(PrivateStorage()),
        linear_tid(RowMajorTid(BLOCK_DIM_X, BLOCK_DIM_Y, BLOCK_DIM_Z))
    {}


    /**
     * \brief Collective constructor using the specified memory allocation as temporary storage.
     */
    __device__ __forceinline__ BlockScan(
        TempStorage &temp_storage)             ///< [in] Reference to memory allocation having layout type TempStorage
    :
        temp_storage(temp_storage.Alias()),
        linear_tid(RowMajorTid(BLOCK_DIM_X, BLOCK_DIM_Y, BLOCK_DIM_Z))
    {}



    //@}  end member group
    /******************************************************************//**
     * \name Exclusive prefix sum operations
     *********************************************************************/
    //@{


    /**
     * \brief Computes an exclusive block-wide prefix scan using addition (+) as the scan operator.  Each thread contributes one input element.  The value of 0 is applied as the initial value, and is assigned to \p output in <em>thread</em><sub>0</sub>.
     *
     * \par
     * - \identityzero
     * - \rowmajor
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates an exclusive prefix sum of 128 integer items that
     * are partitioned across 128 threads.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_scan.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize BlockScan for a 1D block of 128 threads on type int
     *     typedef cub::BlockScan<int, 128> BlockScan;
     *
     *     // Allocate shared memory for BlockScan
     *     __shared__ typename BlockScan::TempStorage temp_storage;
     *
     *     // Obtain input item for each thread
     *     int thread_data;
     *     ...
     *
     *     // Collectively compute the block-wide exclusive prefix sum
     *     BlockScan(temp_storage).ExclusiveSum(thread_data, thread_data);
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data across the block of threads is <tt>1, 1, ..., 1</tt>.  The
     * corresponding output \p thread_data in those threads will be <tt>0, 1, ..., 127</tt>.
     *
     */
    __device__ __forceinline__ void ExclusiveSum(
        T               input,                          ///< [in] Calling thread's input item
        T               &output)                        ///< [out] Calling thread's output item (may be aliased to \p input)
    {
        T initial_value = 0;
        ExclusiveScan(input, output, initial_value, cub::Sum());
    }


    /**
     * \brief Computes an exclusive block-wide prefix scan using addition (+) as the scan operator.  Each thread contributes one input element.  The value of 0 is applied as the initial value, and is assigned to \p output in <em>thread</em><sub>0</sub>.  Also provides every thread with the block-wide \p block_aggregate of all inputs.
     *
     * \par
     * - \identityzero
     * - \rowmajor
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates an exclusive prefix sum of 128 integer items that
     * are partitioned across 128 threads.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_scan.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize BlockScan for a 1D block of 128 threads on type int
     *     typedef cub::BlockScan<int, 128> BlockScan;
     *
     *     // Allocate shared memory for BlockScan
     *     __shared__ typename BlockScan::TempStorage temp_storage;
     *
     *     // Obtain input item for each thread
     *     int thread_data;
     *     ...
     *
     *     // Collectively compute the block-wide exclusive prefix sum
     *     int block_aggregate;
     *     BlockScan(temp_storage).ExclusiveSum(thread_data, thread_data, block_aggregate);
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data across the block of threads is <tt>1, 1, ..., 1</tt>.  The
     * corresponding output \p thread_data in those threads will be <tt>0, 1, ..., 127</tt>.
     * Furthermore the value \p 128 will be stored in \p block_aggregate for all threads.
     *
     */
    __device__ __forceinline__ void ExclusiveSum(
        T               input,                          ///< [in] Calling thread's input item
        T               &output,                        ///< [out] Calling thread's output item (may be aliased to \p input)
        T               &block_aggregate)               ///< [out] block-wide aggregate reduction of input items
    {
        T initial_value = 0;
        ExclusiveScan(input, output, initial_value, cub::Sum(), block_aggregate);
    }


    /**
     * \brief Computes an exclusive block-wide prefix scan using addition (+) as the scan operator.  Each thread contributes one input element.  Instead of using 0 as the block-wide prefix, the call-back functor \p block_prefix_callback_op is invoked by the first warp in the block, and the value returned by <em>lane</em><sub>0</sub> in that warp is used as the "seed" value that logically prefixes the thread block's scan inputs.  Also provides every thread with the block-wide \p block_aggregate of all inputs.
     *
     * \par
     * - \identityzero
     * - The \p block_prefix_callback_op functor must implement a member function <tt>T operator()(T block_aggregate)</tt>.
     *   The functor's input parameter \p block_aggregate is the same value also returned by the scan operation.
     *   The functor will be invoked by the first warp of threads in the block, however only the return value from
     *   <em>lane</em><sub>0</sub> is applied as the block-wide prefix.  Can be stateful.
     * - \rowmajor
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates a single thread block that progressively
     * computes an exclusive prefix sum over multiple "tiles" of input using a
     * prefix functor to maintain a running total between block-wide scans.  Each tile consists
     * of 128 integer items that are partitioned across 128 threads.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_scan.cuh>
     *
     * // A stateful callback functor that maintains a running prefix to be applied
     * // during consecutive scan operations.
     * struct BlockPrefixCallbackOp
     * {
     *     // Running prefix
     *     int running_total;
     *
     *     // Constructor
     *     __device__ BlockPrefixCallbackOp(int running_total) : running_total(running_total) {}
     *
     *     // Callback operator to be entered by the first warp of threads in the block.
     *     // Thread-0 is responsible for returning a value for seeding the block-wide scan.
     *     __device__ int operator()(int block_aggregate)
     *     {
     *         int old_prefix = running_total;
     *         running_total += block_aggregate;
     *         return old_prefix;
     *     }
     * };
     *
     * __global__ void ExampleKernel(int *d_data, int num_items, ...)
     * {
     *     // Specialize BlockScan for a 1D block of 128 threads
     *     typedef cub::BlockScan<int, 128> BlockScan;
     *
     *     // Allocate shared memory for BlockScan
     *     __shared__ typename BlockScan::TempStorage temp_storage;
     *
     *     // Initialize running total
     *     BlockPrefixCallbackOp prefix_op(0);
     *
     *     // Have the block iterate over segments of items
     *     for (int block_offset = 0; block_offset < num_items; block_offset += 128)
     *     {
     *         // Load a segment of consecutive items that are blocked across threads
     *         int thread_data = d_data[block_offset];
     *
     *         // Collectively compute the block-wide exclusive prefix sum
     *         BlockScan(temp_storage).ExclusiveSum(
     *             thread_data, thread_data, prefix_op);
     *         CTA_SYNC();
     *
     *         // Store scanned items to output segment
     *         d_data[block_offset] = thread_data;
     *     }
     * \endcode
     * \par
     * Suppose the input \p d_data is <tt>1, 1, 1, 1, 1, 1, 1, 1, ...</tt>.
     * The corresponding output for the first segment will be <tt>0, 1, ..., 127</tt>.
     * The output for the second segment will be <tt>128, 129, ..., 255</tt>.
     *
     * \tparam BlockPrefixCallbackOp        <b>[inferred]</b> Call-back functor type having member <tt>T operator()(T block_aggregate)</tt>
     */
    template <typename BlockPrefixCallbackOp>
    __device__ __forceinline__ void ExclusiveSum(
        T                       input,                          ///< [in] Calling thread's input item
        T                       &output,                        ///< [out] Calling thread's output item (may be aliased to \p input)
        BlockPrefixCallbackOp   &block_prefix_callback_op)      ///< [in-out] <b>[<em>warp</em><sub>0</sub> only]</b> Call-back functor for specifying a block-wide prefix to be applied to the logical input sequence.
    {
        ExclusiveScan(input, output, cub::Sum(), block_prefix_callback_op);
    }


    //@}  end member group
    /******************************************************************//**
     * \name Exclusive prefix sum operations (multiple data per thread)
     *********************************************************************/
    //@{


    /**
     * \brief Computes an exclusive block-wide prefix scan using addition (+) as the scan operator.  Each thread contributes an array of consecutive input elements.  The value of 0 is applied as the initial value, and is assigned to \p output[0] in <em>thread</em><sub>0</sub>.
     *
     * \par
     * - \identityzero
     * - \blocked
     * - \granularity
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates an exclusive prefix sum of 512 integer items that
     * are partitioned in a [<em>blocked arrangement</em>](index.html#sec5sec3) across 128 threads
     * where each thread owns 4 consecutive items.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_scan.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize BlockScan for a 1D block of 128 threads on type int
     *     typedef cub::BlockScan<int, 128> BlockScan;
     *
     *     // Allocate shared memory for BlockScan
     *     __shared__ typename BlockScan::TempStorage temp_storage;
     *
     *     // Obtain a segment of consecutive items that are blocked across threads
     *     int thread_data[4];
     *     ...
     *
     *     // Collectively compute the block-wide exclusive prefix sum
     *     BlockScan(temp_storage).ExclusiveSum(thread_data, thread_data);
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data across the block of threads is <tt>{ [1,1,1,1], [1,1,1,1], ..., [1,1,1,1] }</tt>.  The
     * corresponding output \p thread_data in those threads will be <tt>{ [0,1,2,3], [4,5,6,7], ..., [508,509,510,511] }</tt>.
     *
     * \tparam ITEMS_PER_THREAD     <b>[inferred]</b> The number of consecutive items partitioned onto each thread.
     */
    template <int ITEMS_PER_THREAD>
    __device__ __forceinline__ void ExclusiveSum(
        T                 (&input)[ITEMS_PER_THREAD],   ///< [in] Calling thread's input items
        T                 (&output)[ITEMS_PER_THREAD])  ///< [out] Calling thread's output items (may be aliased to \p input)
    {
        T initial_value = 0;
        ExclusiveScan(input, output, initial_value, cub::Sum());
    }


    /**
     * \brief Computes an exclusive block-wide prefix scan using addition (+) as the scan operator.  Each thread contributes an array of consecutive input elements.  The value of 0 is applied as the initial value, and is assigned to \p output[0] in <em>thread</em><sub>0</sub>.  Also provides every thread with the block-wide \p block_aggregate of all inputs.
     *
     * \par
     * - \identityzero
     * - \blocked
     * - \granularity
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates an exclusive prefix sum of 512 integer items that
     * are partitioned in a [<em>blocked arrangement</em>](index.html#sec5sec3) across 128 threads
     * where each thread owns 4 consecutive items.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_scan.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize BlockScan for a 1D block of 128 threads on type int
     *     typedef cub::BlockScan<int, 128> BlockScan;
     *
     *     // Allocate shared memory for BlockScan
     *     __shared__ typename BlockScan::TempStorage temp_storage;
     *
     *     // Obtain a segment of consecutive items that are blocked across threads
     *     int thread_data[4];
     *     ...
     *
     *     // Collectively compute the block-wide exclusive prefix sum
     *     int block_aggregate;
     *     BlockScan(temp_storage).ExclusiveSum(thread_data, thread_data, block_aggregate);
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data across the block of threads is <tt>{ [1,1,1,1], [1,1,1,1], ..., [1,1,1,1] }</tt>.  The
     * corresponding output \p thread_data in those threads will be <tt>{ [0,1,2,3], [4,5,6,7], ..., [508,509,510,511] }</tt>.
     * Furthermore the value \p 512 will be stored in \p block_aggregate for all threads.
     *
     * \tparam ITEMS_PER_THREAD     <b>[inferred]</b> The number of consecutive items partitioned onto each thread.
     */
    template <int ITEMS_PER_THREAD>
    __device__ __forceinline__ void ExclusiveSum(
        T                 (&input)[ITEMS_PER_THREAD],       ///< [in] Calling thread's input items
        T                 (&output)[ITEMS_PER_THREAD],      ///< [out] Calling thread's output items (may be aliased to \p input)
        T                 &block_aggregate)                 ///< [out] block-wide aggregate reduction of input items
    {
        // Reduce consecutive thread items in registers
        T initial_value = 0;
        ExclusiveScan(input, output, initial_value, cub::Sum(), block_aggregate);
    }


    /**
     * \brief Computes an exclusive block-wide prefix scan using addition (+) as the scan operator.  Each thread contributes an array of consecutive input elements.  Instead of using 0 as the block-wide prefix, the call-back functor \p block_prefix_callback_op is invoked by the first warp in the block, and the value returned by <em>lane</em><sub>0</sub> in that warp is used as the "seed" value that logically prefixes the thread block's scan inputs.  Also provides every thread with the block-wide \p block_aggregate of all inputs.
     *
     * \par
     * - \identityzero
     * - The \p block_prefix_callback_op functor must implement a member function <tt>T operator()(T block_aggregate)</tt>.
     *   The functor's input parameter \p block_aggregate is the same value also returned by the scan operation.
     *   The functor will be invoked by the first warp of threads in the block, however only the return value from
     *   <em>lane</em><sub>0</sub> is applied as the block-wide prefix.  Can be stateful.
     * - \blocked
     * - \granularity
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates a single thread block that progressively
     * computes an exclusive prefix sum over multiple "tiles" of input using a
     * prefix functor to maintain a running total between block-wide scans.  Each tile consists
     * of 512 integer items that are partitioned in a [<em>blocked arrangement</em>](index.html#sec5sec3)
     * across 128 threads where each thread owns 4 consecutive items.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_scan.cuh>
     *
     * // A stateful callback functor that maintains a running prefix to be applied
     * // during consecutive scan operations.
     * struct BlockPrefixCallbackOp
     * {
     *     // Running prefix
     *     int running_total;
     *
     *     // Constructor
     *     __device__ BlockPrefixCallbackOp(int running_total) : running_total(running_total) {}
     *
     *     // Callback operator to be entered by the first warp of threads in the block.
     *     // Thread-0 is responsible for returning a value for seeding the block-wide scan.
     *     __device__ int operator()(int block_aggregate)
     *     {
     *         int old_prefix = running_total;
     *         running_total += block_aggregate;
     *         return old_prefix;
     *     }
     * };
     *
     * __global__ void ExampleKernel(int *d_data, int num_items, ...)
     * {
     *     // Specialize BlockLoad, BlockStore, and BlockScan for a 1D block of 128 threads, 4 ints per thread
     *     typedef cub::BlockLoad<int*, 128, 4, BLOCK_LOAD_TRANSPOSE>   BlockLoad;
     *     typedef cub::BlockStore<int, 128, 4, BLOCK_STORE_TRANSPOSE>  BlockStore;
     *     typedef cub::BlockScan<int, 128>                             BlockScan;
     *
     *     // Allocate aliased shared memory for BlockLoad, BlockStore, and BlockScan
     *     __shared__ union {
     *         typename BlockLoad::TempStorage     load;
     *         typename BlockScan::TempStorage     scan;
     *         typename BlockStore::TempStorage    store;
     *     } temp_storage;
     *
     *     // Initialize running total
     *     BlockPrefixCallbackOp prefix_op(0);
     *
     *     // Have the block iterate over segments of items
     *     for (int block_offset = 0; block_offset < num_items; block_offset += 128 * 4)
     *     {
     *         // Load a segment of consecutive items that are blocked across threads
     *         int thread_data[4];
     *         BlockLoad(temp_storage.load).Load(d_data + block_offset, thread_data);
     *         CTA_SYNC();
     *
     *         // Collectively compute the block-wide exclusive prefix sum
     *         int block_aggregate;
     *         BlockScan(temp_storage.scan).ExclusiveSum(
     *             thread_data, thread_data, prefix_op);
     *         CTA_SYNC();
     *
     *         // Store scanned items to output segment
     *         BlockStore(temp_storage.store).Store(d_data + block_offset, thread_data);
     *         CTA_SYNC();
     *     }
     * \endcode
     * \par
     * Suppose the input \p d_data is <tt>1, 1, 1, 1, 1, 1, 1, 1, ...</tt>.
     * The corresponding output for the first segment will be <tt>0, 1, 2, 3, ..., 510, 511</tt>.
     * The output for the second segment will be <tt>512, 513, 514, 515, ..., 1022, 1023</tt>.
     *
     * \tparam ITEMS_PER_THREAD     <b>[inferred]</b> The number of consecutive items partitioned onto each thread.
     * \tparam BlockPrefixCallbackOp        <b>[inferred]</b> Call-back functor type having member <tt>T operator()(T block_aggregate)</tt>
     */
    template <
        int ITEMS_PER_THREAD,
        typename BlockPrefixCallbackOp>
    __device__ __forceinline__ void ExclusiveSum(
        T                       (&input)[ITEMS_PER_THREAD],   ///< [in] Calling thread's input items
        T                       (&output)[ITEMS_PER_THREAD],  ///< [out] Calling thread's output items (may be aliased to \p input)
        BlockPrefixCallbackOp   &block_prefix_callback_op)    ///< [in-out] <b>[<em>warp</em><sub>0</sub> only]</b> Call-back functor for specifying a block-wide prefix to be applied to the logical input sequence.
    {
        ExclusiveScan(input, output, cub::Sum(), block_prefix_callback_op);
    }



    //@}  end member group        // Exclusive prefix sums
    /******************************************************************//**
     * \name Exclusive prefix scan operations
     *********************************************************************/
    //@{


    /**
     * \brief Computes an exclusive block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes one input element.
     *
     * \par
     * - Supports non-commutative scan operators.
     * - \rowmajor
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates an exclusive prefix max scan of 128 integer items that
     * are partitioned across 128 threads.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_scan.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize BlockScan for a 1D block of 128 threads on type int
     *     typedef cub::BlockScan<int, 128> BlockScan;
     *
     *     // Allocate shared memory for BlockScan
     *     __shared__ typename BlockScan::TempStorage temp_storage;
     *
     *     // Obtain input item for each thread
     *     int thread_data;
     *     ...
     *
     *     // Collectively compute the block-wide exclusive prefix max scan
     *     BlockScan(temp_storage).ExclusiveScan(thread_data, thread_data, INT_MIN, cub::Max());
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data across the block of threads is <tt>0, -1, 2, -3, ..., 126, -127</tt>.  The
     * corresponding output \p thread_data in those threads will be <tt>INT_MIN, 0, 0, 2, ..., 124, 126</tt>.
     *
     * \tparam ScanOp               <b>[inferred]</b> Binary scan functor  type having member <tt>T operator()(const T &a, const T &b)</tt>
     */
    template <typename ScanOp>
    __device__ __forceinline__ void ExclusiveScan(
        T               input,                          ///< [in] Calling thread's input item
        T               &output,                        ///< [out] Calling thread's output item (may be aliased to \p input)
        T               initial_value,                  ///< [in] Initial value to seed the exclusive scan (and is assigned to \p output[0] in <em>thread</em><sub>0</sub>)
        ScanOp          scan_op)                        ///< [in] Binary scan functor 
    {
        InternalBlockScan(temp_storage).ExclusiveScan(input, output, initial_value, scan_op);
    }


    /**
     * \brief Computes an exclusive block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes one input element.  Also provides every thread with the block-wide \p block_aggregate of all inputs.
     *
     * \par
     * - Supports non-commutative scan operators.
     * - \rowmajor
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates an exclusive prefix max scan of 128 integer items that
     * are partitioned across 128 threads.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_scan.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize BlockScan for a 1D block of 128 threads on type int
     *     typedef cub::BlockScan<int, 128> BlockScan;
     *
     *     // Allocate shared memory for BlockScan
     *     __shared__ typename BlockScan::TempStorage temp_storage;
     *
     *     // Obtain input item for each thread
     *     int thread_data;
     *     ...
     *
     *     // Collectively compute the block-wide exclusive prefix max scan
     *     int block_aggregate;
     *     BlockScan(temp_storage).ExclusiveScan(thread_data, thread_data, INT_MIN, cub::Max(), block_aggregate);
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data across the block of threads is <tt>0, -1, 2, -3, ..., 126, -127</tt>.  The
     * corresponding output \p thread_data in those threads will be <tt>INT_MIN, 0, 0, 2, ..., 124, 126</tt>.
     * Furthermore the value \p 126 will be stored in \p block_aggregate for all threads.
     *
     * \tparam ScanOp   <b>[inferred]</b> Binary scan functor  type having member <tt>T operator()(const T &a, const T &b)</tt>
     */
    template <typename ScanOp>
    __device__ __forceinline__ void ExclusiveScan(
        T               input,              ///< [in] Calling thread's input items
        T               &output,            ///< [out] Calling thread's output items (may be aliased to \p input)
        T               initial_value,      ///< [in] Initial value to seed the exclusive scan (and is assigned to \p output[0] in <em>thread</em><sub>0</sub>)
        ScanOp          scan_op,            ///< [in] Binary scan functor 
        T               &block_aggregate)   ///< [out] block-wide aggregate reduction of input items
    {
        InternalBlockScan(temp_storage).ExclusiveScan(input, output, initial_value, scan_op, block_aggregate);
    }


    /**
     * \brief Computes an exclusive block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes one input element.  the call-back functor \p block_prefix_callback_op is invoked by the first warp in the block, and the value returned by <em>lane</em><sub>0</sub> in that warp is used as the "seed" value that logically prefixes the thread block's scan inputs.  Also provides every thread with the block-wide \p block_aggregate of all inputs.
     *
     * \par
     * - The \p block_prefix_callback_op functor must implement a member function <tt>T operator()(T block_aggregate)</tt>.
     *   The functor's input parameter \p block_aggregate is the same value also returned by the scan operation.
     *   The functor will be invoked by the first warp of threads in the block, however only the return value from
     *   <em>lane</em><sub>0</sub> is applied as the block-wide prefix.  Can be stateful.
     * - Supports non-commutative scan operators.
     * - \rowmajor
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates a single thread block that progressively
     * computes an exclusive prefix max scan over multiple "tiles" of input using a
     * prefix functor to maintain a running total between block-wide scans.  Each tile consists
     * of 128 integer items that are partitioned across 128 threads.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_scan.cuh>
     *
     * // A stateful callback functor that maintains a running prefix to be applied
     * // during consecutive scan operations.
     * struct BlockPrefixCallbackOp
     * {
     *     // Running prefix
     *     int running_total;
     *
     *     // Constructor
     *     __device__ BlockPrefixCallbackOp(int running_total) : running_total(running_total) {}
     *
     *     // Callback operator to be entered by the first warp of threads in the block.
     *     // Thread-0 is responsible for returning a value for seeding the block-wide scan.
     *     __device__ int operator()(int block_aggregate)
     *     {
     *         int old_prefix = running_total;
     *         running_total = (block_aggregate > old_prefix) ? block_aggregate : old_prefix;
     *         return old_prefix;
     *     }
     * };
     *
     * __global__ void ExampleKernel(int *d_data, int num_items, ...)
     * {
     *     // Specialize BlockScan for a 1D block of 128 threads
     *     typedef cub::BlockScan<int, 128> BlockScan;
     *
     *     // Allocate shared memory for BlockScan
     *     __shared__ typename BlockScan::TempStorage temp_storage;
     *
     *     // Initialize running total
     *     BlockPrefixCallbackOp prefix_op(INT_MIN);
     *
     *     // Have the block iterate over segments of items
     *     for (int block_offset = 0; block_offset < num_items; block_offset += 128)
     *     {
     *         // Load a segment of consecutive items that are blocked across threads
     *         int thread_data = d_data[block_offset];
     *
     *         // Collectively compute the block-wide exclusive prefix max scan
     *         BlockScan(temp_storage).ExclusiveScan(
     *             thread_data, thread_data, INT_MIN, cub::Max(), prefix_op);
     *         CTA_SYNC();
     *
     *         // Store scanned items to output segment
     *         d_data[block_offset] = thread_data;
     *     }
     * \endcode
     * \par
     * Suppose the input \p d_data is <tt>0, -1, 2, -3, 4, -5, ...</tt>.
     * The corresponding output for the first segment will be <tt>INT_MIN, 0, 0, 2, ..., 124, 126</tt>.
     * The output for the second segment will be <tt>126, 128, 128, 130, ..., 252, 254</tt>.
     *
     * \tparam ScanOp               <b>[inferred]</b> Binary scan functor  type having member <tt>T operator()(const T &a, const T &b)</tt>
     * \tparam BlockPrefixCallbackOp        <b>[inferred]</b> Call-back functor type having member <tt>T operator()(T block_aggregate)</tt>
     */
    template <
        typename ScanOp,
        typename BlockPrefixCallbackOp>
    __device__ __forceinline__ void ExclusiveScan(
        T                       input,                          ///< [in] Calling thread's input item
        T                       &output,                        ///< [out] Calling thread's output item (may be aliased to \p input)
        ScanOp                  scan_op,                        ///< [in] Binary scan functor 
        BlockPrefixCallbackOp   &block_prefix_callback_op)      ///< [in-out] <b>[<em>warp</em><sub>0</sub> only]</b> Call-back functor for specifying a block-wide prefix to be applied to the logical input sequence.
    {
        InternalBlockScan(temp_storage).ExclusiveScan(input, output, scan_op, block_prefix_callback_op);
    }


    //@}  end member group        // Inclusive prefix sums
    /******************************************************************//**
     * \name Exclusive prefix scan operations (multiple data per thread)
     *********************************************************************/
    //@{


    /**
     * \brief Computes an exclusive block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes an array of consecutive input elements.
     *
     * \par
     * - Supports non-commutative scan operators.
     * - \blocked
     * - \granularity
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates an exclusive prefix max scan of 512 integer items that
     * are partitioned in a [<em>blocked arrangement</em>](index.html#sec5sec3) across 128 threads
     * where each thread owns 4 consecutive items.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_scan.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize BlockScan for a 1D block of 128 threads on type int
     *     typedef cub::BlockScan<int, 128> BlockScan;
     *
     *     // Allocate shared memory for BlockScan
     *     __shared__ typename BlockScan::TempStorage temp_storage;
     *
     *     // Obtain a segment of consecutive items that are blocked across threads
     *     int thread_data[4];
     *     ...
     *
     *     // Collectively compute the block-wide exclusive prefix max scan
     *     BlockScan(temp_storage).ExclusiveScan(thread_data, thread_data, INT_MIN, cub::Max());
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data across the block of threads is
     * <tt>{ [0,-1,2,-3], [4,-5,6,-7], ..., [508,-509,510,-511] }</tt>.
     * The corresponding output \p thread_data in those threads will be
     * <tt>{ [INT_MIN,0,0,2], [2,4,4,6], ..., [506,508,508,510] }</tt>.
     *
     * \tparam ITEMS_PER_THREAD     <b>[inferred]</b> The number of consecutive items partitioned onto each thread.
     * \tparam ScanOp               <b>[inferred]</b> Binary scan functor  type having member <tt>T operator()(const T &a, const T &b)</tt>
     */
    template <
        int             ITEMS_PER_THREAD,
        typename        ScanOp>
    __device__ __forceinline__ void ExclusiveScan(
        T                 (&input)[ITEMS_PER_THREAD],   ///< [in] Calling thread's input items
        T                 (&output)[ITEMS_PER_THREAD],  ///< [out] Calling thread's output items (may be aliased to \p input)
        T                 initial_value,                ///< [in] Initial value to seed the exclusive scan (and is assigned to \p output[0] in <em>thread</em><sub>0</sub>)
        ScanOp            scan_op)                      ///< [in] Binary scan functor
    {
        // Reduce consecutive thread items in registers
        T thread_prefix = internal::ThreadReduce(input, scan_op);

        // Exclusive thread block-scan
        ExclusiveScan(thread_prefix, thread_prefix, initial_value, scan_op);

        // Exclusive scan in registers with prefix as seed
        internal::ThreadScanExclusive(input, output, scan_op, thread_prefix);
    }


    /**
     * \brief Computes an exclusive block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes an array of consecutive input elements.  Also provides every thread with the block-wide \p block_aggregate of all inputs.
     *
     * \par
     * - Supports non-commutative scan operators.
     * - \blocked
     * - \granularity
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates an exclusive prefix max scan of 512 integer items that
     * are partitioned in a [<em>blocked arrangement</em>](index.html#sec5sec3) across 128 threads
     * where each thread owns 4 consecutive items.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_scan.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize BlockScan for a 1D block of 128 threads on type int
     *     typedef cub::BlockScan<int, 128> BlockScan;
     *
     *     // Allocate shared memory for BlockScan
     *     __shared__ typename BlockScan::TempStorage temp_storage;
     *
     *     // Obtain a segment of consecutive items that are blocked across threads
     *     int thread_data[4];
     *     ...
     *
     *     // Collectively compute the block-wide exclusive prefix max scan
     *     int block_aggregate;
     *     BlockScan(temp_storage).ExclusiveScan(thread_data, thread_data, INT_MIN, cub::Max(), block_aggregate);
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data across the block of threads is <tt>{ [0,-1,2,-3], [4,-5,6,-7], ..., [508,-509,510,-511] }</tt>.  The
     * corresponding output \p thread_data in those threads will be <tt>{ [INT_MIN,0,0,2], [2,4,4,6], ..., [506,508,508,510] }</tt>.
     * Furthermore the value \p 510 will be stored in \p block_aggregate for all threads.
     *
     * \tparam ITEMS_PER_THREAD     <b>[inferred]</b> The number of consecutive items partitioned onto each thread.
     * \tparam ScanOp               <b>[inferred]</b> Binary scan functor  type having member <tt>T operator()(const T &a, const T &b)</tt>
     */
    template <
        int             ITEMS_PER_THREAD,
        typename        ScanOp>
    __device__ __forceinline__ void ExclusiveScan(
        T                 (&input)[ITEMS_PER_THREAD],   ///< [in] Calling thread's input items
        T                 (&output)[ITEMS_PER_THREAD],  ///< [out] Calling thread's output items (may be aliased to \p input)
        T                 initial_value,                ///< [in] Initial value to seed the exclusive scan (and is assigned to \p output[0] in <em>thread</em><sub>0</sub>)
        ScanOp            scan_op,                      ///< [in] Binary scan functor
        T                 &block_aggregate)             ///< [out] block-wide aggregate reduction of input items
    {
        // Reduce consecutive thread items in registers
        T thread_prefix = internal::ThreadReduce(input, scan_op);

        // Exclusive thread block-scan
        ExclusiveScan(thread_prefix, thread_prefix, initial_value, scan_op, block_aggregate);

        // Exclusive scan in registers with prefix as seed
        internal::ThreadScanExclusive(input, output, scan_op, thread_prefix);
    }


    /**
     * \brief Computes an exclusive block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes an array of consecutive input elements.  the call-back functor \p block_prefix_callback_op is invoked by the first warp in the block, and the value returned by <em>lane</em><sub>0</sub> in that warp is used as the "seed" value that logically prefixes the thread block's scan inputs.  Also provides every thread with the block-wide \p block_aggregate of all inputs.
     *
     * \par
     * - The \p block_prefix_callback_op functor must implement a member function <tt>T operator()(T block_aggregate)</tt>.
     *   The functor's input parameter \p block_aggregate is the same value also returned by the scan operation.
     *   The functor will be invoked by the first warp of threads in the block, however only the return value from
     *   <em>lane</em><sub>0</sub> is applied as the block-wide prefix.  Can be stateful.
     * - Supports non-commutative scan operators.
     * - \blocked
     * - \granularity
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates a single thread block that progressively
     * computes an exclusive prefix max scan over multiple "tiles" of input using a
     * prefix functor to maintain a running total between block-wide scans.  Each tile consists
     * of 128 integer items that are partitioned across 128 threads.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_scan.cuh>
     *
     * // A stateful callback functor that maintains a running prefix to be applied
     * // during consecutive scan operations.
     * struct BlockPrefixCallbackOp
     * {
     *     // Running prefix
     *     int running_total;
     *
     *     // Constructor
     *     __device__ BlockPrefixCallbackOp(int running_total) : running_total(running_total) {}
     *
     *     // Callback operator to be entered by the first warp of threads in the block.
     *     // Thread-0 is responsible for returning a value for seeding the block-wide scan.
     *     __device__ int operator()(int block_aggregate)
     *     {
     *         int old_prefix = running_total;
     *         running_total = (block_aggregate > old_prefix) ? block_aggregate : old_prefix;
     *         return old_prefix;
     *     }
     * };
     *
     * __global__ void ExampleKernel(int *d_data, int num_items, ...)
     * {
     *     // Specialize BlockLoad, BlockStore, and BlockScan for a 1D block of 128 threads, 4 ints per thread
     *     typedef cub::BlockLoad<int*, 128, 4, BLOCK_LOAD_TRANSPOSE>   BlockLoad;
     *     typedef cub::BlockStore<int, 128, 4, BLOCK_STORE_TRANSPOSE>  BlockStore;
     *     typedef cub::BlockScan<int, 128>                             BlockScan;
     *
     *     // Allocate aliased shared memory for BlockLoad, BlockStore, and BlockScan
     *     __shared__ union {
     *         typename BlockLoad::TempStorage     load;
     *         typename BlockScan::TempStorage     scan;
     *         typename BlockStore::TempStorage    store;
     *     } temp_storage;
     *
     *     // Initialize running total
     *     BlockPrefixCallbackOp prefix_op(0);
     *
     *     // Have the block iterate over segments of items
     *     for (int block_offset = 0; block_offset < num_items; block_offset += 128 * 4)
     *     {
     *         // Load a segment of consecutive items that are blocked across threads
     *         int thread_data[4];
     *         BlockLoad(temp_storage.load).Load(d_data + block_offset, thread_data);
     *         CTA_SYNC();
     *
     *         // Collectively compute the block-wide exclusive prefix max scan
     *         BlockScan(temp_storage.scan).ExclusiveScan(
     *             thread_data, thread_data, INT_MIN, cub::Max(), prefix_op);
     *         CTA_SYNC();
     *
     *         // Store scanned items to output segment
     *         BlockStore(temp_storage.store).Store(d_data + block_offset, thread_data);
     *         CTA_SYNC();
     *     }
     * \endcode
     * \par
     * Suppose the input \p d_data is <tt>0, -1, 2, -3, 4, -5, ...</tt>.
     * The corresponding output for the first segment will be <tt>INT_MIN, 0, 0, 2, 2, 4, ..., 508, 510</tt>.
     * The output for the second segment will be <tt>510, 512, 512, 514, 514, 516, ..., 1020, 1022</tt>.
     *
     * \tparam ITEMS_PER_THREAD         <b>[inferred]</b> The number of consecutive items partitioned onto each thread.
     * \tparam ScanOp                   <b>[inferred]</b> Binary scan functor  type having member <tt>T operator()(const T &a, const T &b)</tt>
     * \tparam BlockPrefixCallbackOp    <b>[inferred]</b> Call-back functor type having member <tt>T operator()(T block_aggregate)</tt>
     */
    template <
        int             ITEMS_PER_THREAD,
        typename        ScanOp,
        typename        BlockPrefixCallbackOp>
    __device__ __forceinline__ void ExclusiveScan(
        T                       (&input)[ITEMS_PER_THREAD],     ///< [in] Calling thread's input items
        T                       (&output)[ITEMS_PER_THREAD],    ///< [out] Calling thread's output items (may be aliased to \p input)
        ScanOp                  scan_op,                        ///< [in] Binary scan functor
        BlockPrefixCallbackOp   &block_prefix_callback_op)      ///< [in-out] <b>[<em>warp</em><sub>0</sub> only]</b> Call-back functor for specifying a block-wide prefix to be applied to the logical input sequence.
    {
        // Reduce consecutive thread items in registers
        T thread_prefix = internal::ThreadReduce(input, scan_op);

        // Exclusive thread block-scan
        ExclusiveScan(thread_prefix, thread_prefix, scan_op, block_prefix_callback_op);

        // Exclusive scan in registers with prefix as seed
        internal::ThreadScanExclusive(input, output, scan_op, thread_prefix);
    }


    //@}  end member group
#ifndef DOXYGEN_SHOULD_SKIP_THIS    // Do not document no-initial-value scans

    /******************************************************************//**
     * \name Exclusive prefix scan operations (no initial value, single datum per thread)
     *********************************************************************/
    //@{


    /**
     * \brief Computes an exclusive block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes one input element.  With no initial value, the output computed for <em>thread</em><sub>0</sub> is undefined.
     *
     * \par
     * - Supports non-commutative scan operators.
     * - \rowmajor
     * - \smemreuse
     *
     * \tparam ScanOp               <b>[inferred]</b> Binary scan functor  type having member <tt>T operator()(const T &a, const T &b)</tt>
     */
    template <typename ScanOp>
    __device__ __forceinline__ void ExclusiveScan(
        T               input,                          ///< [in] Calling thread's input item
        T               &output,                        ///< [out] Calling thread's output item (may be aliased to \p input)
        ScanOp          scan_op)                        ///< [in] Binary scan functor
    {
        InternalBlockScan(temp_storage).ExclusiveScan(input, output, scan_op);
    }


    /**
     * \brief Computes an exclusive block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes one input element.  Also provides every thread with the block-wide \p block_aggregate of all inputs.  With no initial value, the output computed for <em>thread</em><sub>0</sub> is undefined.
     *
     * \par
     * - Supports non-commutative scan operators.
     * - \rowmajor
     * - \smemreuse
     *
     * \tparam ScanOp   <b>[inferred]</b> Binary scan functor  type having member <tt>T operator()(const T &a, const T &b)</tt>
     */
    template <typename ScanOp>
    __device__ __forceinline__ void ExclusiveScan(
        T               input,                          ///< [in] Calling thread's input item
        T               &output,                        ///< [out] Calling thread's output item (may be aliased to \p input)
        ScanOp          scan_op,                        ///< [in] Binary scan functor
        T               &block_aggregate)               ///< [out] block-wide aggregate reduction of input items
    {
        InternalBlockScan(temp_storage).ExclusiveScan(input, output, scan_op, block_aggregate);
    }

    //@}  end member group
    /******************************************************************//**
     * \name Exclusive prefix scan operations (no initial value, multiple data per thread)
     *********************************************************************/
    //@{


    /**
     * \brief Computes an exclusive block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes an array of consecutive input elements.  With no initial value, the output computed for <em>thread</em><sub>0</sub> is undefined.
     *
     * \par
     * - Supports non-commutative scan operators.
     * - \blocked
     * - \granularity
     * - \smemreuse
     *
     * \tparam ITEMS_PER_THREAD     <b>[inferred]</b> The number of consecutive items partitioned onto each thread.
     * \tparam ScanOp               <b>[inferred]</b> Binary scan functor  type having member <tt>T operator()(const T &a, const T &b)</tt>
     */
    template <
        int             ITEMS_PER_THREAD,
        typename        ScanOp>
    __device__ __forceinline__ void ExclusiveScan(
        T                 (&input)[ITEMS_PER_THREAD],   ///< [in] Calling thread's input items
        T                 (&output)[ITEMS_PER_THREAD],  ///< [out] Calling thread's output items (may be aliased to \p input)
        ScanOp            scan_op)                      ///< [in] Binary scan functor
    {
        // Reduce consecutive thread items in registers
        T thread_partial = internal::ThreadReduce(input, scan_op);

        // Exclusive thread block-scan
        ExclusiveScan(thread_partial, thread_partial, scan_op);

        // Exclusive scan in registers with prefix
        internal::ThreadScanExclusive(input, output, scan_op, thread_partial, (linear_tid != 0));
    }


    /**
     * \brief Computes an exclusive block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes an array of consecutive input elements.  Also provides every thread with the block-wide \p block_aggregate of all inputs.  With no initial value, the output computed for <em>thread</em><sub>0</sub> is undefined.
     *
     * \par
     * - Supports non-commutative scan operators.
     * - \blocked
     * - \granularity
     * - \smemreuse
     *
     * \tparam ITEMS_PER_THREAD     <b>[inferred]</b> The number of consecutive items partitioned onto each thread.
     * \tparam ScanOp               <b>[inferred]</b> Binary scan functor  type having member <tt>T operator()(const T &a, const T &b)</tt>
     */
    template <
        int             ITEMS_PER_THREAD,
        typename        ScanOp>
    __device__ __forceinline__ void ExclusiveScan(
        T               (&input)[ITEMS_PER_THREAD],     ///< [in] Calling thread's input items
        T               (&output)[ITEMS_PER_THREAD],    ///< [out] Calling thread's output items (may be aliased to \p input)
        ScanOp          scan_op,                        ///< [in] Binary scan functor
        T               &block_aggregate)               ///< [out] block-wide aggregate reduction of input items
    {
        // Reduce consecutive thread items in registers
        T thread_partial = internal::ThreadReduce(input, scan_op);

        // Exclusive thread block-scan
        ExclusiveScan(thread_partial, thread_partial, scan_op, block_aggregate);

        // Exclusive scan in registers with prefix
        internal::ThreadScanExclusive(input, output, scan_op, thread_partial, (linear_tid != 0));
    }


    //@}  end member group
#endif // DOXYGEN_SHOULD_SKIP_THIS  // Do not document no-initial-value scans

    /******************************************************************//**
     * \name Inclusive prefix sum operations
     *********************************************************************/
    //@{


    /**
     * \brief Computes an inclusive block-wide prefix scan using addition (+) as the scan operator.  Each thread contributes one input element.
     *
     * \par
     * - \rowmajor
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates an inclusive prefix sum of 128 integer items that
     * are partitioned across 128 threads.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_scan.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize BlockScan for a 1D block of 128 threads on type int
     *     typedef cub::BlockScan<int, 128> BlockScan;
     *
     *     // Allocate shared memory for BlockScan
     *     __shared__ typename BlockScan::TempStorage temp_storage;
     *
     *     // Obtain input item for each thread
     *     int thread_data;
     *     ...
     *
     *     // Collectively compute the block-wide inclusive prefix sum
     *     BlockScan(temp_storage).InclusiveSum(thread_data, thread_data);
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data across the block of threads is <tt>1, 1, ..., 1</tt>.  The
     * corresponding output \p thread_data in those threads will be <tt>1, 2, ..., 128</tt>.
     *
     */
    __device__ __forceinline__ void InclusiveSum(
        T               input,                          ///< [in] Calling thread's input item
        T               &output)                        ///< [out] Calling thread's output item (may be aliased to \p input)
    {
        InclusiveScan(input, output, cub::Sum());
    }


    /**
     * \brief Computes an inclusive block-wide prefix scan using addition (+) as the scan operator.  Each thread contributes one input element.  Also provides every thread with the block-wide \p block_aggregate of all inputs.
     *
     * \par
     * - \rowmajor
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates an inclusive prefix sum of 128 integer items that
     * are partitioned across 128 threads.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_scan.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize BlockScan for a 1D block of 128 threads on type int
     *     typedef cub::BlockScan<int, 128> BlockScan;
     *
     *     // Allocate shared memory for BlockScan
     *     __shared__ typename BlockScan::TempStorage temp_storage;
     *
     *     // Obtain input item for each thread
     *     int thread_data;
     *     ...
     *
     *     // Collectively compute the block-wide inclusive prefix sum
     *     int block_aggregate;
     *     BlockScan(temp_storage).InclusiveSum(thread_data, thread_data, block_aggregate);
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data across the block of threads is <tt>1, 1, ..., 1</tt>.  The
     * corresponding output \p thread_data in those threads will be <tt>1, 2, ..., 128</tt>.
     * Furthermore the value \p 128 will be stored in \p block_aggregate for all threads.
     *
     */
    __device__ __forceinline__ void InclusiveSum(
        T               input,                          ///< [in] Calling thread's input item
        T               &output,                        ///< [out] Calling thread's output item (may be aliased to \p input)
        T               &block_aggregate)               ///< [out] block-wide aggregate reduction of input items
    {
        InclusiveScan(input, output, cub::Sum(), block_aggregate);
    }



    /**
     * \brief Computes an inclusive block-wide prefix scan using addition (+) as the scan operator.  Each thread contributes one input element.  Instead of using 0 as the block-wide prefix, the call-back functor \p block_prefix_callback_op is invoked by the first warp in the block, and the value returned by <em>lane</em><sub>0</sub> in that warp is used as the "seed" value that logically prefixes the thread block's scan inputs.  Also provides every thread with the block-wide \p block_aggregate of all inputs.
     *
     * \par
     * - The \p block_prefix_callback_op functor must implement a member function <tt>T operator()(T block_aggregate)</tt>.
     *   The functor's input parameter \p block_aggregate is the same value also returned by the scan operation.
     *   The functor will be invoked by the first warp of threads in the block, however only the return value from
     *   <em>lane</em><sub>0</sub> is applied as the block-wide prefix.  Can be stateful.
     * - \rowmajor
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates a single thread block that progressively
     * computes an inclusive prefix sum over multiple "tiles" of input using a
     * prefix functor to maintain a running total between block-wide scans.  Each tile consists
     * of 128 integer items that are partitioned across 128 threads.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_scan.cuh>
     *
     * // A stateful callback functor that maintains a running prefix to be applied
     * // during consecutive scan operations.
     * struct BlockPrefixCallbackOp
     * {
     *     // Running prefix
     *     int running_total;
     *
     *     // Constructor
     *     __device__ BlockPrefixCallbackOp(int running_total) : running_total(running_total) {}
     *
     *     // Callback operator to be entered by the first warp of threads in the block.
     *     // Thread-0 is responsible for returning a value for seeding the block-wide scan.
     *     __device__ int operator()(int block_aggregate)
     *     {
     *         int old_prefix = running_total;
     *         running_total += block_aggregate;
     *         return old_prefix;
     *     }
     * };
     *
     * __global__ void ExampleKernel(int *d_data, int num_items, ...)
     * {
     *     // Specialize BlockScan for a 1D block of 128 threads
     *     typedef cub::BlockScan<int, 128> BlockScan;
     *
     *     // Allocate shared memory for BlockScan
     *     __shared__ typename BlockScan::TempStorage temp_storage;
     *
     *     // Initialize running total
     *     BlockPrefixCallbackOp prefix_op(0);
     *
     *     // Have the block iterate over segments of items
     *     for (int block_offset = 0; block_offset < num_items; block_offset += 128)
     *     {
     *         // Load a segment of consecutive items that are blocked across threads
     *         int thread_data = d_data[block_offset];
     *
     *         // Collectively compute the block-wide inclusive prefix sum
     *         BlockScan(temp_storage).InclusiveSum(
     *             thread_data, thread_data, prefix_op);
     *         CTA_SYNC();
     *
     *         // Store scanned items to output segment
     *         d_data[block_offset] = thread_data;
     *     }
     * \endcode
     * \par
     * Suppose the input \p d_data is <tt>1, 1, 1, 1, 1, 1, 1, 1, ...</tt>.
     * The corresponding output for the first segment will be <tt>1, 2, ..., 128</tt>.
     * The output for the second segment will be <tt>129, 130, ..., 256</tt>.
     *
     * \tparam BlockPrefixCallbackOp          <b>[inferred]</b> Call-back functor type having member <tt>T operator()(T block_aggregate)</tt>
     */
    template <typename BlockPrefixCallbackOp>
    __device__ __forceinline__ void InclusiveSum(
        T                       input,                          ///< [in] Calling thread's input item
        T                       &output,                        ///< [out] Calling thread's output item (may be aliased to \p input)
        BlockPrefixCallbackOp   &block_prefix_callback_op)      ///< [in-out] <b>[<em>warp</em><sub>0</sub> only]</b> Call-back functor for specifying a block-wide prefix to be applied to the logical input sequence.
    {
        InclusiveScan(input, output, cub::Sum(), block_prefix_callback_op);
    }


    //@}  end member group
    /******************************************************************//**
     * \name Inclusive prefix sum operations (multiple data per thread)
     *********************************************************************/
    //@{


    /**
     * \brief Computes an inclusive block-wide prefix scan using addition (+) as the scan operator.  Each thread contributes an array of consecutive input elements.
     *
     * \par
     * - \blocked
     * - \granularity
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates an inclusive prefix sum of 512 integer items that
     * are partitioned in a [<em>blocked arrangement</em>](index.html#sec5sec3) across 128 threads
     * where each thread owns 4 consecutive items.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_scan.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize BlockScan for a 1D block of 128 threads on type int
     *     typedef cub::BlockScan<int, 128> BlockScan;
     *
     *     // Allocate shared memory for BlockScan
     *     __shared__ typename BlockScan::TempStorage temp_storage;
     *
     *     // Obtain a segment of consecutive items that are blocked across threads
     *     int thread_data[4];
     *     ...
     *
     *     // Collectively compute the block-wide inclusive prefix sum
     *     BlockScan(temp_storage).InclusiveSum(thread_data, thread_data);
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data across the block of threads is <tt>{ [1,1,1,1], [1,1,1,1], ..., [1,1,1,1] }</tt>.  The
     * corresponding output \p thread_data in those threads will be <tt>{ [1,2,3,4], [5,6,7,8], ..., [509,510,511,512] }</tt>.
     *
     * \tparam ITEMS_PER_THREAD     <b>[inferred]</b> The number of consecutive items partitioned onto each thread.
     */
    template <int ITEMS_PER_THREAD>
    __device__ __forceinline__ void InclusiveSum(
        T               (&input)[ITEMS_PER_THREAD],     ///< [in] Calling thread's input items
        T               (&output)[ITEMS_PER_THREAD])    ///< [out] Calling thread's output items (may be aliased to \p input)
    {
        if (ITEMS_PER_THREAD == 1)
        {
            InclusiveSum(input[0], output[0]);
        }
        else
        {
            // Reduce consecutive thread items in registers
            Sum scan_op;
            T thread_prefix = internal::ThreadReduce(input, scan_op);

            // Exclusive thread block-scan
            ExclusiveSum(thread_prefix, thread_prefix);

            // Inclusive scan in registers with prefix as seed
            internal::ThreadScanInclusive(input, output, scan_op, thread_prefix, (linear_tid != 0));
        }
    }


    /**
     * \brief Computes an inclusive block-wide prefix scan using addition (+) as the scan operator.  Each thread contributes an array of consecutive input elements.  Also provides every thread with the block-wide \p block_aggregate of all inputs.
     *
     * \par
     * - \blocked
     * - \granularity
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates an inclusive prefix sum of 512 integer items that
     * are partitioned in a [<em>blocked arrangement</em>](index.html#sec5sec3) across 128 threads
     * where each thread owns 4 consecutive items.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_scan.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize BlockScan for a 1D block of 128 threads on type int
     *     typedef cub::BlockScan<int, 128> BlockScan;
     *
     *     // Allocate shared memory for BlockScan
     *     __shared__ typename BlockScan::TempStorage temp_storage;
     *
     *     // Obtain a segment of consecutive items that are blocked across threads
     *     int thread_data[4];
     *     ...
     *
     *     // Collectively compute the block-wide inclusive prefix sum
     *     int block_aggregate;
     *     BlockScan(temp_storage).InclusiveSum(thread_data, thread_data, block_aggregate);
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data across the block of threads is
     * <tt>{ [1,1,1,1], [1,1,1,1], ..., [1,1,1,1] }</tt>.  The
     * corresponding output \p thread_data in those threads will be
     * <tt>{ [1,2,3,4], [5,6,7,8], ..., [509,510,511,512] }</tt>.
     * Furthermore the value \p 512 will be stored in \p block_aggregate for all threads.
     *
     * \tparam ITEMS_PER_THREAD     <b>[inferred]</b> The number of consecutive items partitioned onto each thread.
     * \tparam ScanOp               <b>[inferred]</b> Binary scan functor  type having member <tt>T operator()(const T &a, const T &b)</tt>
     */
    template <int ITEMS_PER_THREAD>
    __device__ __forceinline__ void InclusiveSum(
        T               (&input)[ITEMS_PER_THREAD],     ///< [in] Calling thread's input items
        T               (&output)[ITEMS_PER_THREAD],    ///< [out] Calling thread's output items (may be aliased to \p input)
        T               &block_aggregate)               ///< [out] block-wide aggregate reduction of input items
    {
        if (ITEMS_PER_THREAD == 1)
        {
            InclusiveSum(input[0], output[0], block_aggregate);
        }
        else
        {
            // Reduce consecutive thread items in registers
            Sum scan_op;
            T thread_prefix = internal::ThreadReduce(input, scan_op);

            // Exclusive thread block-scan
            ExclusiveSum(thread_prefix, thread_prefix, block_aggregate);

            // Inclusive scan in registers with prefix as seed
            internal::ThreadScanInclusive(input, output, scan_op, thread_prefix, (linear_tid != 0));
        }
    }


    /**
     * \brief Computes an inclusive block-wide prefix scan using addition (+) as the scan operator.  Each thread contributes an array of consecutive input elements.  Instead of using 0 as the block-wide prefix, the call-back functor \p block_prefix_callback_op is invoked by the first warp in the block, and the value returned by <em>lane</em><sub>0</sub> in that warp is used as the "seed" value that logically prefixes the thread block's scan inputs.  Also provides every thread with the block-wide \p block_aggregate of all inputs.
     *
     * \par
     * - The \p block_prefix_callback_op functor must implement a member function <tt>T operator()(T block_aggregate)</tt>.
     *   The functor's input parameter \p block_aggregate is the same value also returned by the scan operation.
     *   The functor will be invoked by the first warp of threads in the block, however only the return value from
     *   <em>lane</em><sub>0</sub> is applied as the block-wide prefix.  Can be stateful.
     * - \blocked
     * - \granularity
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates a single thread block that progressively
     * computes an inclusive prefix sum over multiple "tiles" of input using a
     * prefix functor to maintain a running total between block-wide scans.  Each tile consists
     * of 512 integer items that are partitioned in a [<em>blocked arrangement</em>](index.html#sec5sec3)
     * across 128 threads where each thread owns 4 consecutive items.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_scan.cuh>
     *
     * // A stateful callback functor that maintains a running prefix to be applied
     * // during consecutive scan operations.
     * struct BlockPrefixCallbackOp
     * {
     *     // Running prefix
     *     int running_total;
     *
     *     // Constructor
     *     __device__ BlockPrefixCallbackOp(int running_total) : running_total(running_total) {}
     *
     *     // Callback operator to be entered by the first warp of threads in the block.
     *     // Thread-0 is responsible for returning a value for seeding the block-wide scan.
     *     __device__ int operator()(int block_aggregate)
     *     {
     *         int old_prefix = running_total;
     *         running_total += block_aggregate;
     *         return old_prefix;
     *     }
     * };
     *
     * __global__ void ExampleKernel(int *d_data, int num_items, ...)
     * {
     *     // Specialize BlockLoad, BlockStore, and BlockScan for a 1D block of 128 threads, 4 ints per thread
     *     typedef cub::BlockLoad<int*, 128, 4, BLOCK_LOAD_TRANSPOSE>   BlockLoad;
     *     typedef cub::BlockStore<int, 128, 4, BLOCK_STORE_TRANSPOSE>  BlockStore;
     *     typedef cub::BlockScan<int, 128>                             BlockScan;
     *
     *     // Allocate aliased shared memory for BlockLoad, BlockStore, and BlockScan
     *     __shared__ union {
     *         typename BlockLoad::TempStorage     load;
     *         typename BlockScan::TempStorage     scan;
     *         typename BlockStore::TempStorage    store;
     *     } temp_storage;
     *
     *     // Initialize running total
     *     BlockPrefixCallbackOp prefix_op(0);
     *
     *     // Have the block iterate over segments of items
     *     for (int block_offset = 0; block_offset < num_items; block_offset += 128 * 4)
     *     {
     *         // Load a segment of consecutive items that are blocked across threads
     *         int thread_data[4];
     *         BlockLoad(temp_storage.load).Load(d_data + block_offset, thread_data);
     *         CTA_SYNC();
     *
     *         // Collectively compute the block-wide inclusive prefix sum
     *         BlockScan(temp_storage.scan).IncluisveSum(
     *             thread_data, thread_data, prefix_op);
     *         CTA_SYNC();
     *
     *         // Store scanned items to output segment
     *         BlockStore(temp_storage.store).Store(d_data + block_offset, thread_data);
     *         CTA_SYNC();
     *     }
     * \endcode
     * \par
     * Suppose the input \p d_data is <tt>1, 1, 1, 1, 1, 1, 1, 1, ...</tt>.
     * The corresponding output for the first segment will be <tt>1, 2, 3, 4, ..., 511, 512</tt>.
     * The output for the second segment will be <tt>513, 514, 515, 516, ..., 1023, 1024</tt>.
     *
     * \tparam ITEMS_PER_THREAD     <b>[inferred]</b> The number of consecutive items partitioned onto each thread.
     * \tparam BlockPrefixCallbackOp        <b>[inferred]</b> Call-back functor type having member <tt>T operator()(T block_aggregate)</tt>
     */
    template <
        int ITEMS_PER_THREAD,
        typename BlockPrefixCallbackOp>
    __device__ __forceinline__ void InclusiveSum(
        T                       (&input)[ITEMS_PER_THREAD],     ///< [in] Calling thread's input items
        T                       (&output)[ITEMS_PER_THREAD],    ///< [out] Calling thread's output items (may be aliased to \p input)
        BlockPrefixCallbackOp   &block_prefix_callback_op)      ///< [in-out] <b>[<em>warp</em><sub>0</sub> only]</b> Call-back functor for specifying a block-wide prefix to be applied to the logical input sequence.
    {
        if (ITEMS_PER_THREAD == 1)
        {
            InclusiveSum(input[0], output[0], block_prefix_callback_op);
        }
        else
        {
            // Reduce consecutive thread items in registers
            Sum scan_op;
            T thread_prefix = internal::ThreadReduce(input, scan_op);

            // Exclusive thread block-scan
            ExclusiveSum(thread_prefix, thread_prefix, block_prefix_callback_op);

            // Inclusive scan in registers with prefix as seed
            internal::ThreadScanInclusive(input, output, scan_op, thread_prefix);
        }
    }


    //@}  end member group
    /******************************************************************//**
     * \name Inclusive prefix scan operations
     *********************************************************************/
    //@{


    /**
     * \brief Computes an inclusive block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes one input element.
     *
     * \par
     * - Supports non-commutative scan operators.
     * - \rowmajor
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates an inclusive prefix max scan of 128 integer items that
     * are partitioned across 128 threads.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_scan.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize BlockScan for a 1D block of 128 threads on type int
     *     typedef cub::BlockScan<int, 128> BlockScan;
     *
     *     // Allocate shared memory for BlockScan
     *     __shared__ typename BlockScan::TempStorage temp_storage;
     *
     *     // Obtain input item for each thread
     *     int thread_data;
     *     ...
     *
     *     // Collectively compute the block-wide inclusive prefix max scan
     *     BlockScan(temp_storage).InclusiveScan(thread_data, thread_data, cub::Max());
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data across the block of threads is <tt>0, -1, 2, -3, ..., 126, -127</tt>.  The
     * corresponding output \p thread_data in those threads will be <tt>0, 0, 2, 2, ..., 126, 126</tt>.
     *
     * \tparam ScanOp               <b>[inferred]</b> Binary scan functor  type having member <tt>T operator()(const T &a, const T &b)</tt>
     */
    template <typename ScanOp>
    __device__ __forceinline__ void InclusiveScan(
        T               input,                          ///< [in] Calling thread's input item
        T               &output,                        ///< [out] Calling thread's output item (may be aliased to \p input)
        ScanOp          scan_op)                        ///< [in] Binary scan functor 
    {
        InternalBlockScan(temp_storage).InclusiveScan(input, output, scan_op);
    }


    /**
     * \brief Computes an inclusive block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes one input element.  Also provides every thread with the block-wide \p block_aggregate of all inputs.
     *
     * \par
     * - Supports non-commutative scan operators.
     * - \rowmajor
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates an inclusive prefix max scan of 128 integer items that
     * are partitioned across 128 threads.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_scan.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize BlockScan for a 1D block of 128 threads on type int
     *     typedef cub::BlockScan<int, 128> BlockScan;
     *
     *     // Allocate shared memory for BlockScan
     *     __shared__ typename BlockScan::TempStorage temp_storage;
     *
     *     // Obtain input item for each thread
     *     int thread_data;
     *     ...
     *
     *     // Collectively compute the block-wide inclusive prefix max scan
     *     int block_aggregate;
     *     BlockScan(temp_storage).InclusiveScan(thread_data, thread_data, cub::Max(), block_aggregate);
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data across the block of threads is <tt>0, -1, 2, -3, ..., 126, -127</tt>.  The
     * corresponding output \p thread_data in those threads will be <tt>0, 0, 2, 2, ..., 126, 126</tt>.
     * Furthermore the value \p 126 will be stored in \p block_aggregate for all threads.
     *
     * \tparam ScanOp   <b>[inferred]</b> Binary scan functor  type having member <tt>T operator()(const T &a, const T &b)</tt>
     */
    template <typename ScanOp>
    __device__ __forceinline__ void InclusiveScan(
        T               input,                          ///< [in] Calling thread's input item
        T               &output,                        ///< [out] Calling thread's output item (may be aliased to \p input)
        ScanOp          scan_op,                        ///< [in] Binary scan functor 
        T               &block_aggregate)               ///< [out] block-wide aggregate reduction of input items
    {
        InternalBlockScan(temp_storage).InclusiveScan(input, output, scan_op, block_aggregate);
    }


    /**
     * \brief Computes an inclusive block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes one input element.  the call-back functor \p block_prefix_callback_op is invoked by the first warp in the block, and the value returned by <em>lane</em><sub>0</sub> in that warp is used as the "seed" value that logically prefixes the thread block's scan inputs.  Also provides every thread with the block-wide \p block_aggregate of all inputs.
     *
     * \par
     * - The \p block_prefix_callback_op functor must implement a member function <tt>T operator()(T block_aggregate)</tt>.
     *   The functor's input parameter \p block_aggregate is the same value also returned by the scan operation.
     *   The functor will be invoked by the first warp of threads in the block, however only the return value from
     *   <em>lane</em><sub>0</sub> is applied as the block-wide prefix.  Can be stateful.
     * - Supports non-commutative scan operators.
     * - \rowmajor
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates a single thread block that progressively
     * computes an inclusive prefix max scan over multiple "tiles" of input using a
     * prefix functor to maintain a running total between block-wide scans.  Each tile consists
     * of 128 integer items that are partitioned across 128 threads.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_scan.cuh>
     *
     * // A stateful callback functor that maintains a running prefix to be applied
     * // during consecutive scan operations.
     * struct BlockPrefixCallbackOp
     * {
     *     // Running prefix
     *     int running_total;
     *
     *     // Constructor
     *     __device__ BlockPrefixCallbackOp(int running_total) : running_total(running_total) {}
     *
     *     // Callback operator to be entered by the first warp of threads in the block.
     *     // Thread-0 is responsible for returning a value for seeding the block-wide scan.
     *     __device__ int operator()(int block_aggregate)
     *     {
     *         int old_prefix = running_total;
     *         running_total = (block_aggregate > old_prefix) ? block_aggregate : old_prefix;
     *         return old_prefix;
     *     }
     * };
     *
     * __global__ void ExampleKernel(int *d_data, int num_items, ...)
     * {
     *     // Specialize BlockScan for a 1D block of 128 threads
     *     typedef cub::BlockScan<int, 128> BlockScan;
     *
     *     // Allocate shared memory for BlockScan
     *     __shared__ typename BlockScan::TempStorage temp_storage;
     *
     *     // Initialize running total
     *     BlockPrefixCallbackOp prefix_op(INT_MIN);
     *
     *     // Have the block iterate over segments of items
     *     for (int block_offset = 0; block_offset < num_items; block_offset += 128)
     *     {
     *         // Load a segment of consecutive items that are blocked across threads
     *         int thread_data = d_data[block_offset];
     *
     *         // Collectively compute the block-wide inclusive prefix max scan
     *         BlockScan(temp_storage).InclusiveScan(
     *             thread_data, thread_data, cub::Max(), prefix_op);
     *         CTA_SYNC();
     *
     *         // Store scanned items to output segment
     *         d_data[block_offset] = thread_data;
     *     }
     * \endcode
     * \par
     * Suppose the input \p d_data is <tt>0, -1, 2, -3, 4, -5, ...</tt>.
     * The corresponding output for the first segment will be <tt>0, 0, 2, 2, ..., 126, 126</tt>.
     * The output for the second segment will be <tt>128, 128, 130, 130, ..., 254, 254</tt>.
     *
     * \tparam ScanOp               <b>[inferred]</b> Binary scan functor  type having member <tt>T operator()(const T &a, const T &b)</tt>
     * \tparam BlockPrefixCallbackOp        <b>[inferred]</b> Call-back functor type having member <tt>T operator()(T block_aggregate)</tt>
     */
    template <
        typename ScanOp,
        typename BlockPrefixCallbackOp>
    __device__ __forceinline__ void InclusiveScan(
        T                       input,                          ///< [in] Calling thread's input item
        T                       &output,                        ///< [out] Calling thread's output item (may be aliased to \p input)
        ScanOp                  scan_op,                        ///< [in] Binary scan functor 
        BlockPrefixCallbackOp   &block_prefix_callback_op)      ///< [in-out] <b>[<em>warp</em><sub>0</sub> only]</b> Call-back functor for specifying a block-wide prefix to be applied to the logical input sequence.
    {
        InternalBlockScan(temp_storage).InclusiveScan(input, output, scan_op, block_prefix_callback_op);
    }


    //@}  end member group
    /******************************************************************//**
     * \name Inclusive prefix scan operations (multiple data per thread)
     *********************************************************************/
    //@{


    /**
     * \brief Computes an inclusive block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes an array of consecutive input elements.
     *
     * \par
     * - Supports non-commutative scan operators.
     * - \blocked
     * - \granularity
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates an inclusive prefix max scan of 512 integer items that
     * are partitioned in a [<em>blocked arrangement</em>](index.html#sec5sec3) across 128 threads
     * where each thread owns 4 consecutive items.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_scan.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize BlockScan for a 1D block of 128 threads on type int
     *     typedef cub::BlockScan<int, 128> BlockScan;
     *
     *     // Allocate shared memory for BlockScan
     *     __shared__ typename BlockScan::TempStorage temp_storage;
     *
     *     // Obtain a segment of consecutive items that are blocked across threads
     *     int thread_data[4];
     *     ...
     *
     *     // Collectively compute the block-wide inclusive prefix max scan
     *     BlockScan(temp_storage).InclusiveScan(thread_data, thread_data, cub::Max());
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data across the block of threads is <tt>{ [0,-1,2,-3], [4,-5,6,-7], ..., [508,-509,510,-511] }</tt>.  The
     * corresponding output \p thread_data in those threads will be <tt>{ [0,0,2,2], [4,4,6,6], ..., [508,508,510,510] }</tt>.
     *
     * \tparam ITEMS_PER_THREAD     <b>[inferred]</b> The number of consecutive items partitioned onto each thread.
     * \tparam ScanOp               <b>[inferred]</b> Binary scan functor  type having member <tt>T operator()(const T &a, const T &b)</tt>
     */
    template <
        int             ITEMS_PER_THREAD,
        typename        ScanOp>
    __device__ __forceinline__ void InclusiveScan(
        T               (&input)[ITEMS_PER_THREAD],     ///< [in] Calling thread's input items
        T               (&output)[ITEMS_PER_THREAD],    ///< [out] Calling thread's output items (may be aliased to \p input)
        ScanOp          scan_op)                        ///< [in] Binary scan functor 
    {
        if (ITEMS_PER_THREAD == 1)
        {
            InclusiveScan(input[0], output[0], scan_op);
        }
        else
        {
            // Reduce consecutive thread items in registers
            T thread_prefix = internal::ThreadReduce(input, scan_op);

            // Exclusive thread block-scan
            ExclusiveScan(thread_prefix, thread_prefix, scan_op);

            // Inclusive scan in registers with prefix as seed (first thread does not seed)
            internal::ThreadScanInclusive(input, output, scan_op, thread_prefix, (linear_tid != 0));
        }
    }


    /**
     * \brief Computes an inclusive block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes an array of consecutive input elements.  Also provides every thread with the block-wide \p block_aggregate of all inputs.
     *
     * \par
     * - Supports non-commutative scan operators.
     * - \blocked
     * - \granularity
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates an inclusive prefix max scan of 512 integer items that
     * are partitioned in a [<em>blocked arrangement</em>](index.html#sec5sec3) across 128 threads
     * where each thread owns 4 consecutive items.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_scan.cuh>
     *
     * __global__ void ExampleKernel(...)
     * {
     *     // Specialize BlockScan for a 1D block of 128 threads on type int
     *     typedef cub::BlockScan<int, 128> BlockScan;
     *
     *     // Allocate shared memory for BlockScan
     *     __shared__ typename BlockScan::TempStorage temp_storage;
     *
     *     // Obtain a segment of consecutive items that are blocked across threads
     *     int thread_data[4];
     *     ...
     *
     *     // Collectively compute the block-wide inclusive prefix max scan
     *     int block_aggregate;
     *     BlockScan(temp_storage).InclusiveScan(thread_data, thread_data, cub::Max(), block_aggregate);
     *
     * \endcode
     * \par
     * Suppose the set of input \p thread_data across the block of threads is
     * <tt>{ [0,-1,2,-3], [4,-5,6,-7], ..., [508,-509,510,-511] }</tt>.
     * The corresponding output \p thread_data in those threads will be
     * <tt>{ [0,0,2,2], [4,4,6,6], ..., [508,508,510,510] }</tt>.
     * Furthermore the value \p 510 will be stored in \p block_aggregate for all threads.
     *
     * \tparam ITEMS_PER_THREAD     <b>[inferred]</b> The number of consecutive items partitioned onto each thread.
     * \tparam ScanOp               <b>[inferred]</b> Binary scan functor  type having member <tt>T operator()(const T &a, const T &b)</tt>
     */
    template <
        int             ITEMS_PER_THREAD,
        typename         ScanOp>
    __device__ __forceinline__ void InclusiveScan(
        T               (&input)[ITEMS_PER_THREAD],     ///< [in] Calling thread's input items
        T               (&output)[ITEMS_PER_THREAD],    ///< [out] Calling thread's output items (may be aliased to \p input)
        ScanOp          scan_op,                        ///< [in] Binary scan functor 
        T               &block_aggregate)               ///< [out] block-wide aggregate reduction of input items
    {
        if (ITEMS_PER_THREAD == 1)
        {
            InclusiveScan(input[0], output[0], scan_op, block_aggregate);
        }
        else
        {
            // Reduce consecutive thread items in registers
            T thread_prefix = internal::ThreadReduce(input, scan_op);

            // Exclusive thread block-scan (with no initial value)
            ExclusiveScan(thread_prefix, thread_prefix, scan_op, block_aggregate);

            // Inclusive scan in registers with prefix as seed (first thread does not seed)
            internal::ThreadScanInclusive(input, output, scan_op, thread_prefix, (linear_tid != 0));
        }
    }


    /**
     * \brief Computes an inclusive block-wide prefix scan using the specified binary \p scan_op functor.  Each thread contributes an array of consecutive input elements.  the call-back functor \p block_prefix_callback_op is invoked by the first warp in the block, and the value returned by <em>lane</em><sub>0</sub> in that warp is used as the "seed" value that logically prefixes the thread block's scan inputs.  Also provides every thread with the block-wide \p block_aggregate of all inputs.
     *
     * \par
     * - The \p block_prefix_callback_op functor must implement a member function <tt>T operator()(T block_aggregate)</tt>.
     *   The functor's input parameter \p block_aggregate is the same value also returned by the scan operation.
     *   The functor will be invoked by the first warp of threads in the block, however only the return value from
     *   <em>lane</em><sub>0</sub> is applied as the block-wide prefix.  Can be stateful.
     * - Supports non-commutative scan operators.
     * - \blocked
     * - \granularity
     * - \smemreuse
     *
     * \par Snippet
     * The code snippet below illustrates a single thread block that progressively
     * computes an inclusive prefix max scan over multiple "tiles" of input using a
     * prefix functor to maintain a running total between block-wide scans.  Each tile consists
     * of 128 integer items that are partitioned across 128 threads.
     * \par
     * \code
     * #include <cub/cub.cuh>   // or equivalently <cub/block/block_scan.cuh>
     *
     * // A stateful callback functor that maintains a running prefix to be applied
     * // during consecutive scan operations.
     * struct BlockPrefixCallbackOp
     * {
     *     // Running prefix
     *     int running_total;
     *
     *     // Constructor
     *     __device__ BlockPrefixCallbackOp(int running_total) : running_total(running_total) {}
     *
     *     // Callback operator to be entered by the first warp of threads in the block.
     *     // Thread-0 is responsible for returning a value for seeding the block-wide scan.
     *     __device__ int operator()(int block_aggregate)
     *     {
     *         int old_prefix = running_total;
     *         running_total = (block_aggregate > old_prefix) ? block_aggregate : old_prefix;
     *         return old_prefix;
     *     }
     * };
     *
     * __global__ void ExampleKernel(int *d_data, int num_items, ...)
     * {
     *     // Specialize BlockLoad, BlockStore, and BlockScan for a 1D block of 128 threads, 4 ints per thread
     *     typedef cub::BlockLoad<int*, 128, 4, BLOCK_LOAD_TRANSPOSE>   BlockLoad;
     *     typedef cub::BlockStore<int, 128, 4, BLOCK_STORE_TRANSPOSE>  BlockStore;
     *     typedef cub::BlockScan<int, 128>                             BlockScan;
     *
     *     // Allocate aliased shared memory for BlockLoad, BlockStore, and BlockScan
     *     __shared__ union {
     *         typename BlockLoad::TempStorage     load;
     *         typename BlockScan::TempStorage     scan;
     *         typename BlockStore::TempStorage    store;
     *     } temp_storage;
     *
     *     // Initialize running total
     *     BlockPrefixCallbackOp prefix_op(0);
     *
     *     // Have the block iterate over segments of items
     *     for (int block_offset = 0; block_offset < num_items; block_offset += 128 * 4)
     *     {
     *         // Load a segment of consecutive items that are blocked across threads
     *         int thread_data[4];
     *         BlockLoad(temp_storage.load).Load(d_data + block_offset, thread_data);
     *         CTA_SYNC();
     *
     *         // Collectively compute the block-wide inclusive prefix max scan
     *         BlockScan(temp_storage.scan).InclusiveScan(
     *             thread_data, thread_data, cub::Max(), prefix_op);
     *         CTA_SYNC();
     *
     *         // Store scanned items to output segment
     *         BlockStore(temp_storage.store).Store(d_data + block_offset, thread_data);
     *         CTA_SYNC();
     *     }
     * \endcode
     * \par
     * Suppose the input \p d_data is <tt>0, -1, 2, -3, 4, -5, ...</tt>.
     * The corresponding output for the first segment will be <tt>0, 0, 2, 2, 4, 4, ..., 510, 510</tt>.
     * The output for the second segment will be <tt>512, 512, 514, 514, 516, 516, ..., 1022, 1022</tt>.
     *
     * \tparam ITEMS_PER_THREAD         <b>[inferred]</b> The number of consecutive items partitioned onto each thread.
     * \tparam ScanOp                   <b>[inferred]</b> Binary scan functor  type having member <tt>T operator()(const T &a, const T &b)</tt>
     * \tparam BlockPrefixCallbackOp    <b>[inferred]</b> Call-back functor type having member <tt>T operator()(T block_aggregate)</tt>
     */
    template <
        int             ITEMS_PER_THREAD,
        typename        ScanOp,
        typename        BlockPrefixCallbackOp>
    __device__ __forceinline__ void InclusiveScan(
        T                       (&input)[ITEMS_PER_THREAD],     ///< [in] Calling thread's input items
        T                       (&output)[ITEMS_PER_THREAD],    ///< [out] Calling thread's output items (may be aliased to \p input)
        ScanOp                  scan_op,                        ///< [in] Binary scan functor 
        BlockPrefixCallbackOp   &block_prefix_callback_op)      ///< [in-out] <b>[<em>warp</em><sub>0</sub> only]</b> Call-back functor for specifying a block-wide prefix to be applied to the logical input sequence.
    {
        if (ITEMS_PER_THREAD == 1)
        {
            InclusiveScan(input[0], output[0], scan_op, block_prefix_callback_op);
        }
        else
        {
            // Reduce consecutive thread items in registers
            T thread_prefix = internal::ThreadReduce(input, scan_op);

            // Exclusive thread block-scan
            ExclusiveScan(thread_prefix, thread_prefix, scan_op, block_prefix_callback_op);

            // Inclusive scan in registers with prefix as seed
            internal::ThreadScanInclusive(input, output, scan_op, thread_prefix);
        }
    }

    //@}  end member group


};

/**
 * \example example_block_scan.cu
 */

}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)

