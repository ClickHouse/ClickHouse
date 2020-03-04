
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
 * cub::DeviceReduceByKey provides device-wide, parallel operations for reducing segments of values residing within device-accessible memory.
 */

#pragma once

#include <stdio.h>
#include <iterator>

#include "dispatch_scan.cuh"
#include "../../agent/agent_reduce_by_key.cuh"
#include "../../thread/thread_operators.cuh"
#include "../../grid/grid_queue.cuh"
#include "../../util_device.cuh"
#include "../../util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {

/******************************************************************************
 * Kernel entry points
 *****************************************************************************/

/**
 * Multi-block reduce-by-key sweep kernel entry point
 */
template <
    typename            AgentReduceByKeyPolicyT,                 ///< Parameterized AgentReduceByKeyPolicyT tuning policy type
    typename            KeysInputIteratorT,                     ///< Random-access input iterator type for keys
    typename            UniqueOutputIteratorT,                  ///< Random-access output iterator type for keys
    typename            ValuesInputIteratorT,                   ///< Random-access input iterator type for values
    typename            AggregatesOutputIteratorT,              ///< Random-access output iterator type for values
    typename            NumRunsOutputIteratorT,                 ///< Output iterator type for recording number of segments encountered
    typename            ScanTileStateT,                         ///< Tile status interface type
    typename            EqualityOpT,                            ///< KeyT equality operator type
    typename            ReductionOpT,                           ///< ValueT reduction operator type
    typename            OffsetT>                                ///< Signed integer type for global offsets
__launch_bounds__ (int(AgentReduceByKeyPolicyT::BLOCK_THREADS))
__global__ void DeviceReduceByKeyKernel(
    KeysInputIteratorT          d_keys_in,                      ///< Pointer to the input sequence of keys
    UniqueOutputIteratorT       d_unique_out,                   ///< Pointer to the output sequence of unique keys (one key per run)
    ValuesInputIteratorT        d_values_in,                    ///< Pointer to the input sequence of corresponding values
    AggregatesOutputIteratorT   d_aggregates_out,               ///< Pointer to the output sequence of value aggregates (one aggregate per run)
    NumRunsOutputIteratorT      d_num_runs_out,                 ///< Pointer to total number of runs encountered (i.e., the length of d_unique_out)
    ScanTileStateT              tile_state,                     ///< Tile status interface
    int                         start_tile,                     ///< The starting tile for the current grid
    EqualityOpT                 equality_op,                    ///< KeyT equality operator
    ReductionOpT                reduction_op,                   ///< ValueT reduction operator
    OffsetT                     num_items)                      ///< Total number of items to select from
{
    // Thread block type for reducing tiles of value segments
    typedef AgentReduceByKey<
            AgentReduceByKeyPolicyT,
            KeysInputIteratorT,
            UniqueOutputIteratorT,
            ValuesInputIteratorT,
            AggregatesOutputIteratorT,
            NumRunsOutputIteratorT,
            EqualityOpT,
            ReductionOpT,
            OffsetT>
        AgentReduceByKeyT;

    // Shared memory for AgentReduceByKey
    __shared__ typename AgentReduceByKeyT::TempStorage temp_storage;

    // Process tiles
    AgentReduceByKeyT(temp_storage, d_keys_in, d_unique_out, d_values_in, d_aggregates_out, d_num_runs_out, equality_op, reduction_op).ConsumeRange(
        num_items,
        tile_state,
        start_tile);
}




/******************************************************************************
 * Dispatch
 ******************************************************************************/

/**
 * Utility class for dispatching the appropriately-tuned kernels for DeviceReduceByKey
 */
template <
    typename    KeysInputIteratorT,         ///< Random-access input iterator type for keys
    typename    UniqueOutputIteratorT,      ///< Random-access output iterator type for keys
    typename    ValuesInputIteratorT,       ///< Random-access input iterator type for values
    typename    AggregatesOutputIteratorT,  ///< Random-access output iterator type for values
    typename    NumRunsOutputIteratorT,     ///< Output iterator type for recording number of segments encountered
    typename    EqualityOpT,                ///< KeyT equality operator type
    typename    ReductionOpT,               ///< ValueT reduction operator type
    typename    OffsetT>                    ///< Signed integer type for global offsets
struct DispatchReduceByKey
{
    //-------------------------------------------------------------------------
    // Types and constants
    //-------------------------------------------------------------------------

    // The input keys type
    typedef typename std::iterator_traits<KeysInputIteratorT>::value_type KeyInputT;

    // The output keys type
    typedef typename If<(Equals<typename std::iterator_traits<UniqueOutputIteratorT>::value_type, void>::VALUE),    // KeyOutputT =  (if output iterator's value type is void) ?
        typename std::iterator_traits<KeysInputIteratorT>::value_type,                                              // ... then the input iterator's value type,
        typename std::iterator_traits<UniqueOutputIteratorT>::value_type>::Type KeyOutputT;                         // ... else the output iterator's value type

    // The input values type
    typedef typename std::iterator_traits<ValuesInputIteratorT>::value_type ValueInputT;

    // The output values type
    typedef typename If<(Equals<typename std::iterator_traits<AggregatesOutputIteratorT>::value_type, void>::VALUE),    // ValueOutputT =  (if output iterator's value type is void) ?
        typename std::iterator_traits<ValuesInputIteratorT>::value_type,                                                // ... then the input iterator's value type,
        typename std::iterator_traits<AggregatesOutputIteratorT>::value_type>::Type ValueOutputT;                       // ... else the output iterator's value type

    enum
    {
        INIT_KERNEL_THREADS     = 128,
        MAX_INPUT_BYTES         = CUB_MAX(sizeof(KeyOutputT), sizeof(ValueOutputT)),
        COMBINED_INPUT_BYTES    = sizeof(KeyOutputT) + sizeof(ValueOutputT),
    };

    // Tile status descriptor interface type
    typedef ReduceByKeyScanTileState<ValueOutputT, OffsetT> ScanTileStateT;


    //-------------------------------------------------------------------------
    // Tuning policies
    //-------------------------------------------------------------------------

    /// SM35
    struct Policy350
    {
        enum {
            NOMINAL_4B_ITEMS_PER_THREAD = 6,
            ITEMS_PER_THREAD            = (MAX_INPUT_BYTES <= 8) ? 6 : CUB_MIN(NOMINAL_4B_ITEMS_PER_THREAD, CUB_MAX(1, ((NOMINAL_4B_ITEMS_PER_THREAD * 8) + COMBINED_INPUT_BYTES - 1) / COMBINED_INPUT_BYTES)),
        };

        typedef AgentReduceByKeyPolicy<
                128,
                ITEMS_PER_THREAD,
                BLOCK_LOAD_DIRECT,
                LOAD_LDG,
                BLOCK_SCAN_WARP_SCANS>
            ReduceByKeyPolicyT;
    };

    /// SM30
    struct Policy300
    {
        enum {
            NOMINAL_4B_ITEMS_PER_THREAD = 6,
            ITEMS_PER_THREAD            = CUB_MIN(NOMINAL_4B_ITEMS_PER_THREAD, CUB_MAX(1, ((NOMINAL_4B_ITEMS_PER_THREAD * 8) + COMBINED_INPUT_BYTES - 1) / COMBINED_INPUT_BYTES)),
        };

        typedef AgentReduceByKeyPolicy<
                128,
                ITEMS_PER_THREAD,
                BLOCK_LOAD_WARP_TRANSPOSE,
                LOAD_DEFAULT,
                BLOCK_SCAN_WARP_SCANS>
            ReduceByKeyPolicyT;
    };

    /// SM20
    struct Policy200
    {
        enum {
            NOMINAL_4B_ITEMS_PER_THREAD = 11,
            ITEMS_PER_THREAD            = CUB_MIN(NOMINAL_4B_ITEMS_PER_THREAD, CUB_MAX(1, ((NOMINAL_4B_ITEMS_PER_THREAD * 8) + COMBINED_INPUT_BYTES - 1) / COMBINED_INPUT_BYTES)),
        };

        typedef AgentReduceByKeyPolicy<
                128,
                ITEMS_PER_THREAD,
                BLOCK_LOAD_WARP_TRANSPOSE,
                LOAD_DEFAULT,
                BLOCK_SCAN_WARP_SCANS>
            ReduceByKeyPolicyT;
    };

    /// SM13
    struct Policy130
    {
        enum {
            NOMINAL_4B_ITEMS_PER_THREAD = 7,
            ITEMS_PER_THREAD            = CUB_MIN(NOMINAL_4B_ITEMS_PER_THREAD, CUB_MAX(1, ((NOMINAL_4B_ITEMS_PER_THREAD * 8) + COMBINED_INPUT_BYTES - 1) / COMBINED_INPUT_BYTES)),
        };

        typedef AgentReduceByKeyPolicy<
                128,
                ITEMS_PER_THREAD,
                BLOCK_LOAD_WARP_TRANSPOSE,
                LOAD_DEFAULT,
                BLOCK_SCAN_WARP_SCANS>
            ReduceByKeyPolicyT;
    };

    /// SM11
    struct Policy110
    {
        enum {
            NOMINAL_4B_ITEMS_PER_THREAD = 5,
            ITEMS_PER_THREAD            = CUB_MIN(NOMINAL_4B_ITEMS_PER_THREAD, CUB_MAX(1, (NOMINAL_4B_ITEMS_PER_THREAD * 8) / COMBINED_INPUT_BYTES)),
        };

        typedef AgentReduceByKeyPolicy<
                64,
                ITEMS_PER_THREAD,
                BLOCK_LOAD_WARP_TRANSPOSE,
                LOAD_DEFAULT,
                BLOCK_SCAN_RAKING>
            ReduceByKeyPolicyT;
    };


    /******************************************************************************
     * Tuning policies of current PTX compiler pass
     ******************************************************************************/

#if (CUB_PTX_ARCH >= 350)
    typedef Policy350 PtxPolicy;

#elif (CUB_PTX_ARCH >= 300)
    typedef Policy300 PtxPolicy;

#elif (CUB_PTX_ARCH >= 200)
    typedef Policy200 PtxPolicy;

#elif (CUB_PTX_ARCH >= 130)
    typedef Policy130 PtxPolicy;

#else
    typedef Policy110 PtxPolicy;

#endif

    // "Opaque" policies (whose parameterizations aren't reflected in the type signature)
    struct PtxReduceByKeyPolicy : PtxPolicy::ReduceByKeyPolicyT {};


    /******************************************************************************
     * Utilities
     ******************************************************************************/

    /**
     * Initialize kernel dispatch configurations with the policies corresponding to the PTX assembly we will use
     */
    template <typename KernelConfig>
    CUB_RUNTIME_FUNCTION __forceinline__
    static void InitConfigs(
        int             ptx_version,
        KernelConfig    &reduce_by_key_config)
    {
    #if (CUB_PTX_ARCH > 0)
        (void)ptx_version;

        // We're on the device, so initialize the kernel dispatch configurations with the current PTX policy
        reduce_by_key_config.template Init<PtxReduceByKeyPolicy>();

    #else

        // We're on the host, so lookup and initialize the kernel dispatch configurations with the policies that match the device's PTX version
        if (ptx_version >= 350)
        {
            reduce_by_key_config.template Init<typename Policy350::ReduceByKeyPolicyT>();
        }
        else if (ptx_version >= 300)
        {
            reduce_by_key_config.template Init<typename Policy300::ReduceByKeyPolicyT>();
        }
        else if (ptx_version >= 200)
        {
            reduce_by_key_config.template Init<typename Policy200::ReduceByKeyPolicyT>();
        }
        else if (ptx_version >= 130)
        {
            reduce_by_key_config.template Init<typename Policy130::ReduceByKeyPolicyT>();
        }
        else
        {
            reduce_by_key_config.template Init<typename Policy110::ReduceByKeyPolicyT>();
        }

    #endif
    }


    /**
     * Kernel kernel dispatch configuration.
     */
    struct KernelConfig
    {
        int block_threads;
        int items_per_thread;
        int tile_items;

        template <typename PolicyT>
        CUB_RUNTIME_FUNCTION __forceinline__
        void Init()
        {
            block_threads       = PolicyT::BLOCK_THREADS;
            items_per_thread    = PolicyT::ITEMS_PER_THREAD;
            tile_items          = block_threads * items_per_thread;
        }
    };


    //---------------------------------------------------------------------
    // Dispatch entrypoints
    //---------------------------------------------------------------------

    /**
     * Internal dispatch routine for computing a device-wide reduce-by-key using the
     * specified kernel functions.
     */
    template <
        typename                    ScanInitKernelT,         ///< Function type of cub::DeviceScanInitKernel
        typename                    ReduceByKeyKernelT>      ///< Function type of cub::DeviceReduceByKeyKernelT
    CUB_RUNTIME_FUNCTION __forceinline__
    static cudaError_t Dispatch(
        void*                       d_temp_storage,             ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
        size_t&                     temp_storage_bytes,         ///< [in,out] Reference to size in bytes of \p d_temp_storage allocation
        KeysInputIteratorT          d_keys_in,                  ///< [in] Pointer to the input sequence of keys
        UniqueOutputIteratorT       d_unique_out,               ///< [out] Pointer to the output sequence of unique keys (one key per run)
        ValuesInputIteratorT        d_values_in,                ///< [in] Pointer to the input sequence of corresponding values
        AggregatesOutputIteratorT   d_aggregates_out,           ///< [out] Pointer to the output sequence of value aggregates (one aggregate per run)
        NumRunsOutputIteratorT      d_num_runs_out,             ///< [out] Pointer to total number of runs encountered (i.e., the length of d_unique_out)
        EqualityOpT                 equality_op,                ///< [in] KeyT equality operator
        ReductionOpT                reduction_op,               ///< [in] ValueT reduction operator
        OffsetT                     num_items,                  ///< [in] Total number of items to select from
        cudaStream_t                stream,                     ///< [in] CUDA stream to launch kernels within.  Default is stream<sub>0</sub>.
        bool                        debug_synchronous,          ///< [in] Whether or not to synchronize the stream after every kernel launch to check for errors.  Also causes launch configurations to be printed to the console.  Default is \p false.
        int                         /*ptx_version*/,            ///< [in] PTX version of dispatch kernels
        ScanInitKernelT                init_kernel,                ///< [in] Kernel function pointer to parameterization of cub::DeviceScanInitKernel
        ReduceByKeyKernelT             reduce_by_key_kernel,       ///< [in] Kernel function pointer to parameterization of cub::DeviceReduceByKeyKernel
        KernelConfig                reduce_by_key_config)       ///< [in] Dispatch parameters that match the policy that \p reduce_by_key_kernel was compiled for
    {

#ifndef CUB_RUNTIME_ENABLED
      (void)d_temp_storage;
      (void)temp_storage_bytes;
      (void)d_keys_in;
      (void)d_unique_out;
      (void)d_values_in;
      (void)d_aggregates_out;
      (void)d_num_runs_out;
      (void)equality_op;
      (void)reduction_op;
      (void)num_items;
      (void)stream;
      (void)debug_synchronous;
      (void)init_kernel;
      (void)reduce_by_key_kernel;
      (void)reduce_by_key_config;

        // Kernel launch not supported from this device
        return CubDebug(cudaErrorNotSupported);

#else

        cudaError error = cudaSuccess;
        do
        {
            // Get device ordinal
            int device_ordinal;
            if (CubDebug(error = cudaGetDevice(&device_ordinal))) break;

            // Get SM count
            int sm_count;
            if (CubDebug(error = cudaDeviceGetAttribute (&sm_count, cudaDevAttrMultiProcessorCount, device_ordinal))) break;

            // Number of input tiles
            int tile_size = reduce_by_key_config.block_threads * reduce_by_key_config.items_per_thread;
            int num_tiles = (num_items + tile_size - 1) / tile_size;

            // Specify temporary storage allocation requirements
            size_t  allocation_sizes[1];
            if (CubDebug(error = ScanTileStateT::AllocationSize(num_tiles, allocation_sizes[0]))) break;    // bytes needed for tile status descriptors

            // Compute allocation pointers into the single storage blob (or compute the necessary size of the blob)
            void* allocations[1];
            if (CubDebug(error = AliasTemporaries(d_temp_storage, temp_storage_bytes, allocations, allocation_sizes))) break;
            if (d_temp_storage == NULL)
            {
                // Return if the caller is simply requesting the size of the storage allocation
                break;
            }

            // Construct the tile status interface
            ScanTileStateT tile_state;
            if (CubDebug(error = tile_state.Init(num_tiles, allocations[0], allocation_sizes[0]))) break;

            // Log init_kernel configuration
            int init_grid_size = CUB_MAX(1, (num_tiles + INIT_KERNEL_THREADS - 1) / INIT_KERNEL_THREADS);
            if (debug_synchronous) _CubLog("Invoking init_kernel<<<%d, %d, 0, %lld>>>()\n", init_grid_size, INIT_KERNEL_THREADS, (long long) stream);

            // Invoke init_kernel to initialize tile descriptors
            init_kernel<<<init_grid_size, INIT_KERNEL_THREADS, 0, stream>>>(
                tile_state,
                num_tiles,
                d_num_runs_out);

            // Check for failure to launch
            if (CubDebug(error = cudaPeekAtLastError())) break;

            // Sync the stream if specified to flush runtime errors
            if (debug_synchronous && (CubDebug(error = SyncStream(stream)))) break;

            // Return if empty problem
            if (num_items == 0)
                break;

            // Get SM occupancy for reduce_by_key_kernel
            int reduce_by_key_sm_occupancy;
            if (CubDebug(error = MaxSmOccupancy(
                reduce_by_key_sm_occupancy,            // out
                reduce_by_key_kernel,
                reduce_by_key_config.block_threads))) break;

            // Get max x-dimension of grid
            int max_dim_x;
            if (CubDebug(error = cudaDeviceGetAttribute(&max_dim_x, cudaDevAttrMaxGridDimX, device_ordinal))) break;;

            // Run grids in epochs (in case number of tiles exceeds max x-dimension
            int scan_grid_size = CUB_MIN(num_tiles, max_dim_x);
            for (int start_tile = 0; start_tile < num_tiles; start_tile += scan_grid_size)
            {
                // Log reduce_by_key_kernel configuration
                if (debug_synchronous) _CubLog("Invoking %d reduce_by_key_kernel<<<%d, %d, 0, %lld>>>(), %d items per thread, %d SM occupancy\n",
                    start_tile, scan_grid_size, reduce_by_key_config.block_threads, (long long) stream, reduce_by_key_config.items_per_thread, reduce_by_key_sm_occupancy);

                // Invoke reduce_by_key_kernel
                reduce_by_key_kernel<<<scan_grid_size, reduce_by_key_config.block_threads, 0, stream>>>(
                    d_keys_in,
                    d_unique_out,
                    d_values_in,
                    d_aggregates_out,
                    d_num_runs_out,
                    tile_state,
                    start_tile,
                    equality_op,
                    reduction_op,
                    num_items);

                // Check for failure to launch
                if (CubDebug(error = cudaPeekAtLastError())) break;

                // Sync the stream if specified to flush runtime errors
                if (debug_synchronous && (CubDebug(error = SyncStream(stream)))) break;
            }
        }
        while (0);

        return error;

#endif  // CUB_RUNTIME_ENABLED
    }


    /**
     * Internal dispatch routine
     */
    CUB_RUNTIME_FUNCTION __forceinline__
    static cudaError_t Dispatch(
        void*                       d_temp_storage,                 ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
        size_t&                     temp_storage_bytes,             ///< [in,out] Reference to size in bytes of \p d_temp_storage allocation
        KeysInputIteratorT          d_keys_in,                      ///< [in] Pointer to the input sequence of keys
        UniqueOutputIteratorT       d_unique_out,                   ///< [out] Pointer to the output sequence of unique keys (one key per run)
        ValuesInputIteratorT        d_values_in,                    ///< [in] Pointer to the input sequence of corresponding values
        AggregatesOutputIteratorT   d_aggregates_out,               ///< [out] Pointer to the output sequence of value aggregates (one aggregate per run)
        NumRunsOutputIteratorT      d_num_runs_out,                 ///< [out] Pointer to total number of runs encountered (i.e., the length of d_unique_out)
        EqualityOpT                 equality_op,                    ///< [in] KeyT equality operator
        ReductionOpT                reduction_op,                   ///< [in] ValueT reduction operator
        OffsetT                     num_items,                      ///< [in] Total number of items to select from
        cudaStream_t                stream,                         ///< [in] CUDA stream to launch kernels within.  Default is stream<sub>0</sub>.
        bool                        debug_synchronous)              ///< [in] Whether or not to synchronize the stream after every kernel launch to check for errors.  Also causes launch configurations to be printed to the console.  Default is \p false.
    {
        cudaError error = cudaSuccess;
        do
        {
            // Get PTX version
            int ptx_version;
    #if (CUB_PTX_ARCH == 0)
            if (CubDebug(error = PtxVersion(ptx_version))) break;
    #else
            ptx_version = CUB_PTX_ARCH;
    #endif

            // Get kernel kernel dispatch configurations
            KernelConfig reduce_by_key_config;
            InitConfigs(ptx_version, reduce_by_key_config);

            // Dispatch
            if (CubDebug(error = Dispatch(
                d_temp_storage,
                temp_storage_bytes,
                d_keys_in,
                d_unique_out,
                d_values_in,
                d_aggregates_out,
                d_num_runs_out,
                equality_op,
                reduction_op,
                num_items,
                stream,
                debug_synchronous,
                ptx_version,
                DeviceCompactInitKernel<ScanTileStateT, NumRunsOutputIteratorT>,
                DeviceReduceByKeyKernel<PtxReduceByKeyPolicy, KeysInputIteratorT, UniqueOutputIteratorT, ValuesInputIteratorT, AggregatesOutputIteratorT, NumRunsOutputIteratorT, ScanTileStateT, EqualityOpT, ReductionOpT, OffsetT>,
                reduce_by_key_config))) break;
        }
        while (0);

        return error;
    }
};

}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)


