
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
 * cub::DeviceSelect provides device-wide, parallel operations for selecting items from sequences of data items residing within device-accessible memory.
 */

#pragma once

#include <stdio.h>
#include <iterator>

#include "dispatch_scan.cuh"
#include "../../agent/agent_select_if.cuh"
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
 * Select kernel entry point (multi-block)
 *
 * Performs functor-based selection if SelectOpT functor type != NullType
 * Otherwise performs flag-based selection if FlagsInputIterator's value type != NullType
 * Otherwise performs discontinuity selection (keep unique)
 */
template <
    typename            AgentSelectIfPolicyT,       ///< Parameterized AgentSelectIfPolicyT tuning policy type
    typename            InputIteratorT,             ///< Random-access input iterator type for reading input items
    typename            FlagsInputIteratorT,        ///< Random-access input iterator type for reading selection flags (NullType* if a selection functor or discontinuity flagging is to be used for selection)
    typename            SelectedOutputIteratorT,    ///< Random-access output iterator type for writing selected items
    typename            NumSelectedIteratorT,       ///< Output iterator type for recording the number of items selected
    typename            ScanTileStateT,             ///< Tile status interface type
    typename            SelectOpT,                  ///< Selection operator type (NullType if selection flags or discontinuity flagging is to be used for selection)
    typename            EqualityOpT,                ///< Equality operator type (NullType if selection functor or selection flags is to be used for selection)
    typename            OffsetT,                    ///< Signed integer type for global offsets
    bool                KEEP_REJECTS>               ///< Whether or not we push rejected items to the back of the output
__launch_bounds__ (int(AgentSelectIfPolicyT::BLOCK_THREADS))
__global__ void DeviceSelectSweepKernel(
    InputIteratorT          d_in,                   ///< [in] Pointer to the input sequence of data items
    FlagsInputIteratorT     d_flags,                ///< [in] Pointer to the input sequence of selection flags (if applicable)
    SelectedOutputIteratorT d_selected_out,         ///< [out] Pointer to the output sequence of selected data items
    NumSelectedIteratorT    d_num_selected_out,     ///< [out] Pointer to the total number of items selected (i.e., length of \p d_selected_out)
    ScanTileStateT          tile_status,            ///< [in] Tile status interface
    SelectOpT               select_op,              ///< [in] Selection operator
    EqualityOpT             equality_op,            ///< [in] Equality operator
    OffsetT                 num_items,              ///< [in] Total number of input items (i.e., length of \p d_in)
    int                     num_tiles)              ///< [in] Total number of tiles for the entire problem
{
    // Thread block type for selecting data from input tiles
    typedef AgentSelectIf<
        AgentSelectIfPolicyT,
        InputIteratorT,
        FlagsInputIteratorT,
        SelectedOutputIteratorT,
        SelectOpT,
        EqualityOpT,
        OffsetT,
        KEEP_REJECTS> AgentSelectIfT;

    // Shared memory for AgentSelectIf
    __shared__ typename AgentSelectIfT::TempStorage temp_storage;

    // Process tiles
    AgentSelectIfT(temp_storage, d_in, d_flags, d_selected_out, select_op, equality_op, num_items).ConsumeRange(
        num_tiles,
        tile_status,
        d_num_selected_out);
}




/******************************************************************************
 * Dispatch
 ******************************************************************************/

/**
 * Utility class for dispatching the appropriately-tuned kernels for DeviceSelect
 */
template <
    typename    InputIteratorT,                 ///< Random-access input iterator type for reading input items
    typename    FlagsInputIteratorT,            ///< Random-access input iterator type for reading selection flags (NullType* if a selection functor or discontinuity flagging is to be used for selection)
    typename    SelectedOutputIteratorT,        ///< Random-access output iterator type for writing selected items
    typename    NumSelectedIteratorT,           ///< Output iterator type for recording the number of items selected
    typename    SelectOpT,                      ///< Selection operator type (NullType if selection flags or discontinuity flagging is to be used for selection)
    typename    EqualityOpT,                    ///< Equality operator type (NullType if selection functor or selection flags is to be used for selection)
    typename    OffsetT,                        ///< Signed integer type for global offsets
    bool        KEEP_REJECTS>                   ///< Whether or not we push rejected items to the back of the output
struct DispatchSelectIf
{
    /******************************************************************************
     * Types and constants
     ******************************************************************************/

    // The output value type
    typedef typename If<(Equals<typename std::iterator_traits<SelectedOutputIteratorT>::value_type, void>::VALUE),  // OutputT =  (if output iterator's value type is void) ?
        typename std::iterator_traits<InputIteratorT>::value_type,                                                  // ... then the input iterator's value type,
        typename std::iterator_traits<SelectedOutputIteratorT>::value_type>::Type OutputT;                          // ... else the output iterator's value type

    // The flag value type
    typedef typename std::iterator_traits<FlagsInputIteratorT>::value_type FlagT;

    enum
    {
        INIT_KERNEL_THREADS = 128,
    };

    // Tile status descriptor interface type
    typedef ScanTileState<OffsetT> ScanTileStateT;


    /******************************************************************************
     * Tuning policies
     ******************************************************************************/

    /// SM35
    struct Policy350
    {
        enum {
            NOMINAL_4B_ITEMS_PER_THREAD = 10,
            ITEMS_PER_THREAD            = CUB_MIN(NOMINAL_4B_ITEMS_PER_THREAD, CUB_MAX(1, (NOMINAL_4B_ITEMS_PER_THREAD * 4 / sizeof(OutputT)))),
        };

        typedef AgentSelectIfPolicy<
                128,
                ITEMS_PER_THREAD,
                BLOCK_LOAD_DIRECT,
                LOAD_LDG,
                BLOCK_SCAN_WARP_SCANS>
            SelectIfPolicyT;
    };

    /// SM30
    struct Policy300
    {
        enum {
            NOMINAL_4B_ITEMS_PER_THREAD = 7,
            ITEMS_PER_THREAD            = CUB_MIN(NOMINAL_4B_ITEMS_PER_THREAD, CUB_MAX(3, (NOMINAL_4B_ITEMS_PER_THREAD * 4 / sizeof(OutputT)))),
        };

        typedef AgentSelectIfPolicy<
                128,
                ITEMS_PER_THREAD,
                BLOCK_LOAD_WARP_TRANSPOSE,
                LOAD_DEFAULT,
                BLOCK_SCAN_WARP_SCANS>
            SelectIfPolicyT;
    };

    /// SM20
    struct Policy200
    {
        enum {
            NOMINAL_4B_ITEMS_PER_THREAD = (KEEP_REJECTS) ? 7 : 15,
            ITEMS_PER_THREAD            = CUB_MIN(NOMINAL_4B_ITEMS_PER_THREAD, CUB_MAX(1, (NOMINAL_4B_ITEMS_PER_THREAD * 4 / sizeof(OutputT)))),
        };

        typedef AgentSelectIfPolicy<
                128,
                ITEMS_PER_THREAD,
                BLOCK_LOAD_WARP_TRANSPOSE,
                LOAD_DEFAULT,
                BLOCK_SCAN_WARP_SCANS>
            SelectIfPolicyT;
    };

    /// SM13
    struct Policy130
    {
        enum {
            NOMINAL_4B_ITEMS_PER_THREAD = 9,
            ITEMS_PER_THREAD            = CUB_MIN(NOMINAL_4B_ITEMS_PER_THREAD, CUB_MAX(1, (NOMINAL_4B_ITEMS_PER_THREAD * 4 / sizeof(OutputT)))),
        };

        typedef AgentSelectIfPolicy<
                64,
                ITEMS_PER_THREAD,
                BLOCK_LOAD_WARP_TRANSPOSE,
                LOAD_DEFAULT,
                BLOCK_SCAN_RAKING_MEMOIZE>
            SelectIfPolicyT;
    };

    /// SM10
    struct Policy100
    {
        enum {
            NOMINAL_4B_ITEMS_PER_THREAD = 9,
            ITEMS_PER_THREAD            = CUB_MIN(NOMINAL_4B_ITEMS_PER_THREAD, CUB_MAX(1, (NOMINAL_4B_ITEMS_PER_THREAD * 4 / sizeof(OutputT)))),
        };

        typedef AgentSelectIfPolicy<
                64,
                ITEMS_PER_THREAD,
                BLOCK_LOAD_WARP_TRANSPOSE,
                LOAD_DEFAULT,
                BLOCK_SCAN_RAKING>
            SelectIfPolicyT;
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
    typedef Policy100 PtxPolicy;

#endif

    // "Opaque" policies (whose parameterizations aren't reflected in the type signature)
    struct PtxSelectIfPolicyT : PtxPolicy::SelectIfPolicyT {};


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
        KernelConfig    &select_if_config)
    {
    #if (CUB_PTX_ARCH > 0)
        (void)ptx_version;

        // We're on the device, so initialize the kernel dispatch configurations with the current PTX policy
        select_if_config.template Init<PtxSelectIfPolicyT>();

    #else

        // We're on the host, so lookup and initialize the kernel dispatch configurations with the policies that match the device's PTX version
        if (ptx_version >= 350)
        {
            select_if_config.template Init<typename Policy350::SelectIfPolicyT>();
        }
        else if (ptx_version >= 300)
        {
            select_if_config.template Init<typename Policy300::SelectIfPolicyT>();
        }
        else if (ptx_version >= 200)
        {
            select_if_config.template Init<typename Policy200::SelectIfPolicyT>();
        }
        else if (ptx_version >= 130)
        {
            select_if_config.template Init<typename Policy130::SelectIfPolicyT>();
        }
        else
        {
            select_if_config.template Init<typename Policy100::SelectIfPolicyT>();
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


    /******************************************************************************
     * Dispatch entrypoints
     ******************************************************************************/

    /**
     * Internal dispatch routine for computing a device-wide selection using the
     * specified kernel functions.
     */
    template <
        typename                    ScanInitKernelPtrT,             ///< Function type of cub::DeviceScanInitKernel
        typename                    SelectIfKernelPtrT>             ///< Function type of cub::SelectIfKernelPtrT
    CUB_RUNTIME_FUNCTION __forceinline__
    static cudaError_t Dispatch(
        void*                       d_temp_storage,                 ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
        size_t&                     temp_storage_bytes,             ///< [in,out] Reference to size in bytes of \p d_temp_storage allocation
        InputIteratorT              d_in,                           ///< [in] Pointer to the input sequence of data items
        FlagsInputIteratorT         d_flags,                        ///< [in] Pointer to the input sequence of selection flags (if applicable)
        SelectedOutputIteratorT     d_selected_out,                 ///< [in] Pointer to the output sequence of selected data items
        NumSelectedIteratorT        d_num_selected_out,             ///< [in] Pointer to the total number of items selected (i.e., length of \p d_selected_out)
        SelectOpT                   select_op,                      ///< [in] Selection operator
        EqualityOpT                 equality_op,                    ///< [in] Equality operator
        OffsetT                     num_items,                      ///< [in] Total number of input items (i.e., length of \p d_in)
        cudaStream_t                stream,                         ///< [in] CUDA stream to launch kernels within.  Default is stream<sub>0</sub>.
        bool                        debug_synchronous,              ///< [in] Whether or not to synchronize the stream after every kernel launch to check for errors.  Also causes launch configurations to be printed to the console.  Default is \p false.
        int                         /*ptx_version*/,                ///< [in] PTX version of dispatch kernels
        ScanInitKernelPtrT          scan_init_kernel,               ///< [in] Kernel function pointer to parameterization of cub::DeviceScanInitKernel
        SelectIfKernelPtrT          select_if_kernel,               ///< [in] Kernel function pointer to parameterization of cub::DeviceSelectSweepKernel
        KernelConfig                select_if_config)               ///< [in] Dispatch parameters that match the policy that \p select_if_kernel was compiled for
    {

#ifndef CUB_RUNTIME_ENABLED
        (void)d_temp_storage;
        (void)temp_storage_bytes;
        (void)d_in;
        (void)d_flags;
        (void)d_selected_out;
        (void)d_num_selected_out;
        (void)select_op;
        (void)equality_op;
        (void)num_items;
        (void)stream;
        (void)debug_synchronous;
        (void)scan_init_kernel;
        (void)select_if_kernel;
        (void)select_if_config;

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
            int tile_size = select_if_config.block_threads * select_if_config.items_per_thread;
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
            ScanTileStateT tile_status;
            if (CubDebug(error = tile_status.Init(num_tiles, allocations[0], allocation_sizes[0]))) break;

            // Log scan_init_kernel configuration
            int init_grid_size = CUB_MAX(1, (num_tiles + INIT_KERNEL_THREADS - 1) / INIT_KERNEL_THREADS);
            if (debug_synchronous) _CubLog("Invoking scan_init_kernel<<<%d, %d, 0, %lld>>>()\n", init_grid_size, INIT_KERNEL_THREADS, (long long) stream);

            // Invoke scan_init_kernel to initialize tile descriptors
            scan_init_kernel<<<init_grid_size, INIT_KERNEL_THREADS, 0, stream>>>(
                tile_status,
                num_tiles,
                d_num_selected_out);

            // Check for failure to launch
            if (CubDebug(error = cudaPeekAtLastError())) break;

            // Sync the stream if specified to flush runtime errors
            if (debug_synchronous && (CubDebug(error = SyncStream(stream)))) break;

            // Return if empty problem
            if (num_items == 0)
                break;

            // Get SM occupancy for select_if_kernel
            int range_select_sm_occupancy;
            if (CubDebug(error = MaxSmOccupancy(
                range_select_sm_occupancy,            // out
                select_if_kernel,
                select_if_config.block_threads))) break;

            // Get max x-dimension of grid
            int max_dim_x;
            if (CubDebug(error = cudaDeviceGetAttribute(&max_dim_x, cudaDevAttrMaxGridDimX, device_ordinal))) break;;

            // Get grid size for scanning tiles
            dim3 scan_grid_size;
            scan_grid_size.z = 1;
            scan_grid_size.y = ((unsigned int) num_tiles + max_dim_x - 1) / max_dim_x;
            scan_grid_size.x = CUB_MIN(num_tiles, max_dim_x);

            // Log select_if_kernel configuration
            if (debug_synchronous) _CubLog("Invoking select_if_kernel<<<{%d,%d,%d}, %d, 0, %lld>>>(), %d items per thread, %d SM occupancy\n",
                scan_grid_size.x, scan_grid_size.y, scan_grid_size.z, select_if_config.block_threads, (long long) stream, select_if_config.items_per_thread, range_select_sm_occupancy);

            // Invoke select_if_kernel
            select_if_kernel<<<scan_grid_size, select_if_config.block_threads, 0, stream>>>(
                d_in,
                d_flags,
                d_selected_out,
                d_num_selected_out,
                tile_status,
                select_op,
                equality_op,
                num_items,
                num_tiles);

            // Check for failure to launch
            if (CubDebug(error = cudaPeekAtLastError())) break;

            // Sync the stream if specified to flush runtime errors
            if (debug_synchronous && (CubDebug(error = SyncStream(stream)))) break;
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
        InputIteratorT              d_in,                           ///< [in] Pointer to the input sequence of data items
        FlagsInputIteratorT         d_flags,                        ///< [in] Pointer to the input sequence of selection flags (if applicable)
        SelectedOutputIteratorT     d_selected_out,                 ///< [in] Pointer to the output sequence of selected data items
        NumSelectedIteratorT        d_num_selected_out,             ///< [in] Pointer to the total number of items selected (i.e., length of \p d_selected_out)
        SelectOpT                   select_op,                      ///< [in] Selection operator
        EqualityOpT                 equality_op,                    ///< [in] Equality operator
        OffsetT                     num_items,                      ///< [in] Total number of input items (i.e., length of \p d_in)
        cudaStream_t                stream,                         ///< [in] <b>[optional]</b> CUDA stream to launch kernels within.  Default is stream<sub>0</sub>.
        bool                        debug_synchronous)              ///< [in] <b>[optional]</b> Whether or not to synchronize the stream after every kernel launch to check for errors.  Also causes launch configurations to be printed to the console.  Default is \p false.
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
            KernelConfig select_if_config;
            InitConfigs(ptx_version, select_if_config);

            // Dispatch
            if (CubDebug(error = Dispatch(
                d_temp_storage,
                temp_storage_bytes,
                d_in,
                d_flags,
                d_selected_out,
                d_num_selected_out,
                select_op,
                equality_op,
                num_items,
                stream,
                debug_synchronous,
                ptx_version,
                DeviceCompactInitKernel<ScanTileStateT, NumSelectedIteratorT>,
                DeviceSelectSweepKernel<PtxSelectIfPolicyT, InputIteratorT, FlagsInputIteratorT, SelectedOutputIteratorT, NumSelectedIteratorT, ScanTileStateT, SelectOpT, EqualityOpT, OffsetT, KEEP_REJECTS>,
                select_if_config))) break;
        }
        while (0);

        return error;
    }
};


}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)


