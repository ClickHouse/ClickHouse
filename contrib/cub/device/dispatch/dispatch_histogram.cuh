
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
 * cub::DeviceHistogram provides device-wide parallel operations for constructing histogram(s) from a sequence of samples data residing within device-accessible memory.
 */

#pragma once

#include <stdio.h>
#include <iterator>
#include <limits>

#include "../../agent/agent_histogram.cuh"
#include "../../util_debug.cuh"
#include "../../util_device.cuh"
#include "../../thread/thread_search.cuh"
#include "../../grid/grid_queue.cuh"
#include "../../util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {



/******************************************************************************
 * Histogram kernel entry points
 *****************************************************************************/

/**
 * Histogram initialization kernel entry point
 */
template <
    int                                             NUM_ACTIVE_CHANNELS,            ///< Number of channels actively being histogrammed
    typename                                        CounterT,                       ///< Integer type for counting sample occurrences per histogram bin
    typename                                        OffsetT>                        ///< Signed integer type for global offsets
__global__ void DeviceHistogramInitKernel(
    ArrayWrapper<int, NUM_ACTIVE_CHANNELS>          num_output_bins_wrapper,        ///< Number of output histogram bins per channel
    ArrayWrapper<CounterT*, NUM_ACTIVE_CHANNELS>    d_output_histograms_wrapper,    ///< Histogram counter data having logical dimensions <tt>CounterT[NUM_ACTIVE_CHANNELS][num_bins.array[CHANNEL]]</tt>
    GridQueue<int>                                  tile_queue)                     ///< Drain queue descriptor for dynamically mapping tile data onto thread blocks
{
    if ((threadIdx.x == 0) && (blockIdx.x == 0))
        tile_queue.ResetDrain();

    int output_bin = (blockIdx.x * blockDim.x) + threadIdx.x;

    #pragma unroll
    for (int CHANNEL = 0; CHANNEL < NUM_ACTIVE_CHANNELS; ++CHANNEL)
    {
        if (output_bin < num_output_bins_wrapper.array[CHANNEL])
            d_output_histograms_wrapper.array[CHANNEL][output_bin] = 0;
    }
}


/**
 * Histogram privatized sweep kernel entry point (multi-block).  Computes privatized histograms, one per thread block.
 */
template <
    typename                                            AgentHistogramPolicyT,     ///< Parameterized AgentHistogramPolicy tuning policy type
    int                                                 PRIVATIZED_SMEM_BINS,           ///< Maximum number of histogram bins per channel (e.g., up to 256)
    int                                                 NUM_CHANNELS,                   ///< Number of channels interleaved in the input data (may be greater than the number of channels being actively histogrammed)
    int                                                 NUM_ACTIVE_CHANNELS,            ///< Number of channels actively being histogrammed
    typename                                            SampleIteratorT,                ///< The input iterator type. \iterator.
    typename                                            CounterT,                       ///< Integer type for counting sample occurrences per histogram bin
    typename                                            PrivatizedDecodeOpT,            ///< The transform operator type for determining privatized counter indices from samples, one for each channel
    typename                                            OutputDecodeOpT,                ///< The transform operator type for determining output bin-ids from privatized counter indices, one for each channel
    typename                                            OffsetT>                        ///< Signed integer type for global offsets
__launch_bounds__ (int(AgentHistogramPolicyT::BLOCK_THREADS))
__global__ void DeviceHistogramSweepKernel(
    SampleIteratorT                                         d_samples,                          ///< Input data to reduce
    ArrayWrapper<int, NUM_ACTIVE_CHANNELS>                  num_output_bins_wrapper,            ///< The number bins per final output histogram
    ArrayWrapper<int, NUM_ACTIVE_CHANNELS>                  num_privatized_bins_wrapper,        ///< The number bins per privatized histogram
    ArrayWrapper<CounterT*, NUM_ACTIVE_CHANNELS>            d_output_histograms_wrapper,        ///< Reference to final output histograms
    ArrayWrapper<CounterT*, NUM_ACTIVE_CHANNELS>            d_privatized_histograms_wrapper,    ///< Reference to privatized histograms
    ArrayWrapper<OutputDecodeOpT, NUM_ACTIVE_CHANNELS>      output_decode_op_wrapper,           ///< The transform operator for determining output bin-ids from privatized counter indices, one for each channel
    ArrayWrapper<PrivatizedDecodeOpT, NUM_ACTIVE_CHANNELS>  privatized_decode_op_wrapper,       ///< The transform operator for determining privatized counter indices from samples, one for each channel
    OffsetT                                                 num_row_pixels,                     ///< The number of multi-channel pixels per row in the region of interest
    OffsetT                                                 num_rows,                           ///< The number of rows in the region of interest
    OffsetT                                                 row_stride_samples,                 ///< The number of samples between starts of consecutive rows in the region of interest
    int                                                     tiles_per_row,                      ///< Number of image tiles per row
    GridQueue<int>                                          tile_queue)                         ///< Drain queue descriptor for dynamically mapping tile data onto thread blocks
{
    // Thread block type for compositing input tiles
    typedef AgentHistogram<
            AgentHistogramPolicyT,
            PRIVATIZED_SMEM_BINS,
            NUM_CHANNELS,
            NUM_ACTIVE_CHANNELS,
            SampleIteratorT,
            CounterT,
            PrivatizedDecodeOpT,
            OutputDecodeOpT,
            OffsetT>
        AgentHistogramT;

    // Shared memory for AgentHistogram
    __shared__ typename AgentHistogramT::TempStorage temp_storage;

    AgentHistogramT agent(
        temp_storage,
        d_samples,
        num_output_bins_wrapper.array,
        num_privatized_bins_wrapper.array,
        d_output_histograms_wrapper.array,
        d_privatized_histograms_wrapper.array,
        output_decode_op_wrapper.array,
        privatized_decode_op_wrapper.array);

    // Initialize counters
    agent.InitBinCounters();

    // Consume input tiles
    agent.ConsumeTiles(
        num_row_pixels,
        num_rows,
        row_stride_samples,
        tiles_per_row,
        tile_queue);

    // Store output to global (if necessary)
    agent.StoreOutput();

}






/******************************************************************************
 * Dispatch
 ******************************************************************************/

/**
 * Utility class for dispatching the appropriately-tuned kernels for DeviceHistogram
 */
template <
    int         NUM_CHANNELS,               ///< Number of channels interleaved in the input data (may be greater than the number of channels being actively histogrammed)
    int         NUM_ACTIVE_CHANNELS,        ///< Number of channels actively being histogrammed
    typename    SampleIteratorT,            ///< Random-access input iterator type for reading input items \iterator
    typename    CounterT,                   ///< Integer type for counting sample occurrences per histogram bin
    typename    LevelT,                     ///< Type for specifying bin level boundaries
    typename    OffsetT>                    ///< Signed integer type for global offsets
struct DipatchHistogram
{
    //---------------------------------------------------------------------
    // Types and constants
    //---------------------------------------------------------------------

    /// The sample value type of the input iterator
    typedef typename std::iterator_traits<SampleIteratorT>::value_type SampleT;

    enum
    {
        // Maximum number of bins per channel for which we will use a privatized smem strategy
        MAX_PRIVATIZED_SMEM_BINS = 256
    };


    //---------------------------------------------------------------------
    // Transform functors for converting samples to bin-ids
    //---------------------------------------------------------------------

    // Searches for bin given a list of bin-boundary levels
    template <typename LevelIteratorT>
    struct SearchTransform
    {
        LevelIteratorT  d_levels;                   // Pointer to levels array
        int             num_output_levels;          // Number of levels in array

        // Initializer
        __host__ __device__ __forceinline__ void Init(
            LevelIteratorT  d_levels,               // Pointer to levels array
            int             num_output_levels)      // Number of levels in array
        {
            this->d_levels          = d_levels;
            this->num_output_levels = num_output_levels;
        }

        // Method for converting samples to bin-ids
        template <CacheLoadModifier LOAD_MODIFIER, typename _SampleT>
        __host__ __device__ __forceinline__ void BinSelect(_SampleT sample, int &bin, bool valid)
        {
            /// Level iterator wrapper type
            typedef typename If<IsPointer<LevelIteratorT>::VALUE,
                    CacheModifiedInputIterator<LOAD_MODIFIER, LevelT, OffsetT>,     // Wrap the native input pointer with CacheModifiedInputIterator
                    LevelIteratorT>::Type                                           // Directly use the supplied input iterator type
                WrappedLevelIteratorT;

            WrappedLevelIteratorT wrapped_levels(d_levels);

            int num_bins = num_output_levels - 1;
            if (valid)
            {
                bin = UpperBound(wrapped_levels, num_output_levels, (LevelT) sample) - 1;
                if (bin >= num_bins)
                    bin = -1;
            }
        }
    };


    // Scales samples to evenly-spaced bins
    struct ScaleTransform
    {
        int    num_bins;    // Number of levels in array
        LevelT max;         // Max sample level (exclusive)
        LevelT min;         // Min sample level (inclusive)
        LevelT scale;       // Bin scaling factor

        // Initializer
        template <typename _LevelT>
        __host__ __device__ __forceinline__ void Init(
            int     num_output_levels,  // Number of levels in array
            _LevelT max,                // Max sample level (exclusive)
            _LevelT min,                // Min sample level (inclusive)
            _LevelT scale)              // Bin scaling factor
        {
            this->num_bins = num_output_levels - 1;
            this->max = max;
            this->min = min;
            this->scale = scale;
        }

        // Initializer (float specialization)
        __host__ __device__ __forceinline__ void Init(
            int    num_output_levels,   // Number of levels in array
            float   max,                // Max sample level (exclusive)
            float   min,                // Min sample level (inclusive)
            float   scale)              // Bin scaling factor
        {
            this->num_bins = num_output_levels - 1;
            this->max = max;
            this->min = min;
            this->scale = float(1.0) / scale;
        }

        // Initializer (double specialization)
        __host__ __device__ __forceinline__ void Init(
            int    num_output_levels,   // Number of levels in array
            double max,                 // Max sample level (exclusive)
            double min,                 // Min sample level (inclusive)
            double scale)               // Bin scaling factor
        {
            this->num_bins = num_output_levels - 1;
            this->max = max;
            this->min = min;
            this->scale = double(1.0) / scale;
        }

        // Method for converting samples to bin-ids
        template <CacheLoadModifier LOAD_MODIFIER, typename _SampleT>
        __host__ __device__ __forceinline__ void BinSelect(_SampleT sample, int &bin, bool valid)
        {
            LevelT level_sample = (LevelT) sample;

            if (valid && (level_sample >= min) && (level_sample < max))
                bin = (int) ((level_sample - min) / scale);
        }

        // Method for converting samples to bin-ids (float specialization)
        template <CacheLoadModifier LOAD_MODIFIER>
        __host__ __device__ __forceinline__ void BinSelect(float sample, int &bin, bool valid)
        {
            LevelT level_sample = (LevelT) sample;

            if (valid && (level_sample >= min) && (level_sample < max))
                bin = (int) ((level_sample - min) * scale);
        }

        // Method for converting samples to bin-ids (double specialization)
        template <CacheLoadModifier LOAD_MODIFIER>
        __host__ __device__ __forceinline__ void BinSelect(double sample, int &bin, bool valid)
        {
            LevelT level_sample = (LevelT) sample;

            if (valid && (level_sample >= min) && (level_sample < max))
                bin = (int) ((level_sample - min) * scale);
        }
    };


    // Pass-through bin transform operator
    struct PassThruTransform
    {
        // Method for converting samples to bin-ids
        template <CacheLoadModifier LOAD_MODIFIER, typename _SampleT>
        __host__ __device__ __forceinline__ void BinSelect(_SampleT sample, int &bin, bool valid)
        {
            if (valid)
                bin = (int) sample;
        }
    };



    //---------------------------------------------------------------------
    // Tuning policies
    //---------------------------------------------------------------------

    template <int NOMINAL_ITEMS_PER_THREAD>
    struct TScale
    {
        enum
        {
            V_SCALE = (sizeof(SampleT) + sizeof(int) - 1) / sizeof(int),
            VALUE   = CUB_MAX((NOMINAL_ITEMS_PER_THREAD / NUM_ACTIVE_CHANNELS / V_SCALE), 1)
        };
    };


    /// SM11
    struct Policy110
    {
        // HistogramSweepPolicy
        typedef AgentHistogramPolicy<
                512,
                (NUM_CHANNELS == 1) ? 8 : 2,
                BLOCK_LOAD_DIRECT,
                LOAD_DEFAULT,
                true,
                GMEM,
                false>
            HistogramSweepPolicy;
    };

    /// SM20
    struct Policy200
    {
        // HistogramSweepPolicy
        typedef AgentHistogramPolicy<
                (NUM_CHANNELS == 1) ? 256 : 128,
                (NUM_CHANNELS == 1) ? 8 : 3,
                (NUM_CHANNELS == 1) ? BLOCK_LOAD_DIRECT : BLOCK_LOAD_WARP_TRANSPOSE,
                LOAD_DEFAULT,
                true,
                SMEM,
                false>
            HistogramSweepPolicy;
    };

    /// SM30
    struct Policy300
    {
        // HistogramSweepPolicy
        typedef AgentHistogramPolicy<
                512,
                (NUM_CHANNELS == 1) ? 8 : 2,
                BLOCK_LOAD_DIRECT,
                LOAD_DEFAULT,
                true,
                GMEM,
                false>
            HistogramSweepPolicy;
    };

    /// SM35
    struct Policy350
    {
        // HistogramSweepPolicy
        typedef AgentHistogramPolicy<
                128,
                TScale<8>::VALUE,
                BLOCK_LOAD_DIRECT,
                LOAD_LDG,
                true,
                BLEND,
                true>
            HistogramSweepPolicy;
    };

    /// SM50
    struct Policy500
    {
        // HistogramSweepPolicy
        typedef AgentHistogramPolicy<
                384,
                TScale<16>::VALUE,
                BLOCK_LOAD_DIRECT,
                LOAD_LDG,
                true,
                SMEM,
                false>
            HistogramSweepPolicy;
    };



    //---------------------------------------------------------------------
    // Tuning policies of current PTX compiler pass
    //---------------------------------------------------------------------

#if (CUB_PTX_ARCH >= 500)
    typedef Policy500 PtxPolicy;

#elif (CUB_PTX_ARCH >= 350)
    typedef Policy350 PtxPolicy;

#elif (CUB_PTX_ARCH >= 300)
    typedef Policy300 PtxPolicy;

#elif (CUB_PTX_ARCH >= 200)
    typedef Policy200 PtxPolicy;

#else
    typedef Policy110 PtxPolicy;

#endif

    // "Opaque" policies (whose parameterizations aren't reflected in the type signature)
    struct PtxHistogramSweepPolicy : PtxPolicy::HistogramSweepPolicy {};


    //---------------------------------------------------------------------
    // Utilities
    //---------------------------------------------------------------------

    /**
     * Initialize kernel dispatch configurations with the policies corresponding to the PTX assembly we will use
     */
    template <typename KernelConfig>
    CUB_RUNTIME_FUNCTION __forceinline__
    static cudaError_t InitConfigs(
        int             ptx_version,
        KernelConfig    &histogram_sweep_config)
    {
    #if (CUB_PTX_ARCH > 0)

        // We're on the device, so initialize the kernel dispatch configurations with the current PTX policy
        return histogram_sweep_config.template Init<PtxHistogramSweepPolicy>();

    #else

        // We're on the host, so lookup and initialize the kernel dispatch configurations with the policies that match the device's PTX version
        if (ptx_version >= 500)
        {
            return histogram_sweep_config.template Init<typename Policy500::HistogramSweepPolicy>();
        }
        else if (ptx_version >= 350)
        {
            return histogram_sweep_config.template Init<typename Policy350::HistogramSweepPolicy>();
        }
        else if (ptx_version >= 300)
        {
            return histogram_sweep_config.template Init<typename Policy300::HistogramSweepPolicy>();
        }
        else if (ptx_version >= 200)
        {
            return histogram_sweep_config.template Init<typename Policy200::HistogramSweepPolicy>();
        }
        else if (ptx_version >= 110)
        {
            return histogram_sweep_config.template Init<typename Policy110::HistogramSweepPolicy>();
        }
        else
        {
            // No global atomic support
            return cudaErrorNotSupported;
        }

    #endif
    }


    /**
     * Kernel kernel dispatch configuration
     */
    struct KernelConfig
    {
        int                             block_threads;
        int                             pixels_per_thread;

        template <typename BlockPolicy>
        CUB_RUNTIME_FUNCTION __forceinline__
        cudaError_t Init()
        {
            block_threads               = BlockPolicy::BLOCK_THREADS;
            pixels_per_thread           = BlockPolicy::PIXELS_PER_THREAD;

            return cudaSuccess;
        }
    };


    //---------------------------------------------------------------------
    // Dispatch entrypoints
    //---------------------------------------------------------------------

    /**
     * Privatization-based dispatch routine
     */
    template <
        typename                            PrivatizedDecodeOpT,                            ///< The transform operator type for determining privatized counter indices from samples, one for each channel
        typename                            OutputDecodeOpT,                                ///< The transform operator type for determining output bin-ids from privatized counter indices, one for each channel
        typename                            DeviceHistogramInitKernelT,                     ///< Function type of cub::DeviceHistogramInitKernel
        typename                            DeviceHistogramSweepKernelT>                    ///< Function type of cub::DeviceHistogramSweepKernel
    CUB_RUNTIME_FUNCTION __forceinline__
    static cudaError_t PrivatizedDispatch(
        void*                               d_temp_storage,                                 ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
        size_t&                             temp_storage_bytes,                             ///< [in,out] Reference to size in bytes of \p d_temp_storage allocation
        SampleIteratorT                     d_samples,                                      ///< [in] The pointer to the input sequence of sample items. The samples from different channels are assumed to be interleaved (e.g., an array of 32-bit pixels where each pixel consists of four RGBA 8-bit samples).
        CounterT*                           d_output_histograms[NUM_ACTIVE_CHANNELS],       ///< [out] The pointers to the histogram counter output arrays, one for each active channel.  For channel<sub><em>i</em></sub>, the allocation length of <tt>d_histograms[i]</tt> should be <tt>num_output_levels[i]</tt> - 1.
        int                                 num_privatized_levels[NUM_ACTIVE_CHANNELS],     ///< [in] The number of bin level boundaries for delineating histogram samples in each active channel.  Implies that the number of bins for channel<sub><em>i</em></sub> is <tt>num_output_levels[i]</tt> - 1.
        PrivatizedDecodeOpT                 privatized_decode_op[NUM_ACTIVE_CHANNELS],      ///< [in] Transform operators for determining bin-ids from samples, one for each channel
        int                                 num_output_levels[NUM_ACTIVE_CHANNELS],         ///< [in] The number of bin level boundaries for delineating histogram samples in each active channel.  Implies that the number of bins for channel<sub><em>i</em></sub> is <tt>num_output_levels[i]</tt> - 1.
        OutputDecodeOpT                     output_decode_op[NUM_ACTIVE_CHANNELS],          ///< [in] Transform operators for determining bin-ids from samples, one for each channel
        int                                 max_num_output_bins,                            ///< [in] Maximum number of output bins in any channel
        OffsetT                             num_row_pixels,                                 ///< [in] The number of multi-channel pixels per row in the region of interest
        OffsetT                             num_rows,                                       ///< [in] The number of rows in the region of interest
        OffsetT                             row_stride_samples,                             ///< [in] The number of samples between starts of consecutive rows in the region of interest
        DeviceHistogramInitKernelT          histogram_init_kernel,                          ///< [in] Kernel function pointer to parameterization of cub::DeviceHistogramInitKernel
        DeviceHistogramSweepKernelT         histogram_sweep_kernel,                         ///< [in] Kernel function pointer to parameterization of cub::DeviceHistogramSweepKernel
        KernelConfig                        histogram_sweep_config,                         ///< [in] Dispatch parameters that match the policy that \p histogram_sweep_kernel was compiled for
        cudaStream_t                        stream,                                         ///< [in] CUDA stream to launch kernels within.  Default is stream<sub>0</sub>.
        bool                                debug_synchronous)                              ///< [in] Whether or not to synchronize the stream after every kernel launch to check for errors.  May cause significant slowdown.  Default is \p false.
    {
    #ifndef CUB_RUNTIME_ENABLED

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

            // Get SM occupancy for histogram_sweep_kernel
            int histogram_sweep_sm_occupancy;
            if (CubDebug(error = MaxSmOccupancy(
                histogram_sweep_sm_occupancy,
                histogram_sweep_kernel,
                histogram_sweep_config.block_threads))) break;

            // Get device occupancy for histogram_sweep_kernel
            int histogram_sweep_occupancy = histogram_sweep_sm_occupancy * sm_count;

            if (num_row_pixels * NUM_CHANNELS == row_stride_samples)
            {
                // Treat as a single linear array of samples
                num_row_pixels      *= num_rows;
                num_rows            = 1;
                row_stride_samples  = num_row_pixels * NUM_CHANNELS;
            }

            // Get grid dimensions, trying to keep total blocks ~histogram_sweep_occupancy
            int pixels_per_tile     = histogram_sweep_config.block_threads * histogram_sweep_config.pixels_per_thread;
            int tiles_per_row       = int(num_row_pixels + pixels_per_tile - 1) / pixels_per_tile;
            int blocks_per_row      = CUB_MIN(histogram_sweep_occupancy, tiles_per_row);
            int blocks_per_col      = (blocks_per_row > 0) ?
                                        int(CUB_MIN(histogram_sweep_occupancy / blocks_per_row, num_rows)) :
                                        0;
            int num_thread_blocks   = blocks_per_row * blocks_per_col;

            dim3 sweep_grid_dims;
            sweep_grid_dims.x = (unsigned int) blocks_per_row;
            sweep_grid_dims.y = (unsigned int) blocks_per_col;
            sweep_grid_dims.z = 1;

            // Temporary storage allocation requirements
            const int   NUM_ALLOCATIONS = NUM_ACTIVE_CHANNELS + 1;
            void*       allocations[NUM_ALLOCATIONS];
            size_t      allocation_sizes[NUM_ALLOCATIONS];

            for (int CHANNEL = 0; CHANNEL < NUM_ACTIVE_CHANNELS; ++CHANNEL)
                allocation_sizes[CHANNEL] = size_t(num_thread_blocks) * (num_privatized_levels[CHANNEL] - 1) * sizeof(CounterT);

            allocation_sizes[NUM_ALLOCATIONS - 1] = GridQueue<int>::AllocationSize();

            // Alias the temporary allocations from the single storage blob (or compute the necessary size of the blob)
            if (CubDebug(error = AliasTemporaries(d_temp_storage, temp_storage_bytes, allocations, allocation_sizes))) break;
            if (d_temp_storage == NULL)
            {
                // Return if the caller is simply requesting the size of the storage allocation
                break;
            }

            // Construct the grid queue descriptor
            GridQueue<int> tile_queue(allocations[NUM_ALLOCATIONS - 1]);

            // Setup array wrapper for histogram channel output (because we can't pass static arrays as kernel parameters)
            ArrayWrapper<CounterT*, NUM_ACTIVE_CHANNELS> d_output_histograms_wrapper;
            for (int CHANNEL = 0; CHANNEL < NUM_ACTIVE_CHANNELS; ++CHANNEL)
                d_output_histograms_wrapper.array[CHANNEL] = d_output_histograms[CHANNEL];

            // Setup array wrapper for privatized per-block histogram channel output (because we can't pass static arrays as kernel parameters)
            ArrayWrapper<CounterT*, NUM_ACTIVE_CHANNELS> d_privatized_histograms_wrapper;
            for (int CHANNEL = 0; CHANNEL < NUM_ACTIVE_CHANNELS; ++CHANNEL)
                d_privatized_histograms_wrapper.array[CHANNEL] = (CounterT*) allocations[CHANNEL];

            // Setup array wrapper for sweep bin transforms (because we can't pass static arrays as kernel parameters)
            ArrayWrapper<PrivatizedDecodeOpT, NUM_ACTIVE_CHANNELS> privatized_decode_op_wrapper;
            for (int CHANNEL = 0; CHANNEL < NUM_ACTIVE_CHANNELS; ++CHANNEL)
                privatized_decode_op_wrapper.array[CHANNEL] = privatized_decode_op[CHANNEL];

            // Setup array wrapper for aggregation bin transforms (because we can't pass static arrays as kernel parameters)
            ArrayWrapper<OutputDecodeOpT, NUM_ACTIVE_CHANNELS> output_decode_op_wrapper;
            for (int CHANNEL = 0; CHANNEL < NUM_ACTIVE_CHANNELS; ++CHANNEL)
                output_decode_op_wrapper.array[CHANNEL] = output_decode_op[CHANNEL];

            // Setup array wrapper for num privatized bins (because we can't pass static arrays as kernel parameters)
            ArrayWrapper<int, NUM_ACTIVE_CHANNELS> num_privatized_bins_wrapper;
            for (int CHANNEL = 0; CHANNEL < NUM_ACTIVE_CHANNELS; ++CHANNEL)
                num_privatized_bins_wrapper.array[CHANNEL] = num_privatized_levels[CHANNEL] - 1;

            // Setup array wrapper for num output bins (because we can't pass static arrays as kernel parameters)
            ArrayWrapper<int, NUM_ACTIVE_CHANNELS> num_output_bins_wrapper;
            for (int CHANNEL = 0; CHANNEL < NUM_ACTIVE_CHANNELS; ++CHANNEL)
                num_output_bins_wrapper.array[CHANNEL] = num_output_levels[CHANNEL] - 1;

            int histogram_init_block_threads    = 256;
            int histogram_init_grid_dims        = (max_num_output_bins + histogram_init_block_threads - 1) / histogram_init_block_threads;

            // Log DeviceHistogramInitKernel configuration
            if (debug_synchronous) _CubLog("Invoking DeviceHistogramInitKernel<<<%d, %d, 0, %lld>>>()\n",
                histogram_init_grid_dims, histogram_init_block_threads, (long long) stream);

            // Invoke histogram_init_kernel
            histogram_init_kernel<<<histogram_init_grid_dims, histogram_init_block_threads, 0, stream>>>(
                num_output_bins_wrapper,
                d_output_histograms_wrapper,
                tile_queue);

            // Return if empty problem
            if ((blocks_per_row == 0) || (blocks_per_col == 0))
                break;

            // Log histogram_sweep_kernel configuration
            if (debug_synchronous) _CubLog("Invoking histogram_sweep_kernel<<<{%d, %d, %d}, %d, 0, %lld>>>(), %d pixels per thread, %d SM occupancy\n",
                sweep_grid_dims.x, sweep_grid_dims.y, sweep_grid_dims.z,
                histogram_sweep_config.block_threads, (long long) stream, histogram_sweep_config.pixels_per_thread, histogram_sweep_sm_occupancy);

            // Invoke histogram_sweep_kernel
            histogram_sweep_kernel<<<sweep_grid_dims, histogram_sweep_config.block_threads, 0, stream>>>(
                d_samples,
                num_output_bins_wrapper,
                num_privatized_bins_wrapper,
                d_output_histograms_wrapper,
                d_privatized_histograms_wrapper,
                output_decode_op_wrapper,
                privatized_decode_op_wrapper,
                num_row_pixels,
                num_rows,
                row_stride_samples,
                tiles_per_row,
                tile_queue);

            // Check for failure to launch
            if (CubDebug(error = cudaPeekAtLastError())) break;

            // Sync the stream if specified to flush runtime errors
            if (debug_synchronous && (CubDebug(error = SyncStream(stream)))) break;

        }
        while (0);

        return error;

    #endif // CUB_RUNTIME_ENABLED
    }



    /**
     * Dispatch routine for HistogramRange, specialized for sample types larger than 8bit
     */
    CUB_RUNTIME_FUNCTION
    static cudaError_t DispatchRange(
        void*               d_temp_storage,                                ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
        size_t&             temp_storage_bytes,                            ///< [in,out] Reference to size in bytes of \p d_temp_storage allocation
        SampleIteratorT     d_samples,                                  ///< [in] The pointer to the multi-channel input sequence of data samples. The samples from different channels are assumed to be interleaved (e.g., an array of 32-bit pixels where each pixel consists of four RGBA 8-bit samples).
        CounterT*           d_output_histograms[NUM_ACTIVE_CHANNELS],      ///< [out] The pointers to the histogram counter output arrays, one for each active channel.  For channel<sub><em>i</em></sub>, the allocation length of <tt>d_histograms[i]</tt> should be <tt>num_output_levels[i]</tt> - 1.
        int                 num_output_levels[NUM_ACTIVE_CHANNELS],     ///< [in] The number of boundaries (levels) for delineating histogram samples in each active channel.  Implies that the number of bins for channel<sub><em>i</em></sub> is <tt>num_output_levels[i]</tt> - 1.
        LevelT              *d_levels[NUM_ACTIVE_CHANNELS],             ///< [in] The pointers to the arrays of boundaries (levels), one for each active channel.  Bin ranges are defined by consecutive boundary pairings: lower sample value boundaries are inclusive and upper sample value boundaries are exclusive.
        OffsetT             num_row_pixels,                             ///< [in] The number of multi-channel pixels per row in the region of interest
        OffsetT             num_rows,                                   ///< [in] The number of rows in the region of interest
        OffsetT             row_stride_samples,                         ///< [in] The number of samples between starts of consecutive rows in the region of interest
        cudaStream_t        stream,                                     ///< [in] CUDA stream to launch kernels within.  Default is stream<sub>0</sub>.
        bool                debug_synchronous,                          ///< [in] Whether or not to synchronize the stream after every kernel launch to check for errors.  May cause significant slowdown.  Default is \p false.
        Int2Type<false>     is_byte_sample)                             ///< [in] Marker type indicating whether or not SampleT is a 8b type
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

            // Get kernel dispatch configurations
            KernelConfig histogram_sweep_config;
            if (CubDebug(error = InitConfigs(ptx_version, histogram_sweep_config)))
                break;

            // Use the search transform op for converting samples to privatized bins
            typedef SearchTransform<LevelT*> PrivatizedDecodeOpT;

            // Use the pass-thru transform op for converting privatized bins to output bins
            typedef PassThruTransform OutputDecodeOpT;

            PrivatizedDecodeOpT     privatized_decode_op[NUM_ACTIVE_CHANNELS];
            OutputDecodeOpT         output_decode_op[NUM_ACTIVE_CHANNELS];
            int                     max_levels = num_output_levels[0];

            for (int channel = 0; channel < NUM_ACTIVE_CHANNELS; ++channel)
            {
                privatized_decode_op[channel].Init(d_levels[channel], num_output_levels[channel]);
                if (num_output_levels[channel] > max_levels)
                    max_levels = num_output_levels[channel];
            }
            int max_num_output_bins = max_levels - 1;

            // Dispatch
            if (max_num_output_bins > MAX_PRIVATIZED_SMEM_BINS)
            {
                // Too many bins to keep in shared memory.
                const int PRIVATIZED_SMEM_BINS = 0;

                if (CubDebug(error = PrivatizedDispatch(
                    d_temp_storage,
                    temp_storage_bytes,
                    d_samples,
                    d_output_histograms,
                    num_output_levels,
                    privatized_decode_op,
                    num_output_levels,
                    output_decode_op,
                    max_num_output_bins,
                    num_row_pixels,
                    num_rows,
                    row_stride_samples,
                    DeviceHistogramInitKernel<NUM_ACTIVE_CHANNELS, CounterT, OffsetT>,
                    DeviceHistogramSweepKernel<PtxHistogramSweepPolicy, PRIVATIZED_SMEM_BINS, NUM_CHANNELS, NUM_ACTIVE_CHANNELS, SampleIteratorT, CounterT, PrivatizedDecodeOpT, OutputDecodeOpT, OffsetT>,
                    histogram_sweep_config,
                    stream,
                    debug_synchronous))) break;
            }
            else
            {
                // Dispatch shared-privatized approach
                const int PRIVATIZED_SMEM_BINS = MAX_PRIVATIZED_SMEM_BINS;

                if (CubDebug(error = PrivatizedDispatch(
                    d_temp_storage,
                    temp_storage_bytes,
                    d_samples,
                    d_output_histograms,
                    num_output_levels,
                    privatized_decode_op,
                    num_output_levels,
                    output_decode_op,
                    max_num_output_bins,
                    num_row_pixels,
                    num_rows,
                    row_stride_samples,
                    DeviceHistogramInitKernel<NUM_ACTIVE_CHANNELS, CounterT, OffsetT>,
                    DeviceHistogramSweepKernel<PtxHistogramSweepPolicy, PRIVATIZED_SMEM_BINS, NUM_CHANNELS, NUM_ACTIVE_CHANNELS, SampleIteratorT, CounterT, PrivatizedDecodeOpT, OutputDecodeOpT, OffsetT>,
                    histogram_sweep_config,
                    stream,
                    debug_synchronous))) break;
            }

        } while (0);

        return error;
    }


    /**
     * Dispatch routine for HistogramRange, specialized for 8-bit sample types (computes 256-bin privatized histograms and then reduces to user-specified levels)
     */
    CUB_RUNTIME_FUNCTION
    static cudaError_t DispatchRange(
        void*               d_temp_storage,                             ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
        size_t&             temp_storage_bytes,                         ///< [in,out] Reference to size in bytes of \p d_temp_storage allocation
        SampleIteratorT     d_samples,                                  ///< [in] The pointer to the multi-channel input sequence of data samples. The samples from different channels are assumed to be interleaved (e.g., an array of 32-bit pixels where each pixel consists of four RGBA 8-bit samples).
        CounterT*           d_output_histograms[NUM_ACTIVE_CHANNELS],   ///< [out] The pointers to the histogram counter output arrays, one for each active channel.  For channel<sub><em>i</em></sub>, the allocation length of <tt>d_histograms[i]</tt> should be <tt>num_output_levels[i]</tt> - 1.
        int                 num_output_levels[NUM_ACTIVE_CHANNELS],     ///< [in] The number of boundaries (levels) for delineating histogram samples in each active channel.  Implies that the number of bins for channel<sub><em>i</em></sub> is <tt>num_output_levels[i]</tt> - 1.
        LevelT              *d_levels[NUM_ACTIVE_CHANNELS],             ///< [in] The pointers to the arrays of boundaries (levels), one for each active channel.  Bin ranges are defined by consecutive boundary pairings: lower sample value boundaries are inclusive and upper sample value boundaries are exclusive.
        OffsetT             num_row_pixels,                             ///< [in] The number of multi-channel pixels per row in the region of interest
        OffsetT             num_rows,                                   ///< [in] The number of rows in the region of interest
        OffsetT             row_stride_samples,                         ///< [in] The number of samples between starts of consecutive rows in the region of interest
        cudaStream_t        stream,                                     ///< [in] CUDA stream to launch kernels within.  Default is stream<sub>0</sub>.
        bool                debug_synchronous,                          ///< [in] Whether or not to synchronize the stream after every kernel launch to check for errors.  May cause significant slowdown.  Default is \p false.
        Int2Type<true>      is_byte_sample)                             ///< [in] Marker type indicating whether or not SampleT is a 8b type
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

            // Get kernel dispatch configurations
            KernelConfig histogram_sweep_config;
            if (CubDebug(error = InitConfigs(ptx_version, histogram_sweep_config)))
                break;

            // Use the pass-thru transform op for converting samples to privatized bins
            typedef PassThruTransform PrivatizedDecodeOpT;

            // Use the search transform op for converting privatized bins to output bins
            typedef SearchTransform<LevelT*> OutputDecodeOpT;

            int                         num_privatized_levels[NUM_ACTIVE_CHANNELS];
            PrivatizedDecodeOpT         privatized_decode_op[NUM_ACTIVE_CHANNELS];
            OutputDecodeOpT             output_decode_op[NUM_ACTIVE_CHANNELS];
            int                         max_levels = num_output_levels[0];              // Maximum number of levels in any channel

            for (int channel = 0; channel < NUM_ACTIVE_CHANNELS; ++channel)
            {
                num_privatized_levels[channel] = 257;
                output_decode_op[channel].Init(d_levels[channel], num_output_levels[channel]);

                if (num_output_levels[channel] > max_levels)
                    max_levels = num_output_levels[channel];
            }
            int max_num_output_bins = max_levels - 1;

            const int PRIVATIZED_SMEM_BINS = 256;

            if (CubDebug(error = PrivatizedDispatch(
                d_temp_storage,
                temp_storage_bytes,
                d_samples,
                d_output_histograms,
                num_privatized_levels,
                privatized_decode_op,
                num_output_levels,
                output_decode_op,
                max_num_output_bins,
                num_row_pixels,
                num_rows,
                row_stride_samples,
                DeviceHistogramInitKernel<NUM_ACTIVE_CHANNELS, CounterT, OffsetT>,
                DeviceHistogramSweepKernel<PtxHistogramSweepPolicy, PRIVATIZED_SMEM_BINS, NUM_CHANNELS, NUM_ACTIVE_CHANNELS, SampleIteratorT, CounterT, PrivatizedDecodeOpT, OutputDecodeOpT, OffsetT>,
                histogram_sweep_config,
                stream,
                debug_synchronous))) break;

        } while (0);

        return error;
    }


    /**
     * Dispatch routine for HistogramEven, specialized for sample types larger than 8-bit
     */
    CUB_RUNTIME_FUNCTION __forceinline__
    static cudaError_t DispatchEven(
        void*               d_temp_storage,                            ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
        size_t&             temp_storage_bytes,                        ///< [in,out] Reference to size in bytes of \p d_temp_storage allocation
        SampleIteratorT     d_samples,                                  ///< [in] The pointer to the input sequence of sample items. The samples from different channels are assumed to be interleaved (e.g., an array of 32-bit pixels where each pixel consists of four RGBA 8-bit samples).
        CounterT*           d_output_histograms[NUM_ACTIVE_CHANNELS],  ///< [out] The pointers to the histogram counter output arrays, one for each active channel.  For channel<sub><em>i</em></sub>, the allocation length of <tt>d_histograms[i]</tt> should be <tt>num_output_levels[i]</tt> - 1.
        int                 num_output_levels[NUM_ACTIVE_CHANNELS],     ///< [in] The number of bin level boundaries for delineating histogram samples in each active channel.  Implies that the number of bins for channel<sub><em>i</em></sub> is <tt>num_output_levels[i]</tt> - 1.
        LevelT              lower_level[NUM_ACTIVE_CHANNELS],           ///< [in] The lower sample value bound (inclusive) for the lowest histogram bin in each active channel.
        LevelT              upper_level[NUM_ACTIVE_CHANNELS],           ///< [in] The upper sample value bound (exclusive) for the highest histogram bin in each active channel.
        OffsetT             num_row_pixels,                             ///< [in] The number of multi-channel pixels per row in the region of interest
        OffsetT             num_rows,                                   ///< [in] The number of rows in the region of interest
        OffsetT             row_stride_samples,                         ///< [in] The number of samples between starts of consecutive rows in the region of interest
        cudaStream_t        stream,                                     ///< [in] CUDA stream to launch kernels within.  Default is stream<sub>0</sub>.
        bool                debug_synchronous,                          ///< [in] Whether or not to synchronize the stream after every kernel launch to check for errors.  May cause significant slowdown.  Default is \p false.
        Int2Type<false>     is_byte_sample)                             ///< [in] Marker type indicating whether or not SampleT is a 8b type
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

            // Get kernel dispatch configurations
            KernelConfig histogram_sweep_config;
            if (CubDebug(error = InitConfigs(ptx_version, histogram_sweep_config)))
                break;

            // Use the scale transform op for converting samples to privatized bins
            typedef ScaleTransform PrivatizedDecodeOpT;

            // Use the pass-thru transform op for converting privatized bins to output bins
            typedef PassThruTransform OutputDecodeOpT;

            PrivatizedDecodeOpT         privatized_decode_op[NUM_ACTIVE_CHANNELS];
            OutputDecodeOpT             output_decode_op[NUM_ACTIVE_CHANNELS];
            int                         max_levels = num_output_levels[0];

            for (int channel = 0; channel < NUM_ACTIVE_CHANNELS; ++channel)
            {
                int     bins    = num_output_levels[channel] - 1;
                LevelT  scale   = (upper_level[channel] - lower_level[channel]) / bins;

                privatized_decode_op[channel].Init(num_output_levels[channel], upper_level[channel], lower_level[channel], scale);

                if (num_output_levels[channel] > max_levels)
                    max_levels = num_output_levels[channel];
            }
            int max_num_output_bins = max_levels - 1;

            if (max_num_output_bins > MAX_PRIVATIZED_SMEM_BINS)
            {
                // Dispatch shared-privatized approach
                const int PRIVATIZED_SMEM_BINS = 0;

                if (CubDebug(error = PrivatizedDispatch(
                    d_temp_storage,
                    temp_storage_bytes,
                    d_samples,
                    d_output_histograms,
                    num_output_levels,
                    privatized_decode_op,
                    num_output_levels,
                    output_decode_op,
                    max_num_output_bins,
                    num_row_pixels,
                    num_rows,
                    row_stride_samples,
                    DeviceHistogramInitKernel<NUM_ACTIVE_CHANNELS, CounterT, OffsetT>,
                    DeviceHistogramSweepKernel<PtxHistogramSweepPolicy, PRIVATIZED_SMEM_BINS, NUM_CHANNELS, NUM_ACTIVE_CHANNELS, SampleIteratorT, CounterT, PrivatizedDecodeOpT, OutputDecodeOpT, OffsetT>,
                    histogram_sweep_config,
                    stream,
                    debug_synchronous))) break;
            }
            else
            {
                // Dispatch shared-privatized approach
                const int PRIVATIZED_SMEM_BINS = MAX_PRIVATIZED_SMEM_BINS;

                if (CubDebug(error = PrivatizedDispatch(
                    d_temp_storage,
                    temp_storage_bytes,
                    d_samples,
                    d_output_histograms,
                    num_output_levels,
                    privatized_decode_op,
                    num_output_levels,
                    output_decode_op,
                    max_num_output_bins,
                    num_row_pixels,
                    num_rows,
                    row_stride_samples,
                    DeviceHistogramInitKernel<NUM_ACTIVE_CHANNELS, CounterT, OffsetT>,
                    DeviceHistogramSweepKernel<PtxHistogramSweepPolicy, PRIVATIZED_SMEM_BINS, NUM_CHANNELS, NUM_ACTIVE_CHANNELS, SampleIteratorT, CounterT, PrivatizedDecodeOpT, OutputDecodeOpT, OffsetT>,
                    histogram_sweep_config,
                    stream,
                    debug_synchronous))) break;
            }
        }
        while (0);

        return error;
    }


    /**
     * Dispatch routine for HistogramEven, specialized for 8-bit sample types (computes 256-bin privatized histograms and then reduces to user-specified levels)
     */
    CUB_RUNTIME_FUNCTION __forceinline__
    static cudaError_t DispatchEven(
        void*               d_temp_storage,                            ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
        size_t&             temp_storage_bytes,                        ///< [in,out] Reference to size in bytes of \p d_temp_storage allocation
        SampleIteratorT     d_samples,                                  ///< [in] The pointer to the input sequence of sample items. The samples from different channels are assumed to be interleaved (e.g., an array of 32-bit pixels where each pixel consists of four RGBA 8-bit samples).
        CounterT*           d_output_histograms[NUM_ACTIVE_CHANNELS],  ///< [out] The pointers to the histogram counter output arrays, one for each active channel.  For channel<sub><em>i</em></sub>, the allocation length of <tt>d_histograms[i]</tt> should be <tt>num_output_levels[i]</tt> - 1.
        int                 num_output_levels[NUM_ACTIVE_CHANNELS],     ///< [in] The number of bin level boundaries for delineating histogram samples in each active channel.  Implies that the number of bins for channel<sub><em>i</em></sub> is <tt>num_output_levels[i]</tt> - 1.
        LevelT              lower_level[NUM_ACTIVE_CHANNELS],           ///< [in] The lower sample value bound (inclusive) for the lowest histogram bin in each active channel.
        LevelT              upper_level[NUM_ACTIVE_CHANNELS],           ///< [in] The upper sample value bound (exclusive) for the highest histogram bin in each active channel.
        OffsetT             num_row_pixels,                             ///< [in] The number of multi-channel pixels per row in the region of interest
        OffsetT             num_rows,                                   ///< [in] The number of rows in the region of interest
        OffsetT             row_stride_samples,                         ///< [in] The number of samples between starts of consecutive rows in the region of interest
        cudaStream_t        stream,                                     ///< [in] CUDA stream to launch kernels within.  Default is stream<sub>0</sub>.
        bool                debug_synchronous,                          ///< [in] Whether or not to synchronize the stream after every kernel launch to check for errors.  May cause significant slowdown.  Default is \p false.
        Int2Type<true>      is_byte_sample)                             ///< [in] Marker type indicating whether or not SampleT is a 8b type
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

            // Get kernel dispatch configurations
            KernelConfig histogram_sweep_config;
            if (CubDebug(error = InitConfigs(ptx_version, histogram_sweep_config)))
                break;

            // Use the pass-thru transform op for converting samples to privatized bins
            typedef PassThruTransform PrivatizedDecodeOpT;

            // Use the scale transform op for converting privatized bins to output bins
            typedef ScaleTransform OutputDecodeOpT;

            int                     num_privatized_levels[NUM_ACTIVE_CHANNELS];
            PrivatizedDecodeOpT     privatized_decode_op[NUM_ACTIVE_CHANNELS];
            OutputDecodeOpT         output_decode_op[NUM_ACTIVE_CHANNELS];
            int                     max_levels = num_output_levels[0];

            for (int channel = 0; channel < NUM_ACTIVE_CHANNELS; ++channel)
            {
                num_privatized_levels[channel] = 257;

                int     bins    = num_output_levels[channel] - 1;
                LevelT  scale   = (upper_level[channel] - lower_level[channel]) / bins;
                output_decode_op[channel].Init(num_output_levels[channel], upper_level[channel], lower_level[channel], scale);

                if (num_output_levels[channel] > max_levels)
                    max_levels = num_output_levels[channel];
            }
            int max_num_output_bins = max_levels - 1;

            const int PRIVATIZED_SMEM_BINS = 256;

            if (CubDebug(error = PrivatizedDispatch(
                d_temp_storage,
                temp_storage_bytes,
                d_samples,
                d_output_histograms,
                num_privatized_levels,
                privatized_decode_op,
                num_output_levels,
                output_decode_op,
                max_num_output_bins,
                num_row_pixels,
                num_rows,
                row_stride_samples,
                DeviceHistogramInitKernel<NUM_ACTIVE_CHANNELS, CounterT, OffsetT>,
                DeviceHistogramSweepKernel<PtxHistogramSweepPolicy, PRIVATIZED_SMEM_BINS, NUM_CHANNELS, NUM_ACTIVE_CHANNELS, SampleIteratorT, CounterT, PrivatizedDecodeOpT, OutputDecodeOpT, OffsetT>,
                histogram_sweep_config,
                stream,
                debug_synchronous))) break;

        }
        while (0);

        return error;
    }

};


}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)


