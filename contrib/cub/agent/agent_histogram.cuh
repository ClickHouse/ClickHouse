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
 * cub::AgentHistogram implements a stateful abstraction of CUDA thread blocks for participating in device-wide histogram .
 */

#pragma once

#include <iterator>

#include "../util_type.cuh"
#include "../block/block_load.cuh"
#include "../grid/grid_queue.cuh"
#include "../iterator/cache_modified_input_iterator.cuh"
#include "../util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {


/******************************************************************************
 * Tuning policy
 ******************************************************************************/

/**
 *
 */
enum BlockHistogramMemoryPreference
{
    GMEM,
    SMEM,
    BLEND
};


/**
 * Parameterizable tuning policy type for AgentHistogram
 */
template <
    int                             _BLOCK_THREADS,                 ///< Threads per thread block
    int                             _PIXELS_PER_THREAD,             ///< Pixels per thread (per tile of input)
    BlockLoadAlgorithm              _LOAD_ALGORITHM,                ///< The BlockLoad algorithm to use
    CacheLoadModifier               _LOAD_MODIFIER,                 ///< Cache load modifier for reading input elements
    bool                            _RLE_COMPRESS,                  ///< Whether to perform localized RLE to compress samples before histogramming
    BlockHistogramMemoryPreference  _MEM_PREFERENCE,                ///< Whether to prefer privatized shared-memory bins (versus privatized global-memory bins)
    bool                            _WORK_STEALING>                 ///< Whether to dequeue tiles from a global work queue
struct AgentHistogramPolicy
{
    enum
    {
        BLOCK_THREADS           = _BLOCK_THREADS,                   ///< Threads per thread block
        PIXELS_PER_THREAD       = _PIXELS_PER_THREAD,               ///< Pixels per thread (per tile of input)
        IS_RLE_COMPRESS         = _RLE_COMPRESS,                    ///< Whether to perform localized RLE to compress samples before histogramming
        MEM_PREFERENCE          = _MEM_PREFERENCE,                  ///< Whether to prefer privatized shared-memory bins (versus privatized global-memory bins)
        IS_WORK_STEALING        = _WORK_STEALING,                   ///< Whether to dequeue tiles from a global work queue
    };

    static const BlockLoadAlgorithm     LOAD_ALGORITHM          = _LOAD_ALGORITHM;          ///< The BlockLoad algorithm to use
    static const CacheLoadModifier      LOAD_MODIFIER           = _LOAD_MODIFIER;           ///< Cache load modifier for reading input elements
};


/******************************************************************************
 * Thread block abstractions
 ******************************************************************************/

/**
 * \brief AgentHistogram implements a stateful abstraction of CUDA thread blocks for participating in device-wide histogram .
 */
template <
    typename    AgentHistogramPolicyT,     ///< Parameterized AgentHistogramPolicy tuning policy type
    int         PRIVATIZED_SMEM_BINS,           ///< Number of privatized shared-memory histogram bins of any channel.  Zero indicates privatized counters to be maintained in device-accessible memory.
    int         NUM_CHANNELS,                   ///< Number of channels interleaved in the input data.  Supports up to four channels.
    int         NUM_ACTIVE_CHANNELS,            ///< Number of channels actively being histogrammed
    typename    SampleIteratorT,                ///< Random-access input iterator type for reading samples
    typename    CounterT,                       ///< Integer type for counting sample occurrences per histogram bin
    typename    PrivatizedDecodeOpT,            ///< The transform operator type for determining privatized counter indices from samples, one for each channel
    typename    OutputDecodeOpT,                ///< The transform operator type for determining output bin-ids from privatized counter indices, one for each channel
    typename    OffsetT,                        ///< Signed integer type for global offsets
    int         PTX_ARCH = CUB_PTX_ARCH>        ///< PTX compute capability
struct AgentHistogram
{
    //---------------------------------------------------------------------
    // Types and constants
    //---------------------------------------------------------------------

    /// The sample type of the input iterator
    typedef typename std::iterator_traits<SampleIteratorT>::value_type SampleT;

    /// The pixel type of SampleT
    typedef typename CubVector<SampleT, NUM_CHANNELS>::Type PixelT;

    /// The quad type of SampleT
    typedef typename CubVector<SampleT, 4>::Type QuadT;

    /// Constants
    enum
    {
        BLOCK_THREADS           = AgentHistogramPolicyT::BLOCK_THREADS,

        PIXELS_PER_THREAD       = AgentHistogramPolicyT::PIXELS_PER_THREAD,
        SAMPLES_PER_THREAD      = PIXELS_PER_THREAD * NUM_CHANNELS,
        QUADS_PER_THREAD        = SAMPLES_PER_THREAD / 4,

        TILE_PIXELS             = PIXELS_PER_THREAD * BLOCK_THREADS,
        TILE_SAMPLES            = SAMPLES_PER_THREAD * BLOCK_THREADS,

        IS_RLE_COMPRESS            = AgentHistogramPolicyT::IS_RLE_COMPRESS,

        MEM_PREFERENCE          = (PRIVATIZED_SMEM_BINS > 0) ?
                                        AgentHistogramPolicyT::MEM_PREFERENCE :
                                        GMEM,

        IS_WORK_STEALING           = AgentHistogramPolicyT::IS_WORK_STEALING,
    };

    /// Cache load modifier for reading input elements
    static const CacheLoadModifier LOAD_MODIFIER = AgentHistogramPolicyT::LOAD_MODIFIER;


    /// Input iterator wrapper type (for applying cache modifier)
    typedef typename If<IsPointer<SampleIteratorT>::VALUE,
            CacheModifiedInputIterator<LOAD_MODIFIER, SampleT, OffsetT>,     // Wrap the native input pointer with CacheModifiedInputIterator
            SampleIteratorT>::Type                                           // Directly use the supplied input iterator type
        WrappedSampleIteratorT;

    /// Pixel input iterator type (for applying cache modifier)
    typedef CacheModifiedInputIterator<LOAD_MODIFIER, PixelT, OffsetT>
        WrappedPixelIteratorT;

    /// Qaud input iterator type (for applying cache modifier)
    typedef CacheModifiedInputIterator<LOAD_MODIFIER, QuadT, OffsetT>
        WrappedQuadIteratorT;

    /// Parameterized BlockLoad type for samples
    typedef BlockLoad<
            SampleT,
            BLOCK_THREADS,
            SAMPLES_PER_THREAD,
            AgentHistogramPolicyT::LOAD_ALGORITHM>
        BlockLoadSampleT;

    /// Parameterized BlockLoad type for pixels
    typedef BlockLoad<
            PixelT,
            BLOCK_THREADS,
            PIXELS_PER_THREAD,
            AgentHistogramPolicyT::LOAD_ALGORITHM>
        BlockLoadPixelT;

    /// Parameterized BlockLoad type for quads
    typedef BlockLoad<
            QuadT,
            BLOCK_THREADS,
            QUADS_PER_THREAD,
            AgentHistogramPolicyT::LOAD_ALGORITHM>
        BlockLoadQuadT;

    /// Shared memory type required by this thread block
    struct _TempStorage
    {
        CounterT histograms[NUM_ACTIVE_CHANNELS][PRIVATIZED_SMEM_BINS + 1];     // Smem needed for block-privatized smem histogram (with 1 word of padding)

        int tile_idx;

        // Aliasable storage layout
        union Aliasable
        {
            typename BlockLoadSampleT::TempStorage sample_load;     // Smem needed for loading a tile of samples
            typename BlockLoadPixelT::TempStorage pixel_load;       // Smem needed for loading a tile of pixels
            typename BlockLoadQuadT::TempStorage quad_load;         // Smem needed for loading a tile of quads

        } aliasable;
    };


    /// Temporary storage type (unionable)
    struct TempStorage : Uninitialized<_TempStorage> {};


    //---------------------------------------------------------------------
    // Per-thread fields
    //---------------------------------------------------------------------

    /// Reference to temp_storage
    _TempStorage &temp_storage;

    /// Sample input iterator (with cache modifier applied, if possible)
    WrappedSampleIteratorT d_wrapped_samples;

    /// Native pointer for input samples (possibly NULL if unavailable)
    SampleT* d_native_samples;

    /// The number of output bins for each channel
    int (&num_output_bins)[NUM_ACTIVE_CHANNELS];

    /// The number of privatized bins for each channel
    int (&num_privatized_bins)[NUM_ACTIVE_CHANNELS];

    /// Reference to gmem privatized histograms for each channel
    CounterT* d_privatized_histograms[NUM_ACTIVE_CHANNELS];

    /// Reference to final output histograms (gmem)
    CounterT* (&d_output_histograms)[NUM_ACTIVE_CHANNELS];

    /// The transform operator for determining output bin-ids from privatized counter indices, one for each channel
    OutputDecodeOpT (&output_decode_op)[NUM_ACTIVE_CHANNELS];

    /// The transform operator for determining privatized counter indices from samples, one for each channel
    PrivatizedDecodeOpT (&privatized_decode_op)[NUM_ACTIVE_CHANNELS];

    /// Whether to prefer privatized smem counters vs privatized global counters
    bool prefer_smem;


    //---------------------------------------------------------------------
    // Initialize privatized bin counters
    //---------------------------------------------------------------------

    // Initialize privatized bin counters
    __device__ __forceinline__ void InitBinCounters(CounterT* privatized_histograms[NUM_ACTIVE_CHANNELS])
    {
        // Initialize histogram bin counts to zeros
        #pragma unroll
        for (int CHANNEL = 0; CHANNEL < NUM_ACTIVE_CHANNELS; ++CHANNEL)
        {
            for (int privatized_bin = threadIdx.x; privatized_bin < num_privatized_bins[CHANNEL]; privatized_bin += BLOCK_THREADS)
            {
                privatized_histograms[CHANNEL][privatized_bin] = 0;
            }
        }

        // Barrier to make sure all threads are done updating counters
        CTA_SYNC();
    }


    // Initialize privatized bin counters.  Specialized for privatized shared-memory counters
    __device__ __forceinline__ void InitSmemBinCounters()
    {
        CounterT* privatized_histograms[NUM_ACTIVE_CHANNELS];

        for (int CHANNEL = 0; CHANNEL < NUM_ACTIVE_CHANNELS; ++CHANNEL)
            privatized_histograms[CHANNEL] = temp_storage.histograms[CHANNEL];

        InitBinCounters(privatized_histograms);
    }


    // Initialize privatized bin counters.  Specialized for privatized global-memory counters
    __device__ __forceinline__ void InitGmemBinCounters()
    {
        InitBinCounters(d_privatized_histograms);
    }


    //---------------------------------------------------------------------
    // Update final output histograms
    //---------------------------------------------------------------------

    // Update final output histograms from privatized histograms
    __device__ __forceinline__ void StoreOutput(CounterT* privatized_histograms[NUM_ACTIVE_CHANNELS])
    {
        // Barrier to make sure all threads are done updating counters
        CTA_SYNC();

        // Apply privatized bin counts to output bin counts
        #pragma unroll
        for (int CHANNEL = 0; CHANNEL < NUM_ACTIVE_CHANNELS; ++CHANNEL)
        {
            int channel_bins = num_privatized_bins[CHANNEL];
            for (int privatized_bin = threadIdx.x; 
                    privatized_bin < channel_bins;  
                    privatized_bin += BLOCK_THREADS)
            {
                int         output_bin  = -1;
                CounterT    count       = privatized_histograms[CHANNEL][privatized_bin];
                bool        is_valid    = count > 0;

                output_decode_op[CHANNEL].template BinSelect<LOAD_MODIFIER>((SampleT) privatized_bin, output_bin, is_valid);

                if (output_bin >= 0)
                {
                    atomicAdd(&d_output_histograms[CHANNEL][output_bin], count);
                }

            }
        }
    }


    // Update final output histograms from privatized histograms.  Specialized for privatized shared-memory counters
    __device__ __forceinline__ void StoreSmemOutput()
    {
        CounterT* privatized_histograms[NUM_ACTIVE_CHANNELS];
        for (int CHANNEL = 0; CHANNEL < NUM_ACTIVE_CHANNELS; ++CHANNEL)
            privatized_histograms[CHANNEL] = temp_storage.histograms[CHANNEL];

        StoreOutput(privatized_histograms);
    }


    // Update final output histograms from privatized histograms.  Specialized for privatized global-memory counters
    __device__ __forceinline__ void StoreGmemOutput()
    {
        StoreOutput(d_privatized_histograms);
    }


    //---------------------------------------------------------------------
    // Tile accumulation
    //---------------------------------------------------------------------

    // Accumulate pixels.  Specialized for RLE compression.
    __device__ __forceinline__ void AccumulatePixels(
        SampleT             samples[PIXELS_PER_THREAD][NUM_CHANNELS],
        bool                is_valid[PIXELS_PER_THREAD],
        CounterT*           privatized_histograms[NUM_ACTIVE_CHANNELS],
        Int2Type<true>      is_rle_compress)
    {
        #pragma unroll
        for (int CHANNEL = 0; CHANNEL < NUM_ACTIVE_CHANNELS; ++CHANNEL)
        {
            // Bin pixels
            int bins[PIXELS_PER_THREAD];

            #pragma unroll
            for (int PIXEL = 0; PIXEL < PIXELS_PER_THREAD; ++PIXEL)
            {
                bins[PIXEL] = -1;
                privatized_decode_op[CHANNEL].template BinSelect<LOAD_MODIFIER>(samples[PIXEL][CHANNEL], bins[PIXEL], is_valid[PIXEL]);
            }

            CounterT accumulator = 1;

            #pragma unroll
            for (int PIXEL = 0; PIXEL < PIXELS_PER_THREAD - 1; ++PIXEL)
            {
                if (bins[PIXEL] != bins[PIXEL + 1])
                {
                    if (bins[PIXEL] >= 0)
                        atomicAdd(privatized_histograms[CHANNEL] + bins[PIXEL], accumulator);

                     accumulator = 0;
                }
                accumulator++;
            }

            // Last pixel
            if (bins[PIXELS_PER_THREAD - 1] >= 0)
                atomicAdd(privatized_histograms[CHANNEL] + bins[PIXELS_PER_THREAD - 1], accumulator);
        }
    }


    // Accumulate pixels.  Specialized for individual accumulation of each pixel.
    __device__ __forceinline__ void AccumulatePixels(
        SampleT             samples[PIXELS_PER_THREAD][NUM_CHANNELS],
        bool                is_valid[PIXELS_PER_THREAD],
        CounterT*           privatized_histograms[NUM_ACTIVE_CHANNELS],
        Int2Type<false>     is_rle_compress)
    {
        #pragma unroll
        for (int PIXEL = 0; PIXEL < PIXELS_PER_THREAD; ++PIXEL)
        {
            #pragma unroll
            for (int CHANNEL = 0; CHANNEL < NUM_ACTIVE_CHANNELS; ++CHANNEL)
            {
                int bin = -1;
                privatized_decode_op[CHANNEL].template BinSelect<LOAD_MODIFIER>(samples[PIXEL][CHANNEL], bin, is_valid[PIXEL]);
                if (bin >= 0)
                    atomicAdd(privatized_histograms[CHANNEL] + bin, 1);
            }
        }
    }


    /**
     * Accumulate pixel, specialized for smem privatized histogram
     */
    __device__ __forceinline__ void AccumulateSmemPixels(
        SampleT             samples[PIXELS_PER_THREAD][NUM_CHANNELS],
        bool                is_valid[PIXELS_PER_THREAD])
    {
        CounterT* privatized_histograms[NUM_ACTIVE_CHANNELS];

        for (int CHANNEL = 0; CHANNEL < NUM_ACTIVE_CHANNELS; ++CHANNEL)
            privatized_histograms[CHANNEL] = temp_storage.histograms[CHANNEL];

        AccumulatePixels(samples, is_valid, privatized_histograms, Int2Type<IS_RLE_COMPRESS>());
    }


    /**
     * Accumulate pixel, specialized for gmem privatized histogram
     */
    __device__ __forceinline__ void AccumulateGmemPixels(
        SampleT             samples[PIXELS_PER_THREAD][NUM_CHANNELS],
        bool                is_valid[PIXELS_PER_THREAD])
    {
        AccumulatePixels(samples, is_valid, d_privatized_histograms, Int2Type<IS_RLE_COMPRESS>());
    }



    //---------------------------------------------------------------------
    // Tile loading
    //---------------------------------------------------------------------

    // Load full, aligned tile using pixel iterator (multi-channel)
    template <int _NUM_ACTIVE_CHANNELS>
    __device__ __forceinline__ void LoadFullAlignedTile(
        OffsetT                         block_offset,
        int                             valid_samples,
        SampleT                         (&samples)[PIXELS_PER_THREAD][NUM_CHANNELS],
        Int2Type<_NUM_ACTIVE_CHANNELS>  num_active_channels)
    {
        typedef PixelT AliasedPixels[PIXELS_PER_THREAD];

        WrappedPixelIteratorT d_wrapped_pixels((PixelT*) (d_native_samples + block_offset));

        // Load using a wrapped pixel iterator
        BlockLoadPixelT(temp_storage.aliasable.pixel_load).Load(
            d_wrapped_pixels,
            reinterpret_cast<AliasedPixels&>(samples));
    }

    // Load full, aligned tile using quad iterator (single-channel)
    __device__ __forceinline__ void LoadFullAlignedTile(
        OffsetT                         block_offset,
        int                             valid_samples,
        SampleT                         (&samples)[PIXELS_PER_THREAD][NUM_CHANNELS],
        Int2Type<1>                     num_active_channels)
    {
        typedef QuadT AliasedQuads[QUADS_PER_THREAD];

        WrappedQuadIteratorT d_wrapped_quads((QuadT*) (d_native_samples + block_offset));

        // Load using a wrapped quad iterator
        BlockLoadQuadT(temp_storage.aliasable.quad_load).Load(
            d_wrapped_quads,
            reinterpret_cast<AliasedQuads&>(samples));
    }

    // Load full, aligned tile
    __device__ __forceinline__ void LoadTile(
        OffsetT         block_offset,
        int             valid_samples,
        SampleT         (&samples)[PIXELS_PER_THREAD][NUM_CHANNELS],
        Int2Type<true>  is_full_tile,
        Int2Type<true>  is_aligned)
    {
        LoadFullAlignedTile(block_offset, valid_samples, samples, Int2Type<NUM_ACTIVE_CHANNELS>());
    }

    // Load full, mis-aligned tile using sample iterator
    __device__ __forceinline__ void LoadTile(
        OffsetT         block_offset,
        int             valid_samples,
        SampleT         (&samples)[PIXELS_PER_THREAD][NUM_CHANNELS],
        Int2Type<true>  is_full_tile,
        Int2Type<false> is_aligned)
    {
        typedef SampleT AliasedSamples[SAMPLES_PER_THREAD];

        // Load using sample iterator
        BlockLoadSampleT(temp_storage.aliasable.sample_load).Load(
            d_wrapped_samples + block_offset,
            reinterpret_cast<AliasedSamples&>(samples));
    }

    // Load partially-full, aligned tile using the pixel iterator
    __device__ __forceinline__ void LoadTile(
        OffsetT         block_offset,
        int             valid_samples,
        SampleT         (&samples)[PIXELS_PER_THREAD][NUM_CHANNELS],
        Int2Type<false> is_full_tile,
        Int2Type<true>  is_aligned)
    {
        typedef PixelT AliasedPixels[PIXELS_PER_THREAD];

        WrappedPixelIteratorT d_wrapped_pixels((PixelT*) (d_native_samples + block_offset));

        int valid_pixels = valid_samples / NUM_CHANNELS;

        // Load using a wrapped pixel iterator
        BlockLoadPixelT(temp_storage.aliasable.pixel_load).Load(
            d_wrapped_pixels,
            reinterpret_cast<AliasedPixels&>(samples),
            valid_pixels);
    }

    // Load partially-full, mis-aligned tile using sample iterator
    __device__ __forceinline__ void LoadTile(
        OffsetT         block_offset,
        int             valid_samples,
        SampleT         (&samples)[PIXELS_PER_THREAD][NUM_CHANNELS],
        Int2Type<false> is_full_tile,
        Int2Type<false> is_aligned)
    {
        typedef SampleT AliasedSamples[SAMPLES_PER_THREAD];

        BlockLoadSampleT(temp_storage.aliasable.sample_load).Load(
            d_wrapped_samples + block_offset,
            reinterpret_cast<AliasedSamples&>(samples),
            valid_samples);
    }


    //---------------------------------------------------------------------
    // Tile processing
    //---------------------------------------------------------------------

    // Consume a tile of data samples
    template <
        bool IS_ALIGNED,        // Whether the tile offset is aligned (quad-aligned for single-channel, pixel-aligned for multi-channel)
        bool IS_FULL_TILE>      // Whether the tile is full
    __device__ __forceinline__ void ConsumeTile(OffsetT block_offset, int valid_samples)
    {
        SampleT     samples[PIXELS_PER_THREAD][NUM_CHANNELS];
        bool        is_valid[PIXELS_PER_THREAD];

        // Load tile
        LoadTile(
            block_offset,
            valid_samples,
            samples,
            Int2Type<IS_FULL_TILE>(),
            Int2Type<IS_ALIGNED>());

        // Set valid flags
        #pragma unroll
        for (int PIXEL = 0; PIXEL < PIXELS_PER_THREAD; ++PIXEL)
            is_valid[PIXEL] = IS_FULL_TILE || (((threadIdx.x * PIXELS_PER_THREAD + PIXEL) * NUM_CHANNELS) < valid_samples);

        // Accumulate samples
#if CUB_PTX_ARCH >= 120
        if (prefer_smem)
            AccumulateSmemPixels(samples, is_valid);
        else
            AccumulateGmemPixels(samples, is_valid);
#else
        AccumulateGmemPixels(samples, is_valid);
#endif

    }


    // Consume row tiles.  Specialized for work-stealing from queue
    template <bool IS_ALIGNED>
    __device__ __forceinline__ void ConsumeTiles(
        OffsetT             num_row_pixels,             ///< The number of multi-channel pixels per row in the region of interest
        OffsetT             num_rows,                   ///< The number of rows in the region of interest
        OffsetT             row_stride_samples,         ///< The number of samples between starts of consecutive rows in the region of interest
        int                 tiles_per_row,              ///< Number of image tiles per row
        GridQueue<int>      tile_queue,
        Int2Type<true>      is_work_stealing)
    {

        int         num_tiles                   = num_rows * tiles_per_row;
        int         tile_idx                    = (blockIdx.y  * gridDim.x) + blockIdx.x;
        OffsetT     num_even_share_tiles        = gridDim.x * gridDim.y;

        while (tile_idx < num_tiles)
        {
            int     row             = tile_idx / tiles_per_row;
            int     col             = tile_idx - (row * tiles_per_row);
            OffsetT row_offset      = row * row_stride_samples;
            OffsetT col_offset      = (col * TILE_SAMPLES);
            OffsetT tile_offset     = row_offset + col_offset;

            if (col == tiles_per_row - 1)
            {
                // Consume a partially-full tile at the end of the row
                OffsetT num_remaining = (num_row_pixels * NUM_CHANNELS) - col_offset;
                ConsumeTile<IS_ALIGNED, false>(tile_offset, num_remaining);
            } 
            else
            {
                // Consume full tile
                ConsumeTile<IS_ALIGNED, true>(tile_offset, TILE_SAMPLES);
            }

            CTA_SYNC();

            // Get next tile
            if (threadIdx.x == 0)
                temp_storage.tile_idx = tile_queue.Drain(1) + num_even_share_tiles;

            CTA_SYNC();

            tile_idx = temp_storage.tile_idx;
        }
    }


    // Consume row tiles.  Specialized for even-share (striped across thread blocks)
    template <bool IS_ALIGNED>
    __device__ __forceinline__ void ConsumeTiles(
        OffsetT             num_row_pixels,             ///< The number of multi-channel pixels per row in the region of interest
        OffsetT             num_rows,                   ///< The number of rows in the region of interest
        OffsetT             row_stride_samples,         ///< The number of samples between starts of consecutive rows in the region of interest
        int                 tiles_per_row,              ///< Number of image tiles per row
        GridQueue<int>      tile_queue,
        Int2Type<false>     is_work_stealing)
    {
        for (int row = blockIdx.y; row < num_rows; row += gridDim.y)
        {
            OffsetT row_begin   = row * row_stride_samples;
            OffsetT row_end     = row_begin + (num_row_pixels * NUM_CHANNELS);
            OffsetT tile_offset = row_begin + (blockIdx.x * TILE_SAMPLES);

            while (tile_offset < row_end)
            {
                OffsetT num_remaining = row_end - tile_offset;

                if (num_remaining < TILE_SAMPLES)
                {
                    // Consume partial tile
                    ConsumeTile<IS_ALIGNED, false>(tile_offset, num_remaining);
                    break;
                }

                // Consume full tile
                ConsumeTile<IS_ALIGNED, true>(tile_offset, TILE_SAMPLES);
                tile_offset += gridDim.x * TILE_SAMPLES;
            }
        }
    }


    //---------------------------------------------------------------------
    // Parameter extraction
    //---------------------------------------------------------------------

    // Return a native pixel pointer (specialized for CacheModifiedInputIterator types)
    template <
        CacheLoadModifier   _MODIFIER,
        typename            _ValueT,
        typename            _OffsetT>
    __device__ __forceinline__ SampleT* NativePointer(CacheModifiedInputIterator<_MODIFIER, _ValueT, _OffsetT> itr)
    {
        return itr.ptr;
    }

    // Return a native pixel pointer (specialized for other types)
    template <typename IteratorT>
    __device__ __forceinline__ SampleT* NativePointer(IteratorT itr)
    {
        return NULL;
    }



    //---------------------------------------------------------------------
    // Interface
    //---------------------------------------------------------------------


    /**
     * Constructor
     */
    __device__ __forceinline__ AgentHistogram(
        TempStorage         &temp_storage,                                      ///< Reference to temp_storage
        SampleIteratorT     d_samples,                                          ///< Input data to reduce
        int                 (&num_output_bins)[NUM_ACTIVE_CHANNELS],            ///< The number bins per final output histogram
        int                 (&num_privatized_bins)[NUM_ACTIVE_CHANNELS],        ///< The number bins per privatized histogram
        CounterT*           (&d_output_histograms)[NUM_ACTIVE_CHANNELS],        ///< Reference to final output histograms
        CounterT*           (&d_privatized_histograms)[NUM_ACTIVE_CHANNELS],    ///< Reference to privatized histograms
        OutputDecodeOpT     (&output_decode_op)[NUM_ACTIVE_CHANNELS],           ///< The transform operator for determining output bin-ids from privatized counter indices, one for each channel
        PrivatizedDecodeOpT (&privatized_decode_op)[NUM_ACTIVE_CHANNELS])       ///< The transform operator for determining privatized counter indices from samples, one for each channel
    :
        temp_storage(temp_storage.Alias()),
        d_wrapped_samples(d_samples),
        num_output_bins(num_output_bins),
        num_privatized_bins(num_privatized_bins),
        d_output_histograms(d_output_histograms),
        privatized_decode_op(privatized_decode_op),
        output_decode_op(output_decode_op),
        d_native_samples(NativePointer(d_wrapped_samples)),
        prefer_smem((MEM_PREFERENCE == SMEM) ?
            true :                              // prefer smem privatized histograms
            (MEM_PREFERENCE == GMEM) ?
                false :                         // prefer gmem privatized histograms
                blockIdx.x & 1)                 // prefer blended privatized histograms
    {
        int blockId = (blockIdx.y * gridDim.x) + blockIdx.x;

        // Initialize the locations of this block's privatized histograms
        for (int CHANNEL = 0; CHANNEL < NUM_ACTIVE_CHANNELS; ++CHANNEL)
            this->d_privatized_histograms[CHANNEL] = d_privatized_histograms[CHANNEL] + (blockId * num_privatized_bins[CHANNEL]);
    }


    /**
     * Consume image
     */
    __device__ __forceinline__ void ConsumeTiles(
        OffsetT             num_row_pixels,             ///< The number of multi-channel pixels per row in the region of interest
        OffsetT             num_rows,                   ///< The number of rows in the region of interest
        OffsetT             row_stride_samples,         ///< The number of samples between starts of consecutive rows in the region of interest
        int                 tiles_per_row,              ///< Number of image tiles per row
        GridQueue<int>      tile_queue)                 ///< Queue descriptor for assigning tiles of work to thread blocks
    {
        // Check whether all row starting offsets are quad-aligned (in single-channel) or pixel-aligned (in multi-channel)
        int     quad_mask           = AlignBytes<QuadT>::ALIGN_BYTES - 1;
        int     pixel_mask          = AlignBytes<PixelT>::ALIGN_BYTES - 1;
        size_t  row_bytes           = sizeof(SampleT) * row_stride_samples;

        bool quad_aligned_rows      = (NUM_CHANNELS == 1) && (SAMPLES_PER_THREAD % 4 == 0) &&     // Single channel
                                        ((size_t(d_native_samples) & quad_mask) == 0) &&        // ptr is quad-aligned
                                        ((num_rows == 1) || ((row_bytes & quad_mask) == 0));    // number of row-samples is a multiple of the alignment of the quad

        bool pixel_aligned_rows     = (NUM_CHANNELS > 1) &&                                     // Multi channel
                                        ((size_t(d_native_samples) & pixel_mask) == 0) &&       // ptr is pixel-aligned
                                        ((row_bytes & pixel_mask) == 0);                        // number of row-samples is a multiple of the alignment of the pixel

        // Whether rows are aligned and can be vectorized
        if ((d_native_samples != NULL) && (quad_aligned_rows || pixel_aligned_rows))
            ConsumeTiles<true>(num_row_pixels, num_rows, row_stride_samples, tiles_per_row, tile_queue, Int2Type<IS_WORK_STEALING>());
        else
            ConsumeTiles<false>(num_row_pixels, num_rows, row_stride_samples, tiles_per_row, tile_queue, Int2Type<IS_WORK_STEALING>());
    }


    /**
     * Initialize privatized bin counters.  Specialized for privatized shared-memory counters
     */
    __device__ __forceinline__ void InitBinCounters()
    {
        if (prefer_smem)
            InitSmemBinCounters();
        else
            InitGmemBinCounters();
    }


    /**
     * Store privatized histogram to device-accessible memory.  Specialized for privatized shared-memory counters
     */
    __device__ __forceinline__ void StoreOutput()
    {
        if (prefer_smem)
            StoreSmemOutput();
        else
            StoreGmemOutput();
    }


};




}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)

