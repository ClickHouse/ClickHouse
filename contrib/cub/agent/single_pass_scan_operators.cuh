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
 * Callback operator types for supplying BlockScan prefixes
 */

#pragma once

#include <iterator>

#include "../thread/thread_load.cuh"
#include "../thread/thread_store.cuh"
#include "../warp/warp_reduce.cuh"
#include "../util_arch.cuh"
#include "../util_device.cuh"
#include "../util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {


/******************************************************************************
 * Prefix functor type for maintaining a running prefix while scanning a
 * region independent of other thread blocks
 ******************************************************************************/

/**
 * Stateful callback operator type for supplying BlockScan prefixes.
 * Maintains a running prefix that can be applied to consecutive
 * BlockScan operations.
 */
template <
    typename T,                 ///< BlockScan value type
    typename ScanOpT>            ///< Wrapped scan operator type
struct BlockScanRunningPrefixOp
{
    ScanOpT     op;                 ///< Wrapped scan operator
    T           running_total;      ///< Running block-wide prefix

    /// Constructor
    __device__ __forceinline__ BlockScanRunningPrefixOp(ScanOpT op)
    :
        op(op)
    {}

    /// Constructor
    __device__ __forceinline__ BlockScanRunningPrefixOp(
        T starting_prefix,
        ScanOpT op)
    :
        op(op),
        running_total(starting_prefix)
    {}

    /**
     * Prefix callback operator.  Returns the block-wide running_total in thread-0.
     */
    __device__ __forceinline__ T operator()(
        const T &block_aggregate)              ///< The aggregate sum of the BlockScan inputs
    {
        T retval = running_total;
        running_total = op(running_total, block_aggregate);
        return retval;
    }
};


/******************************************************************************
 * Generic tile status interface types for block-cooperative scans
 ******************************************************************************/

/**
 * Enumerations of tile status
 */
enum ScanTileStatus
{
    SCAN_TILE_OOB,          // Out-of-bounds (e.g., padding)
    SCAN_TILE_INVALID = 99, // Not yet processed
    SCAN_TILE_PARTIAL,      // Tile aggregate is available
    SCAN_TILE_INCLUSIVE,    // Inclusive tile prefix is available
};


/**
 * Tile status interface.
 */
template <
    typename    T,
    bool        SINGLE_WORD = Traits<T>::PRIMITIVE>
struct ScanTileState;


/**
 * Tile status interface specialized for scan status and value types
 * that can be combined into one machine word that can be
 * read/written coherently in a single access.
 */
template <typename T>
struct ScanTileState<T, true>
{
    // Status word type
    typedef typename If<(sizeof(T) == 8),
        long long,
        typename If<(sizeof(T) == 4),
            int,
            typename If<(sizeof(T) == 2),
                short,
                char>::Type>::Type>::Type StatusWord;


    // Unit word type
    typedef typename If<(sizeof(T) == 8),
        longlong2,
        typename If<(sizeof(T) == 4),
            int2,
            typename If<(sizeof(T) == 2),
                int,
                uchar2>::Type>::Type>::Type TxnWord;


    // Device word type
    struct TileDescriptor
    {
        StatusWord  status;
        T           value;
    };


    // Constants
    enum
    {
        TILE_STATUS_PADDING = CUB_PTX_WARP_THREADS,
    };


    // Device storage
    TxnWord *d_tile_descriptors;

    /// Constructor
    __host__ __device__ __forceinline__
    ScanTileState()
    :
        d_tile_descriptors(NULL)
    {}


    /// Initializer
    __host__ __device__ __forceinline__
    cudaError_t Init(
        int     /*num_tiles*/,                      ///< [in] Number of tiles
        void    *d_temp_storage,                    ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
        size_t  /*temp_storage_bytes*/)             ///< [in] Size in bytes of \t d_temp_storage allocation
    {
        d_tile_descriptors = reinterpret_cast<TxnWord*>(d_temp_storage);
        return cudaSuccess;
    }


    /**
     * Compute device memory needed for tile status
     */
    __host__ __device__ __forceinline__
    static cudaError_t AllocationSize(
        int     num_tiles,                          ///< [in] Number of tiles
        size_t  &temp_storage_bytes)                ///< [out] Size in bytes of \t d_temp_storage allocation
    {
        temp_storage_bytes = (num_tiles + TILE_STATUS_PADDING) * sizeof(TileDescriptor);       // bytes needed for tile status descriptors
        return cudaSuccess;
    }


    /**
     * Initialize (from device)
     */
    __device__ __forceinline__ void InitializeStatus(int num_tiles)
    {
        int tile_idx = (blockIdx.x * blockDim.x) + threadIdx.x;

        TxnWord val = TxnWord();
        TileDescriptor *descriptor = reinterpret_cast<TileDescriptor*>(&val);

        if (tile_idx < num_tiles)
        {
            // Not-yet-set
            descriptor->status = StatusWord(SCAN_TILE_INVALID);
            d_tile_descriptors[TILE_STATUS_PADDING + tile_idx] = val;
        }

        if ((blockIdx.x == 0) && (threadIdx.x < TILE_STATUS_PADDING))
        {
            // Padding
            descriptor->status = StatusWord(SCAN_TILE_OOB);
            d_tile_descriptors[threadIdx.x] = val;
        }
    }


    /**
     * Update the specified tile's inclusive value and corresponding status
     */
    __device__ __forceinline__ void SetInclusive(int tile_idx, T tile_inclusive)
    {
        TileDescriptor tile_descriptor;
        tile_descriptor.status = SCAN_TILE_INCLUSIVE;
        tile_descriptor.value = tile_inclusive;

        TxnWord alias;
        *reinterpret_cast<TileDescriptor*>(&alias) = tile_descriptor;
        ThreadStore<STORE_CG>(d_tile_descriptors + TILE_STATUS_PADDING + tile_idx, alias);
    }


    /**
     * Update the specified tile's partial value and corresponding status
     */
    __device__ __forceinline__ void SetPartial(int tile_idx, T tile_partial)
    {
        TileDescriptor tile_descriptor;
        tile_descriptor.status = SCAN_TILE_PARTIAL;
        tile_descriptor.value = tile_partial;

        TxnWord alias;
        *reinterpret_cast<TileDescriptor*>(&alias) = tile_descriptor;
        ThreadStore<STORE_CG>(d_tile_descriptors + TILE_STATUS_PADDING + tile_idx, alias);
    }

    /**
     * Wait for the corresponding tile to become non-invalid
     */
    __device__ __forceinline__ void WaitForValid(
        int             tile_idx,
        StatusWord      &status,
        T               &value)
    {
        TileDescriptor tile_descriptor;
        do
        {
            __threadfence_block(); // prevent hoisting loads from loop
            TxnWord alias = ThreadLoad<LOAD_CG>(d_tile_descriptors + TILE_STATUS_PADDING + tile_idx);
            tile_descriptor = reinterpret_cast<TileDescriptor&>(alias);

        } while (WARP_ANY((tile_descriptor.status == SCAN_TILE_INVALID), 0xffffffff));

        status = tile_descriptor.status;
        value = tile_descriptor.value;
    }

};



/**
 * Tile status interface specialized for scan status and value types that
 * cannot be combined into one machine word.
 */
template <typename T>
struct ScanTileState<T, false>
{
    // Status word type
    typedef char StatusWord;

    // Constants
    enum
    {
        TILE_STATUS_PADDING = CUB_PTX_WARP_THREADS,
    };

    // Device storage
    StatusWord  *d_tile_status;
    T           *d_tile_partial;
    T           *d_tile_inclusive;

    /// Constructor
    __host__ __device__ __forceinline__
    ScanTileState()
    :
        d_tile_status(NULL),
        d_tile_partial(NULL),
        d_tile_inclusive(NULL)
    {}


    /// Initializer
    __host__ __device__ __forceinline__
    cudaError_t Init(
        int     num_tiles,                          ///< [in] Number of tiles
        void    *d_temp_storage,                    ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
        size_t  temp_storage_bytes)                 ///< [in] Size in bytes of \t d_temp_storage allocation
    {
        cudaError_t error = cudaSuccess;
        do
        {
            void*   allocations[3];
            size_t  allocation_sizes[3];

            allocation_sizes[0] = (num_tiles + TILE_STATUS_PADDING) * sizeof(StatusWord);           // bytes needed for tile status descriptors
            allocation_sizes[1] = (num_tiles + TILE_STATUS_PADDING) * sizeof(Uninitialized<T>);     // bytes needed for partials
            allocation_sizes[2] = (num_tiles + TILE_STATUS_PADDING) * sizeof(Uninitialized<T>);     // bytes needed for inclusives

            // Compute allocation pointers into the single storage blob
            if (CubDebug(error = AliasTemporaries(d_temp_storage, temp_storage_bytes, allocations, allocation_sizes))) break;

            // Alias the offsets
            d_tile_status       = reinterpret_cast<StatusWord*>(allocations[0]);
            d_tile_partial      = reinterpret_cast<T*>(allocations[1]);
            d_tile_inclusive    = reinterpret_cast<T*>(allocations[2]);
        }
        while (0);

        return error;
    }


    /**
     * Compute device memory needed for tile status
     */
    __host__ __device__ __forceinline__
    static cudaError_t AllocationSize(
        int     num_tiles,                          ///< [in] Number of tiles
        size_t  &temp_storage_bytes)                ///< [out] Size in bytes of \t d_temp_storage allocation
    {
        // Specify storage allocation requirements
        size_t  allocation_sizes[3];
        allocation_sizes[0] = (num_tiles + TILE_STATUS_PADDING) * sizeof(StatusWord);         // bytes needed for tile status descriptors
        allocation_sizes[1] = (num_tiles + TILE_STATUS_PADDING) * sizeof(Uninitialized<T>);   // bytes needed for partials
        allocation_sizes[2] = (num_tiles + TILE_STATUS_PADDING) * sizeof(Uninitialized<T>);   // bytes needed for inclusives

        // Set the necessary size of the blob
        void* allocations[3];
        return CubDebug(AliasTemporaries(NULL, temp_storage_bytes, allocations, allocation_sizes));
    }


    /**
     * Initialize (from device)
     */
    __device__ __forceinline__ void InitializeStatus(int num_tiles)
    {
        int tile_idx = (blockIdx.x * blockDim.x) + threadIdx.x;
        if (tile_idx < num_tiles)
        {
            // Not-yet-set
            d_tile_status[TILE_STATUS_PADDING + tile_idx] = StatusWord(SCAN_TILE_INVALID);
        }

        if ((blockIdx.x == 0) && (threadIdx.x < TILE_STATUS_PADDING))
        {
            // Padding
            d_tile_status[threadIdx.x] = StatusWord(SCAN_TILE_OOB);
        }
    }


    /**
     * Update the specified tile's inclusive value and corresponding status
     */
    __device__ __forceinline__ void SetInclusive(int tile_idx, T tile_inclusive)
    {
        // Update tile inclusive value
        ThreadStore<STORE_CG>(d_tile_inclusive + TILE_STATUS_PADDING + tile_idx, tile_inclusive);

        // Fence
        __threadfence();

        // Update tile status
        ThreadStore<STORE_CG>(d_tile_status + TILE_STATUS_PADDING + tile_idx, StatusWord(SCAN_TILE_INCLUSIVE));
    }


    /**
     * Update the specified tile's partial value and corresponding status
     */
    __device__ __forceinline__ void SetPartial(int tile_idx, T tile_partial)
    {
        // Update tile partial value
        ThreadStore<STORE_CG>(d_tile_partial + TILE_STATUS_PADDING + tile_idx, tile_partial);

        // Fence
        __threadfence();

        // Update tile status
        ThreadStore<STORE_CG>(d_tile_status + TILE_STATUS_PADDING + tile_idx, StatusWord(SCAN_TILE_PARTIAL));
    }

    /**
     * Wait for the corresponding tile to become non-invalid
     */
    __device__ __forceinline__ void WaitForValid(
        int             tile_idx,
        StatusWord      &status,
        T               &value)
    {
        do {
            status = ThreadLoad<LOAD_CG>(d_tile_status + TILE_STATUS_PADDING + tile_idx);

            __threadfence();    // prevent hoisting loads from loop or loads below above this one

        } while (status == SCAN_TILE_INVALID);

        if (status == StatusWord(SCAN_TILE_PARTIAL)) 
            value = ThreadLoad<LOAD_CG>(d_tile_partial + TILE_STATUS_PADDING + tile_idx);
        else
            value = ThreadLoad<LOAD_CG>(d_tile_inclusive + TILE_STATUS_PADDING + tile_idx);
    }
};


/******************************************************************************
 * ReduceByKey tile status interface types for block-cooperative scans
 ******************************************************************************/

/**
 * Tile status interface for reduction by key.
 *
 */
template <
    typename    ValueT,
    typename    KeyT,
    bool        SINGLE_WORD = (Traits<ValueT>::PRIMITIVE) && (sizeof(ValueT) + sizeof(KeyT) < 16)>
struct ReduceByKeyScanTileState;


/**
 * Tile status interface for reduction by key, specialized for scan status and value types that
 * cannot be combined into one machine word.
 */
template <
    typename    ValueT,
    typename    KeyT>
struct ReduceByKeyScanTileState<ValueT, KeyT, false> :
    ScanTileState<KeyValuePair<KeyT, ValueT> >
{
    typedef ScanTileState<KeyValuePair<KeyT, ValueT> > SuperClass;

    /// Constructor
    __host__ __device__ __forceinline__
    ReduceByKeyScanTileState() : SuperClass() {}
};


/**
 * Tile status interface for reduction by key, specialized for scan status and value types that
 * can be combined into one machine word that can be read/written coherently in a single access.
 */
template <
    typename ValueT,
    typename KeyT>
struct ReduceByKeyScanTileState<ValueT, KeyT, true>
{
    typedef KeyValuePair<KeyT, ValueT>KeyValuePairT;

    // Constants
    enum
    {
        PAIR_SIZE           = sizeof(ValueT) + sizeof(KeyT),
        TXN_WORD_SIZE       = 1 << Log2<PAIR_SIZE + 1>::VALUE,
        STATUS_WORD_SIZE    = TXN_WORD_SIZE - PAIR_SIZE,

        TILE_STATUS_PADDING = CUB_PTX_WARP_THREADS,
    };

    // Status word type
    typedef typename If<(STATUS_WORD_SIZE == 8),
        long long,
        typename If<(STATUS_WORD_SIZE == 4),
            int,
            typename If<(STATUS_WORD_SIZE == 2),
                short,
                char>::Type>::Type>::Type StatusWord;

    // Status word type
    typedef typename If<(TXN_WORD_SIZE == 16),
        longlong2,
        typename If<(TXN_WORD_SIZE == 8),
            long long,
            int>::Type>::Type TxnWord;

    // Device word type (for when sizeof(ValueT) == sizeof(KeyT))
    struct TileDescriptorBigStatus
    {
        KeyT        key;
        ValueT      value;
        StatusWord  status;
    };

    // Device word type (for when sizeof(ValueT) != sizeof(KeyT))
    struct TileDescriptorLittleStatus
    {
        ValueT      value;
        StatusWord  status;
        KeyT        key;
    };

    // Device word type
    typedef typename If<
            (sizeof(ValueT) == sizeof(KeyT)),
            TileDescriptorBigStatus,
            TileDescriptorLittleStatus>::Type
        TileDescriptor;


    // Device storage
    TxnWord *d_tile_descriptors;


    /// Constructor
    __host__ __device__ __forceinline__
    ReduceByKeyScanTileState()
    :
        d_tile_descriptors(NULL)
    {}


    /// Initializer
    __host__ __device__ __forceinline__
    cudaError_t Init(
        int     /*num_tiles*/,                      ///< [in] Number of tiles
        void    *d_temp_storage,                    ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
        size_t  /*temp_storage_bytes*/)             ///< [in] Size in bytes of \t d_temp_storage allocation
    {
        d_tile_descriptors = reinterpret_cast<TxnWord*>(d_temp_storage);
        return cudaSuccess;
    }


    /**
     * Compute device memory needed for tile status
     */
    __host__ __device__ __forceinline__
    static cudaError_t AllocationSize(
        int     num_tiles,                          ///< [in] Number of tiles
        size_t  &temp_storage_bytes)                ///< [out] Size in bytes of \t d_temp_storage allocation
    {
        temp_storage_bytes = (num_tiles + TILE_STATUS_PADDING) * sizeof(TileDescriptor);       // bytes needed for tile status descriptors
        return cudaSuccess;
    }


    /**
     * Initialize (from device)
     */
    __device__ __forceinline__ void InitializeStatus(int num_tiles)
    {
        int             tile_idx    = (blockIdx.x * blockDim.x) + threadIdx.x;
        TxnWord         val         = TxnWord();
        TileDescriptor  *descriptor = reinterpret_cast<TileDescriptor*>(&val);

        if (tile_idx < num_tiles)
        {
            // Not-yet-set
            descriptor->status = StatusWord(SCAN_TILE_INVALID);
            d_tile_descriptors[TILE_STATUS_PADDING + tile_idx] = val;
        }

        if ((blockIdx.x == 0) && (threadIdx.x < TILE_STATUS_PADDING))
        {
            // Padding
            descriptor->status = StatusWord(SCAN_TILE_OOB);
            d_tile_descriptors[threadIdx.x] = val;
        }
    }


    /**
     * Update the specified tile's inclusive value and corresponding status
     */
    __device__ __forceinline__ void SetInclusive(int tile_idx, KeyValuePairT tile_inclusive)
    {
        TileDescriptor tile_descriptor;
        tile_descriptor.status  = SCAN_TILE_INCLUSIVE;
        tile_descriptor.value   = tile_inclusive.value;
        tile_descriptor.key     = tile_inclusive.key;

        TxnWord alias;
        *reinterpret_cast<TileDescriptor*>(&alias) = tile_descriptor;
        ThreadStore<STORE_CG>(d_tile_descriptors + TILE_STATUS_PADDING + tile_idx, alias);
    }


    /**
     * Update the specified tile's partial value and corresponding status
     */
    __device__ __forceinline__ void SetPartial(int tile_idx, KeyValuePairT tile_partial)
    {
        TileDescriptor tile_descriptor;
        tile_descriptor.status  = SCAN_TILE_PARTIAL;
        tile_descriptor.value   = tile_partial.value;
        tile_descriptor.key     = tile_partial.key;

        TxnWord alias;
        *reinterpret_cast<TileDescriptor*>(&alias) = tile_descriptor;
        ThreadStore<STORE_CG>(d_tile_descriptors + TILE_STATUS_PADDING + tile_idx, alias);
    }

    /**
     * Wait for the corresponding tile to become non-invalid
     */
    __device__ __forceinline__ void WaitForValid(
        int                     tile_idx,
        StatusWord              &status,
        KeyValuePairT           &value)
    {
//        TxnWord         alias           = ThreadLoad<LOAD_CG>(d_tile_descriptors + TILE_STATUS_PADDING + tile_idx);
//        TileDescriptor  tile_descriptor = reinterpret_cast<TileDescriptor&>(alias);
//
//        while (tile_descriptor.status == SCAN_TILE_INVALID)
//        {
//            __threadfence_block(); // prevent hoisting loads from loop
//
//            alias           = ThreadLoad<LOAD_CG>(d_tile_descriptors + TILE_STATUS_PADDING + tile_idx);
//            tile_descriptor = reinterpret_cast<TileDescriptor&>(alias);
//        }
//
//        status      = tile_descriptor.status;
//        value.value = tile_descriptor.value;
//        value.key   = tile_descriptor.key;

        TileDescriptor tile_descriptor;
        do
        {
            __threadfence_block(); // prevent hoisting loads from loop
            TxnWord alias = ThreadLoad<LOAD_CG>(d_tile_descriptors + TILE_STATUS_PADDING + tile_idx);
            tile_descriptor = reinterpret_cast<TileDescriptor&>(alias);

        } while (WARP_ANY((tile_descriptor.status == SCAN_TILE_INVALID), 0xffffffff));

        status      = tile_descriptor.status;
        value.value = tile_descriptor.value;
        value.key   = tile_descriptor.key;
    }

};


/******************************************************************************
 * Prefix call-back operator for coupling local block scan within a
 * block-cooperative scan
 ******************************************************************************/

/**
 * Stateful block-scan prefix functor.  Provides the the running prefix for
 * the current tile by using the call-back warp to wait on on
 * aggregates/prefixes from predecessor tiles to become available.
 */
template <
    typename    T,
    typename    ScanOpT,
    typename    ScanTileStateT,
    int         PTX_ARCH = CUB_PTX_ARCH>
struct TilePrefixCallbackOp
{
    // Parameterized warp reduce
    typedef WarpReduce<T, CUB_PTX_WARP_THREADS, PTX_ARCH> WarpReduceT;

    // Temporary storage type
    struct _TempStorage
    {
        typename WarpReduceT::TempStorage   warp_reduce;
        T                                   exclusive_prefix;
        T                                   inclusive_prefix;
        T                                   block_aggregate;
    };

    // Alias wrapper allowing temporary storage to be unioned
    struct TempStorage : Uninitialized<_TempStorage> {};

    // Type of status word
    typedef typename ScanTileStateT::StatusWord StatusWord;

    // Fields
    _TempStorage&               temp_storage;       ///< Reference to a warp-reduction instance
    ScanTileStateT&             tile_status;        ///< Interface to tile status
    ScanOpT                     scan_op;            ///< Binary scan operator
    int                         tile_idx;           ///< The current tile index
    T                           exclusive_prefix;   ///< Exclusive prefix for the tile
    T                           inclusive_prefix;   ///< Inclusive prefix for the tile

    // Constructor
    __device__ __forceinline__
    TilePrefixCallbackOp(
        ScanTileStateT       &tile_status,
        TempStorage         &temp_storage,
        ScanOpT              scan_op,
        int                 tile_idx)
    :
        temp_storage(temp_storage.Alias()),
        tile_status(tile_status),
        scan_op(scan_op),
        tile_idx(tile_idx) {}


    // Block until all predecessors within the warp-wide window have non-invalid status
    __device__ __forceinline__
    void ProcessWindow(
        int         predecessor_idx,        ///< Preceding tile index to inspect
        StatusWord  &predecessor_status,    ///< [out] Preceding tile status
        T           &window_aggregate)      ///< [out] Relevant partial reduction from this window of preceding tiles
    {
        T value;
        tile_status.WaitForValid(predecessor_idx, predecessor_status, value);

        // Perform a segmented reduction to get the prefix for the current window.
        // Use the swizzled scan operator because we are now scanning *down* towards thread0.

        int tail_flag = (predecessor_status == StatusWord(SCAN_TILE_INCLUSIVE));
        window_aggregate = WarpReduceT(temp_storage.warp_reduce).TailSegmentedReduce(
            value,
            tail_flag,
            SwizzleScanOp<ScanOpT>(scan_op));
    }


    // BlockScan prefix callback functor (called by the first warp)
    __device__ __forceinline__
    T operator()(T block_aggregate)
    {

        // Update our status with our tile-aggregate
        if (threadIdx.x == 0)
        {
            temp_storage.block_aggregate = block_aggregate;
            tile_status.SetPartial(tile_idx, block_aggregate);
        }

        int         predecessor_idx = tile_idx - threadIdx.x - 1;
        StatusWord  predecessor_status;
        T           window_aggregate;

        // Wait for the warp-wide window of predecessor tiles to become valid
        ProcessWindow(predecessor_idx, predecessor_status, window_aggregate);

        // The exclusive tile prefix starts out as the current window aggregate
        exclusive_prefix = window_aggregate;

        // Keep sliding the window back until we come across a tile whose inclusive prefix is known
        while (WARP_ALL((predecessor_status != StatusWord(SCAN_TILE_INCLUSIVE)), 0xffffffff))
        {
            predecessor_idx -= CUB_PTX_WARP_THREADS;

            // Update exclusive tile prefix with the window prefix
            ProcessWindow(predecessor_idx, predecessor_status, window_aggregate);
            exclusive_prefix = scan_op(window_aggregate, exclusive_prefix);
        }

        // Compute the inclusive tile prefix and update the status for this tile
        if (threadIdx.x == 0)
        {
            inclusive_prefix = scan_op(exclusive_prefix, block_aggregate);
            tile_status.SetInclusive(tile_idx, inclusive_prefix);

            temp_storage.exclusive_prefix = exclusive_prefix;
            temp_storage.inclusive_prefix = inclusive_prefix;
        }

        // Return exclusive_prefix
        return exclusive_prefix;
    }

    // Get the exclusive prefix stored in temporary storage
    __device__ __forceinline__
    T GetExclusivePrefix()
    {
        return temp_storage.exclusive_prefix;
    }

    // Get the inclusive prefix stored in temporary storage
    __device__ __forceinline__
    T GetInclusivePrefix()
    {
        return temp_storage.inclusive_prefix;
    }

    // Get the block aggregate stored in temporary storage
    __device__ __forceinline__
    T GetBlockAggregate()
    {
        return temp_storage.block_aggregate;
    }

};


}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)

