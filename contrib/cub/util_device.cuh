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
 * Properties of a given CUDA device and the corresponding PTX bundle
 */

#pragma once

#include "util_type.cuh"
#include "util_arch.cuh"
#include "util_debug.cuh"
#include "util_namespace.cuh"
#include "util_macro.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {


/**
 * \addtogroup UtilMgmt
 * @{
 */

#ifndef DOXYGEN_SHOULD_SKIP_THIS    // Do not document


/**
 * Alias temporaries to externally-allocated device storage (or simply return the amount of storage needed).
 */
template <int ALLOCATIONS>
__host__ __device__ __forceinline__
cudaError_t AliasTemporaries(
    void    *d_temp_storage,                    ///< [in] %Device-accessible allocation of temporary storage.  When NULL, the required allocation size is written to \p temp_storage_bytes and no work is done.
    size_t  &temp_storage_bytes,                ///< [in,out] Size in bytes of \t d_temp_storage allocation
    void*   (&allocations)[ALLOCATIONS],        ///< [in,out] Pointers to device allocations needed
    size_t  (&allocation_sizes)[ALLOCATIONS])   ///< [in] Sizes in bytes of device allocations needed
{
    const int ALIGN_BYTES   = 256;
    const int ALIGN_MASK    = ~(ALIGN_BYTES - 1);

    // Compute exclusive prefix sum over allocation requests
    size_t allocation_offsets[ALLOCATIONS];
    size_t bytes_needed = 0;
    for (int i = 0; i < ALLOCATIONS; ++i)
    {
        size_t allocation_bytes = (allocation_sizes[i] + ALIGN_BYTES - 1) & ALIGN_MASK;
        allocation_offsets[i] = bytes_needed;
        bytes_needed += allocation_bytes;
    }
    bytes_needed += ALIGN_BYTES - 1;

    // Check if the caller is simply requesting the size of the storage allocation
    if (!d_temp_storage)
    {
        temp_storage_bytes = bytes_needed;
        return cudaSuccess;
    }

    // Check if enough storage provided
    if (temp_storage_bytes < bytes_needed)
    {
        return CubDebug(cudaErrorInvalidValue);
    }

    // Alias
    d_temp_storage = (void *) ((size_t(d_temp_storage) + ALIGN_BYTES - 1) & ALIGN_MASK);
    for (int i = 0; i < ALLOCATIONS; ++i)
    {
        allocations[i] = static_cast<char*>(d_temp_storage) + allocation_offsets[i];
    }

    return cudaSuccess;
}


/**
 * Empty kernel for querying PTX manifest metadata (e.g., version) for the current device
 */
template <typename T>
__global__ void EmptyKernel(void) { }


#endif  // DOXYGEN_SHOULD_SKIP_THIS

/**
 * \brief Retrieves the PTX version that will be used on the current device (major * 100 + minor * 10)
 */
CUB_RUNTIME_FUNCTION __forceinline__ cudaError_t PtxVersion(int &ptx_version)
{
    struct Dummy
    {
        /// Type definition of the EmptyKernel kernel entry point
        typedef void (*EmptyKernelPtr)();

        /// Force EmptyKernel<void> to be generated if this class is used
        CUB_RUNTIME_FUNCTION __forceinline__
        EmptyKernelPtr Empty()
        {
            return EmptyKernel<void>;
        }
    };


#ifndef CUB_RUNTIME_ENABLED
    (void)ptx_version;

    // CUDA API calls not supported from this device
    return cudaErrorInvalidConfiguration;

#elif (CUB_PTX_ARCH > 0)

    ptx_version = CUB_PTX_ARCH;
    return cudaSuccess;

#else

    cudaError_t error = cudaSuccess;
    do
    {
        cudaFuncAttributes empty_kernel_attrs;
        if (CubDebug(error = cudaFuncGetAttributes(&empty_kernel_attrs, EmptyKernel<void>))) break;
        ptx_version = empty_kernel_attrs.ptxVersion * 10;
    }
    while (0);

    return error;

#endif
}


/**
 * \brief Retrieves the SM version (major * 100 + minor * 10)
 */
CUB_RUNTIME_FUNCTION __forceinline__ cudaError_t SmVersion(int &sm_version, int device_ordinal)
{
#ifndef CUB_RUNTIME_ENABLED
    (void)sm_version;
    (void)device_ordinal;

    // CUDA API calls not supported from this device
    return cudaErrorInvalidConfiguration;

#else

    cudaError_t error = cudaSuccess;
    do
    {
        // Fill in SM version
        int major, minor;
        if (CubDebug(error = cudaDeviceGetAttribute(&major, cudaDevAttrComputeCapabilityMajor, device_ordinal))) break;
        if (CubDebug(error = cudaDeviceGetAttribute(&minor, cudaDevAttrComputeCapabilityMinor, device_ordinal))) break;
        sm_version = major * 100 + minor * 10;
    }
    while (0);

    return error;

#endif
}


#ifndef DOXYGEN_SHOULD_SKIP_THIS    // Do not document

/**
 * Synchronize the stream if specified
 */
CUB_RUNTIME_FUNCTION __forceinline__
static cudaError_t SyncStream(cudaStream_t stream)
{
#if (CUB_PTX_ARCH == 0)
    return cudaStreamSynchronize(stream);
#else
    (void)stream;
    // Device can't yet sync on a specific stream
    return cudaDeviceSynchronize();
#endif
}


/**
 * \brief Computes maximum SM occupancy in thread blocks for executing the given kernel function pointer \p kernel_ptr on the current device with \p block_threads per thread block.
 *
 * \par Snippet
 * The code snippet below illustrates the use of the MaxSmOccupancy function.
 * \par
 * \code
 * #include <cub/cub.cuh>   // or equivalently <cub/util_device.cuh>
 *
 * template <typename T>
 * __global__ void ExampleKernel()
 * {
 *     // Allocate shared memory for BlockScan
 *     __shared__ volatile T buffer[4096];
 *
 *        ...
 * }
 *
 *     ...
 *
 * // Determine SM occupancy for ExampleKernel specialized for unsigned char
 * int max_sm_occupancy;
 * MaxSmOccupancy(max_sm_occupancy, ExampleKernel<unsigned char>, 64);
 *
 * // max_sm_occupancy  <-- 4 on SM10
 * // max_sm_occupancy  <-- 8 on SM20
 * // max_sm_occupancy  <-- 12 on SM35
 *
 * \endcode
 *
 */
template <typename KernelPtr>
CUB_RUNTIME_FUNCTION __forceinline__
cudaError_t MaxSmOccupancy(
    int                 &max_sm_occupancy,          ///< [out] maximum number of thread blocks that can reside on a single SM
    KernelPtr           kernel_ptr,                 ///< [in] Kernel pointer for which to compute SM occupancy
    int                 block_threads,              ///< [in] Number of threads per thread block
    int                 dynamic_smem_bytes = 0)
{
#ifndef CUB_RUNTIME_ENABLED
    (void)dynamic_smem_bytes;
    (void)block_threads;
    (void)kernel_ptr;
    (void)max_sm_occupancy;

    // CUDA API calls not supported from this device
    return CubDebug(cudaErrorInvalidConfiguration);

#else

    return cudaOccupancyMaxActiveBlocksPerMultiprocessor (
        &max_sm_occupancy,
        kernel_ptr,
        block_threads,
        dynamic_smem_bytes);

#endif  // CUB_RUNTIME_ENABLED
}


/******************************************************************************
 * Policy management
 ******************************************************************************/

/**
 * Kernel dispatch configuration
 */
struct KernelConfig
{
    int block_threads;
    int items_per_thread;
    int tile_size;
    int sm_occupancy;

    CUB_RUNTIME_FUNCTION __forceinline__
    KernelConfig() : block_threads(0), items_per_thread(0), tile_size(0), sm_occupancy(0) {}

    template <typename AgentPolicyT, typename KernelPtrT>
    CUB_RUNTIME_FUNCTION __forceinline__
    cudaError_t Init(KernelPtrT kernel_ptr)
    {
        block_threads        = AgentPolicyT::BLOCK_THREADS;
        items_per_thread     = AgentPolicyT::ITEMS_PER_THREAD;
        tile_size            = block_threads * items_per_thread;
        cudaError_t retval   = MaxSmOccupancy(sm_occupancy, kernel_ptr, block_threads);
        return retval;
    }
};



/// Helper for dispatching into a policy chain
template <int PTX_VERSION, typename PolicyT, typename PrevPolicyT>
struct ChainedPolicy
{
   /// The policy for the active compiler pass
   typedef typename If<(CUB_PTX_ARCH < PTX_VERSION), typename PrevPolicyT::ActivePolicy, PolicyT>::Type ActivePolicy;

   /// Specializes and dispatches op in accordance to the first policy in the chain of adequate PTX version
   template <typename FunctorT>
   CUB_RUNTIME_FUNCTION __forceinline__
   static cudaError_t Invoke(int ptx_version, FunctorT &op)
   {
       if (ptx_version < PTX_VERSION) {
           return PrevPolicyT::Invoke(ptx_version, op);
       }
       return op.template Invoke<PolicyT>();
   }
};

/// Helper for dispatching into a policy chain (end-of-chain specialization)
template <int PTX_VERSION, typename PolicyT>
struct ChainedPolicy<PTX_VERSION, PolicyT, PolicyT>
{
    /// The policy for the active compiler pass
    typedef PolicyT ActivePolicy;

    /// Specializes and dispatches op in accordance to the first policy in the chain of adequate PTX version
    template <typename FunctorT>
    CUB_RUNTIME_FUNCTION __forceinline__
    static cudaError_t Invoke(int /*ptx_version*/, FunctorT &op) {
        return op.template Invoke<PolicyT>();
    }
};




#endif  // Do not document




/** @} */       // end group UtilMgmt

}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)
