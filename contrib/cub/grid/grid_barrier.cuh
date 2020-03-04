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
 * cub::GridBarrier implements a software global barrier among thread blocks within a CUDA grid
 */

#pragma once

#include "../util_debug.cuh"
#include "../util_namespace.cuh"
#include "../thread/thread_load.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {


/**
 * \addtogroup GridModule
 * @{
 */


/**
 * \brief GridBarrier implements a software global barrier among thread blocks within a CUDA grid
 */
class GridBarrier
{
protected :

    typedef unsigned int SyncFlag;

    // Counters in global device memory
    SyncFlag* d_sync;

public:

    /**
     * Constructor
     */
    GridBarrier() : d_sync(NULL) {}


    /**
     * Synchronize
     */
    __device__ __forceinline__ void Sync() const
    {
        volatile SyncFlag *d_vol_sync = d_sync;

        // Threadfence and syncthreads to make sure global writes are visible before
        // thread-0 reports in with its sync counter
        __threadfence();
        CTA_SYNC();

        if (blockIdx.x == 0)
        {
            // Report in ourselves
            if (threadIdx.x == 0)
            {
                d_vol_sync[blockIdx.x] = 1;
            }

            CTA_SYNC();

            // Wait for everyone else to report in
            for (int peer_block = threadIdx.x; peer_block < gridDim.x; peer_block += blockDim.x)
            {
                while (ThreadLoad<LOAD_CG>(d_sync + peer_block) == 0)
                {
                    __threadfence_block();
                }
            }

            CTA_SYNC();

            // Let everyone know it's safe to proceed
            for (int peer_block = threadIdx.x; peer_block < gridDim.x; peer_block += blockDim.x)
            {
                d_vol_sync[peer_block] = 0;
            }
        }
        else
        {
            if (threadIdx.x == 0)
            {
                // Report in
                d_vol_sync[blockIdx.x] = 1;

                // Wait for acknowledgment
                while (ThreadLoad<LOAD_CG>(d_sync + blockIdx.x) == 1)
                {
                    __threadfence_block();
                }
            }

            CTA_SYNC();
        }
    }
};


/**
 * \brief GridBarrierLifetime extends GridBarrier to provide lifetime management of the temporary device storage needed for cooperation.
 *
 * Uses RAII for lifetime, i.e., device resources are reclaimed when
 * the destructor is called.
 */
class GridBarrierLifetime : public GridBarrier
{
protected:

    // Number of bytes backed by d_sync
    size_t sync_bytes;

public:

    /**
     * Constructor
     */
    GridBarrierLifetime() : GridBarrier(), sync_bytes(0) {}


    /**
     * DeviceFrees and resets the progress counters
     */
    cudaError_t HostReset()
    {
        cudaError_t retval = cudaSuccess;
        if (d_sync)
        {
            CubDebug(retval = cudaFree(d_sync));
            d_sync = NULL;
        }
        sync_bytes = 0;
        return retval;
    }


    /**
     * Destructor
     */
    virtual ~GridBarrierLifetime()
    {
        HostReset();
    }


    /**
     * Sets up the progress counters for the next kernel launch (lazily
     * allocating and initializing them if necessary)
     */
    cudaError_t Setup(int sweep_grid_size)
    {
        cudaError_t retval = cudaSuccess;
        do {
            size_t new_sync_bytes = sweep_grid_size * sizeof(SyncFlag);
            if (new_sync_bytes > sync_bytes)
            {
                if (d_sync)
                {
                    if (CubDebug(retval = cudaFree(d_sync))) break;
                }

                sync_bytes = new_sync_bytes;

                // Allocate and initialize to zero
                if (CubDebug(retval = cudaMalloc((void**) &d_sync, sync_bytes))) break;
                if (CubDebug(retval = cudaMemset(d_sync, 0, new_sync_bytes))) break;
            }
        } while (0);

        return retval;
    }
};


/** @} */       // end group GridModule

}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)

