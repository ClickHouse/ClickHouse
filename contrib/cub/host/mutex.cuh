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
 * Simple portable mutex
 */


#pragma once

#if (__cplusplus > 199711L) || (defined(_MSC_VER) && _MSC_VER >= 1800)
    #include <mutex>
#else
    #if defined(_WIN32) || defined(_WIN64)
        #include <intrin.h>

        #define WIN32_LEAN_AND_MEAN
        #define NOMINMAX
        #include <windows.h>
        #undef WIN32_LEAN_AND_MEAN
        #undef NOMINMAX

        /**
         * Compiler read/write barrier
         */
        #pragma intrinsic(_ReadWriteBarrier)

    #endif
#endif

#include "../util_namespace.cuh"


/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {


/**
 * Simple portable mutex
 *   - Wraps std::mutex when compiled with C++11 or newer (supported on all platforms)
 *   - Uses GNU/Windows spinlock mechanisms for pre C++11 (supported on x86/x64 when compiled with cl.exe or g++)
 */
struct Mutex
{
#if (__cplusplus > 199711L) || (defined(_MSC_VER) && _MSC_VER >= 1800)

    std::mutex mtx;

    void Lock()
    {
        mtx.lock();
    }

    void Unlock()
    {
        mtx.unlock();
    }

    void TryLock()
    {
        mtx.try_lock();
    }

#else       //__cplusplus > 199711L

    #if defined(_MSC_VER)

        // Microsoft VC++
        typedef long Spinlock;

    #else

        // GNU g++
        typedef int Spinlock;

        /**
         * Compiler read/write barrier
         */
        __forceinline__ void _ReadWriteBarrier()
        {
            __sync_synchronize();
        }

        /**
         * Atomic exchange
         */
        __forceinline__ long _InterlockedExchange(volatile int * const Target, const int Value)
        {
            // NOTE: __sync_lock_test_and_set would be an acquire barrier, so we force a full barrier
            _ReadWriteBarrier();
            return __sync_lock_test_and_set(Target, Value);
        }

        /**
         * Pause instruction to prevent excess processor bus usage
         */
        __forceinline__ void YieldProcessor()
        {
        }

    #endif  // defined(_MSC_VER)

        /// Lock member
        volatile Spinlock lock;

        /**
         * Constructor
         */
        Mutex() : lock(0) {}

        /**
         * Return when the specified spinlock has been acquired
         */
        __forceinline__ void Lock()
        {
            while (1)
            {
                if (!_InterlockedExchange(&lock, 1)) return;
                while (lock) YieldProcessor();
            }
        }


        /**
         * Release the specified spinlock
         */
        __forceinline__ void Unlock()
        {
            _ReadWriteBarrier();
            lock = 0;
        }

#endif      // __cplusplus > 199711L

};




}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)

