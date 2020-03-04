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

/******************************************************************************
 * Common C/C++ macro utilities
 ******************************************************************************/

#pragma once

#include "util_namespace.cuh"

/// Optional outer namespace(s)
CUB_NS_PREFIX

/// CUB namespace
namespace cub {


/**
 * \addtogroup UtilModule
 * @{
 */

#ifndef CUB_ALIGN
    #if defined(_WIN32) || defined(_WIN64)
        /// Align struct
        #define CUB_ALIGN(bytes) __declspec(align(32))
    #else
        /// Align struct
        #define CUB_ALIGN(bytes) __attribute__((aligned(bytes)))
    #endif
#endif

#ifndef CUB_MAX
    /// Select maximum(a, b)
    #define CUB_MAX(a, b) (((b) > (a)) ? (b) : (a))
#endif

#ifndef CUB_MIN
    /// Select minimum(a, b)
    #define CUB_MIN(a, b) (((b) < (a)) ? (b) : (a))
#endif

#ifndef CUB_QUOTIENT_FLOOR
    /// Quotient of x/y rounded down to nearest integer
    #define CUB_QUOTIENT_FLOOR(x, y) ((x) / (y))
#endif

#ifndef CUB_QUOTIENT_CEILING
    /// Quotient of x/y rounded up to nearest integer
    #define CUB_QUOTIENT_CEILING(x, y) (((x) + (y) - 1) / (y))
#endif

#ifndef CUB_ROUND_UP_NEAREST
    /// x rounded up to the nearest multiple of y
    #define CUB_ROUND_UP_NEAREST(x, y) ((((x) + (y) - 1) / (y)) * y)
#endif

#ifndef CUB_ROUND_DOWN_NEAREST
    /// x rounded down to the nearest multiple of y
    #define CUB_ROUND_DOWN_NEAREST(x, y) (((x) / (y)) * y)
#endif


#ifndef CUB_STATIC_ASSERT
    #ifndef DOXYGEN_SHOULD_SKIP_THIS    // Do not document
        #define CUB_CAT_(a, b) a ## b
        #define CUB_CAT(a, b) CUB_CAT_(a, b)
    #endif // DOXYGEN_SHOULD_SKIP_THIS

    /// Static assert
    #define CUB_STATIC_ASSERT(cond, msg) typedef int CUB_CAT(cub_static_assert, __LINE__)[(cond) ? 1 : -1]
#endif

/** @} */       // end group UtilModule

}               // CUB namespace
CUB_NS_POSTFIX  // Optional outer namespace(s)
