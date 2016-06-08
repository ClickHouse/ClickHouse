/*
    zstd - buffered version of compression library
    experimental complementary API, for static linking only
    Copyright (C) 2015-2016, Yann Collet.

    BSD 2-Clause License (http://www.opensource.org/licenses/bsd-license.php)

    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions are
    met:
    * Redistributions of source code must retain the above copyright
    notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
    copyright notice, this list of conditions and the following disclaimer
    in the documentation and/or other materials provided with the
    distribution.
    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
    A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
    OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
    SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
    LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
    DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
    THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
    (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
    OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

    You can contact the author at :
    - zstd homepage : http://www.zstd.net
*/
#ifndef ZSTD_BUFFERED_STATIC_H
#define ZSTD_BUFFERED_STATIC_H

/* The objects defined into this file should be considered experimental.
 * They are not labelled stable, as their prototype may change in the future.
 * You can use them for tests, provide feedback, or if you can endure risk of future changes.
 */

#if defined (__cplusplus)
extern "C" {
#endif

/* *************************************
*  Includes
***************************************/
#include "zstd_static.h"     /* ZSTD_parameters */
#include "zbuff.h"
#include "zstd_internal.h"  /* MIN  */


/* *************************************
*  Advanced Streaming functions
***************************************/
ZSTDLIB_API size_t ZBUFF_compressInit_advanced(ZBUFF_CCtx* cctx,
                                               const void* dict, size_t dictSize,
                                               ZSTD_parameters params, U64 pledgedSrcSize);

MEM_STATIC size_t ZBUFF_limitCopy(void* dst, size_t dstCapacity, const void* src, size_t srcSize)
{
    size_t length = MIN(dstCapacity, srcSize);
    memcpy(dst, src, length);
    return length;
}


#if defined (__cplusplus)
}
#endif

#endif  /* ZSTD_BUFFERED_STATIC_H */
