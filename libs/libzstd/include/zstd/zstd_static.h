/*
    zstd - standard compression library
    Header File for static linking only
    Copyright (C) 2014-2015, Yann Collet.

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
    - zstd source repository : https://github.com/Cyan4973/zstd
    - ztsd public forum : https://groups.google.com/forum/#!forum/lz4c
*/
#pragma once

#if defined (__cplusplus)
extern "C" {
#endif

/**************************************
*  Includes
**************************************/
#include "zstd.h"


/**************************************
*  Streaming functions
**************************************/
typedef void* ZSTD_cctx_t;
ZSTD_cctx_t ZSTD_createCCtx(void);
size_t      ZSTD_freeCCtx(ZSTD_cctx_t cctx);

size_t ZSTD_compressBegin(ZSTD_cctx_t cctx, void* dst, size_t maxDstSize);
size_t ZSTD_compressContinue(ZSTD_cctx_t cctx, void* dst, size_t maxDstSize, const void* src, size_t srcSize);
size_t ZSTD_compressEnd(ZSTD_cctx_t cctx, void* dst, size_t maxDstSize);

typedef void* ZSTD_dctx_t;
ZSTD_dctx_t ZSTD_createDCtx(void);
size_t      ZSTD_freeDCtx(ZSTD_dctx_t dctx);

size_t ZSTD_nextSrcSizeToDecompress(ZSTD_dctx_t dctx);
size_t ZSTD_decompressContinue(ZSTD_dctx_t dctx, void* dst, size_t maxDstSize, const void* src, size_t srcSize);
/*
  Use above functions alternatively.
  ZSTD_nextSrcSizeToDecompress() tells how much bytes to provide as input to ZSTD_decompressContinue().
  This value is expected to be provided, precisely, as 'srcSize'.
  Otherwise, compression will fail (result is an error code, which can be tested using ZSTD_isError() )
  ZSTD_decompressContinue() result is the number of bytes regenerated within 'dst'.
  It can be zero, which is not an error; it just means ZSTD_decompressContinue() has decoded some header.
*/

/**************************************
*  Error management
**************************************/
#define ZSTD_LIST_ERRORS(ITEM) \
        ITEM(ZSTD_OK_NoError) ITEM(ZSTD_ERROR_GENERIC) \
        ITEM(ZSTD_ERROR_wrongMagicNumber) \
        ITEM(ZSTD_ERROR_wrongSrcSize) ITEM(ZSTD_ERROR_maxDstSize_tooSmall) \
        ITEM(ZSTD_ERROR_wrongLBlockSize) \
        ITEM(ZSTD_ERROR_maxCode)

#define ZSTD_GENERATE_ENUM(ENUM) ENUM,
typedef enum { ZSTD_LIST_ERRORS(ZSTD_GENERATE_ENUM) } ZSTD_errorCodes;   /* exposed list of errors; static linking only */


#if defined (__cplusplus)
}
#endif
