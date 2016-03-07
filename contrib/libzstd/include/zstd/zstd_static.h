/*
    zstd - standard compression library
    Header File for static linking only
    Copyright (C) 2014-2016, Yann Collet.

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
#ifndef ZSTD_STATIC_H
#define ZSTD_STATIC_H

/* The prototypes defined within this file are considered experimental.
 * They should not be used in the context DLL as they may change in the future.
 * Prefer static linking if you need them, to control breaking version changes issues.
 */

#if defined (__cplusplus)
extern "C" {
#endif

/*-*************************************
*  Dependencies
***************************************/
#include "zstd.h"
#include "mem.h"


/*-*************************************
*  Constants
***************************************/
#define ZSTD_MAGICNUMBER 0xFD2FB525   /* v0.5 */


/*-*************************************
*  Types
***************************************/
#define ZSTD_WINDOWLOG_MAX 26
#define ZSTD_WINDOWLOG_MIN 18
#define ZSTD_WINDOWLOG_ABSOLUTEMIN 11
#define ZSTD_CONTENTLOG_MAX (ZSTD_WINDOWLOG_MAX+1)
#define ZSTD_CONTENTLOG_MIN 4
#define ZSTD_HASHLOG_MAX 28
#define ZSTD_HASHLOG_MIN 12
#define ZSTD_SEARCHLOG_MAX (ZSTD_CONTENTLOG_MAX-1)
#define ZSTD_SEARCHLOG_MIN 1
#define ZSTD_SEARCHLENGTH_MAX 7
#define ZSTD_SEARCHLENGTH_MIN 4
#define ZSTD_TARGETLENGTH_MIN 4
#define ZSTD_TARGETLENGTH_MAX 999

/* from faster to stronger */
typedef enum { ZSTD_fast, ZSTD_greedy, ZSTD_lazy, ZSTD_lazy2, ZSTD_btlazy2, ZSTD_opt, ZSTD_btopt } ZSTD_strategy;

typedef struct
{
    U64 srcSize;       /* optional : tells how much bytes are present in the frame. Use 0 if not known. */
    U32 windowLog;     /* largest match distance : larger == more compression, more memory needed during decompression */
    U32 contentLog;    /* full search segment : larger == more compression, slower, more memory (useless for fast) */
    U32 hashLog;       /* dispatch table : larger == faster, more memory */
    U32 searchLog;     /* nb of searches : larger == more compression, slower */
    U32 searchLength;  /* match length searched : larger == faster decompression, sometimes less compression */
    U32 targetLength;  /* acceptable match size for optimal parser (only) : larger == more compression, slower */
    ZSTD_strategy strategy;
} ZSTD_parameters;


/*-*************************************
*  Advanced functions
***************************************/
ZSTDLIB_API unsigned ZSTD_maxCLevel (void);

/*! ZSTD_getParams() :
*   @return ZSTD_parameters structure for a selected compression level and srcSize.
*   `srcSizeHint` value is optional, select 0 if not known */
ZSTDLIB_API ZSTD_parameters ZSTD_getParams(int compressionLevel, U64 srcSizeHint);

/*! ZSTD_validateParams() :
*   correct params value to remain within authorized range */
ZSTDLIB_API void ZSTD_validateParams(ZSTD_parameters* params);

/*! ZSTD_compress_advanced() :
*   Same as ZSTD_compress_usingDict(), with fine-tune control of each compression parameter */
ZSTDLIB_API size_t ZSTD_compress_advanced (ZSTD_CCtx* ctx,
                                           void* dst, size_t dstCapacity,
                                     const void* src, size_t srcSize,
                                     const void* dict,size_t dictSize,
                                           ZSTD_parameters params);

/*! ZSTD_compress_usingPreparedDCtx() :
*   Same as ZSTD_compress_usingDict, but using a reference context `preparedCCtx`, where dictionary has been loaded.
*   It avoids reloading the dictionary each time.
*   `preparedCCtx` must have been properly initialized using ZSTD_compressBegin_usingDict() or ZSTD_compressBegin_advanced().
*   Requires 2 contexts : 1 for reference, which will not be modified, and 1 to run the compression operation */
ZSTDLIB_API size_t ZSTD_compress_usingPreparedCCtx(
                                           ZSTD_CCtx* cctx, const ZSTD_CCtx* preparedCCtx,
                                           void* dst, size_t dstCapacity,
                                     const void* src, size_t srcSize);

/*- Advanced Decompression functions -*/

/*! ZSTD_decompress_usingPreparedDCtx() :
*   Same as ZSTD_decompress_usingDict, but using a reference context `preparedDCtx`, where dictionary has been loaded.
*   It avoids reloading the dictionary each time.
*   `preparedDCtx` must have been properly initialized using ZSTD_decompressBegin_usingDict().
*   Requires 2 contexts : 1 for reference, which will not be modified, and 1 to run the decompression operation */
ZSTDLIB_API size_t ZSTD_decompress_usingPreparedDCtx(
                                             ZSTD_DCtx* dctx, const ZSTD_DCtx* preparedDCtx,
                                             void* dst, size_t dstCapacity,
                                       const void* src, size_t srcSize);


/* **************************************
*  Streaming functions (direct mode)
****************************************/
ZSTDLIB_API size_t ZSTD_compressBegin(ZSTD_CCtx* cctx, int compressionLevel);
ZSTDLIB_API size_t ZSTD_compressBegin_usingDict(ZSTD_CCtx* cctx, const void* dict,size_t dictSize, int compressionLevel);
ZSTDLIB_API size_t ZSTD_compressBegin_advanced(ZSTD_CCtx* cctx, const void* dict,size_t dictSize, ZSTD_parameters params);
ZSTDLIB_API size_t ZSTD_copyCCtx(ZSTD_CCtx* cctx, const ZSTD_CCtx* preparedCCtx);

ZSTDLIB_API size_t ZSTD_compressContinue(ZSTD_CCtx* cctx, void* dst, size_t dstCapacity, const void* src, size_t srcSize);
ZSTDLIB_API size_t ZSTD_compressEnd(ZSTD_CCtx* cctx, void* dst, size_t dstCapacity);

/*
  Streaming compression, synchronous mode (bufferless)

  A ZSTD_CCtx object is required to track streaming operations.
  Use ZSTD_createCCtx() / ZSTD_freeCCtx() to manage it.
  ZSTD_CCtx object can be re-used multiple times within successive compression operations.

  Start by initializing a context.
  Use ZSTD_compressBegin(), or ZSTD_compressBegin_usingDict() for dictionary compression,
  or ZSTD_compressBegin_advanced(), for finer parameter control.
  It's also possible to duplicate a reference context which has been initialized, using ZSTD_copyCCtx()

  Then, consume your input using ZSTD_compressContinue().
  The interface is synchronous, so all input will be consumed and produce a compressed output.
  You must ensure there is enough space in destination buffer to store compressed data under worst case scenario.
  Worst case evaluation is provided by ZSTD_compressBound().

  Finish a frame with ZSTD_compressEnd(), which will write the epilogue.
  Without the epilogue, frames will be considered incomplete by decoder.

  You can then reuse ZSTD_CCtx to compress some new frame.
*/


ZSTDLIB_API size_t ZSTD_decompressBegin(ZSTD_DCtx* dctx);
ZSTDLIB_API size_t ZSTD_decompressBegin_usingDict(ZSTD_DCtx* dctx, const void* dict, size_t dictSize);
ZSTDLIB_API void   ZSTD_copyDCtx(ZSTD_DCtx* dctx, const ZSTD_DCtx* preparedDCtx);

ZSTDLIB_API size_t ZSTD_getFrameParams(ZSTD_parameters* params, const void* src, size_t srcSize);

ZSTDLIB_API size_t ZSTD_nextSrcSizeToDecompress(ZSTD_DCtx* dctx);
ZSTDLIB_API size_t ZSTD_decompressContinue(ZSTD_DCtx* dctx, void* dst, size_t dstCapacity, const void* src, size_t srcSize);

/*
  Streaming decompression, direct mode (bufferless)

  A ZSTD_DCtx object is required to track streaming operations.
  Use ZSTD_createDCtx() / ZSTD_freeDCtx() to manage it.
  A ZSTD_DCtx object can be re-used multiple times.

  First typical operation is to retrieve frame parameters, using ZSTD_getFrameParams().
  This operation is independent, and just needs enough input data to properly decode the frame header.
  Objective is to retrieve *params.windowlog, to know minimum amount of memory required during decoding.
  Result : 0 when successful, it means the ZSTD_parameters structure has been filled.
           >0 : means there is not enough data into src. Provides the expected size to successfully decode header.
           errorCode, which can be tested using ZSTD_isError()

  Start decompression, with ZSTD_decompressBegin() or ZSTD_decompressBegin_usingDict()
  Alternatively, you can copy a prepared context, using ZSTD_copyDCtx()

  Then use ZSTD_nextSrcSizeToDecompress() and ZSTD_decompressContinue() alternatively.
  ZSTD_nextSrcSizeToDecompress() tells how much bytes to provide as 'srcSize' to ZSTD_decompressContinue().
  ZSTD_decompressContinue() requires this exact amount of bytes, or it will fail.
  ZSTD_decompressContinue() needs previous data blocks during decompression, up to (1 << windowlog).
  They should preferably be located contiguously, prior to current block. Alternatively, a round buffer is also possible.

  @result of ZSTD_decompressContinue() is the number of bytes regenerated within 'dst'.
  It can be zero, which is not an error; it just means ZSTD_decompressContinue() has decoded some header.

  A frame is fully decoded when ZSTD_nextSrcSizeToDecompress() returns zero.
  Context can then be reset to start a new decompression.
*/


/* **************************************
*  Block functions
****************************************/
/*! Block functions produce and decode raw zstd blocks, without frame metadata.
    User will have to take in charge required information to regenerate data, such as block sizes.

    A few rules to respect :
    - Uncompressed block size must be <= 128 KB
    - Compressing or decompressing requires a context structure
      + Use ZSTD_createCCtx() and ZSTD_createDCtx()
    - It is necessary to init context before starting
      + compression : ZSTD_compressBegin()
      + decompression : ZSTD_decompressBegin()
      + variants _usingDict() are also allowed
      + copyCCtx() and copyDCtx() work too
    - When a block is considered not compressible enough, ZSTD_compressBlock() result will be zero.
      In which case, nothing is produced into `dst`.
      + User must test for such outcome and deal directly with uncompressed data
      + ZSTD_decompressBlock() doesn't accept uncompressed data as input !!
*/

size_t ZSTD_compressBlock  (ZSTD_CCtx* cctx, void* dst, size_t dstCapacity, const void* src, size_t srcSize);
size_t ZSTD_decompressBlock(ZSTD_DCtx* dctx, void* dst, size_t dstCapacity, const void* src, size_t srcSize);


/*-*************************************
*  Error management
***************************************/
#include "error_public.h"
/*! ZSTD_getErrorCode() :
    convert a `size_t` function result into a `ZSTD_error_code` enum type,
    which can be used to compare directly with enum list published into "error_public.h" */
ZSTD_ErrorCode ZSTD_getError(size_t code);


#if defined (__cplusplus)
}
#endif

#endif  /* ZSTD_STATIC_H */
