/**
 * Copyright (c) 2016-present, Yann Collet, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

/* ***************************************************************
*  NOTES/WARNINGS
*****************************************************************/
/* The streaming API defined here will soon be deprecated by the
* new one in 'zstd.h'; consider migrating towards newer streaming
* API. See 'lib/README.md'.
*****************************************************************/

#ifndef ZSTD_BUFFERED_H_23987
#define ZSTD_BUFFERED_H_23987

#if defined (__cplusplus)
extern "C" {
#endif

/* *************************************
*  Dependencies
***************************************/
#include <stddef.h>      /* size_t */


/* ***************************************************************
*  Compiler specifics
*****************************************************************/
/* ZSTD_DLL_EXPORT :
*  Enable exporting of functions when building a Windows DLL */
#if defined(_WIN32) && defined(ZSTD_DLL_EXPORT) && (ZSTD_DLL_EXPORT==1)
#  define ZSTDLIB_API __declspec(dllexport)
#else
#  define ZSTDLIB_API
#endif


/* *************************************
*  Streaming functions
***************************************/
/* This is the easier "buffered" streaming API,
*  using an internal buffer to lift all restrictions on user-provided buffers
*  which can be any size, any place, for both input and output.
*  ZBUFF and ZSTD are 100% interoperable,
*  frames created by one can be decoded by the other one */

typedef struct ZBUFF_CCtx_s ZBUFF_CCtx;
ZSTDLIB_API ZBUFF_CCtx* ZBUFF_createCCtx(void);
ZSTDLIB_API size_t      ZBUFF_freeCCtx(ZBUFF_CCtx* cctx);

ZSTDLIB_API size_t ZBUFF_compressInit(ZBUFF_CCtx* cctx, int compressionLevel);
ZSTDLIB_API size_t ZBUFF_compressInitDictionary(ZBUFF_CCtx* cctx, const void* dict, size_t dictSize, int compressionLevel);

ZSTDLIB_API size_t ZBUFF_compressContinue(ZBUFF_CCtx* cctx, void* dst, size_t* dstCapacityPtr, const void* src, size_t* srcSizePtr);
ZSTDLIB_API size_t ZBUFF_compressFlush(ZBUFF_CCtx* cctx, void* dst, size_t* dstCapacityPtr);
ZSTDLIB_API size_t ZBUFF_compressEnd(ZBUFF_CCtx* cctx, void* dst, size_t* dstCapacityPtr);

/*-*************************************************
*  Streaming compression - howto
*
*  A ZBUFF_CCtx object is required to track streaming operation.
*  Use ZBUFF_createCCtx() and ZBUFF_freeCCtx() to create/release resources.
*  ZBUFF_CCtx objects can be reused multiple times.
*
*  Start by initializing ZBUF_CCtx.
*  Use ZBUFF_compressInit() to start a new compression operation.
*  Use ZBUFF_compressInitDictionary() for a compression which requires a dictionary.
*
*  Use ZBUFF_compressContinue() repetitively to consume input stream.
*  *srcSizePtr and *dstCapacityPtr can be any size.
*  The function will report how many bytes were read or written within *srcSizePtr and *dstCapacityPtr.
*  Note that it may not consume the entire input, in which case it's up to the caller to present again remaining data.
*  The content of `dst` will be overwritten (up to *dstCapacityPtr) at each call, so save its content if it matters or change @dst .
*  @return : a hint to preferred nb of bytes to use as input for next function call (it's just a hint, to improve latency)
*            or an error code, which can be tested using ZBUFF_isError().
*
*  At any moment, it's possible to flush whatever data remains within buffer, using ZBUFF_compressFlush().
*  The nb of bytes written into `dst` will be reported into *dstCapacityPtr.
*  Note that the function cannot output more than *dstCapacityPtr,
*  therefore, some content might still be left into internal buffer if *dstCapacityPtr is too small.
*  @return : nb of bytes still present into internal buffer (0 if it's empty)
*            or an error code, which can be tested using ZBUFF_isError().
*
*  ZBUFF_compressEnd() instructs to finish a frame.
*  It will perform a flush and write frame epilogue.
*  The epilogue is required for decoders to consider a frame completed.
*  Similar to ZBUFF_compressFlush(), it may not be able to output the entire internal buffer content if *dstCapacityPtr is too small.
*  In which case, call again ZBUFF_compressFlush() to complete the flush.
*  @return : nb of bytes still present into internal buffer (0 if it's empty)
*            or an error code, which can be tested using ZBUFF_isError().
*
*  Hint : _recommended buffer_ sizes (not compulsory) : ZBUFF_recommendedCInSize() / ZBUFF_recommendedCOutSize()
*  input : ZBUFF_recommendedCInSize==128 KB block size is the internal unit, use this value to reduce intermediate stages (better latency)
*  output : ZBUFF_recommendedCOutSize==ZSTD_compressBound(128 KB) + 3 + 3 : ensures it's always possible to write/flush/end a full block. Skip some buffering.
*  By using both, it ensures that input will be entirely consumed, and output will always contain the result, reducing intermediate buffering.
* **************************************************/


typedef struct ZBUFF_DCtx_s ZBUFF_DCtx;
ZSTDLIB_API ZBUFF_DCtx* ZBUFF_createDCtx(void);
ZSTDLIB_API size_t      ZBUFF_freeDCtx(ZBUFF_DCtx* dctx);

ZSTDLIB_API size_t ZBUFF_decompressInit(ZBUFF_DCtx* dctx);
ZSTDLIB_API size_t ZBUFF_decompressInitDictionary(ZBUFF_DCtx* dctx, const void* dict, size_t dictSize);

ZSTDLIB_API size_t ZBUFF_decompressContinue(ZBUFF_DCtx* dctx,
                                            void* dst, size_t* dstCapacityPtr,
                                      const void* src, size_t* srcSizePtr);

/*-***************************************************************************
*  Streaming decompression howto
*
*  A ZBUFF_DCtx object is required to track streaming operations.
*  Use ZBUFF_createDCtx() and ZBUFF_freeDCtx() to create/release resources.
*  Use ZBUFF_decompressInit() to start a new decompression operation,
*   or ZBUFF_decompressInitDictionary() if decompression requires a dictionary.
*  Note that ZBUFF_DCtx objects can be re-init multiple times.
*
*  Use ZBUFF_decompressContinue() repetitively to consume your input.
*  *srcSizePtr and *dstCapacityPtr can be any size.
*  The function will report how many bytes were read or written by modifying *srcSizePtr and *dstCapacityPtr.
*  Note that it may not consume the entire input, in which case it's up to the caller to present remaining input again.
*  The content of `dst` will be overwritten (up to *dstCapacityPtr) at each function call, so save its content if it matters, or change `dst`.
*  @return : 0 when a frame is completely decoded and fully flushed,
*            1 when there is still some data left within internal buffer to flush,
*            >1 when more data is expected, with value being a suggested next input size (it's just a hint, which helps latency),
*            or an error code, which can be tested using ZBUFF_isError().
*
*  Hint : recommended buffer sizes (not compulsory) : ZBUFF_recommendedDInSize() and ZBUFF_recommendedDOutSize()
*  output : ZBUFF_recommendedDOutSize== 128 KB block size is the internal unit, it ensures it's always possible to write a full block when decoded.
*  input  : ZBUFF_recommendedDInSize == 128KB + 3;
*           just follow indications from ZBUFF_decompressContinue() to minimize latency. It should always be <= 128 KB + 3 .
* *******************************************************************************/


/* *************************************
*  Tool functions
***************************************/
ZSTDLIB_API unsigned ZBUFF_isError(size_t errorCode);
ZSTDLIB_API const char* ZBUFF_getErrorName(size_t errorCode);

/** Functions below provide recommended buffer sizes for Compression or Decompression operations.
*   These sizes are just hints, they tend to offer better latency */
ZSTDLIB_API size_t ZBUFF_recommendedCInSize(void);
ZSTDLIB_API size_t ZBUFF_recommendedCOutSize(void);
ZSTDLIB_API size_t ZBUFF_recommendedDInSize(void);
ZSTDLIB_API size_t ZBUFF_recommendedDOutSize(void);


#ifdef ZBUFF_STATIC_LINKING_ONLY

/* ====================================================================================
 * The definitions in this section are considered experimental.
 * They should never be used in association with a dynamic library, as they may change in the future.
 * They are provided for advanced usages.
 * Use them only in association with static linking.
 * ==================================================================================== */

/*--- Dependency ---*/
#define ZSTD_STATIC_LINKING_ONLY   /* ZSTD_parameters, ZSTD_customMem */
#include "zstd.h"


/*--- Custom memory allocator ---*/
/*! ZBUFF_createCCtx_advanced() :
 *  Create a ZBUFF compression context using external alloc and free functions */
ZSTDLIB_API ZBUFF_CCtx* ZBUFF_createCCtx_advanced(ZSTD_customMem customMem);

/*! ZBUFF_createDCtx_advanced() :
 *  Create a ZBUFF decompression context using external alloc and free functions */
ZSTDLIB_API ZBUFF_DCtx* ZBUFF_createDCtx_advanced(ZSTD_customMem customMem);


/*--- Advanced Streaming Initialization ---*/
ZSTDLIB_API size_t ZBUFF_compressInit_advanced(ZBUFF_CCtx* zbc,
                                               const void* dict, size_t dictSize,
                                               ZSTD_parameters params, unsigned long long pledgedSrcSize);

#endif /* ZBUFF_STATIC_LINKING_ONLY */


#if defined (__cplusplus)
}
#endif

#endif  /* ZSTD_BUFFERED_H_23987 */
