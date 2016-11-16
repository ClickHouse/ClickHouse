/**
 * Copyright (c) 2016-present, Yann Collet, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */



/* *************************************
*  Dependencies
***************************************/
#include <stdlib.h>
#include "error_private.h"
#include "zstd_internal.h"  /* MIN, ZSTD_BLOCKHEADERSIZE, defaultCustomMem */
#define ZBUFF_STATIC_LINKING_ONLY
#include "zbuff.h"


/* *************************************
*  Constants
***************************************/
static size_t const ZBUFF_endFrameSize = ZSTD_BLOCKHEADERSIZE;


/*-***********************************************************
*  Streaming compression
*
*  A ZBUFF_CCtx object is required to track streaming operation.
*  Use ZBUFF_createCCtx() and ZBUFF_freeCCtx() to create/release resources.
*  Use ZBUFF_compressInit() to start a new compression operation.
*  ZBUFF_CCtx objects can be reused multiple times.
*
*  Use ZBUFF_compressContinue() repetitively to consume your input.
*  *srcSizePtr and *dstCapacityPtr can be any size.
*  The function will report how many bytes were read or written by modifying *srcSizePtr and *dstCapacityPtr.
*  Note that it may not consume the entire input, in which case it's up to the caller to call again the function with remaining input.
*  The content of dst will be overwritten (up to *dstCapacityPtr) at each function call, so save its content if it matters or change dst .
*  @return : a hint to preferred nb of bytes to use as input for next function call (it's only a hint, to improve latency)
*            or an error code, which can be tested using ZBUFF_isError().
*
*  ZBUFF_compressFlush() can be used to instruct ZBUFF to compress and output whatever remains within its buffer.
*  Note that it will not output more than *dstCapacityPtr.
*  Therefore, some content might still be left into its internal buffer if dst buffer is too small.
*  @return : nb of bytes still present into internal buffer (0 if it's empty)
*            or an error code, which can be tested using ZBUFF_isError().
*
*  ZBUFF_compressEnd() instructs to finish a frame.
*  It will perform a flush and write frame epilogue.
*  Similar to ZBUFF_compressFlush(), it may not be able to output the entire internal buffer content if *dstCapacityPtr is too small.
*  @return : nb of bytes still present into internal buffer (0 if it's empty)
*            or an error code, which can be tested using ZBUFF_isError().
*
*  Hint : recommended buffer sizes (not compulsory)
*  input : ZSTD_BLOCKSIZE_MAX (128 KB), internal unit size, it improves latency to use this value.
*  output : ZSTD_compressBound(ZSTD_BLOCKSIZE_MAX) + ZSTD_blockHeaderSize + ZBUFF_endFrameSize : ensures it's always possible to write/flush/end a full block at best speed.
* ***********************************************************/

typedef enum { ZBUFFcs_init, ZBUFFcs_load, ZBUFFcs_flush, ZBUFFcs_final } ZBUFF_cStage;

/* *** Resources *** */
struct ZBUFF_CCtx_s {
    ZSTD_CCtx* zc;
    char*  inBuff;
    size_t inBuffSize;
    size_t inToCompress;
    size_t inBuffPos;
    size_t inBuffTarget;
    size_t blockSize;
    char*  outBuff;
    size_t outBuffSize;
    size_t outBuffContentSize;
    size_t outBuffFlushedSize;
    ZBUFF_cStage stage;
    U32    checksum;
    U32    frameEnded;
    ZSTD_customMem customMem;
};   /* typedef'd tp ZBUFF_CCtx within "zbuff.h" */

ZBUFF_CCtx* ZBUFF_createCCtx(void)
{
    return ZBUFF_createCCtx_advanced(defaultCustomMem);
}

ZBUFF_CCtx* ZBUFF_createCCtx_advanced(ZSTD_customMem customMem)
{
    ZBUFF_CCtx* zbc;

    if (!customMem.customAlloc && !customMem.customFree)
        customMem = defaultCustomMem;

    if (!customMem.customAlloc || !customMem.customFree)
        return NULL;

    zbc = (ZBUFF_CCtx*)customMem.customAlloc(customMem.opaque, sizeof(ZBUFF_CCtx));
    if (zbc==NULL) return NULL;
    memset(zbc, 0, sizeof(ZBUFF_CCtx));
    memcpy(&zbc->customMem, &customMem, sizeof(ZSTD_customMem));
    zbc->zc = ZSTD_createCCtx_advanced(customMem);
    if (zbc->zc == NULL) { ZBUFF_freeCCtx(zbc); return NULL; }
    return zbc;
}

size_t ZBUFF_freeCCtx(ZBUFF_CCtx* zbc)
{
    if (zbc==NULL) return 0;   /* support free on NULL */
    ZSTD_freeCCtx(zbc->zc);
    if (zbc->inBuff) zbc->customMem.customFree(zbc->customMem.opaque, zbc->inBuff);
    if (zbc->outBuff) zbc->customMem.customFree(zbc->customMem.opaque, zbc->outBuff);
    zbc->customMem.customFree(zbc->customMem.opaque, zbc);
    return 0;
}


/* ======   Initialization   ====== */

size_t ZBUFF_compressInit_advanced(ZBUFF_CCtx* zbc,
                                   const void* dict, size_t dictSize,
                                   ZSTD_parameters params, unsigned long long pledgedSrcSize)
{
    /* allocate buffers */
    {   size_t const neededInBuffSize = (size_t)1 << params.cParams.windowLog;
        if (zbc->inBuffSize < neededInBuffSize) {
            zbc->inBuffSize = neededInBuffSize;
            zbc->customMem.customFree(zbc->customMem.opaque, zbc->inBuff);   /* should not be necessary */
            zbc->inBuff = (char*)zbc->customMem.customAlloc(zbc->customMem.opaque, neededInBuffSize);
            if (zbc->inBuff == NULL) return ERROR(memory_allocation);
        }
        zbc->blockSize = MIN(ZSTD_BLOCKSIZE_ABSOLUTEMAX, neededInBuffSize);
    }
    if (zbc->outBuffSize < ZSTD_compressBound(zbc->blockSize)+1) {
        zbc->outBuffSize = ZSTD_compressBound(zbc->blockSize)+1;
        zbc->customMem.customFree(zbc->customMem.opaque, zbc->outBuff);   /* should not be necessary */
        zbc->outBuff = (char*)zbc->customMem.customAlloc(zbc->customMem.opaque, zbc->outBuffSize);
        if (zbc->outBuff == NULL) return ERROR(memory_allocation);
    }

    { size_t const errorCode = ZSTD_compressBegin_advanced(zbc->zc, dict, dictSize, params, pledgedSrcSize);
      if (ZSTD_isError(errorCode)) return errorCode; }

    zbc->inToCompress = 0;
    zbc->inBuffPos = 0;
    zbc->inBuffTarget = zbc->blockSize;
    zbc->outBuffContentSize = zbc->outBuffFlushedSize = 0;
    zbc->stage = ZBUFFcs_load;
    zbc->checksum = params.fParams.checksumFlag > 0;
    zbc->frameEnded = 0;
    return 0;   /* ready to go */
}


size_t ZBUFF_compressInitDictionary(ZBUFF_CCtx* zbc, const void* dict, size_t dictSize, int compressionLevel)
{
    ZSTD_parameters const params = ZSTD_getParams(compressionLevel, 0, dictSize);
    return ZBUFF_compressInit_advanced(zbc, dict, dictSize, params, 0);
}

size_t ZBUFF_compressInit(ZBUFF_CCtx* zbc, int compressionLevel)
{
    return ZBUFF_compressInitDictionary(zbc, NULL, 0, compressionLevel);
}


/* internal util function */
MEM_STATIC size_t ZBUFF_limitCopy(void* dst, size_t dstCapacity, const void* src, size_t srcSize)
{
    size_t const length = MIN(dstCapacity, srcSize);
    memcpy(dst, src, length);
    return length;
}


/* ======   Compression   ====== */

typedef enum { zbf_gather, zbf_flush, zbf_end } ZBUFF_flush_e;

static size_t ZBUFF_compressContinue_generic(ZBUFF_CCtx* zbc,
                              void* dst, size_t* dstCapacityPtr,
                        const void* src, size_t* srcSizePtr,
                              ZBUFF_flush_e const flush)
{
    U32 someMoreWork = 1;
    const char* const istart = (const char*)src;
    const char* const iend = istart + *srcSizePtr;
    const char* ip = istart;
    char* const ostart = (char*)dst;
    char* const oend = ostart + *dstCapacityPtr;
    char* op = ostart;

    while (someMoreWork) {
        switch(zbc->stage)
        {
        case ZBUFFcs_init: return ERROR(init_missing);   /* call ZBUFF_compressInit() first ! */

        case ZBUFFcs_load:
            /* complete inBuffer */
            {   size_t const toLoad = zbc->inBuffTarget - zbc->inBuffPos;
                size_t const loaded = ZBUFF_limitCopy(zbc->inBuff + zbc->inBuffPos, toLoad, ip, iend-ip);
                zbc->inBuffPos += loaded;
                ip += loaded;
                if ( (zbc->inBuffPos==zbc->inToCompress) || (!flush && (toLoad != loaded)) ) {
                    someMoreWork = 0; break;  /* not enough input to get a full block : stop there, wait for more */
            }   }
            /* compress current block (note : this stage cannot be stopped in the middle) */
            {   void* cDst;
                size_t cSize;
                size_t const iSize = zbc->inBuffPos - zbc->inToCompress;
                size_t oSize = oend-op;
                if (oSize >= ZSTD_compressBound(iSize))
                    cDst = op;   /* compress directly into output buffer (avoid flush stage) */
                else
                    cDst = zbc->outBuff, oSize = zbc->outBuffSize;
                cSize = (flush == zbf_end) ?
                        ZSTD_compressEnd(zbc->zc, cDst, oSize, zbc->inBuff + zbc->inToCompress, iSize) :
                        ZSTD_compressContinue(zbc->zc, cDst, oSize, zbc->inBuff + zbc->inToCompress, iSize);
                if (ZSTD_isError(cSize)) return cSize;
                if (flush == zbf_end) zbc->frameEnded = 1;
                /* prepare next block */
                zbc->inBuffTarget = zbc->inBuffPos + zbc->blockSize;
                if (zbc->inBuffTarget > zbc->inBuffSize)
                    zbc->inBuffPos = 0, zbc->inBuffTarget = zbc->blockSize;   /* note : inBuffSize >= blockSize */
                zbc->inToCompress = zbc->inBuffPos;
                if (cDst == op) { op += cSize; break; }   /* no need to flush */
                zbc->outBuffContentSize = cSize;
                zbc->outBuffFlushedSize = 0;
                zbc->stage = ZBUFFcs_flush;   /* continue to flush stage */
            }

        case ZBUFFcs_flush:
            {   size_t const toFlush = zbc->outBuffContentSize - zbc->outBuffFlushedSize;
                size_t const flushed = ZBUFF_limitCopy(op, oend-op, zbc->outBuff + zbc->outBuffFlushedSize, toFlush);
                op += flushed;
                zbc->outBuffFlushedSize += flushed;
                if (toFlush!=flushed) { someMoreWork = 0; break; } /* dst too small to store flushed data : stop there */
                zbc->outBuffContentSize = zbc->outBuffFlushedSize = 0;
                zbc->stage = ZBUFFcs_load;
                break;
            }

        case ZBUFFcs_final:
            someMoreWork = 0;   /* do nothing */
            break;

        default:
            return ERROR(GENERIC);   /* impossible */
        }
    }

    *srcSizePtr = ip - istart;
    *dstCapacityPtr = op - ostart;
    if (zbc->frameEnded) return 0;
    {   size_t hintInSize = zbc->inBuffTarget - zbc->inBuffPos;
        if (hintInSize==0) hintInSize = zbc->blockSize;
        return hintInSize;
    }
}

size_t ZBUFF_compressContinue(ZBUFF_CCtx* zbc,
                              void* dst, size_t* dstCapacityPtr,
                        const void* src, size_t* srcSizePtr)
{
    return ZBUFF_compressContinue_generic(zbc, dst, dstCapacityPtr, src, srcSizePtr, zbf_gather);
}



/* ======   Finalize   ====== */

size_t ZBUFF_compressFlush(ZBUFF_CCtx* zbc, void* dst, size_t* dstCapacityPtr)
{
    size_t srcSize = 0;
    ZBUFF_compressContinue_generic(zbc, dst, dstCapacityPtr, &srcSize, &srcSize, zbf_flush);  /* use a valid src address instead of NULL */
    return zbc->outBuffContentSize - zbc->outBuffFlushedSize;
}


size_t ZBUFF_compressEnd(ZBUFF_CCtx* zbc, void* dst, size_t* dstCapacityPtr)
{
    BYTE* const ostart = (BYTE*)dst;
    BYTE* const oend = ostart + *dstCapacityPtr;
    BYTE* op = ostart;

    if (zbc->stage != ZBUFFcs_final) {
        /* flush whatever remains */
        size_t outSize = *dstCapacityPtr;
        size_t srcSize = 0;
        size_t const notEnded = ZBUFF_compressContinue_generic(zbc, dst, &outSize, &srcSize, &srcSize, zbf_end);  /* use a valid address instead of NULL */
        size_t const remainingToFlush = zbc->outBuffContentSize - zbc->outBuffFlushedSize;
        op += outSize;
        if (remainingToFlush) {
            *dstCapacityPtr = op-ostart;
            return remainingToFlush + ZBUFF_endFrameSize + (zbc->checksum * 4);
        }
        /* create epilogue */
        zbc->stage = ZBUFFcs_final;
        zbc->outBuffContentSize = !notEnded ? 0 :
            ZSTD_compressEnd(zbc->zc, zbc->outBuff, zbc->outBuffSize, NULL, 0);  /* write epilogue into outBuff */
    }

    /* flush epilogue */
    {   size_t const toFlush = zbc->outBuffContentSize - zbc->outBuffFlushedSize;
        size_t const flushed = ZBUFF_limitCopy(op, oend-op, zbc->outBuff + zbc->outBuffFlushedSize, toFlush);
        op += flushed;
        zbc->outBuffFlushedSize += flushed;
        *dstCapacityPtr = op-ostart;
        if (toFlush==flushed) zbc->stage = ZBUFFcs_init;  /* end reached */
        return toFlush - flushed;
    }
}



/* *************************************
*  Tool functions
***************************************/
size_t ZBUFF_recommendedCInSize(void)  { return ZSTD_BLOCKSIZE_ABSOLUTEMAX; }
size_t ZBUFF_recommendedCOutSize(void) { return ZSTD_compressBound(ZSTD_BLOCKSIZE_ABSOLUTEMAX) + ZSTD_blockHeaderSize + ZBUFF_endFrameSize; }
