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
#include "zstd_internal.h"  /* MIN, ZSTD_blockHeaderSize, ZSTD_BLOCKSIZE_MAX */
#define ZBUFF_STATIC_LINKING_ONLY
#include "zbuff.h"


typedef enum { ZBUFFds_init, ZBUFFds_loadHeader,
               ZBUFFds_read, ZBUFFds_load, ZBUFFds_flush } ZBUFF_dStage;

/* *** Resource management *** */
struct ZBUFF_DCtx_s {
    ZSTD_DCtx* zd;
    ZSTD_frameParams fParams;
    ZBUFF_dStage stage;
    char*  inBuff;
    size_t inBuffSize;
    size_t inPos;
    char*  outBuff;
    size_t outBuffSize;
    size_t outStart;
    size_t outEnd;
    size_t blockSize;
    BYTE headerBuffer[ZSTD_FRAMEHEADERSIZE_MAX];
    size_t lhSize;
    ZSTD_customMem customMem;
};   /* typedef'd to ZBUFF_DCtx within "zbuff.h" */


ZBUFF_DCtx* ZBUFF_createDCtx(void)
{
    return ZBUFF_createDCtx_advanced(defaultCustomMem);
}

ZBUFF_DCtx* ZBUFF_createDCtx_advanced(ZSTD_customMem customMem)
{
    ZBUFF_DCtx* zbd;

    if (!customMem.customAlloc && !customMem.customFree)
        customMem = defaultCustomMem;

    if (!customMem.customAlloc || !customMem.customFree)
        return NULL;

    zbd = (ZBUFF_DCtx*)customMem.customAlloc(customMem.opaque, sizeof(ZBUFF_DCtx));
    if (zbd==NULL) return NULL;
    memset(zbd, 0, sizeof(ZBUFF_DCtx));
    memcpy(&zbd->customMem, &customMem, sizeof(ZSTD_customMem));
    zbd->zd = ZSTD_createDCtx_advanced(customMem);
    if (zbd->zd == NULL) { ZBUFF_freeDCtx(zbd); return NULL; }
    zbd->stage = ZBUFFds_init;
    return zbd;
}

size_t ZBUFF_freeDCtx(ZBUFF_DCtx* zbd)
{
    if (zbd==NULL) return 0;   /* support free on null */
    ZSTD_freeDCtx(zbd->zd);
    if (zbd->inBuff) zbd->customMem.customFree(zbd->customMem.opaque, zbd->inBuff);
    if (zbd->outBuff) zbd->customMem.customFree(zbd->customMem.opaque, zbd->outBuff);
    zbd->customMem.customFree(zbd->customMem.opaque, zbd);
    return 0;
}


/* *** Initialization *** */

size_t ZBUFF_decompressInitDictionary(ZBUFF_DCtx* zbd, const void* dict, size_t dictSize)
{
    zbd->stage = ZBUFFds_loadHeader;
    zbd->lhSize = zbd->inPos = zbd->outStart = zbd->outEnd = 0;
    return ZSTD_decompressBegin_usingDict(zbd->zd, dict, dictSize);
}

size_t ZBUFF_decompressInit(ZBUFF_DCtx* zbd)
{
    return ZBUFF_decompressInitDictionary(zbd, NULL, 0);
}


/* internal util function */
MEM_STATIC size_t ZBUFF_limitCopy(void* dst, size_t dstCapacity, const void* src, size_t srcSize)
{
    size_t const length = MIN(dstCapacity, srcSize);
    memcpy(dst, src, length);
    return length;
}


/* *** Decompression *** */

size_t ZBUFF_decompressContinue(ZBUFF_DCtx* zbd,
                                void* dst, size_t* dstCapacityPtr,
                          const void* src, size_t* srcSizePtr)
{
    const char* const istart = (const char*)src;
    const char* const iend = istart + *srcSizePtr;
    const char* ip = istart;
    char* const ostart = (char*)dst;
    char* const oend = ostart + *dstCapacityPtr;
    char* op = ostart;
    U32 someMoreWork = 1;

    while (someMoreWork) {
        switch(zbd->stage)
        {
        case ZBUFFds_init :
            return ERROR(init_missing);

        case ZBUFFds_loadHeader :
            {   size_t const hSize = ZSTD_getFrameParams(&(zbd->fParams), zbd->headerBuffer, zbd->lhSize);
                if (ZSTD_isError(hSize)) return hSize;
                if (hSize != 0) {   /* need more input */
                    size_t const toLoad = hSize - zbd->lhSize;   /* if hSize!=0, hSize > zbd->lhSize */
                    if (toLoad > (size_t)(iend-ip)) {   /* not enough input to load full header */
                        memcpy(zbd->headerBuffer + zbd->lhSize, ip, iend-ip);
                        zbd->lhSize += iend-ip;
                        *dstCapacityPtr = 0;
                        return (hSize - zbd->lhSize) + ZSTD_blockHeaderSize;   /* remaining header bytes + next block header */
                    }
                    memcpy(zbd->headerBuffer + zbd->lhSize, ip, toLoad); zbd->lhSize = hSize; ip += toLoad;
                    break;
            }   }

            /* Consume header */
            {   size_t const h1Size = ZSTD_nextSrcSizeToDecompress(zbd->zd);  /* == ZSTD_frameHeaderSize_min */
                size_t const h1Result = ZSTD_decompressContinue(zbd->zd, NULL, 0, zbd->headerBuffer, h1Size);
                if (ZSTD_isError(h1Result)) return h1Result;   /* should not happen : already checked */
                if (h1Size < zbd->lhSize) {   /* long header */
                    size_t const h2Size = ZSTD_nextSrcSizeToDecompress(zbd->zd);
                    size_t const h2Result = ZSTD_decompressContinue(zbd->zd, NULL, 0, zbd->headerBuffer+h1Size, h2Size);
                    if (ZSTD_isError(h2Result)) return h2Result;
            }   }

            zbd->fParams.windowSize = MAX(zbd->fParams.windowSize, 1U << ZSTD_WINDOWLOG_ABSOLUTEMIN);

            /* Frame header instruct buffer sizes */
            {   size_t const blockSize = MIN(zbd->fParams.windowSize, ZSTD_BLOCKSIZE_ABSOLUTEMAX);
                size_t const neededOutSize = zbd->fParams.windowSize + blockSize;
                zbd->blockSize = blockSize;
                if (zbd->inBuffSize < blockSize) {
                    zbd->customMem.customFree(zbd->customMem.opaque, zbd->inBuff);
                    zbd->inBuffSize = blockSize;
                    zbd->inBuff = (char*)zbd->customMem.customAlloc(zbd->customMem.opaque, blockSize);
                    if (zbd->inBuff == NULL) return ERROR(memory_allocation);
                }
                if (zbd->outBuffSize < neededOutSize) {
                    zbd->customMem.customFree(zbd->customMem.opaque, zbd->outBuff);
                    zbd->outBuffSize = neededOutSize;
                    zbd->outBuff = (char*)zbd->customMem.customAlloc(zbd->customMem.opaque, neededOutSize);
                    if (zbd->outBuff == NULL) return ERROR(memory_allocation);
            }   }
            zbd->stage = ZBUFFds_read;
            /* pass-through */

        case ZBUFFds_read:
            {   size_t const neededInSize = ZSTD_nextSrcSizeToDecompress(zbd->zd);
                if (neededInSize==0) {  /* end of frame */
                    zbd->stage = ZBUFFds_init;
                    someMoreWork = 0;
                    break;
                }
                if ((size_t)(iend-ip) >= neededInSize) {  /* decode directly from src */
                    const int isSkipFrame = ZSTD_isSkipFrame(zbd->zd);
                    size_t const decodedSize = ZSTD_decompressContinue(zbd->zd,
                        zbd->outBuff + zbd->outStart, (isSkipFrame ? 0 : zbd->outBuffSize - zbd->outStart),
                        ip, neededInSize);
                    if (ZSTD_isError(decodedSize)) return decodedSize;
                    ip += neededInSize;
                    if (!decodedSize && !isSkipFrame) break;   /* this was just a header */
                    zbd->outEnd = zbd->outStart +  decodedSize;
                    zbd->stage = ZBUFFds_flush;
                    break;
                }
                if (ip==iend) { someMoreWork = 0; break; }   /* no more input */
                zbd->stage = ZBUFFds_load;
                /* pass-through */
            }

        case ZBUFFds_load:
            {   size_t const neededInSize = ZSTD_nextSrcSizeToDecompress(zbd->zd);
                size_t const toLoad = neededInSize - zbd->inPos;   /* should always be <= remaining space within inBuff */
                size_t loadedSize;
                if (toLoad > zbd->inBuffSize - zbd->inPos) return ERROR(corruption_detected);   /* should never happen */
                loadedSize = ZBUFF_limitCopy(zbd->inBuff + zbd->inPos, toLoad, ip, iend-ip);
                ip += loadedSize;
                zbd->inPos += loadedSize;
                if (loadedSize < toLoad) { someMoreWork = 0; break; }   /* not enough input, wait for more */

                /* decode loaded input */
                {  const int isSkipFrame = ZSTD_isSkipFrame(zbd->zd);
                   size_t const decodedSize = ZSTD_decompressContinue(zbd->zd,
                        zbd->outBuff + zbd->outStart, zbd->outBuffSize - zbd->outStart,
                        zbd->inBuff, neededInSize);
                    if (ZSTD_isError(decodedSize)) return decodedSize;
                    zbd->inPos = 0;   /* input is consumed */
                    if (!decodedSize && !isSkipFrame) { zbd->stage = ZBUFFds_read; break; }   /* this was just a header */
                    zbd->outEnd = zbd->outStart +  decodedSize;
                    zbd->stage = ZBUFFds_flush;
                    /* pass-through */
            }   }

        case ZBUFFds_flush:
            {   size_t const toFlushSize = zbd->outEnd - zbd->outStart;
                size_t const flushedSize = ZBUFF_limitCopy(op, oend-op, zbd->outBuff + zbd->outStart, toFlushSize);
                op += flushedSize;
                zbd->outStart += flushedSize;
                if (flushedSize == toFlushSize) {  /* flush completed */
                    zbd->stage = ZBUFFds_read;
                    if (zbd->outStart + zbd->blockSize > zbd->outBuffSize)
                        zbd->outStart = zbd->outEnd = 0;
                    break;
                }
                /* cannot flush everything */
                someMoreWork = 0;
                break;
            }
        default: return ERROR(GENERIC);   /* impossible */
    }   }

    /* result */
    *srcSizePtr = ip-istart;
    *dstCapacityPtr = op-ostart;
    {   size_t nextSrcSizeHint = ZSTD_nextSrcSizeToDecompress(zbd->zd);
        if (!nextSrcSizeHint) return (zbd->outEnd != zbd->outStart);   /* return 0 only if fully flushed too */
        nextSrcSizeHint += ZSTD_blockHeaderSize * (ZSTD_nextInputType(zbd->zd) == ZSTDnit_block);
        if (zbd->inPos > nextSrcSizeHint) return ERROR(GENERIC);   /* should never happen */
        nextSrcSizeHint -= zbd->inPos;   /* already loaded*/
        return nextSrcSizeHint;
    }
}


/* *************************************
*  Tool functions
***************************************/
size_t ZBUFF_recommendedDInSize(void)  { return ZSTD_BLOCKSIZE_ABSOLUTEMAX + ZSTD_blockHeaderSize /* block header size*/ ; }
size_t ZBUFF_recommendedDOutSize(void) { return ZSTD_BLOCKSIZE_ABSOLUTEMAX; }
