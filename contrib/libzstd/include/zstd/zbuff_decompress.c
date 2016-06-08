/*
    Buffered version of Zstd compression library
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
    - zstd homepage : http://www.zstd.net/
*/


/* *************************************
*  Dependencies
***************************************/
#include <stdlib.h>
#include "error_private.h"
#include "zstd_internal.h"  /* MIN, ZSTD_blockHeaderSize */
#include "zstd_static.h"    /* ZSTD_BLOCKSIZE_MAX */
#include "zbuff_static.h"


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
*  The content of @dst will be overwritten (up to *dstCapacityPtr) at each function call, so save its content if it matters, or change @dst.
*  @return : a hint to preferred nb of bytes to use as input for next function call (it's only a hint, to help latency),
*            or 0 when a frame is completely decoded,
*            or an error code, which can be tested using ZBUFF_isError().
*
*  Hint : recommended buffer sizes (not compulsory) : ZBUFF_recommendedDInSize() and ZBUFF_recommendedDOutSize()
*  output : ZBUFF_recommendedDOutSize==128 KB block size is the internal unit, it ensures it's always possible to write a full block when decoded.
*  input  : ZBUFF_recommendedDInSize == 128KB + 3;
*           just follow indications from ZBUFF_decompressContinue() to minimize latency. It should always be <= 128 KB + 3 .
* *******************************************************************************/

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
};   /* typedef'd to ZBUFF_DCtx within "zstd_buffered.h" */


ZBUFF_DCtx* ZBUFF_createDCtx(void)
{
    ZBUFF_DCtx* zbd = (ZBUFF_DCtx*)malloc(sizeof(ZBUFF_DCtx));
    if (zbd==NULL) return NULL;
    memset(zbd, 0, sizeof(*zbd));
    zbd->zd = ZSTD_createDCtx();
    zbd->stage = ZBUFFds_init;
    return zbd;
}

size_t ZBUFF_freeDCtx(ZBUFF_DCtx* zbd)
{
    if (zbd==NULL) return 0;   /* support free on null */
    ZSTD_freeDCtx(zbd->zd);
    free(zbd->inBuff);
    free(zbd->outBuff);
    free(zbd);
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
    U32 notDone = 1;

    while (notDone) {
        switch(zbd->stage)
        {
        case ZBUFFds_init :
            return ERROR(init_missing);

        case ZBUFFds_loadHeader :
            {   size_t const hSize = ZSTD_getFrameParams(&(zbd->fParams), zbd->headerBuffer, zbd->lhSize);
                if (hSize != 0) {
                    size_t const toLoad = hSize - zbd->lhSize;   /* if hSize!=0, hSize > zbd->lhSize */
                    if (ZSTD_isError(hSize)) return hSize;
                    if (toLoad > (size_t)(iend-ip)) {   /* not enough input to load full header */
                        memcpy(zbd->headerBuffer + zbd->lhSize, ip, iend-ip);
                        zbd->lhSize += iend-ip; ip = iend; notDone = 0;
                        *dstCapacityPtr = 0;
                        return (hSize - zbd->lhSize) + ZSTD_blockHeaderSize;   /* remaining header bytes + next block header */
                    }
                    memcpy(zbd->headerBuffer + zbd->lhSize, ip, toLoad); zbd->lhSize = hSize; ip += toLoad;
                    break;
            }   }

            /* Consume header */
            {   size_t const h1Size = ZSTD_nextSrcSizeToDecompress(zbd->zd);  /* == ZSTD_frameHeaderSize_min */
                size_t const h1Result = ZSTD_decompressContinue(zbd->zd, NULL, 0, zbd->headerBuffer, h1Size);
                if (ZSTD_isError(h1Result)) return h1Result;
                if (h1Size < zbd->lhSize) {   /* long header */
                    size_t const h2Size = ZSTD_nextSrcSizeToDecompress(zbd->zd);
                    size_t const h2Result = ZSTD_decompressContinue(zbd->zd, NULL, 0, zbd->headerBuffer+h1Size, h2Size);
                    if (ZSTD_isError(h2Result)) return h2Result;
            }   }

            /* Frame header instruct buffer sizes */
            {   size_t const blockSize = MIN(1 << zbd->fParams.windowLog, ZSTD_BLOCKSIZE_MAX);
                zbd->blockSize = blockSize;
                if (zbd->inBuffSize < blockSize) {
                    free(zbd->inBuff);
                    zbd->inBuffSize = blockSize;
                    zbd->inBuff = (char*)malloc(blockSize);
                    if (zbd->inBuff == NULL) return ERROR(memory_allocation);
                }
                {   size_t const neededOutSize = ((size_t)1 << zbd->fParams.windowLog) + blockSize;
                    if (zbd->outBuffSize < neededOutSize) {
                        free(zbd->outBuff);
                        zbd->outBuffSize = neededOutSize;
                        zbd->outBuff = (char*)malloc(neededOutSize);
                        if (zbd->outBuff == NULL) return ERROR(memory_allocation);
            }   }   }
            zbd->stage = ZBUFFds_read;

        case ZBUFFds_read:
            {   size_t const neededInSize = ZSTD_nextSrcSizeToDecompress(zbd->zd);
                if (neededInSize==0) {  /* end of frame */
                    zbd->stage = ZBUFFds_init;
                    notDone = 0;
                    break;
                }
                if ((size_t)(iend-ip) >= neededInSize) {  /* decode directly from src */
                    size_t const decodedSize = ZSTD_decompressContinue(zbd->zd,
                        zbd->outBuff + zbd->outStart, zbd->outBuffSize - zbd->outStart,
                        ip, neededInSize);
                    if (ZSTD_isError(decodedSize)) return decodedSize;
                    ip += neededInSize;
                    if (!decodedSize) break;   /* this was just a header */
                    zbd->outEnd = zbd->outStart +  decodedSize;
                    zbd->stage = ZBUFFds_flush;
                    break;
                }
                if (ip==iend) { notDone = 0; break; }   /* no more input */
                zbd->stage = ZBUFFds_load;
            }

        case ZBUFFds_load:
            {   size_t const neededInSize = ZSTD_nextSrcSizeToDecompress(zbd->zd);
                size_t const toLoad = neededInSize - zbd->inPos;   /* should always be <= remaining space within inBuff */
                size_t loadedSize;
                if (toLoad > zbd->inBuffSize - zbd->inPos) return ERROR(corruption_detected);   /* should never happen */
                loadedSize = ZBUFF_limitCopy(zbd->inBuff + zbd->inPos, toLoad, ip, iend-ip);
                ip += loadedSize;
                zbd->inPos += loadedSize;
                if (loadedSize < toLoad) { notDone = 0; break; }   /* not enough input, wait for more */

                /* decode loaded input */
                {   size_t const decodedSize = ZSTD_decompressContinue(zbd->zd,
                        zbd->outBuff + zbd->outStart, zbd->outBuffSize - zbd->outStart,
                        zbd->inBuff, neededInSize);
                    if (ZSTD_isError(decodedSize)) return decodedSize;
                    zbd->inPos = 0;   /* input is consumed */
                    if (!decodedSize) { zbd->stage = ZBUFFds_read; break; }   /* this was just a header */
                    zbd->outEnd = zbd->outStart +  decodedSize;
                    zbd->stage = ZBUFFds_flush;
                    // break; /* ZBUFFds_flush follows */
            }   }

        case ZBUFFds_flush:
            {   size_t const toFlushSize = zbd->outEnd - zbd->outStart;
                size_t const flushedSize = ZBUFF_limitCopy(op, oend-op, zbd->outBuff + zbd->outStart, toFlushSize);
                op += flushedSize;
                zbd->outStart += flushedSize;
                if (flushedSize == toFlushSize) {
                    zbd->stage = ZBUFFds_read;
                    if (zbd->outStart + zbd->blockSize > zbd->outBuffSize)
                        zbd->outStart = zbd->outEnd = 0;
                    break;
                }
                /* cannot flush everything */
                notDone = 0;
                break;
            }
        default: return ERROR(GENERIC);   /* impossible */
    }   }

    /* result */
    *srcSizePtr = ip-istart;
    *dstCapacityPtr = op-ostart;
    {   size_t nextSrcSizeHint = ZSTD_nextSrcSizeToDecompress(zbd->zd);
        if (nextSrcSizeHint > ZSTD_blockHeaderSize) nextSrcSizeHint+= ZSTD_blockHeaderSize;   /* get following block header too */
        nextSrcSizeHint -= zbd->inPos;   /* already loaded*/
        return nextSrcSizeHint;
    }
}



/* *************************************
*  Tool functions
***************************************/
size_t ZBUFF_recommendedDInSize(void)  { return ZSTD_BLOCKSIZE_MAX + ZSTD_blockHeaderSize /* block header size*/ ; }
size_t ZBUFF_recommendedDOutSize(void) { return ZSTD_BLOCKSIZE_MAX; }
