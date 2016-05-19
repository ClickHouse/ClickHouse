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


/* *************************************
*  Constants
***************************************/
static size_t const ZBUFF_endFrameSize = ZSTD_BLOCKHEADERSIZE;


/*_**************************************************
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
* **************************************************/

typedef enum { ZBUFFcs_init, ZBUFFcs_load, ZBUFFcs_flush } ZBUFF_cStage;

/* *** Ressources *** */
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
};   /* typedef'd tp ZBUFF_CCtx within "zstd_buffered.h" */

ZBUFF_CCtx* ZBUFF_createCCtx(void)
{
    ZBUFF_CCtx* zbc = (ZBUFF_CCtx*)malloc(sizeof(ZBUFF_CCtx));
    if (zbc==NULL) return NULL;
    memset(zbc, 0, sizeof(*zbc));
    zbc->zc = ZSTD_createCCtx();
    return zbc;
}

size_t ZBUFF_freeCCtx(ZBUFF_CCtx* zbc)
{
    if (zbc==NULL) return 0;   /* support free on NULL */
    ZSTD_freeCCtx(zbc->zc);
    free(zbc->inBuff);
    free(zbc->outBuff);
    free(zbc);
    return 0;
}


/* *** Initialization *** */

size_t ZBUFF_compressInit_advanced(ZBUFF_CCtx* zbc,
                                   const void* dict, size_t dictSize,
                                   ZSTD_parameters params, U64 pledgedSrcSize)
{
    /* allocate buffers */
    {   size_t const neededInBuffSize = (size_t)1 << params.cParams.windowLog;
        if (zbc->inBuffSize < neededInBuffSize) {
            zbc->inBuffSize = neededInBuffSize;
            free(zbc->inBuff);   /* should not be necessary */
            zbc->inBuff = (char*)malloc(neededInBuffSize);
            if (zbc->inBuff == NULL) return ERROR(memory_allocation);
        }
        zbc->blockSize = MIN(ZSTD_BLOCKSIZE_MAX, neededInBuffSize/2);
    }
    if (zbc->outBuffSize < ZSTD_compressBound(zbc->blockSize)+1) {
        zbc->outBuffSize = ZSTD_compressBound(zbc->blockSize)+1;
        free(zbc->outBuff);   /* should not be necessary */
        zbc->outBuff = (char*)malloc(zbc->outBuffSize);
        if (zbc->outBuff == NULL) return ERROR(memory_allocation);
    }

    { size_t const errorCode = ZSTD_compressBegin_advanced(zbc->zc, dict, dictSize, params, pledgedSrcSize);
      if (ZSTD_isError(errorCode)) return errorCode; }

    zbc->inToCompress = 0;
    zbc->inBuffPos = 0;
    zbc->inBuffTarget = zbc->blockSize;
    zbc->outBuffFlushedSize = 0;
    zbc->stage = ZBUFFcs_load;
    return 0;   /* ready to go */
}


size_t ZBUFF_compressInitDictionary(ZBUFF_CCtx* zbc, const void* dict, size_t dictSize, int compressionLevel)
{
    ZSTD_parameters params;
    params.cParams = ZSTD_getCParams(compressionLevel, 0, dictSize);
    params.fParams.contentSizeFlag = 0;
    ZSTD_adjustCParams(&params.cParams, 0, dictSize);
    return ZBUFF_compressInit_advanced(zbc, dict, dictSize, params, 0);
}

size_t ZBUFF_compressInit(ZBUFF_CCtx* zbc, int compressionLevel)
{
    return ZBUFF_compressInitDictionary(zbc, NULL, 0, compressionLevel);
}


/* *** Compression *** */

static size_t ZBUFF_compressContinue_generic(ZBUFF_CCtx* zbc,
                              void* dst, size_t* dstCapacityPtr,
                        const void* src, size_t* srcSizePtr,
                              int flush)   /* aggregate : wait for full block before compressing */
{
    U32 notDone = 1;
    const char* const istart = (const char*)src;
    const char* const iend = istart + *srcSizePtr;
    const char* ip = istart;
    char* const ostart = (char*)dst;
    char* const oend = ostart + *dstCapacityPtr;
    char* op = ostart;

    while (notDone) {
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
                    notDone = 0; break;  /* not enough input to get a full block : stop there, wait for more */
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
                cSize = ZSTD_compressContinue(zbc->zc, cDst, oSize, zbc->inBuff + zbc->inToCompress, iSize);
                if (ZSTD_isError(cSize)) return cSize;
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
            /* flush into dst */
            {   size_t const toFlush = zbc->outBuffContentSize - zbc->outBuffFlushedSize;
                size_t const flushed = ZBUFF_limitCopy(op, oend-op, zbc->outBuff + zbc->outBuffFlushedSize, toFlush);
                op += flushed;
                zbc->outBuffFlushedSize += flushed;
                if (toFlush!=flushed) { notDone = 0; break; } /* not enough space within dst to store compressed block : stop there */
                zbc->outBuffContentSize = 0;
                zbc->outBuffFlushedSize = 0;
                zbc->stage = ZBUFFcs_load;
                break;
            }
        default:
            return ERROR(GENERIC);   /* impossible */
        }
    }

    *srcSizePtr = ip - istart;
    *dstCapacityPtr = op - ostart;
    {   size_t hintInSize = zbc->inBuffTarget - zbc->inBuffPos;
        if (hintInSize==0) hintInSize = zbc->blockSize;
        return hintInSize;
    }
}

size_t ZBUFF_compressContinue(ZBUFF_CCtx* zbc,
                              void* dst, size_t* dstCapacityPtr,
                        const void* src, size_t* srcSizePtr)
{
    return ZBUFF_compressContinue_generic(zbc, dst, dstCapacityPtr, src, srcSizePtr, 0);
}



/* *** Finalize *** */

size_t ZBUFF_compressFlush(ZBUFF_CCtx* zbc, void* dst, size_t* dstCapacityPtr)
{
    size_t srcSize = 0;
    ZBUFF_compressContinue_generic(zbc, dst, dstCapacityPtr, &srcSize, &srcSize, 1);  /* use a valid src address instead of NULL */
    return zbc->outBuffContentSize - zbc->outBuffFlushedSize;
}


size_t ZBUFF_compressEnd(ZBUFF_CCtx* zbc, void* dst, size_t* dstCapacityPtr)
{
    BYTE* const ostart = (BYTE*)dst;
    BYTE* const oend = ostart + *dstCapacityPtr;
    BYTE* op = ostart;
    size_t outSize = *dstCapacityPtr;
    size_t epilogueSize, remaining;
    ZBUFF_compressFlush(zbc, dst, &outSize);     /* flush any remaining inBuff */
    op += outSize;
    epilogueSize = ZSTD_compressEnd(zbc->zc, zbc->outBuff + zbc->outBuffContentSize, zbc->outBuffSize - zbc->outBuffContentSize);   /* epilogue into outBuff */
    zbc->outBuffContentSize += epilogueSize;
    outSize = oend-op;
    zbc->stage = ZBUFFcs_flush;
    remaining = ZBUFF_compressFlush(zbc, op, &outSize);   /* attempt to flush epilogue into dst */
    op += outSize;
    if (!remaining) zbc->stage = ZBUFFcs_init;   /* close only if nothing left to flush */
    *dstCapacityPtr = op-ostart;                 /* tells how many bytes were written */
    return remaining;
}



/* *************************************
*  Tool functions
***************************************/
size_t ZBUFF_recommendedCInSize(void)  { return ZSTD_BLOCKSIZE_MAX; }
size_t ZBUFF_recommendedCOutSize(void) { return ZSTD_compressBound(ZSTD_BLOCKSIZE_MAX) + ZSTD_blockHeaderSize + ZBUFF_endFrameSize; }
