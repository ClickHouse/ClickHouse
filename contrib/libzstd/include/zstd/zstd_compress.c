/*
    ZSTD HC - High Compression Mode of Zstandard
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
       - Zstd source repository : https://www.zstd.net
*/


/* *******************************************************
*  Compiler specifics
*********************************************************/
#ifdef _MSC_VER    /* Visual Studio */
#  define FORCE_INLINE static __forceinline
#  include <intrin.h>                    /* For Visual 2005 */
#  pragma warning(disable : 4127)        /* disable: C4127: conditional expression is constant */
#else
#  ifdef __GNUC__
#    define FORCE_INLINE static inline __attribute__((always_inline))
#  else
#    define FORCE_INLINE static inline
#  endif
#endif


/*-*************************************
*  Dependencies
***************************************/
#include <stdlib.h>   /* malloc */
#include <string.h>   /* memset */
#include "mem.h"
#include "fse_static.h"
#include "huff0_static.h"
#include "zstd_internal.h"


/*-*************************************
*  Constants
***************************************/
static const U32 g_searchStrength = 8;


/*-*************************************
*  Helper functions
***************************************/
size_t ZSTD_compressBound(size_t srcSize) { return FSE_compressBound(srcSize) + 12; }


/*-*************************************
*  Sequence storage
***************************************/
/** ZSTD_resetFreqs() : for opt variants */
static void ZSTD_resetFreqs(seqStore_t* ssPtr)
{
    unsigned u;
    ssPtr->matchLengthSum = 512; // (1<<MLbits);
    ssPtr->litLengthSum = 256; // (1<<LLbits);
    ssPtr->litSum = (1<<Litbits);
    ssPtr->offCodeSum = (1<<Offbits);

    for (u=0; u<=MaxLit; u++)
        ssPtr->litFreq[u] = 1;
    for (u=0; u<=MaxLL; u++)
        ssPtr->litLengthFreq[u] = 1;
    for (u=0; u<=MaxML; u++)
        ssPtr->matchLengthFreq[u] = 1;
    for (u=0; u<=MaxOff; u++)
        ssPtr->offCodeFreq[u] = 1;
}

static void ZSTD_resetSeqStore(seqStore_t* ssPtr)
{
    ssPtr->offset = ssPtr->offsetStart;
    ssPtr->lit = ssPtr->litStart;
    ssPtr->litLength = ssPtr->litLengthStart;
    ssPtr->matchLength = ssPtr->matchLengthStart;
    ssPtr->dumps = ssPtr->dumpsStart;
}


/*-*************************************
*  Context memory management
***************************************/
struct ZSTD_CCtx_s
{
    const BYTE* nextSrc;    /* next block here to continue on current prefix */
    const BYTE* base;       /* All regular indexes relative to this position */
    const BYTE* dictBase;   /* extDict indexes relative to this position */
    U32   dictLimit;        /* below that point, need extDict */
    U32   lowLimit;         /* below that point, no more data */
    U32   nextToUpdate;     /* index from which to continue dictionary update */
    U32   loadedDictEnd;
    U32   stage;
    ZSTD_parameters params;
    void* workSpace;
    size_t workSpaceSize;
    size_t blockSize;
    size_t hbSize;
    char headerBuffer[ZSTD_frameHeaderSize_max];

    seqStore_t seqStore;    /* sequences storage ptrs */
    U32* hashTable;
    U32* contentTable;
    HUF_CElt* hufTable;
    U32 flagStaticTables;
    FSE_CTable offcodeCTable   [FSE_CTABLE_SIZE_U32(OffFSELog, MaxOff)];
    FSE_CTable matchlengthCTable [FSE_CTABLE_SIZE_U32(MLFSELog, MaxML)];
    FSE_CTable litlengthCTable   [FSE_CTABLE_SIZE_U32(LLFSELog, MaxLL)];
};

ZSTD_CCtx* ZSTD_createCCtx(void)
{
    return (ZSTD_CCtx*) calloc(1, sizeof(ZSTD_CCtx));
}

size_t ZSTD_freeCCtx(ZSTD_CCtx* cctx)
{
    free(cctx->workSpace);
    free(cctx);
    return 0;   /* reserved as a potential error code in the future */
}

seqStore_t ZSTD_copySeqStore(const ZSTD_CCtx* ctx)
{
    return ctx->seqStore;
}


static unsigned ZSTD_highbit(U32 val);

#define CLAMP(val,min,max) { if (val<min) val=min; else if (val>max) val=max; }

/** ZSTD_validateParams() :
    correct params value to remain within authorized range,
    optimize for `srcSize` if srcSize > 0 */
void ZSTD_validateParams(ZSTD_parameters* params)
{
    const U32 btPlus = (params->strategy == ZSTD_btlazy2) || (params->strategy == ZSTD_btopt);

    /* validate params */
    if (MEM_32bits()) if (params->windowLog > 25) params->windowLog = 25;   /* 32 bits mode cannot flush > 24 bits */
    CLAMP(params->windowLog, ZSTD_WINDOWLOG_MIN, ZSTD_WINDOWLOG_MAX);
    CLAMP(params->contentLog, ZSTD_CONTENTLOG_MIN, ZSTD_CONTENTLOG_MAX);
    CLAMP(params->hashLog, ZSTD_HASHLOG_MIN, ZSTD_HASHLOG_MAX);
    CLAMP(params->searchLog, ZSTD_SEARCHLOG_MIN, ZSTD_SEARCHLOG_MAX);
    CLAMP(params->searchLength, ZSTD_SEARCHLENGTH_MIN, ZSTD_SEARCHLENGTH_MAX);
    CLAMP(params->targetLength, ZSTD_TARGETLENGTH_MIN, ZSTD_TARGETLENGTH_MAX);
    if ((U32)params->strategy>(U32)ZSTD_btopt) params->strategy = ZSTD_btopt;

    /* correct params, to use less memory */
    if ((params->srcSize > 0) && (params->srcSize < (1<<ZSTD_WINDOWLOG_MAX))) {
        U32 srcLog = ZSTD_highbit((U32)(params->srcSize)-1) + 1;
        if (params->windowLog > srcLog) params->windowLog = srcLog;
    }
    if (params->windowLog   < ZSTD_WINDOWLOG_ABSOLUTEMIN) params->windowLog = ZSTD_WINDOWLOG_ABSOLUTEMIN;  /* required for frame header */
    if (params->contentLog  > params->windowLog+btPlus) params->contentLog = params->windowLog+btPlus;   /* <= ZSTD_CONTENTLOG_MAX */
}


static size_t ZSTD_resetCCtx_advanced (ZSTD_CCtx* zc,
                                       ZSTD_parameters params)
{   /* note : params considered validated here */
    const size_t blockSize = MIN(BLOCKSIZE, (size_t)1 << params.windowLog);
    /* reserve table memory */
    const U32 contentLog = (params.strategy == ZSTD_fast) ? 1 : params.contentLog;
    const size_t tableSpace = ((1 << contentLog) + (1 << params.hashLog)) * sizeof(U32);
    const size_t neededSpace = tableSpace + (256*sizeof(U32)) + (3*blockSize) + ((1<<MLbits) + (1<<LLbits) + (1<<Offbits) + (1<<Litbits))*sizeof(U32);
    if (zc->workSpaceSize < neededSpace) {
        free(zc->workSpace);
        zc->workSpace = malloc(neededSpace);
        if (zc->workSpace == NULL) return ERROR(memory_allocation);
        zc->workSpaceSize = neededSpace;
    }
    memset(zc->workSpace, 0, tableSpace );   /* reset only tables */
    zc->hashTable = (U32*)(zc->workSpace);
    zc->contentTable = zc->hashTable + ((size_t)1 << params.hashLog);
    zc->seqStore.buffer = zc->contentTable + ((size_t)1 << contentLog);
    zc->hufTable = (HUF_CElt*)zc->seqStore.buffer;
    zc->flagStaticTables = 0;
    zc->seqStore.buffer = (U32*)(zc->seqStore.buffer) + 256;

    zc->nextToUpdate = 1;
    zc->nextSrc = NULL;
    zc->base = NULL;
    zc->dictBase = NULL;
    zc->dictLimit = 0;
    zc->lowLimit = 0;
    zc->params = params;
    zc->blockSize = blockSize;

    zc->seqStore.litFreq = (U32*) (zc->seqStore.buffer);
    zc->seqStore.litLengthFreq = zc->seqStore.litFreq + (1<<Litbits);
    zc->seqStore.matchLengthFreq = zc->seqStore.litLengthFreq + (1<<LLbits);
    zc->seqStore.offCodeFreq = zc->seqStore.matchLengthFreq + (1<<MLbits);

    zc->seqStore.offsetStart = zc->seqStore.offCodeFreq + (1<<Offbits);
    zc->seqStore.offCodeStart = (BYTE*) (zc->seqStore.offsetStart + (blockSize>>2));
    zc->seqStore.litStart = zc->seqStore.offCodeStart + (blockSize>>2);
    zc->seqStore.litLengthStart =  zc->seqStore.litStart + blockSize;
    zc->seqStore.matchLengthStart = zc->seqStore.litLengthStart + (blockSize>>2);
    zc->seqStore.dumpsStart = zc->seqStore.matchLengthStart + (blockSize>>2);
    // zc->seqStore.XXX = zc->seqStore.dumpsStart + (blockSize>>4);

    zc->hbSize = 0;
    zc->stage = 0;
    zc->loadedDictEnd = 0;

    return 0;
}


/*! ZSTD_copyCCtx
*   Duplicate an existing context @srcCCtx into another one @dstCCtx.
*   Only works during stage 0 (i.e. before first call to ZSTD_compressContinue())
*   @return : 0, or an error code */
size_t ZSTD_copyCCtx(ZSTD_CCtx* dstCCtx, const ZSTD_CCtx* srcCCtx)
{
    const U32 contentLog = (srcCCtx->params.strategy == ZSTD_fast) ? 1 : srcCCtx->params.contentLog;
    const size_t tableSpace = ((1 << contentLog) + (1 << srcCCtx->params.hashLog)) * sizeof(U32);

    if (srcCCtx->stage!=0) return ERROR(stage_wrong);

    ZSTD_resetCCtx_advanced(dstCCtx, srcCCtx->params);

    /* copy tables */
    memcpy(dstCCtx->hashTable, srcCCtx->hashTable, tableSpace);

    /* copy frame header */
    dstCCtx->hbSize = srcCCtx->hbSize;
    memcpy(dstCCtx->headerBuffer , srcCCtx->headerBuffer, srcCCtx->hbSize);

    /* copy dictionary pointers */
    dstCCtx->nextToUpdate= srcCCtx->nextToUpdate;
    dstCCtx->nextSrc     = srcCCtx->nextSrc;
    dstCCtx->base        = srcCCtx->base;
    dstCCtx->dictBase    = srcCCtx->dictBase;
    dstCCtx->dictLimit   = srcCCtx->dictLimit;
    dstCCtx->lowLimit    = srcCCtx->lowLimit;
    dstCCtx->loadedDictEnd = srcCCtx->loadedDictEnd;

    /* copy entropy tables */
    dstCCtx->flagStaticTables = srcCCtx->flagStaticTables;
    if (srcCCtx->flagStaticTables) {
        memcpy(dstCCtx->hufTable, srcCCtx->hufTable, 256*4);
        memcpy(dstCCtx->litlengthCTable, srcCCtx->litlengthCTable, sizeof(dstCCtx->litlengthCTable));
        memcpy(dstCCtx->matchlengthCTable, srcCCtx->matchlengthCTable, sizeof(dstCCtx->matchlengthCTable));
        memcpy(dstCCtx->offcodeCTable, srcCCtx->offcodeCTable, sizeof(dstCCtx->offcodeCTable));
    }

    return 0;
}


/*! ZSTD_reduceIndex
*   rescale indexes to avoid future overflow (indexes are U32) */
static void ZSTD_reduceIndex (ZSTD_CCtx* zc,
                        const U32 reducerValue)
{
    const U32 contentLog = (zc->params.strategy == ZSTD_fast) ? 1 : zc->params.contentLog;
    const U32 tableSpaceU32 = (1 << contentLog) + (1 << zc->params.hashLog);
    U32* table32 = zc->hashTable;
    U32 index;

    for (index=0 ; index < tableSpaceU32 ; index++) {
        if (table32[index] < reducerValue) table32[index] = 0;
        else table32[index] -= reducerValue;
    }
}


/*-*******************************************************
*  Block entropic compression
*********************************************************/

/* Block format description

   Block = Literal Section - Sequences Section
   Prerequisite : size of (compressed) block, maximum size of regenerated data

   1) Literal Section

   1.1) Header : 1-5 bytes
        flags: 2 bits
            00 compressed by Huff0
            01 unused
            10 is Raw (uncompressed)
            11 is Rle
            Note : using 01 => Huff0 with precomputed table ?
            Note : delta map ? => compressed ?

   1.1.1) Huff0-compressed literal block : 3-5 bytes
            srcSize < 1 KB => 3 bytes (2-2-10-10) => single stream
            srcSize < 1 KB => 3 bytes (2-2-10-10)
            srcSize < 16KB => 4 bytes (2-2-14-14)
            else           => 5 bytes (2-2-18-18)
            big endian convention

   1.1.2) Raw (uncompressed) literal block header : 1-3 bytes
        size :  5 bits: (IS_RAW<<6) + (0<<4) + size
               12 bits: (IS_RAW<<6) + (2<<4) + (size>>8)
                        size&255
               20 bits: (IS_RAW<<6) + (3<<4) + (size>>16)
                        size>>8&255
                        size&255

   1.1.3) Rle (repeated single byte) literal block header : 1-3 bytes
        size :  5 bits: (IS_RLE<<6) + (0<<4) + size
               12 bits: (IS_RLE<<6) + (2<<4) + (size>>8)
                        size&255
               20 bits: (IS_RLE<<6) + (3<<4) + (size>>16)
                        size>>8&255
                        size&255

   1.1.4) Huff0-compressed literal block, using precomputed CTables : 3-5 bytes
            srcSize < 1 KB => 3 bytes (2-2-10-10) => single stream
            srcSize < 1 KB => 3 bytes (2-2-10-10)
            srcSize < 16KB => 4 bytes (2-2-14-14)
            else           => 5 bytes (2-2-18-18)
            big endian convention

        1- CTable available (stored into workspace ?)
        2- Small input (fast heuristic ? Full comparison ? depend on clevel ?)


   1.2) Literal block content

   1.2.1) Huff0 block, using sizes from header
        See Huff0 format

   1.2.2) Huff0 block, using prepared table

   1.2.3) Raw content

   1.2.4) single byte


   2) Sequences section

      - Nb Sequences : 2 bytes, little endian
      - Control Token : 1 byte (see below)
      - Dumps Length : 1 or 2 bytes (depending on control token)
      - Dumps : as stated by dumps length
      - Literal Lengths FSE table (as needed depending on encoding method)
      - Offset Codes FSE table (as needed depending on encoding method)
      - Match Lengths FSE table (as needed depending on encoding method)

    2.1) Control Token
      8 bits, divided as :
      0-1 : dumpsLength
      2-3 : MatchLength, FSE encoding method
      4-5 : Offset Codes, FSE encoding method
      6-7 : Literal Lengths, FSE encoding method

      FSE encoding method :
      FSE_ENCODING_RAW : uncompressed; no header
      FSE_ENCODING_RLE : single repeated value; header 1 byte
      FSE_ENCODING_STATIC : use prepared table; no header
      FSE_ENCODING_DYNAMIC : read NCount
*/

size_t ZSTD_noCompressBlock (void* dst, size_t maxDstSize, const void* src, size_t srcSize)
{
    BYTE* const ostart = (BYTE* const)dst;

    if (srcSize + ZSTD_blockHeaderSize > maxDstSize) return ERROR(dstSize_tooSmall);
    memcpy(ostart + ZSTD_blockHeaderSize, src, srcSize);

    /* Build header */
    ostart[0]  = (BYTE)(srcSize>>16);
    ostart[1]  = (BYTE)(srcSize>>8);
    ostart[2]  = (BYTE) srcSize;
    ostart[0] += (BYTE)(bt_raw<<6);   /* is a raw (uncompressed) block */

    return ZSTD_blockHeaderSize+srcSize;
}


static size_t ZSTD_noCompressLiterals (void* dst, size_t maxDstSize, const void* src, size_t srcSize)
{
    BYTE* const ostart = (BYTE* const)dst;
    const U32 flSize = 1 + (srcSize>31) + (srcSize>4095);

    if (srcSize + flSize > maxDstSize) return ERROR(dstSize_tooSmall);

    switch(flSize)
    {
        case 1: /* 2 - 1 - 5 */
            ostart[0] = (BYTE)((IS_RAW<<6) + (0<<5) + srcSize);
            break;
        case 2: /* 2 - 2 - 12 */
            ostart[0] = (BYTE)((IS_RAW<<6) + (2<<4) + (srcSize >> 8));
            ostart[1] = (BYTE)srcSize;
            break;
        default:   /*note : should not be necessary : flSize is within {1,2,3} */
        case 3: /* 2 - 2 - 20 */
            ostart[0] = (BYTE)((IS_RAW<<6) + (3<<4) + (srcSize >> 16));
            ostart[1] = (BYTE)(srcSize>>8);
            ostart[2] = (BYTE)srcSize;
            break;
    }

    memcpy(ostart + flSize, src, srcSize);
    return srcSize + flSize;
}

static size_t ZSTD_compressRleLiteralsBlock (void* dst, size_t maxDstSize, const void* src, size_t srcSize)
{
    BYTE* const ostart = (BYTE* const)dst;
    U32 flSize = 1 + (srcSize>31) + (srcSize>4095);

    (void)maxDstSize;  /* maxDstSize guaranteed to be >=4, hence large enough */

    switch(flSize)
    {
        case 1: /* 2 - 1 - 5 */
            ostart[0] = (BYTE)((IS_RLE<<6) + (0<<5) + srcSize);
            break;
        case 2: /* 2 - 2 - 12 */
            ostart[0] = (BYTE)((IS_RLE<<6) + (2<<4) + (srcSize >> 8));
            ostart[1] = (BYTE)srcSize;
            break;
        default:   /*note : should not be necessary : flSize is necessary within {1,2,3} */
        case 3: /* 2 - 2 - 20 */
            ostart[0] = (BYTE)((IS_RLE<<6) + (3<<4) + (srcSize >> 16));
            ostart[1] = (BYTE)(srcSize>>8);
            ostart[2] = (BYTE)srcSize;
            break;
    }

    ostart[flSize] = *(const BYTE*)src;
    return flSize+1;
}


size_t ZSTD_minGain(size_t srcSize) { return (srcSize >> 6) + 2; }

static size_t ZSTD_compressLiterals (ZSTD_CCtx* zc,
                                     void* dst, size_t maxDstSize,
                               const void* src, size_t srcSize)
{
    const size_t minGain = ZSTD_minGain(srcSize);
    BYTE* const ostart = (BYTE*)dst;
    const size_t lhSize = 3 + (srcSize >= 1 KB) + (srcSize >= 16 KB);
    U32 singleStream = srcSize < 256;
    U32 hType = IS_HUF;
    size_t clitSize;

    if (maxDstSize < lhSize+1) return ERROR(dstSize_tooSmall);   /* not enough space for compression */

    if (zc->flagStaticTables && (lhSize==3)) {
        hType = IS_PCH;
        singleStream = 1;
        clitSize = HUF_compress1X_usingCTable(ostart+lhSize, maxDstSize-lhSize, src, srcSize, zc->hufTable);
    } else {
        clitSize = singleStream ? HUF_compress1X(ostart+lhSize, maxDstSize-lhSize, src, srcSize, 255, 12)
                                : HUF_compress2 (ostart+lhSize, maxDstSize-lhSize, src, srcSize, 255, 12);
    }

    if ((clitSize==0) || (clitSize >= srcSize - minGain)) return ZSTD_noCompressLiterals(dst, maxDstSize, src, srcSize);
    if (clitSize==1) return ZSTD_compressRleLiteralsBlock(dst, maxDstSize, src, srcSize);

    /* Build header */
    switch(lhSize)
    {
    case 3: /* 2 - 2 - 10 - 10 */
        ostart[0] = (BYTE)((srcSize>>6) + (singleStream << 4) + (hType<<6));
        ostart[1] = (BYTE)((srcSize<<2) + (clitSize>>8));
        ostart[2] = (BYTE)(clitSize);
        break;
    case 4: /* 2 - 2 - 14 - 14 */
        ostart[0] = (BYTE)((srcSize>>10) + (2<<4) +  (hType<<6));
        ostart[1] = (BYTE)(srcSize>> 2);
        ostart[2] = (BYTE)((srcSize<<6) + (clitSize>>8));
        ostart[3] = (BYTE)(clitSize);
        break;
    default:   /* should not be necessary, lhSize is {3,4,5} */
    case 5: /* 2 - 2 - 18 - 18 */
        ostart[0] = (BYTE)((srcSize>>14) + (3<<4) +  (hType<<6));
        ostart[1] = (BYTE)(srcSize>>6);
        ostart[2] = (BYTE)((srcSize<<2) + (clitSize>>16));
        ostart[3] = (BYTE)(clitSize>>8);
        ostart[4] = (BYTE)(clitSize);
        break;
    }

    return lhSize+clitSize;
}


#define LITERAL_NOENTROPY 63   /* don't even attempt to compress literals below this threshold (cheap heuristic) */

size_t ZSTD_compressSequences(ZSTD_CCtx* zc,
                              void* dst, size_t maxDstSize,
                              size_t srcSize)
{
    const seqStore_t* seqStorePtr = &(zc->seqStore);
    U32 count[MaxSeq+1];
    S16 norm[MaxSeq+1];
    size_t mostFrequent;
    U32 max;
    FSE_CTable* CTable_LitLength = zc->litlengthCTable;
    FSE_CTable* CTable_OffsetBits = zc->offcodeCTable;
    FSE_CTable* CTable_MatchLength = zc->matchlengthCTable;
    U32 LLtype, Offtype, MLtype;   /* compressed, raw or rle */
    const BYTE* const op_lit_start = seqStorePtr->litStart;
    const BYTE* const llTable = seqStorePtr->litLengthStart;
    const BYTE* const llPtr = seqStorePtr->litLength;
    const BYTE* const mlTable = seqStorePtr->matchLengthStart;
    const U32*  const offsetTable = seqStorePtr->offsetStart;
    BYTE* const offCodeTable = seqStorePtr->offCodeStart;
    BYTE* const ostart = (BYTE*)dst;
    BYTE* op = ostart;
    BYTE* const oend = ostart + maxDstSize;
    const size_t nbSeq = llPtr - llTable;
    const size_t minGain = ZSTD_minGain(srcSize);
    const size_t maxCSize = srcSize - minGain;
    BYTE* seqHead;

    /* Compress literals */
    {
        size_t cSize;
        size_t litSize = seqStorePtr->lit - op_lit_start;
        const size_t minLitSize = zc->flagStaticTables ? 6 : LITERAL_NOENTROPY;

        if (litSize <= minLitSize)
            cSize = ZSTD_noCompressLiterals(op, maxDstSize, op_lit_start, litSize);
        else
            cSize = ZSTD_compressLiterals(zc, op, maxDstSize, op_lit_start, litSize);
        if (ZSTD_isError(cSize)) return cSize;
        op += cSize;
    }

    /* Sequences Header */
    if ((oend-op) < MIN_SEQUENCES_SIZE) return ERROR(dstSize_tooSmall);
    if (nbSeq < 128) *op++ = (BYTE)nbSeq;
    else {
        op[0] = (BYTE)((nbSeq>>8) + 128); op[1] = (BYTE)nbSeq; op+=2;
    }
    if (nbSeq==0) goto _check_compressibility;

    /* dumps : contains rests of large lengths */
    if ((oend-op) < 3 /* dumps */ + 1 /*seqHead*/)
        return ERROR(dstSize_tooSmall);
    seqHead = op;
    {
        size_t dumpsLength = seqStorePtr->dumps - seqStorePtr->dumpsStart;
        if (dumpsLength < 512) {
            op[0] = (BYTE)(dumpsLength >> 8);
            op[1] = (BYTE)(dumpsLength);
            op += 2;
        } else {
            op[0] = 2;
            op[1] = (BYTE)(dumpsLength>>8);
            op[2] = (BYTE)(dumpsLength);
            op += 3;
        }
        if ((size_t)(oend-op) < dumpsLength+6) return ERROR(dstSize_tooSmall);
        memcpy(op, seqStorePtr->dumpsStart, dumpsLength);
        op += dumpsLength;
    }

#define MIN_SEQ_FOR_DYNAMIC_FSE   64
#define MAX_SEQ_FOR_STATIC_FSE  1000

    /* CTable for Literal Lengths */
    max = MaxLL;
    mostFrequent = FSE_countFast(count, &max, llTable, nbSeq);
    if ((mostFrequent == nbSeq) && (nbSeq > 2)) {
        *op++ = llTable[0];
        FSE_buildCTable_rle(CTable_LitLength, (BYTE)max);
        LLtype = FSE_ENCODING_RLE;
    } else if ((zc->flagStaticTables) && (nbSeq < MAX_SEQ_FOR_STATIC_FSE)) {
        LLtype = FSE_ENCODING_STATIC;
    } else if ((nbSeq < MIN_SEQ_FOR_DYNAMIC_FSE) || (mostFrequent < (nbSeq >> (LLbits-1)))) {
        FSE_buildCTable_raw(CTable_LitLength, LLbits);
        LLtype = FSE_ENCODING_RAW;
    } else {
        size_t NCountSize;
        size_t nbSeq_1 = nbSeq;
        U32 tableLog = FSE_optimalTableLog(LLFSELog, nbSeq, max);
        if (count[llTable[nbSeq-1]]>1) { count[llTable[nbSeq-1]]--; nbSeq_1--; }
        FSE_normalizeCount(norm, tableLog, count, nbSeq_1, max);
        NCountSize = FSE_writeNCount(op, oend-op, norm, max, tableLog);   /* overflow protected */
        if (FSE_isError(NCountSize)) return ERROR(GENERIC);
        op += NCountSize;
        FSE_buildCTable(CTable_LitLength, norm, max, tableLog);
        LLtype = FSE_ENCODING_DYNAMIC;
    }

    /* CTable for Offset codes */
    {   /* create Offset codes */
        size_t i; for (i=0; i<nbSeq; i++) {
            offCodeTable[i] = (BYTE)ZSTD_highbit(offsetTable[i]) + 1;
            if (offsetTable[i]==0) offCodeTable[i]=0;
        }
    }
    max = MaxOff;
    mostFrequent = FSE_countFast(count, &max, offCodeTable, nbSeq);
    if ((mostFrequent == nbSeq) && (nbSeq > 2)) {
        *op++ = offCodeTable[0];
        FSE_buildCTable_rle(CTable_OffsetBits, (BYTE)max);
        Offtype = FSE_ENCODING_RLE;
    } else if ((zc->flagStaticTables) && (nbSeq < MAX_SEQ_FOR_STATIC_FSE)) {
        Offtype = FSE_ENCODING_STATIC;
    } else if ((nbSeq < MIN_SEQ_FOR_DYNAMIC_FSE) || (mostFrequent < (nbSeq >> (Offbits-1)))) {
        FSE_buildCTable_raw(CTable_OffsetBits, Offbits);
        Offtype = FSE_ENCODING_RAW;
    } else {
        size_t NCountSize;
        size_t nbSeq_1 = nbSeq;
        U32 tableLog = FSE_optimalTableLog(OffFSELog, nbSeq, max);
        if (count[offCodeTable[nbSeq-1]]>1) { count[offCodeTable[nbSeq-1]]--; nbSeq_1--; }
        FSE_normalizeCount(norm, tableLog, count, nbSeq_1, max);
        NCountSize = FSE_writeNCount(op, oend-op, norm, max, tableLog);   /* overflow protected */
        if (FSE_isError(NCountSize)) return ERROR(GENERIC);
        op += NCountSize;
        FSE_buildCTable(CTable_OffsetBits, norm, max, tableLog);
        Offtype = FSE_ENCODING_DYNAMIC;
    }

    /* CTable for MatchLengths */
    max = MaxML;
    mostFrequent = FSE_countFast(count, &max, mlTable, nbSeq);
    if ((mostFrequent == nbSeq) && (nbSeq > 2)) {
        *op++ = *mlTable;
        FSE_buildCTable_rle(CTable_MatchLength, (BYTE)max);
        MLtype = FSE_ENCODING_RLE;
    } else if ((zc->flagStaticTables) && (nbSeq < MAX_SEQ_FOR_STATIC_FSE)) {
        MLtype = FSE_ENCODING_STATIC;
    } else if ((nbSeq < MIN_SEQ_FOR_DYNAMIC_FSE) || (mostFrequent < (nbSeq >> (MLbits-1)))) {
        FSE_buildCTable_raw(CTable_MatchLength, MLbits);
        MLtype = FSE_ENCODING_RAW;
    } else {
        size_t NCountSize;
        U32 tableLog = FSE_optimalTableLog(MLFSELog, nbSeq, max);
        FSE_normalizeCount(norm, tableLog, count, nbSeq, max);
        NCountSize = FSE_writeNCount(op, oend-op, norm, max, tableLog);   /* overflow protected */
        if (FSE_isError(NCountSize)) return ERROR(GENERIC);
        op += NCountSize;
        FSE_buildCTable(CTable_MatchLength, norm, max, tableLog);
        MLtype = FSE_ENCODING_DYNAMIC;
    }

    seqHead[0] += (BYTE)((LLtype<<6) + (Offtype<<4) + (MLtype<<2));
    zc->flagStaticTables = 0;

    /* Encoding Sequences */
    {
        size_t streamSize, errorCode;
        BIT_CStream_t blockStream;
        FSE_CState_t stateMatchLength;
        FSE_CState_t stateOffsetBits;
        FSE_CState_t stateLitLength;
        int i;

        errorCode = BIT_initCStream(&blockStream, op, oend-op);
        if (ERR_isError(errorCode)) return ERROR(dstSize_tooSmall);   /* not enough space remaining */

        /* first symbols */
        FSE_initCState2(&stateMatchLength, CTable_MatchLength, mlTable[nbSeq-1]);
        FSE_initCState2(&stateOffsetBits,  CTable_OffsetBits,  offCodeTable[nbSeq-1]);
        FSE_initCState2(&stateLitLength,   CTable_LitLength,   llTable[nbSeq-1]);
        BIT_addBits(&blockStream, offsetTable[nbSeq-1], offCodeTable[nbSeq-1] ? (offCodeTable[nbSeq-1]-1) : 0);
        BIT_flushBits(&blockStream);

        for (i=(int)nbSeq-2; i>=0; i--) {
            BYTE mlCode = mlTable[i];
            U32  offset = offsetTable[i];
            BYTE offCode = offCodeTable[i];                                 /* 32b*/  /* 64b*/
            U32 nbBits = (offCode-1) + (!offCode);
            BYTE litLength = llTable[i];                                    /* (7)*/  /* (7)*/
            FSE_encodeSymbol(&blockStream, &stateMatchLength, mlCode);      /* 17 */  /* 17 */
            if (MEM_32bits()) BIT_flushBits(&blockStream);                  /*  7 */
            FSE_encodeSymbol(&blockStream, &stateLitLength, litLength);     /* 26 */  /* 61 */
            FSE_encodeSymbol(&blockStream, &stateOffsetBits, offCode);      /* 16 */  /* 51 */
            if (MEM_32bits()) BIT_flushBits(&blockStream);                  /*  7 */
            BIT_addBits(&blockStream, offset, nbBits);                      /* 31 */  /* 42 */   /* 24 bits max in 32-bits mode */
            BIT_flushBits(&blockStream);                                    /*  7 */  /*  7 */
        }

        FSE_flushCState(&blockStream, &stateMatchLength);
        FSE_flushCState(&blockStream, &stateOffsetBits);
        FSE_flushCState(&blockStream, &stateLitLength);

        streamSize = BIT_closeCStream(&blockStream);
        if (streamSize==0) return ERROR(dstSize_tooSmall);   /* not enough space */
        op += streamSize;
    }

    /* check compressibility */
_check_compressibility:
    if ((size_t)(op-ostart) >= maxCSize) return 0;

    return op - ostart;
}


/*! ZSTD_storeSeq
    Store a sequence (literal length, literals, offset code and match length code) into seqStore_t
    @offsetCode : distance to match, or 0 == repCode
    @matchCode : matchLength - MINMATCH
*/
MEM_STATIC void ZSTD_storeSeq(seqStore_t* seqStorePtr, size_t litLength, const BYTE* literals, size_t offsetCode, size_t matchCode)
{
#if 0  /* for debug */
    static const BYTE* g_start = NULL;
    if (g_start==NULL) g_start = literals;
    //if (literals - g_start == 8695)
    printf("pos %6u : %3u literals & match %3u bytes at distance %6u \n",
           (U32)(literals - g_start), (U32)litLength, (U32)matchCode+4, (U32)offsetCode);
#endif

    /* copy Literals */
    ZSTD_wildcopy(seqStorePtr->lit, literals, litLength);
    seqStorePtr->lit += litLength;

    /* literal Length */
    if (litLength >= MaxLL) {
        *(seqStorePtr->litLength++) = MaxLL;
        if (litLength<255 + MaxLL) {
            *(seqStorePtr->dumps++) = (BYTE)(litLength - MaxLL);
        } else {
            *(seqStorePtr->dumps++) = 255;
            if (litLength < (1<<15)) {
                MEM_writeLE16(seqStorePtr->dumps, (U16)(litLength<<1));
                seqStorePtr->dumps += 2;
            } else {
                MEM_writeLE32(seqStorePtr->dumps, (U32)((litLength<<1)+1));
                seqStorePtr->dumps += 3;
            }
    }   }
    else *(seqStorePtr->litLength++) = (BYTE)litLength;

    /* match offset */
    *(seqStorePtr->offset++) = (U32)offsetCode;

    /* match Length */
    if (matchCode >= MaxML) {
        *(seqStorePtr->matchLength++) = MaxML;
        if (matchCode < 255+MaxML) {
            *(seqStorePtr->dumps++) = (BYTE)(matchCode - MaxML);
        } else {
            *(seqStorePtr->dumps++) = 255;
            if (matchCode < (1<<15)) {
                MEM_writeLE16(seqStorePtr->dumps, (U16)(matchCode<<1));
                seqStorePtr->dumps += 2;
            } else {
                MEM_writeLE32(seqStorePtr->dumps, (U32)((matchCode<<1)+1));
                seqStorePtr->dumps += 3;
            }
    }   }
    else *(seqStorePtr->matchLength++) = (BYTE)matchCode;
}


/*-*************************************
*  Match length counter
***************************************/
static unsigned ZSTD_NbCommonBytes (register size_t val)
{
    if (MEM_isLittleEndian()) {
        if (MEM_64bits()) {
#       if defined(_MSC_VER) && defined(_WIN64)
            unsigned long r = 0;
            _BitScanForward64( &r, (U64)val );
            return (unsigned)(r>>3);
#       elif defined(__GNUC__) && (__GNUC__ >= 3)
            return (__builtin_ctzll((U64)val) >> 3);
#       else
            static const int DeBruijnBytePos[64] = { 0, 0, 0, 0, 0, 1, 1, 2, 0, 3, 1, 3, 1, 4, 2, 7, 0, 2, 3, 6, 1, 5, 3, 5, 1, 3, 4, 4, 2, 5, 6, 7, 7, 0, 1, 2, 3, 3, 4, 6, 2, 6, 5, 5, 3, 4, 5, 6, 7, 1, 2, 4, 6, 4, 4, 5, 7, 2, 6, 5, 7, 6, 7, 7 };
            return DeBruijnBytePos[((U64)((val & -(long long)val) * 0x0218A392CDABBD3FULL)) >> 58];
#       endif
        } else { /* 32 bits */
#       if defined(_MSC_VER)
            unsigned long r=0;
            _BitScanForward( &r, (U32)val );
            return (unsigned)(r>>3);
#       elif defined(__GNUC__) && (__GNUC__ >= 3)
            return (__builtin_ctz((U32)val) >> 3);
#       else
            static const int DeBruijnBytePos[32] = { 0, 0, 3, 0, 3, 1, 3, 0, 3, 2, 2, 1, 3, 2, 0, 1, 3, 3, 1, 2, 2, 2, 2, 0, 3, 1, 2, 0, 1, 0, 1, 1 };
            return DeBruijnBytePos[((U32)((val & -(S32)val) * 0x077CB531U)) >> 27];
#       endif
        }
    } else {  /* Big Endian CPU */
        if (MEM_64bits()) {
#       if defined(_MSC_VER) && defined(_WIN64)
            unsigned long r = 0;
            _BitScanReverse64( &r, val );
            return (unsigned)(r>>3);
#       elif defined(__GNUC__) && (__GNUC__ >= 3)
            return (__builtin_clzll(val) >> 3);
#       else
            unsigned r;
            const unsigned n32 = sizeof(size_t)*4;   /* calculate this way due to compiler complaining in 32-bits mode */
            if (!(val>>n32)) { r=4; } else { r=0; val>>=n32; }
            if (!(val>>16)) { r+=2; val>>=8; } else { val>>=24; }
            r += (!val);
            return r;
#       endif
        } else { /* 32 bits */
#       if defined(_MSC_VER)
            unsigned long r = 0;
            _BitScanReverse( &r, (unsigned long)val );
            return (unsigned)(r>>3);
#       elif defined(__GNUC__) && (__GNUC__ >= 3)
            return (__builtin_clz((U32)val) >> 3);
#       else
            unsigned r;
            if (!(val>>16)) { r=2; val>>=8; } else { r=0; val>>=24; }
            r += (!val);
            return r;
#       endif
    }   }
}


static size_t ZSTD_count(const BYTE* pIn, const BYTE* pMatch, const BYTE* pInLimit)
{
    const BYTE* const pStart = pIn;

    while ((pIn<pInLimit-(sizeof(size_t)-1))) {
        size_t diff = MEM_readST(pMatch) ^ MEM_readST(pIn);
        if (!diff) { pIn+=sizeof(size_t); pMatch+=sizeof(size_t); continue; }
        pIn += ZSTD_NbCommonBytes(diff);
        return (size_t)(pIn - pStart);
    }
    if (MEM_64bits()) if ((pIn<(pInLimit-3)) && (MEM_read32(pMatch) == MEM_read32(pIn))) { pIn+=4; pMatch+=4; }
    if ((pIn<(pInLimit-1)) && (MEM_read16(pMatch) == MEM_read16(pIn))) { pIn+=2; pMatch+=2; }
    if ((pIn<pInLimit) && (*pMatch == *pIn)) pIn++;
    return (size_t)(pIn - pStart);
}

/** ZSTD_count_2segments() :
*   can count match length with `ip` & `match` in 2 different segments.
*   convention : on reaching mEnd, match count continue starting from iStart
*/
static size_t ZSTD_count_2segments(const BYTE* ip, const BYTE* match, const BYTE* iEnd, const BYTE* mEnd, const BYTE* iStart)
{
    size_t matchLength;
    const BYTE* vEnd = ip + (mEnd - match);
    if (vEnd > iEnd) vEnd = iEnd;
    matchLength = ZSTD_count(ip, match, vEnd);
    if (match + matchLength == mEnd)
        matchLength += ZSTD_count(ip+matchLength, iStart, iEnd);
    return matchLength;
}


/*-*************************************
*  Hashes
***************************************/
static const U32 prime4bytes = 2654435761U;
static U32    ZSTD_hash4(U32 u, U32 h) { return (u * prime4bytes) >> (32-h) ; }
static size_t ZSTD_hash4Ptr(const void* ptr, U32 h) { return ZSTD_hash4(MEM_read32(ptr), h); }

static const U64 prime5bytes = 889523592379ULL;
static size_t ZSTD_hash5(U64 u, U32 h) { return (size_t)(((u  << (64-40)) * prime5bytes) >> (64-h)) ; }
static size_t ZSTD_hash5Ptr(const void* p, U32 h) { return ZSTD_hash5(MEM_readLE64(p), h); }

static const U64 prime6bytes = 227718039650203ULL;
static size_t ZSTD_hash6(U64 u, U32 h) { return (size_t)(((u  << (64-48)) * prime6bytes) >> (64-h)) ; }
static size_t ZSTD_hash6Ptr(const void* p, U32 h) { return ZSTD_hash6(MEM_readLE64(p), h); }

static const U64 prime7bytes = 58295818150454627ULL;
static size_t ZSTD_hash7(U64 u, U32 h) { return (size_t)(((u  << (64-56)) * prime7bytes) >> (64-h)) ; }
static size_t ZSTD_hash7Ptr(const void* p, U32 h) { return ZSTD_hash7(MEM_readLE64(p), h); }

static size_t ZSTD_hashPtr(const void* p, U32 hBits, U32 mls)
{
    switch(mls)
    {
    default:
    case 4: return ZSTD_hash4Ptr(p, hBits);
    case 5: return ZSTD_hash5Ptr(p, hBits);
    case 6: return ZSTD_hash6Ptr(p, hBits);
    case 7: return ZSTD_hash7Ptr(p, hBits);
    }
}


/*-*************************************
*  Fast Scan
***************************************/
#define FILLHASHSTEP 3
static void ZSTD_fillHashTable (ZSTD_CCtx* zc, const void* end, const U32 mls)
{
    U32* const hashTable = zc->hashTable;
    const U32 hBits = zc->params.hashLog;
    const BYTE* const base = zc->base;
    const BYTE* ip = base + zc->nextToUpdate;
    const BYTE* const iend = ((const BYTE*)end) - 8;

    while(ip <= iend) {
        hashTable[ZSTD_hashPtr(ip, hBits, mls)] = (U32)(ip - base);
        ip += FILLHASHSTEP;
    }
}


FORCE_INLINE
void ZSTD_compressBlock_fast_generic(ZSTD_CCtx* zc,
                                 const void* src, size_t srcSize,
                                 const U32 mls)
{
    U32* const hashTable = zc->hashTable;
    const U32 hBits = zc->params.hashLog;
    seqStore_t* seqStorePtr = &(zc->seqStore);
    const BYTE* const base = zc->base;
    const BYTE* const istart = (const BYTE*)src;
    const BYTE* ip = istart;
    const BYTE* anchor = istart;
    const U32 lowIndex = zc->dictLimit;
    const BYTE* const lowest = base + lowIndex;
    const BYTE* const iend = istart + srcSize;
    const BYTE* const ilimit = iend - 8;

    size_t offset_2=REPCODE_STARTVALUE, offset_1=REPCODE_STARTVALUE;


    /* init */
    ZSTD_resetSeqStore(seqStorePtr);
    if (ip < lowest+REPCODE_STARTVALUE) ip = lowest+REPCODE_STARTVALUE;

    /* Main Search Loop */
    while (ip < ilimit) {  /* < instead of <=, because repcode check at (ip+1) */
        size_t mlCode;
        size_t offset;
        const size_t h = ZSTD_hashPtr(ip, hBits, mls);
        const U32 matchIndex = hashTable[h];
        const BYTE* match = base + matchIndex;
        const U32 current = (U32)(ip-base);
        hashTable[h] = current;   /* update hash table */

        if (MEM_read32(ip+1-offset_1) == MEM_read32(ip+1)) {   /* note : by construction, offset_1 <= current */
            mlCode = ZSTD_count(ip+1+MINMATCH, ip+1+MINMATCH-offset_1, iend);
            ip++;
            offset = 0;
        } else {
            if ( (matchIndex <= lowIndex) ||
                 (MEM_read32(match) != MEM_read32(ip)) ) {
                ip += ((ip-anchor) >> g_searchStrength) + 1;
                continue;
            }
            mlCode = ZSTD_count(ip+MINMATCH, match+MINMATCH, iend);
            offset = ip-match;
            while ((ip>anchor) && (match>lowest) && (ip[-1] == match[-1])) { ip--; match--; mlCode++; }  /* catch up */
            offset_2 = offset_1;
            offset_1 = offset;
        }

        /* match found */
        ZSTD_storeSeq(seqStorePtr, ip-anchor, anchor, offset, mlCode);
        ip += mlCode + MINMATCH;
        anchor = ip;

        if (ip <= ilimit) {
            /* Fill Table */
            hashTable[ZSTD_hashPtr(base+current+2, hBits, mls)] = current+2;  /* here because current+2 could be > iend-8 */
            hashTable[ZSTD_hashPtr(ip-2, hBits, mls)] = (U32)(ip-2-base);
            /* check immediate repcode */
            while ( (ip <= ilimit)
                 && (MEM_read32(ip) == MEM_read32(ip - offset_2)) ) {
                /* store sequence */
                size_t rlCode = ZSTD_count(ip+MINMATCH, ip+MINMATCH-offset_2, iend);
                size_t tmpOff = offset_2; offset_2 = offset_1; offset_1 = tmpOff;   /* swap offset_2 <=> offset_1 */
                hashTable[ZSTD_hashPtr(ip, hBits, mls)] = (U32)(ip-base);
                ZSTD_storeSeq(seqStorePtr, 0, anchor, 0, rlCode);
                ip += rlCode+MINMATCH;
                anchor = ip;
                continue;   /* faster when present ... (?) */
    }   }   }

    {   /* Last Literals */
        size_t lastLLSize = iend - anchor;
        memcpy(seqStorePtr->lit, anchor, lastLLSize);
        seqStorePtr->lit += lastLLSize;
    }
}


static void ZSTD_compressBlock_fast(ZSTD_CCtx* ctx,
                       const void* src, size_t srcSize)
{
    const U32 mls = ctx->params.searchLength;
    switch(mls)
    {
    default:
    case 4 :
        ZSTD_compressBlock_fast_generic(ctx, src, srcSize, 4); return;
    case 5 :
        ZSTD_compressBlock_fast_generic(ctx, src, srcSize, 5); return;
    case 6 :
        ZSTD_compressBlock_fast_generic(ctx, src, srcSize, 6); return;
    case 7 :
        ZSTD_compressBlock_fast_generic(ctx, src, srcSize, 7); return;
    }
}


static void ZSTD_compressBlock_fast_extDict_generic(ZSTD_CCtx* ctx,
                                 const void* src, size_t srcSize,
                                 const U32 mls)
{
    U32* hashTable = ctx->hashTable;
    const U32 hBits = ctx->params.hashLog;
    seqStore_t* seqStorePtr = &(ctx->seqStore);
    const BYTE* const base = ctx->base;
    const BYTE* const dictBase = ctx->dictBase;
    const BYTE* const istart = (const BYTE*)src;
    const BYTE* ip = istart;
    const BYTE* anchor = istart;
    const U32   lowLimit = ctx->lowLimit;
    const BYTE* const dictStart = dictBase + lowLimit;
    const U32   dictLimit = ctx->dictLimit;
    const BYTE* const lowPrefixPtr = base + dictLimit;
    const BYTE* const dictEnd = dictBase + dictLimit;
    const BYTE* const iend = istart + srcSize;
    const BYTE* const ilimit = iend - 8;

    U32 offset_2=REPCODE_STARTVALUE, offset_1=REPCODE_STARTVALUE;


    /* init */
    ZSTD_resetSeqStore(seqStorePtr);
    /* skip first position to avoid read overflow during repcode match check */
    hashTable[ZSTD_hashPtr(ip+0, hBits, mls)] = (U32)(ip-base+0);
    ip += REPCODE_STARTVALUE;

    /* Main Search Loop */
    while (ip < ilimit) {  /* < instead of <=, because (ip+1) */
        const size_t h = ZSTD_hashPtr(ip, hBits, mls);
        const U32 matchIndex = hashTable[h];
        const BYTE* matchBase = matchIndex < dictLimit ? dictBase : base;
        const BYTE* match = matchBase + matchIndex;
        const U32 current = (U32)(ip-base);
        const U32 repIndex = current + 1 - offset_1;
        const BYTE* repBase = repIndex < dictLimit ? dictBase : base;
        const BYTE* repMatch = repBase + repIndex;
        size_t mlCode;
        U32 offset;
        hashTable[h] = current;   /* update hash table */

        if ( ((repIndex <= dictLimit-4) || (repIndex >= dictLimit))
          && (MEM_read32(repMatch) == MEM_read32(ip+1)) ) {
            const BYTE* repMatchEnd = repIndex < dictLimit ? dictEnd : iend;
            mlCode = ZSTD_count_2segments(ip+1+MINMATCH, repMatch+MINMATCH, iend, repMatchEnd, lowPrefixPtr);
            ip++;
            offset = 0;
        } else {
            if ( (matchIndex < lowLimit) ||
                 (MEM_read32(match) != MEM_read32(ip)) )
            { ip += ((ip-anchor) >> g_searchStrength) + 1; continue; }
            {
                const BYTE* matchEnd = matchIndex < dictLimit ? dictEnd : iend;
                const BYTE* lowMatchPtr = matchIndex < dictLimit ? dictStart : lowPrefixPtr;
                mlCode = ZSTD_count_2segments(ip+MINMATCH, match+MINMATCH, iend, matchEnd, lowPrefixPtr);
                while ((ip>anchor) && (match>lowMatchPtr) && (ip[-1] == match[-1])) { ip--; match--; mlCode++; }   /* catch up */
                offset = current - matchIndex;
                offset_2 = offset_1;
                offset_1 = offset;
        }   }

        /* found a match : store it */
        ZSTD_storeSeq(seqStorePtr, ip-anchor, anchor, offset, mlCode);
        ip += mlCode + MINMATCH;
        anchor = ip;

        if (ip <= ilimit) {
            /* Fill Table */
			hashTable[ZSTD_hashPtr(base+current+2, hBits, mls)] = current+2;
            hashTable[ZSTD_hashPtr(ip-2, hBits, mls)] = (U32)(ip-2-base);
            /* check immediate repcode */
            while (ip <= ilimit) {
                U32 current2 = (U32)(ip-base);
                const U32 repIndex2 = current2 - offset_2;
                const BYTE* repMatch2 = repIndex2 < dictLimit ? dictBase + repIndex2 : base + repIndex2;
                if ( ((repIndex2 <= dictLimit-4) || (repIndex2 >= dictLimit))
                  && (MEM_read32(repMatch2) == MEM_read32(ip)) ) {
                    const BYTE* const repEnd2 = repIndex2 < dictLimit ? dictEnd : iend;
                    size_t repLength2 = ZSTD_count_2segments(ip+MINMATCH, repMatch2+MINMATCH, iend, repEnd2, lowPrefixPtr);
                    U32 tmpOffset = offset_2; offset_2 = offset_1; offset_1 = tmpOffset;   /* swap offset_2 <=> offset_1 */
                    ZSTD_storeSeq(seqStorePtr, 0, anchor, 0, repLength2);
                    hashTable[ZSTD_hashPtr(ip, hBits, mls)] = current2;
                    ip += repLength2+MINMATCH;
                    anchor = ip;
                    continue;
                }
                break;
    }   }   }

    /* Last Literals */
    {
        size_t lastLLSize = iend - anchor;
        memcpy(seqStorePtr->lit, anchor, lastLLSize);
        seqStorePtr->lit += lastLLSize;
    }
}


static void ZSTD_compressBlock_fast_extDict(ZSTD_CCtx* ctx,
                         const void* src, size_t srcSize)
{
    const U32 mls = ctx->params.searchLength;
    switch(mls)
    {
    default:
    case 4 :
        ZSTD_compressBlock_fast_extDict_generic(ctx, src, srcSize, 4); return;
    case 5 :
        ZSTD_compressBlock_fast_extDict_generic(ctx, src, srcSize, 5); return;
    case 6 :
        ZSTD_compressBlock_fast_extDict_generic(ctx, src, srcSize, 6); return;
    case 7 :
        ZSTD_compressBlock_fast_extDict_generic(ctx, src, srcSize, 7); return;
    }
}


/*-*************************************
*  Binary Tree search
***************************************/
/** ZSTD_insertBt1() : add one or multiple positions to tree.
*   ip : assumed <= iend-8 .
*   @return : nb of positions added */
static U32 ZSTD_insertBt1(ZSTD_CCtx* zc, const BYTE* const ip, const U32 mls, const BYTE* const iend, U32 nbCompares,
                          U32 extDict)
{
    U32* const hashTable = zc->hashTable;
    const U32 hashLog = zc->params.hashLog;
    const size_t h  = ZSTD_hashPtr(ip, hashLog, mls);
    U32* const bt   = zc->contentTable;
    const U32 btLog = zc->params.contentLog - 1;
    const U32 btMask= (1 << btLog) - 1;
    U32 matchIndex  = hashTable[h];
    size_t commonLengthSmaller=0, commonLengthLarger=0;
    const BYTE* const base = zc->base;
    const BYTE* const dictBase = zc->dictBase;
    const U32 dictLimit = zc->dictLimit;
    const BYTE* const dictEnd = dictBase + dictLimit;
    const BYTE* const prefixStart = base + dictLimit;
    const BYTE* match = base + matchIndex;
    const U32 current = (U32)(ip-base);
    const U32 btLow = btMask >= current ? 0 : current - btMask;
    U32* smallerPtr = bt + 2*(current&btMask);
    U32* largerPtr  = smallerPtr + 1;
    U32 dummy32;   /* to be nullified at the end */
    const U32 windowLow = zc->lowLimit;
    U32 matchEndIdx = current+8;
    size_t bestLength = 8;
    U32 predictedSmall = *(bt + 2*((current-1)&btMask) + 0);
    U32 predictedLarge = *(bt + 2*((current-1)&btMask) + 1);
    predictedSmall += (predictedSmall>0);
    predictedLarge += (predictedLarge>0);

    hashTable[h] = current;   /* Update Hash Table */

    while (nbCompares-- && (matchIndex > windowLow)) {
        U32* nextPtr = bt + 2*(matchIndex & btMask);
        size_t matchLength = MIN(commonLengthSmaller, commonLengthLarger);   /* guaranteed minimum nb of common bytes */
#if 1   /* note : can create issues when hlog small <= 11 */
        const U32* predictPtr = bt + 2*((matchIndex-1) & btMask);   /* written this way, as bt is a roll buffer */
        if (matchIndex == predictedSmall) {
            /* no need to check length, result known */
            *smallerPtr = matchIndex;
            if (matchIndex <= btLow) { smallerPtr=&dummy32; break; }   /* beyond tree size, stop the search */
            smallerPtr = nextPtr+1;               /* new "smaller" => larger of match */
            matchIndex = nextPtr[1];              /* new matchIndex larger than previous (closer to current) */
            predictedSmall = predictPtr[1] + (predictPtr[1]>0);
            continue;
        }
        if (matchIndex == predictedLarge) {
            *largerPtr = matchIndex;
            if (matchIndex <= btLow) { largerPtr=&dummy32; break; }   /* beyond tree size, stop the search */
            largerPtr = nextPtr;
            matchIndex = nextPtr[0];
            predictedLarge = predictPtr[0] + (predictPtr[0]>0);
            continue;
        }
#endif
        if ((!extDict) || (matchIndex+matchLength >= dictLimit)) {
            match = base + matchIndex;
            if (match[matchLength] == ip[matchLength])
                matchLength += ZSTD_count(ip+matchLength+1, match+matchLength+1, iend) +1;
        } else {
            match = dictBase + matchIndex;
            matchLength += ZSTD_count_2segments(ip+matchLength, match+matchLength, iend, dictEnd, prefixStart);
            if (matchIndex+matchLength >= dictLimit)
				match = base + matchIndex;   /* to prepare for next usage of match[matchLength] */
        }

        if (matchLength > bestLength) {
            bestLength = matchLength;
            if (matchLength > matchEndIdx - matchIndex)
                matchEndIdx = matchIndex + (U32)matchLength;
        }

        if (ip+matchLength == iend)   /* equal : no way to know if inf or sup */
            break;   /* drop , to guarantee consistency ; miss a bit of compression, but other solutions can corrupt the tree */

        if (match[matchLength] < ip[matchLength]) {  /* necessarily within correct buffer */
            /* match is smaller than current */
            *smallerPtr = matchIndex;             /* update smaller idx */
            commonLengthSmaller = matchLength;    /* all smaller will now have at least this guaranteed common length */
            if (matchIndex <= btLow) { smallerPtr=&dummy32; break; }   /* beyond tree size, stop the search */
            smallerPtr = nextPtr+1;               /* new "smaller" => larger of match */
            matchIndex = nextPtr[1];              /* new matchIndex larger than previous (closer to current) */
        } else {
            /* match is larger than current */
            *largerPtr = matchIndex;
            commonLengthLarger = matchLength;
            if (matchIndex <= btLow) { largerPtr=&dummy32; break; }   /* beyond tree size, stop the search */
            largerPtr = nextPtr;
            matchIndex = nextPtr[0];
    }   }

    *smallerPtr = *largerPtr = 0;
    if (bestLength > 384) return MIN(192, (U32)(bestLength - 384));
    if (matchEndIdx > current + 8) return matchEndIdx - current - 8;
    return 1;
}


static size_t ZSTD_insertBtAndFindBestMatch (
                        ZSTD_CCtx* zc,
                        const BYTE* const ip, const BYTE* const iend,
                        size_t* offsetPtr,
                        U32 nbCompares, const U32 mls,
                        U32 extDict)
{
    U32* const hashTable = zc->hashTable;
    const U32 hashLog = zc->params.hashLog;
    const size_t h  = ZSTD_hashPtr(ip, hashLog, mls);
    U32* const bt   = zc->contentTable;
    const U32 btLog = zc->params.contentLog - 1;
    const U32 btMask= (1 << btLog) - 1;
    U32 matchIndex  = hashTable[h];
    size_t commonLengthSmaller=0, commonLengthLarger=0;
    const BYTE* const base = zc->base;
    const BYTE* const dictBase = zc->dictBase;
    const U32 dictLimit = zc->dictLimit;
    const BYTE* const dictEnd = dictBase + dictLimit;
    const BYTE* const prefixStart = base + dictLimit;
    const U32 current = (U32)(ip-base);
    const U32 btLow = btMask >= current ? 0 : current - btMask;
    const U32 windowLow = zc->lowLimit;
    U32* smallerPtr = bt + 2*(current&btMask);
    U32* largerPtr  = bt + 2*(current&btMask) + 1;
    size_t bestLength = 0;
    U32 matchEndIdx = current+8;
    U32 dummy32;   /* to be nullified at the end */

    hashTable[h] = current;   /* Update Hash Table */

    while (nbCompares-- && (matchIndex > windowLow)) {
        U32* nextPtr = bt + 2*(matchIndex & btMask);
        size_t matchLength = MIN(commonLengthSmaller, commonLengthLarger);   /* guaranteed minimum nb of common bytes */
        const BYTE* match;

        if ((!extDict) || (matchIndex+matchLength >= dictLimit)) {
            match = base + matchIndex;
            if (match[matchLength] == ip[matchLength])
                matchLength += ZSTD_count(ip+matchLength+1, match+matchLength+1, iend) +1;
        } else {
            match = dictBase + matchIndex;
            matchLength += ZSTD_count_2segments(ip+matchLength, match+matchLength, iend, dictEnd, prefixStart);
            if (matchIndex+matchLength >= dictLimit)
				match = base + matchIndex;   /* to prepare for next usage of match[matchLength] */
        }

        if (matchLength > bestLength) {
            if (matchLength > matchEndIdx - matchIndex)
                matchEndIdx = matchIndex + (U32)matchLength;
            if ( (4*(int)(matchLength-bestLength)) > (int)(ZSTD_highbit(current-matchIndex+1) - ZSTD_highbit((U32)offsetPtr[0]+1)) )
                bestLength = matchLength, *offsetPtr = current - matchIndex;
            if (ip+matchLength == iend)   /* equal : no way to know if inf or sup */
                break;   /* drop, to guarantee consistency (miss a little bit of compression) */
        }

        if (match[matchLength] < ip[matchLength]) {
            /* match is smaller than current */
            *smallerPtr = matchIndex;             /* update smaller idx */
            commonLengthSmaller = matchLength;    /* all smaller will now have at least this guaranteed common length */
            if (matchIndex <= btLow) { smallerPtr=&dummy32; break; }   /* beyond tree size, stop the search */
            smallerPtr = nextPtr+1;               /* new "smaller" => larger of match */
            matchIndex = nextPtr[1];              /* new matchIndex larger than previous (closer to current) */
        } else {
            /* match is larger than current */
            *largerPtr = matchIndex;
            commonLengthLarger = matchLength;
            if (matchIndex <= btLow) { largerPtr=&dummy32; break; }   /* beyond tree size, stop the search */
            largerPtr = nextPtr;
            matchIndex = nextPtr[0];
    }   }

    *smallerPtr = *largerPtr = 0;

    zc->nextToUpdate = (matchEndIdx > current + 8) ? matchEndIdx - 8 : current+1;
    return bestLength;
}


static void ZSTD_updateTree(ZSTD_CCtx* zc, const BYTE* const ip, const BYTE* const iend, const U32 nbCompares, const U32 mls)
{
    const BYTE* const base = zc->base;
    const U32 target = (U32)(ip - base);
    U32 idx = zc->nextToUpdate;

    while(idx < target)
        idx += ZSTD_insertBt1(zc, base+idx, mls, iend, nbCompares, 0);
}

/** Tree updater, providing best match */
static size_t ZSTD_BtFindBestMatch (
                        ZSTD_CCtx* zc,
                        const BYTE* const ip, const BYTE* const iLimit,
                        size_t* offsetPtr,
                        const U32 maxNbAttempts, const U32 mls)
{
    if (ip < zc->base + zc->nextToUpdate) return 0;   /* skipped area */
    ZSTD_updateTree(zc, ip, iLimit, maxNbAttempts, mls);
    return ZSTD_insertBtAndFindBestMatch(zc, ip, iLimit, offsetPtr, maxNbAttempts, mls, 0);
}


static size_t ZSTD_BtFindBestMatch_selectMLS (
                        ZSTD_CCtx* zc,   /* Index table will be updated */
                        const BYTE* ip, const BYTE* const iLimit,
                        size_t* offsetPtr,
                        const U32 maxNbAttempts, const U32 matchLengthSearch)
{
    switch(matchLengthSearch)
    {
    default :
    case 4 : return ZSTD_BtFindBestMatch(zc, ip, iLimit, offsetPtr, maxNbAttempts, 4);
    case 5 : return ZSTD_BtFindBestMatch(zc, ip, iLimit, offsetPtr, maxNbAttempts, 5);
    case 6 : return ZSTD_BtFindBestMatch(zc, ip, iLimit, offsetPtr, maxNbAttempts, 6);
    }
}


static void ZSTD_updateTree_extDict(ZSTD_CCtx* zc, const BYTE* const ip, const BYTE* const iend, const U32 nbCompares, const U32 mls)
{
    const BYTE* const base = zc->base;
    const U32 target = (U32)(ip - base);
    U32 idx = zc->nextToUpdate;

    while (idx < target) idx += ZSTD_insertBt1(zc, base+idx, mls, iend, nbCompares, 1);
}


/** Tree updater, providing best match */
static size_t ZSTD_BtFindBestMatch_extDict (
                        ZSTD_CCtx* zc,
                        const BYTE* const ip, const BYTE* const iLimit,
                        size_t* offsetPtr,
                        const U32 maxNbAttempts, const U32 mls)
{
    if (ip < zc->base + zc->nextToUpdate) return 0;   /* skipped area */
    ZSTD_updateTree_extDict(zc, ip, iLimit, maxNbAttempts, mls);
    return ZSTD_insertBtAndFindBestMatch(zc, ip, iLimit, offsetPtr, maxNbAttempts, mls, 1);
}


static size_t ZSTD_BtFindBestMatch_selectMLS_extDict (
                        ZSTD_CCtx* zc,   /* Index table will be updated */
                        const BYTE* ip, const BYTE* const iLimit,
                        size_t* offsetPtr,
                        const U32 maxNbAttempts, const U32 matchLengthSearch)
{
    switch(matchLengthSearch)
    {
    default :
    case 4 : return ZSTD_BtFindBestMatch_extDict(zc, ip, iLimit, offsetPtr, maxNbAttempts, 4);
    case 5 : return ZSTD_BtFindBestMatch_extDict(zc, ip, iLimit, offsetPtr, maxNbAttempts, 5);
    case 6 : return ZSTD_BtFindBestMatch_extDict(zc, ip, iLimit, offsetPtr, maxNbAttempts, 6);
    }
}


/* ***********************
*  Hash Chain
*************************/

#define NEXT_IN_CHAIN(d, mask)   chainTable[(d) & mask]

/* Update chains up to ip (excluded)
   Assumption : always within prefix (ie. not within extDict) */
FORCE_INLINE
U32 ZSTD_insertAndFindFirstIndex (ZSTD_CCtx* zc, const BYTE* ip, U32 mls)
{
    U32* const hashTable  = zc->hashTable;
    const U32 hashLog = zc->params.hashLog;
    U32* const chainTable = zc->contentTable;
    const U32 chainMask = (1 << zc->params.contentLog) - 1;
    const BYTE* const base = zc->base;
    const U32 target = (U32)(ip - base);
    U32 idx = zc->nextToUpdate;

    while(idx < target) {
        size_t h = ZSTD_hashPtr(base+idx, hashLog, mls);
        NEXT_IN_CHAIN(idx, chainMask) = hashTable[h];
        hashTable[h] = idx;
        idx++;
    }

    zc->nextToUpdate = target;
    return hashTable[ZSTD_hashPtr(ip, hashLog, mls)];
}


FORCE_INLINE /* inlining is important to hardwire a hot branch (template emulation) */
size_t ZSTD_HcFindBestMatch_generic (
                        ZSTD_CCtx* zc,   /* Index table will be updated */
                        const BYTE* const ip, const BYTE* const iLimit,
                        size_t* offsetPtr,
                        const U32 maxNbAttempts, const U32 mls, const U32 extDict)
{
    U32* const chainTable = zc->contentTable;
    const U32 chainSize = (1 << zc->params.contentLog);
    const U32 chainMask = chainSize-1;
    const BYTE* const base = zc->base;
    const BYTE* const dictBase = zc->dictBase;
    const U32 dictLimit = zc->dictLimit;
    const BYTE* const prefixStart = base + dictLimit;
    const BYTE* const dictEnd = dictBase + dictLimit;
    const U32 lowLimit = zc->lowLimit;
    const U32 current = (U32)(ip-base);
    const U32 minChain = current > chainSize ? current - chainSize : 0;
    U32 matchIndex;
    const BYTE* match;
    int nbAttempts=maxNbAttempts;
    size_t ml=MINMATCH-1;

    /* HC4 match finder */
    matchIndex = ZSTD_insertAndFindFirstIndex (zc, ip, mls);

    while ((matchIndex>lowLimit) && (nbAttempts)) {
        size_t currentMl=0;
        nbAttempts--;
        if ((!extDict) || matchIndex >= dictLimit) {
            match = base + matchIndex;
            if (match[ml] == ip[ml])   /* potentially better */
                currentMl = ZSTD_count(ip, match, iLimit);
        } else {
            match = dictBase + matchIndex;
            if (MEM_read32(match) == MEM_read32(ip))   /* assumption : matchIndex <= dictLimit-4 (by table construction) */
                currentMl = ZSTD_count_2segments(ip+MINMATCH, match+MINMATCH, iLimit, dictEnd, prefixStart) + MINMATCH;
        }

        /* save best solution */
        if (currentMl > ml) { ml = currentMl; *offsetPtr = current - matchIndex; if (ip+currentMl == iLimit) break; /* best possible, and avoid read overflow*/ }

        if (matchIndex <= minChain) break;
        matchIndex = NEXT_IN_CHAIN(matchIndex, chainMask);
    }

    return ml;
}


FORCE_INLINE size_t ZSTD_HcFindBestMatch_selectMLS (
                        ZSTD_CCtx* zc,
                        const BYTE* ip, const BYTE* const iLimit,
                        size_t* offsetPtr,
                        const U32 maxNbAttempts, const U32 matchLengthSearch)
{
    switch(matchLengthSearch)
    {
    default :
    case 4 : return ZSTD_HcFindBestMatch_generic(zc, ip, iLimit, offsetPtr, maxNbAttempts, 4, 0);
    case 5 : return ZSTD_HcFindBestMatch_generic(zc, ip, iLimit, offsetPtr, maxNbAttempts, 5, 0);
    case 6 : return ZSTD_HcFindBestMatch_generic(zc, ip, iLimit, offsetPtr, maxNbAttempts, 6, 0);
    }
}


FORCE_INLINE size_t ZSTD_HcFindBestMatch_extDict_selectMLS (
                        ZSTD_CCtx* zc,
                        const BYTE* ip, const BYTE* const iLimit,
                        size_t* offsetPtr,
                        const U32 maxNbAttempts, const U32 matchLengthSearch)
{
    switch(matchLengthSearch)
    {
    default :
    case 4 : return ZSTD_HcFindBestMatch_generic(zc, ip, iLimit, offsetPtr, maxNbAttempts, 4, 1);
    case 5 : return ZSTD_HcFindBestMatch_generic(zc, ip, iLimit, offsetPtr, maxNbAttempts, 5, 1);
    case 6 : return ZSTD_HcFindBestMatch_generic(zc, ip, iLimit, offsetPtr, maxNbAttempts, 6, 1);
    }
}


/* *******************************
*  Common parser - lazy strategy
*********************************/
FORCE_INLINE
void ZSTD_compressBlock_lazy_generic(ZSTD_CCtx* ctx,
                                     const void* src, size_t srcSize,
                                     const U32 searchMethod, const U32 depth)
{
    seqStore_t* seqStorePtr = &(ctx->seqStore);
    const BYTE* const istart = (const BYTE*)src;
    const BYTE* ip = istart;
    const BYTE* anchor = istart;
    const BYTE* const iend = istart + srcSize;
    const BYTE* const ilimit = iend - 8;
    const BYTE* const base = ctx->base + ctx->dictLimit;

    size_t offset_2=REPCODE_STARTVALUE, offset_1=REPCODE_STARTVALUE;
    const U32 maxSearches = 1 << ctx->params.searchLog;
    const U32 mls = ctx->params.searchLength;

    typedef size_t (*searchMax_f)(ZSTD_CCtx* zc, const BYTE* ip, const BYTE* iLimit,
                        size_t* offsetPtr,
                        U32 maxNbAttempts, U32 matchLengthSearch);
    searchMax_f searchMax = searchMethod ? ZSTD_BtFindBestMatch_selectMLS : ZSTD_HcFindBestMatch_selectMLS;

    /* init */
    ZSTD_resetSeqStore(seqStorePtr);
    if ((ip-base) < REPCODE_STARTVALUE) ip = base + REPCODE_STARTVALUE;

    /* Match Loop */
    while (ip < ilimit) {
        size_t matchLength=0;
        size_t offset=0;
        const BYTE* start=ip+1;

        /* check repCode */
        if (MEM_read32(ip+1) == MEM_read32(ip+1 - offset_1)) {
            /* repcode : we take it */
            matchLength = ZSTD_count(ip+1+MINMATCH, ip+1+MINMATCH-offset_1, iend) + MINMATCH;
            if (depth==0) goto _storeSequence;
        }

        {
            /* first search (depth 0) */
            size_t offsetFound = 99999999;
            size_t ml2 = searchMax(ctx, ip, iend, &offsetFound, maxSearches, mls);
            if (ml2 > matchLength)
                matchLength = ml2, start = ip, offset=offsetFound;
        }

        if (matchLength < MINMATCH) {
            ip += ((ip-anchor) >> g_searchStrength) + 1;   /* jump faster over incompressible sections */
            continue;
        }

        /* let's try to find a better solution */
        if (depth>=1)
        while (ip<ilimit) {
            ip ++;
            if ((offset) && (MEM_read32(ip) == MEM_read32(ip - offset_1))) {
                size_t mlRep = ZSTD_count(ip+MINMATCH, ip+MINMATCH-offset_1, iend) + MINMATCH;
                int gain2 = (int)(mlRep * 3);
                int gain1 = (int)(matchLength*3 - ZSTD_highbit((U32)offset+1) + 1);
                if ((mlRep >= MINMATCH) && (gain2 > gain1))
                    matchLength = mlRep, offset = 0, start = ip;
            }
            {
                size_t offset2=999999;
                size_t ml2 = searchMax(ctx, ip, iend, &offset2, maxSearches, mls);
                int gain2 = (int)(ml2*4 - ZSTD_highbit((U32)offset2+1));   /* raw approx */
                int gain1 = (int)(matchLength*4 - ZSTD_highbit((U32)offset+1) + 4);
                if ((ml2 >= MINMATCH) && (gain2 > gain1)) {
                    matchLength = ml2, offset = offset2, start = ip;
                    continue;   /* search a better one */
            }   }

            /* let's find an even better one */
            if ((depth==2) && (ip<ilimit)) {
                ip ++;
                if ((offset) && (MEM_read32(ip) == MEM_read32(ip - offset_1))) {
                    size_t ml2 = ZSTD_count(ip+MINMATCH, ip+MINMATCH-offset_1, iend) + MINMATCH;
                    int gain2 = (int)(ml2 * 4);
                    int gain1 = (int)(matchLength*4 - ZSTD_highbit((U32)offset+1) + 1);
                    if ((ml2 >= MINMATCH) && (gain2 > gain1))
                        matchLength = ml2, offset = 0, start = ip;
                }
                {
                    size_t offset2=999999;
                    size_t ml2 = searchMax(ctx, ip, iend, &offset2, maxSearches, mls);
                    int gain2 = (int)(ml2*4 - ZSTD_highbit((U32)offset2+1));   /* raw approx */
                    int gain1 = (int)(matchLength*4 - ZSTD_highbit((U32)offset+1) + 7);
                    if ((ml2 >= MINMATCH) && (gain2 > gain1)) {
                        matchLength = ml2, offset = offset2, start = ip;
                        continue;
            }   }   }
            break;  /* nothing found : store previous solution */
        }

        /* catch up */
        if (offset) {
            while ((start>anchor) && (start>base+offset) && (start[-1] == start[-1-offset]))   /* only search for offset within prefix */
                { start--; matchLength++; }
            offset_2 = offset_1; offset_1 = offset;
        }

        /* store sequence */
_storeSequence:
        {
            size_t litLength = start - anchor;
            ZSTD_storeSeq(seqStorePtr, litLength, anchor, offset, matchLength-MINMATCH);
            anchor = ip = start + matchLength;
        }

        /* check immediate repcode */
        while ( (ip <= ilimit)
             && (MEM_read32(ip) == MEM_read32(ip - offset_2)) ) {
            /* store sequence */
            matchLength = ZSTD_count(ip+MINMATCH, ip+MINMATCH-offset_2, iend);
            offset = offset_2;
            offset_2 = offset_1;
            offset_1 = offset;
            ZSTD_storeSeq(seqStorePtr, 0, anchor, 0, matchLength);
            ip += matchLength+MINMATCH;
            anchor = ip;
            continue;   /* faster when present ... (?) */
    }   }

    /* Last Literals */
    {
        size_t lastLLSize = iend - anchor;
        memcpy(seqStorePtr->lit, anchor, lastLLSize);
        seqStorePtr->lit += lastLLSize;
    }
}

#include "zstd_opt.h"

static void ZSTD_compressBlock_opt_bt(ZSTD_CCtx* ctx, const void* src, size_t srcSize)
{
    ZSTD_compressBlock_opt_generic(ctx, src, srcSize, 1, 2);
}

static void ZSTD_compressBlock_opt(ZSTD_CCtx* ctx, const void* src, size_t srcSize)
{
    ZSTD_compressBlock_opt_generic(ctx, src, srcSize, 0, 2);
}

static void ZSTD_compressBlock_btlazy2(ZSTD_CCtx* ctx, const void* src, size_t srcSize)
{
    ZSTD_compressBlock_lazy_generic(ctx, src, srcSize, 1, 2);
}

static void ZSTD_compressBlock_lazy2(ZSTD_CCtx* ctx, const void* src, size_t srcSize)
{
    ZSTD_compressBlock_lazy_generic(ctx, src, srcSize, 0, 2);
}

static void ZSTD_compressBlock_lazy(ZSTD_CCtx* ctx, const void* src, size_t srcSize)
{
    ZSTD_compressBlock_lazy_generic(ctx, src, srcSize, 0, 1);
}

static void ZSTD_compressBlock_greedy(ZSTD_CCtx* ctx, const void* src, size_t srcSize)
{
    ZSTD_compressBlock_lazy_generic(ctx, src, srcSize, 0, 0);
}


FORCE_INLINE
void ZSTD_compressBlock_lazy_extDict_generic(ZSTD_CCtx* ctx,
                                     const void* src, size_t srcSize,
                                     const U32 searchMethod, const U32 depth)
{
    seqStore_t* seqStorePtr = &(ctx->seqStore);
    const BYTE* const istart = (const BYTE*)src;
    const BYTE* ip = istart;
    const BYTE* anchor = istart;
    const BYTE* const iend = istart + srcSize;
    const BYTE* const ilimit = iend - 8;
    const BYTE* const base = ctx->base;
    const U32 dictLimit = ctx->dictLimit;
    const BYTE* const prefixStart = base + dictLimit;
    const BYTE* const dictBase = ctx->dictBase;
    const BYTE* const dictEnd  = dictBase + dictLimit;
    const BYTE* const dictStart  = dictBase + ctx->lowLimit;

    size_t offset_2=REPCODE_STARTVALUE, offset_1=REPCODE_STARTVALUE;
    const U32 maxSearches = 1 << ctx->params.searchLog;
    const U32 mls = ctx->params.searchLength;

    typedef size_t (*searchMax_f)(ZSTD_CCtx* zc, const BYTE* ip, const BYTE* iLimit,
                        size_t* offsetPtr,
                        U32 maxNbAttempts, U32 matchLengthSearch);
    searchMax_f searchMax = searchMethod ? ZSTD_BtFindBestMatch_selectMLS_extDict : ZSTD_HcFindBestMatch_extDict_selectMLS;

    /* init */
    ZSTD_resetSeqStore(seqStorePtr);
    if ((ip - prefixStart) < REPCODE_STARTVALUE) ip += REPCODE_STARTVALUE;

    /* Match Loop */
    while (ip < ilimit) {
        size_t matchLength=0;
        size_t offset=0;
        const BYTE* start=ip+1;
        U32 current = (U32)(ip-base);

        /* check repCode */
        {
            const U32 repIndex = (U32)(current+1 - offset_1);
            const BYTE* const repBase = repIndex < dictLimit ? dictBase : base;
            const BYTE* const repMatch = repBase + repIndex;
            if ((U32)((dictLimit-1) - repIndex) >= 3)   /* intentional overflow */
            if (MEM_read32(ip+1) == MEM_read32(repMatch)) {
                /* repcode detected we should take it */
                const BYTE* const repEnd = repIndex < dictLimit ? dictEnd : iend;
                matchLength = ZSTD_count_2segments(ip+1+MINMATCH, repMatch+MINMATCH, iend, repEnd, prefixStart) + MINMATCH;
                if (depth==0) goto _storeSequence;
        }   }

        {
            /* first search (depth 0) */
            size_t offsetFound = 99999999;
            size_t ml2 = searchMax(ctx, ip, iend, &offsetFound, maxSearches, mls);
            if (ml2 > matchLength)
                matchLength = ml2, start = ip, offset=offsetFound;
        }

         if (matchLength < MINMATCH) {
            ip += ((ip-anchor) >> g_searchStrength) + 1;   /* jump faster over incompressible sections */
            continue;
        }

        /* let's try to find a better solution */
        if (depth>=1)
        while (ip<ilimit) {
            ip ++;
            current++;
            /* check repCode */
            if (offset) {
                const U32 repIndex = (U32)(current - offset_1);
                const BYTE* const repBase = repIndex < dictLimit ? dictBase : base;
                const BYTE* const repMatch = repBase + repIndex;
                if ((U32)((dictLimit-1) - repIndex) >= 3)   /* intentional overflow */
                if (MEM_read32(ip) == MEM_read32(repMatch)) {
                    /* repcode detected */
                    const BYTE* const repEnd = repIndex < dictLimit ? dictEnd : iend;
                    size_t repLength = ZSTD_count_2segments(ip+MINMATCH, repMatch+MINMATCH, iend, repEnd, prefixStart) + MINMATCH;
                    int gain2 = (int)(repLength * 3);
                    int gain1 = (int)(matchLength*3 - ZSTD_highbit((U32)offset+1) + 1);
                    if ((repLength >= MINMATCH) && (gain2 > gain1))
                        matchLength = repLength, offset = 0, start = ip;
            }   }

            /* search match, depth 1 */
            {
                size_t offset2=999999;
                size_t ml2 = searchMax(ctx, ip, iend, &offset2, maxSearches, mls);
                int gain2 = (int)(ml2*4 - ZSTD_highbit((U32)offset2+1));   /* raw approx */
                int gain1 = (int)(matchLength*4 - ZSTD_highbit((U32)offset+1) + 4);
                if ((ml2 >= MINMATCH) && (gain2 > gain1)) {
                    matchLength = ml2, offset = offset2, start = ip;
                    continue;   /* search a better one */
            }   }

            /* let's find an even better one */
            if ((depth==2) && (ip<ilimit)) {
                ip ++;
                current++;
                /* check repCode */
                if (offset) {
                    const U32 repIndex = (U32)(current - offset_1);
                    const BYTE* const repBase = repIndex < dictLimit ? dictBase : base;
                    const BYTE* const repMatch = repBase + repIndex;
                    if ((U32)((dictLimit-1) - repIndex) >= 3)   /* intentional overflow */
                    if (MEM_read32(ip) == MEM_read32(repMatch)) {
                        /* repcode detected */
                        const BYTE* const repEnd = repIndex < dictLimit ? dictEnd : iend;
                        size_t repLength = ZSTD_count_2segments(ip+MINMATCH, repMatch+MINMATCH, iend, repEnd, prefixStart) + MINMATCH;
                        int gain2 = (int)(repLength * 4);
                        int gain1 = (int)(matchLength*4 - ZSTD_highbit((U32)offset+1) + 1);
                        if ((repLength >= MINMATCH) && (gain2 > gain1))
                            matchLength = repLength, offset = 0, start = ip;
                }   }

                /* search match, depth 2 */
                {
                    size_t offset2=999999;
                    size_t ml2 = searchMax(ctx, ip, iend, &offset2, maxSearches, mls);
                    int gain2 = (int)(ml2*4 - ZSTD_highbit((U32)offset2+1));   /* raw approx */
                    int gain1 = (int)(matchLength*4 - ZSTD_highbit((U32)offset+1) + 7);
                    if ((ml2 >= MINMATCH) && (gain2 > gain1)) {
                        matchLength = ml2, offset = offset2, start = ip;
                        continue;
            }   }   }
            break;  /* nothing found : store previous solution */
        }

        /* catch up */
        if (offset) {
            U32 matchIndex = (U32)((start-base) - offset);
            const BYTE* match = (matchIndex < dictLimit) ? dictBase + matchIndex : base + matchIndex;
            const BYTE* const mStart = (matchIndex < dictLimit) ? dictStart : prefixStart;
            while ((start>anchor) && (match>mStart) && (start[-1] == match[-1])) { start--; match--; matchLength++; }  /* catch up */
            offset_2 = offset_1; offset_1 = offset;
        }

        /* store sequence */
_storeSequence:
        {
            size_t litLength = start - anchor;
            ZSTD_storeSeq(seqStorePtr, litLength, anchor, offset, matchLength-MINMATCH);
            anchor = ip = start + matchLength;
        }

        /* check immediate repcode */
        while (ip <= ilimit) {
            const U32 repIndex = (U32)((ip-base) - offset_2);
            const BYTE* const repBase = repIndex < dictLimit ? dictBase : base;
            const BYTE* const repMatch = repBase + repIndex;
            if ((U32)((dictLimit-1) - repIndex) >= 3)   /* intentional overflow */
            if (MEM_read32(ip) == MEM_read32(repMatch)) {
                /* repcode detected we should take it */
                const BYTE* const repEnd = repIndex < dictLimit ? dictEnd : iend;
                matchLength = ZSTD_count_2segments(ip+MINMATCH, repMatch+MINMATCH, iend, repEnd, prefixStart) + MINMATCH;
                offset = offset_2; offset_2 = offset_1; offset_1 = offset;   /* swap offset history */
                ZSTD_storeSeq(seqStorePtr, 0, anchor, 0, matchLength-MINMATCH);
                ip += matchLength;
                anchor = ip;
                continue;   /* faster when present ... (?) */
            }
            break;
    }   }

    /* Last Literals */
    {
        size_t lastLLSize = iend - anchor;
        memcpy(seqStorePtr->lit, anchor, lastLLSize);
        seqStorePtr->lit += lastLLSize;
    }
}

void ZSTD_compressBlock_greedy_extDict(ZSTD_CCtx* ctx, const void* src, size_t srcSize)
{
    ZSTD_compressBlock_lazy_extDict_generic(ctx, src, srcSize, 0, 0);
}

static void ZSTD_compressBlock_lazy_extDict(ZSTD_CCtx* ctx, const void* src, size_t srcSize)
{
    ZSTD_compressBlock_lazy_extDict_generic(ctx, src, srcSize, 0, 1);
}

static void ZSTD_compressBlock_lazy2_extDict(ZSTD_CCtx* ctx, const void* src, size_t srcSize)
{
    ZSTD_compressBlock_lazy_extDict_generic(ctx, src, srcSize, 0, 2);
}

static void ZSTD_compressBlock_btlazy2_extDict(ZSTD_CCtx* ctx, const void* src, size_t srcSize)
{
    ZSTD_compressBlock_lazy_extDict_generic(ctx, src, srcSize, 1, 2);
}

static void ZSTD_compressBlock_opt_extDict(ZSTD_CCtx* ctx, const void* src, size_t srcSize)
{
    ZSTD_compressBlock_opt_extDict_generic(ctx, src, srcSize, 0, 2);
}

static void ZSTD_compressBlock_opt_bt_extDict(ZSTD_CCtx* ctx, const void* src, size_t srcSize)
{
    ZSTD_compressBlock_opt_extDict_generic(ctx, src, srcSize, 1, 2);
}


typedef void (*ZSTD_blockCompressor) (ZSTD_CCtx* ctx, const void* src, size_t srcSize);

static ZSTD_blockCompressor ZSTD_selectBlockCompressor(ZSTD_strategy strat, int extDict)
{
    static const ZSTD_blockCompressor blockCompressor[2][7] = {
        { ZSTD_compressBlock_fast, ZSTD_compressBlock_greedy, ZSTD_compressBlock_lazy,ZSTD_compressBlock_lazy2, ZSTD_compressBlock_btlazy2, ZSTD_compressBlock_opt, ZSTD_compressBlock_opt_bt },
        { ZSTD_compressBlock_fast_extDict, ZSTD_compressBlock_greedy_extDict, ZSTD_compressBlock_lazy_extDict,ZSTD_compressBlock_lazy2_extDict, ZSTD_compressBlock_btlazy2_extDict, ZSTD_compressBlock_opt_extDict, ZSTD_compressBlock_opt_bt_extDict }
    };

    return blockCompressor[extDict][(U32)strat];
}


static size_t ZSTD_compressBlock_internal(ZSTD_CCtx* zc, void* dst, size_t maxDstSize, const void* src, size_t srcSize)
{
    ZSTD_blockCompressor blockCompressor = ZSTD_selectBlockCompressor(zc->params.strategy, zc->lowLimit < zc->dictLimit);
    if (srcSize < MIN_CBLOCK_SIZE+ZSTD_blockHeaderSize+1) return 0;   /* don't even attempt compression below a certain srcSize */
    blockCompressor(zc, src, srcSize);
    return ZSTD_compressSequences(zc, dst, maxDstSize, srcSize);
}


static size_t ZSTD_compress_generic (ZSTD_CCtx* zc,
                                        void* dst, size_t maxDstSize,
                                  const void* src, size_t srcSize)
{
    size_t blockSize = zc->blockSize;
    size_t remaining = srcSize;
    const BYTE* ip = (const BYTE*)src;
    BYTE* const ostart = (BYTE*)dst;
    BYTE* op = ostart;
    const U32 maxDist = 1 << zc->params.windowLog;

    while (remaining) {
        size_t cSize;

        if (maxDstSize < ZSTD_blockHeaderSize + MIN_CBLOCK_SIZE) return ERROR(dstSize_tooSmall);   /* not enough space to store compressed block */
        if (remaining < blockSize) blockSize = remaining;

        if ((U32)(ip+blockSize - zc->base) > zc->loadedDictEnd + maxDist) { /* enforce maxDist */
            U32 newLowLimit = (U32)(ip+blockSize - zc->base) - maxDist;
            if (zc->lowLimit < newLowLimit) zc->lowLimit = newLowLimit;
            if (zc->dictLimit < zc->lowLimit) zc->dictLimit = zc->lowLimit;
        }

        cSize = ZSTD_compressBlock_internal(zc, op+ZSTD_blockHeaderSize, maxDstSize-ZSTD_blockHeaderSize, ip, blockSize);
        if (ZSTD_isError(cSize)) return cSize;

        if (cSize == 0) {  /* block is not compressible */
            cSize = ZSTD_noCompressBlock(op, maxDstSize, ip, blockSize);
            if (ZSTD_isError(cSize)) return cSize;
        } else {
            op[0] = (BYTE)(cSize>>16);
            op[1] = (BYTE)(cSize>>8);
            op[2] = (BYTE)cSize;
            op[0] += (BYTE)(bt_compressed << 6); /* is a compressed block */
            cSize += 3;
        }

        remaining -= blockSize;
        maxDstSize -= cSize;
        ip += blockSize;
        op += cSize;
    }

    return op-ostart;
}


static size_t ZSTD_compressContinue_internal (ZSTD_CCtx* zc,
                              void* dst, size_t dstSize,
                        const void* src, size_t srcSize,
                               U32 frame)
{
    const BYTE* const ip = (const BYTE*) src;
    size_t hbSize = 0;

    if (frame && (zc->stage==0)) {
        hbSize = zc->hbSize;
        if (dstSize <= hbSize) return ERROR(dstSize_tooSmall);
        zc->stage = 1;
        memcpy(dst, zc->headerBuffer, hbSize);
        dstSize -= hbSize;
        dst = (char*)dst + hbSize;
    }

    /* Check if blocks follow each other */
    if (src != zc->nextSrc) {
        /* not contiguous */
        size_t delta = zc->nextSrc - ip;
        zc->lowLimit = zc->dictLimit;
        zc->dictLimit = (U32)(zc->nextSrc - zc->base);
        zc->dictBase = zc->base;
        zc->base -= delta;
        zc->nextToUpdate = zc->dictLimit;
        if (zc->dictLimit - zc->lowLimit < 8) zc->lowLimit = zc->dictLimit;   /* too small extDict */
    }

    /* preemptive overflow correction */
    if (zc->lowLimit > (1<<30)) {
        U32 btplus = (zc->params.strategy == ZSTD_btlazy2) || (zc->params.strategy == ZSTD_btopt);
        U32 contentMask = (1 << (zc->params.contentLog - btplus)) - 1;
        U32 newLowLimit = zc->lowLimit & contentMask;   /* preserve position % contentSize */
        U32 correction = zc->lowLimit - newLowLimit;
        ZSTD_reduceIndex(zc, correction);
        zc->base += correction;
        zc->dictBase += correction;
        zc->lowLimit = newLowLimit;
        zc->dictLimit -= correction;
        if (zc->nextToUpdate < correction) zc->nextToUpdate = 0;
        else zc->nextToUpdate -= correction;
    }

    /* if input and dictionary overlap : reduce dictionary (presumed modified by input) */
    if ((ip+srcSize > zc->dictBase + zc->lowLimit) && (ip < zc->dictBase + zc->dictLimit)) {
        zc->lowLimit = (U32)(ip + srcSize - zc->dictBase);
        if (zc->lowLimit > zc->dictLimit) zc->lowLimit = zc->dictLimit;
    }

    zc->nextSrc = ip + srcSize;
    {
        size_t cSize;
        if (frame) cSize = ZSTD_compress_generic (zc, dst, dstSize, src, srcSize);
        else cSize = ZSTD_compressBlock_internal (zc, dst, dstSize, src, srcSize);
        if (ZSTD_isError(cSize)) return cSize;
        return cSize + hbSize;
    }
}


size_t ZSTD_compressContinue (ZSTD_CCtx* zc,
                              void* dst, size_t dstSize,
                        const void* src, size_t srcSize)
{
    return ZSTD_compressContinue_internal(zc, dst, dstSize, src, srcSize, 1);
}


size_t ZSTD_compressBlock(ZSTD_CCtx* zc, void* dst, size_t maxDstSize, const void* src, size_t srcSize)
{
    if (srcSize > BLOCKSIZE) return ERROR(srcSize_wrong);
    return ZSTD_compressContinue_internal(zc, dst, maxDstSize, src, srcSize, 0);
}


static size_t ZSTD_loadDictionaryContent(ZSTD_CCtx* zc, const void* src, size_t srcSize)
{
    const BYTE* const ip = (const BYTE*) src;
    const BYTE* const iend = ip + srcSize;

    /* input becomes current prefix */
    zc->lowLimit = zc->dictLimit;
    zc->dictLimit = (U32)(zc->nextSrc - zc->base);
    zc->dictBase = zc->base;
    zc->base += ip - zc->nextSrc;
    zc->nextToUpdate = zc->dictLimit;
    zc->loadedDictEnd = (U32)(iend - zc->base);

    zc->nextSrc = iend;
    if (srcSize <= 8) return 0;

    switch(zc->params.strategy)
    {
    case ZSTD_fast:
        ZSTD_fillHashTable (zc, iend, zc->params.searchLength);
        break;

    case ZSTD_greedy:
    case ZSTD_lazy:
    case ZSTD_lazy2:
    case ZSTD_opt:
        ZSTD_insertAndFindFirstIndex (zc, iend-8, zc->params.searchLength);
        break;

    case ZSTD_btlazy2:
    case ZSTD_btopt:
        ZSTD_updateTree(zc, iend-8, iend, 1 << zc->params.searchLog, zc->params.searchLength);
        break;

    default:
        return ERROR(GENERIC);   /* strategy doesn't exist; impossible */
    }

    zc->nextToUpdate = zc->loadedDictEnd;
    return 0;
}


/* Dictionary format :
     Magic == ZSTD_DICT_MAGIC (4 bytes)
     Huff0 CTable (256 * 4 bytes)  => to be changed to read from writeCTable
     Dictionary content
*/
/*! ZSTD_loadDictEntropyStats
    @return : size read from dictionary */
static size_t ZSTD_loadDictEntropyStats(ZSTD_CCtx* zc, const void* dict, size_t dictSize)
{
    /* note : magic number already checked */
    size_t offcodeHeaderSize, matchlengthHeaderSize, litlengthHeaderSize, errorCode;
    short offcodeNCount[MaxOff+1];
    unsigned offcodeMaxValue = MaxOff, offcodeLog = OffFSELog;
    short matchlengthNCount[MaxML+1];
    unsigned matchlengthMaxValue = MaxML, matchlengthLog = MLFSELog;
    short litlengthNCount[MaxLL+1];
    unsigned litlengthMaxValue = MaxLL, litlengthLog = LLFSELog;

    const size_t hufHeaderSize = HUF_readCTable(zc->hufTable, 255, dict, dictSize);
    if (HUF_isError(hufHeaderSize)) return ERROR(dictionary_corrupted);
    zc->flagStaticTables = 1;
    dict = (const char*)dict + hufHeaderSize;
    dictSize -= hufHeaderSize;

    offcodeHeaderSize = FSE_readNCount(offcodeNCount, &offcodeMaxValue, &offcodeLog, dict, dictSize);
    if (FSE_isError(offcodeHeaderSize)) return ERROR(dictionary_corrupted);
    errorCode = FSE_buildCTable(zc->offcodeCTable, offcodeNCount, offcodeMaxValue, offcodeLog);
    if (FSE_isError(errorCode)) return ERROR(dictionary_corrupted);
    dict = (const char*)dict + offcodeHeaderSize;
    dictSize -= offcodeHeaderSize;

    matchlengthHeaderSize = FSE_readNCount(matchlengthNCount, &matchlengthMaxValue, &matchlengthLog, dict, dictSize);
    if (FSE_isError(matchlengthHeaderSize)) return ERROR(dictionary_corrupted);
    errorCode = FSE_buildCTable(zc->matchlengthCTable, matchlengthNCount, matchlengthMaxValue, matchlengthLog);
    if (FSE_isError(errorCode)) return ERROR(dictionary_corrupted);
    dict = (const char*)dict + matchlengthHeaderSize;
    dictSize -= matchlengthHeaderSize;

    litlengthHeaderSize = FSE_readNCount(litlengthNCount, &litlengthMaxValue, &litlengthLog, dict, dictSize);
    if (FSE_isError(litlengthHeaderSize)) return ERROR(dictionary_corrupted);
    errorCode = FSE_buildCTable(zc->litlengthCTable, litlengthNCount, litlengthMaxValue, litlengthLog);
    if (FSE_isError(errorCode)) return ERROR(dictionary_corrupted);

    return hufHeaderSize + offcodeHeaderSize + matchlengthHeaderSize + litlengthHeaderSize;
}


static size_t ZSTD_compress_insertDictionary(ZSTD_CCtx* zc, const void* dict, size_t dictSize)
{
    if (dict && (dictSize>4)) {
        U32 magic = MEM_readLE32(dict);
        size_t eSize;
        if (magic != ZSTD_DICT_MAGIC)
            return ZSTD_loadDictionaryContent(zc, dict, dictSize);

        eSize = ZSTD_loadDictEntropyStats(zc, (const char*)dict+4, dictSize-4) + 4;
        if (ZSTD_isError(eSize)) return eSize;
        return ZSTD_loadDictionaryContent(zc, (const char*)dict+eSize, dictSize-eSize);
    }
    return 0;
}


/*! ZSTD_compressBegin_advanced
*   @return : 0, or an error code */
size_t ZSTD_compressBegin_advanced(ZSTD_CCtx* zc,
                             const void* dict, size_t dictSize,
                                   ZSTD_parameters params)
{
    size_t errorCode;

    ZSTD_validateParams(&params);

    errorCode = ZSTD_resetCCtx_advanced(zc, params);
    if (ZSTD_isError(errorCode)) return errorCode;

    MEM_writeLE32(zc->headerBuffer, ZSTD_MAGICNUMBER);   /* Write Header */
    ((BYTE*)zc->headerBuffer)[4] = (BYTE)(params.windowLog - ZSTD_WINDOWLOG_ABSOLUTEMIN);
    zc->hbSize = ZSTD_frameHeaderSize_min;
    zc->stage = 0;

    return ZSTD_compress_insertDictionary(zc, dict, dictSize);
}


size_t ZSTD_compressBegin_usingDict(ZSTD_CCtx* zc, const void* dict, size_t dictSize, int compressionLevel)
{
    return ZSTD_compressBegin_advanced(zc, dict, dictSize, ZSTD_getParams(compressionLevel, MAX(128 KB, dictSize)));
}

size_t ZSTD_compressBegin(ZSTD_CCtx* zc, int compressionLevel)
{
    return ZSTD_compressBegin_advanced(zc, NULL, 0, ZSTD_getParams(compressionLevel, 0));
}


/*! ZSTD_compressEnd
*   Write frame epilogue
*   @return : nb of bytes written into dst (or an error code) */
size_t ZSTD_compressEnd(ZSTD_CCtx* zc, void* dst, size_t maxDstSize)
{
    BYTE* op = (BYTE*)dst;
    size_t hbSize = 0;

    /* empty frame */
    if (zc->stage==0) {
        hbSize = zc->hbSize;
        if (maxDstSize <= hbSize) return ERROR(dstSize_tooSmall);
        zc->stage = 1;
        memcpy(dst, zc->headerBuffer, hbSize);
        maxDstSize -= hbSize;
        op += hbSize;
    }

    /* frame epilogue */
    if (maxDstSize < 3) return ERROR(dstSize_tooSmall);
    op[0] = (BYTE)(bt_end << 6);
    op[1] = 0;
    op[2] = 0;

    return 3+hbSize;
}


size_t ZSTD_compress_usingPreparedCCtx(ZSTD_CCtx* cctx, const ZSTD_CCtx* preparedCCtx,
                                       void* dst, size_t maxDstSize,
                                 const void* src, size_t srcSize)
{
    size_t outSize;
    size_t errorCode = ZSTD_copyCCtx(cctx, preparedCCtx);
    if (ZSTD_isError(errorCode)) return errorCode;
    errorCode = ZSTD_compressContinue(cctx, dst, maxDstSize, src, srcSize);
    if (ZSTD_isError(errorCode)) return errorCode;
    outSize = errorCode;
    errorCode = ZSTD_compressEnd(cctx, (char*)dst+outSize, maxDstSize-outSize);
    if (ZSTD_isError(errorCode)) return errorCode;
    outSize += errorCode;
    return outSize;
}


size_t ZSTD_compress_advanced (ZSTD_CCtx* ctx,
                               void* dst, size_t maxDstSize,
                         const void* src, size_t srcSize,
                         const void* dict,size_t dictSize,
                               ZSTD_parameters params)
{
    BYTE* const ostart = (BYTE*)dst;
    BYTE* op = ostart;
    size_t oSize;

    /* Init */
    oSize = ZSTD_compressBegin_advanced(ctx, dict, dictSize, params);
    if(ZSTD_isError(oSize)) return oSize;

    /* body (compression) */
    oSize = ZSTD_compressContinue (ctx, op,  maxDstSize, src, srcSize);
    if(ZSTD_isError(oSize)) return oSize;
    op += oSize;
    maxDstSize -= oSize;

    /* Close frame */
    oSize = ZSTD_compressEnd(ctx, op, maxDstSize);
    if(ZSTD_isError(oSize)) return oSize;
    op += oSize;

    return (op - ostart);
}

size_t ZSTD_compress_usingDict(ZSTD_CCtx* ctx, void* dst, size_t maxDstSize, const void* src, size_t srcSize, const void* dict, size_t dictSize, int compressionLevel)
{
    return ZSTD_compress_advanced(ctx, dst, maxDstSize, src, srcSize, dict, dictSize, ZSTD_getParams(compressionLevel, srcSize));
}

size_t ZSTD_compressCCtx (ZSTD_CCtx* ctx, void* dst, size_t maxDstSize, const void* src, size_t srcSize, int compressionLevel)
{
    return ZSTD_compress_advanced(ctx, dst, maxDstSize, src, srcSize, NULL, 0, ZSTD_getParams(compressionLevel, srcSize));
}

size_t ZSTD_compress(void* dst, size_t maxDstSize, const void* src, size_t srcSize, int compressionLevel)
{
    size_t result;
    ZSTD_CCtx ctxBody;
    memset(&ctxBody, 0, sizeof(ctxBody));
    result = ZSTD_compressCCtx(&ctxBody, dst, maxDstSize, src, srcSize, compressionLevel);
    free(ctxBody.workSpace);   /* can't free ctxBody, since it's on stack; just free heap content */
    return result;
}


/*-=====  Pre-defined compression levels  =====-*/

#define ZSTD_MAX_CLEVEL 21
unsigned ZSTD_maxCLevel(void) { return ZSTD_MAX_CLEVEL; }

static const ZSTD_parameters ZSTD_defaultParameters[4][ZSTD_MAX_CLEVEL+1] = {
{   /* "default" */
    /* l,  W,  C,  H,  S,  L, SL, strat */
    {  0,  0,  0,  0,  0,  0,  0, ZSTD_fast    },  /* level  0 - never used */
    {  0, 19, 13, 14,  1,  7,  4, ZSTD_fast    },  /* level  1 */
    {  0, 19, 15, 16,  1,  6,  4, ZSTD_fast    },  /* level  2 */
    {  0, 20, 18, 20,  1,  6,  4, ZSTD_fast    },  /* level  3 */
    {  0, 21, 19, 21,  1,  6,  4, ZSTD_fast    },  /* level  4 */
    {  0, 20, 14, 18,  3,  5,  4, ZSTD_greedy  },  /* level  5 */
    {  0, 20, 18, 19,  3,  5,  4, ZSTD_greedy  },  /* level  6 */
    {  0, 21, 17, 20,  3,  5,  4, ZSTD_lazy    },  /* level  7 */
    {  0, 21, 19, 20,  3,  5,  4, ZSTD_lazy    },  /* level  8 */
    {  0, 21, 20, 20,  3,  5,  4, ZSTD_lazy2   },  /* level  9 */
    {  0, 21, 19, 21,  4,  5,  4, ZSTD_lazy2   },  /* level 10 */
    {  0, 22, 20, 22,  4,  5,  4, ZSTD_lazy2   },  /* level 11 */
    {  0, 22, 20, 22,  5,  5,  4, ZSTD_lazy2   },  /* level 12 */
    {  0, 22, 21, 22,  5,  5,  4, ZSTD_lazy2   },  /* level 13 */
    {  0, 22, 22, 23,  5,  5,  4, ZSTD_lazy2   },  /* level 14 */
    {  0, 23, 23, 23,  5,  5,  4, ZSTD_lazy2   },  /* level 15 */
    {  0, 23, 22, 22,  5,  5,  4, ZSTD_btlazy2 },  /* level 16 */
    {  0, 24, 24, 23,  4,  5,  4, ZSTD_btlazy2 },  /* level 17 */
    {  0, 24, 24, 23,  5,  5, 30, ZSTD_btopt   },  /* level 18 */
    {  0, 25, 25, 24,  5,  4, 40, ZSTD_btopt   },  /* level 19 */
    {  0, 26, 26, 25,  8,  4,256, ZSTD_btopt   },  /* level 20 */
    {  0, 26, 27, 25, 10,  4,256, ZSTD_btopt   },  /* level 21 */
},
{   /* for srcSize <= 256 KB */
    /* l,  W,  C,  H,  S,  L,  T, strat */
    {  0,  0,  0,  0,  0,  0,  0, ZSTD_fast    },  /* level  0 */
    {  0, 18, 14, 15,  1,  6,  4, ZSTD_fast    },  /* level  1 */
    {  0, 18, 14, 16,  1,  5,  4, ZSTD_fast    },  /* level  2 */
    {  0, 18, 14, 17,  1,  5,  4, ZSTD_fast    },  /* level  3.*/
    {  0, 18, 14, 15,  4,  4,  4, ZSTD_greedy  },  /* level  4 */
    {  0, 18, 16, 17,  4,  4,  4, ZSTD_greedy  },  /* level  5 */
    {  0, 18, 17, 17,  3,  4,  4, ZSTD_lazy    },  /* level  6 */
    {  0, 18, 17, 17,  4,  4,  4, ZSTD_lazy    },  /* level  7 */
    {  0, 18, 17, 17,  4,  4,  4, ZSTD_lazy2   },  /* level  8 */
    {  0, 18, 17, 17,  5,  4,  4, ZSTD_lazy2   },  /* level  9 */
    {  0, 18, 17, 17,  6,  4,  4, ZSTD_lazy2   },  /* level 10 */
    {  0, 18, 17, 17,  7,  4,  4, ZSTD_lazy2   },  /* level 11 */
    {  0, 18, 18, 17,  4,  4,  4, ZSTD_btlazy2 },  /* level 12 */
    {  0, 18, 19, 17,  7,  4,  4, ZSTD_btlazy2 },  /* level 13.*/
    {  0, 18, 17, 19,  8,  4, 24, ZSTD_btopt   },  /* level 14.*/
    {  0, 18, 19, 19,  8,  4, 48, ZSTD_btopt   },  /* level 15.*/
    {  0, 18, 19, 18,  9,  4,128, ZSTD_btopt   },  /* level 16.*/
    {  0, 18, 19, 18,  9,  4,192, ZSTD_btopt   },  /* level 17.*/
    {  0, 18, 19, 18,  9,  4,256, ZSTD_btopt   },  /* level 18.*/
    {  0, 18, 19, 18, 10,  4,256, ZSTD_btopt   },  /* level 19.*/
    {  0, 18, 19, 18, 11,  4,256, ZSTD_btopt   },  /* level 20.*/
    {  0, 18, 19, 18, 12,  4,256, ZSTD_btopt   },  /* level 21.*/
},
{   /* for srcSize <= 128 KB */
    /* l,  W,  C,  H,  S,  L,  T, strat */
    {  0,  0,  0,  0,  0,  0,  0, ZSTD_fast    },  /* level  0 - never used */
    {  0, 17, 12, 13,  1,  6,  4, ZSTD_fast    },  /* level  1 */
    {  0, 17, 13, 16,  1,  5,  4, ZSTD_fast    },  /* level  2 */
    {  0, 17, 13, 14,  2,  5,  4, ZSTD_greedy  },  /* level  3 */
    {  0, 17, 13, 15,  3,  4,  4, ZSTD_greedy  },  /* level  4 */
    {  0, 17, 15, 17,  4,  4,  4, ZSTD_greedy  },  /* level  5 */
    {  0, 17, 16, 17,  3,  4,  4, ZSTD_lazy    },  /* level  6 */
    {  0, 17, 16, 17,  4,  4,  4, ZSTD_lazy    },  /* level  7 */
    {  0, 17, 17, 16,  4,  4,  4, ZSTD_lazy2   },  /* level  8 */
    {  0, 17, 17, 16,  5,  4,  4, ZSTD_lazy2   },  /* level  9 */
    {  0, 17, 17, 16,  6,  4,  4, ZSTD_lazy2   },  /* level 10 */
    {  0, 17, 17, 17,  7,  4,  4, ZSTD_lazy2   },  /* level 11 */
    {  0, 17, 17, 17,  8,  4,  4, ZSTD_lazy2   },  /* level 12 */
    {  0, 17, 17, 17,  9,  4,  4, ZSTD_lazy2   },  /* level 13 */
    {  0, 17, 18, 16,  5,  4, 20, ZSTD_btopt   },  /* level 14 */
    {  0, 17, 18, 16,  9,  4, 48, ZSTD_btopt   },  /* level 15 */
    {  0, 17, 18, 17,  7,  4,128, ZSTD_btopt   },  /* level 16 */
    {  0, 17, 18, 17,  8,  4,128, ZSTD_btopt   },  /* level 17 */
    {  0, 17, 18, 17,  8,  4,256, ZSTD_btopt   },  /* level 18 */
    {  0, 17, 18, 17,  9,  4,256, ZSTD_btopt   },  /* level 19 */
    {  0, 17, 18, 17, 10,  4,512, ZSTD_btopt   },  /* level 20 */
    {  0, 17, 18, 17, 11,  4,512, ZSTD_btopt   },  /* level 21 */

},
{   /* for srcSize <= 16 KB */
    /* l,  W,  C,  H,  S,  L,  T, strat */
    {  0,  0,  0,  0,  0,  0,  0, ZSTD_fast    },  /* level  0 -- never used */
    {  0, 14, 14, 14,  1,  4,  4, ZSTD_fast    },  /* level  1 */
    {  0, 14, 14, 15,  1,  4,  4, ZSTD_fast    },  /* level  2 */
    {  0, 14, 13, 15,  4,  4,  4, ZSTD_greedy  },  /* level  3 */
    {  0, 14, 14, 15,  3,  4,  4, ZSTD_lazy    },  /* level  4 */
    {  0, 14, 14, 14,  6,  4,  4, ZSTD_lazy    },  /* level  5 */
    {  0, 14, 14, 14,  5,  4,  4, ZSTD_lazy2   },  /* level  6 */
    {  0, 14, 14, 14,  7,  4,  4, ZSTD_lazy2   },  /* level  7 */
    {  0, 14, 14, 14,  8,  4,  4, ZSTD_lazy2   },  /* level  8 */
    {  0, 14, 14, 14,  9,  4,  4, ZSTD_lazy2   },  /* level  9 */
    {  0, 14, 14, 14, 10,  4,  4, ZSTD_lazy2   },  /* level 10 */
    {  0, 14, 14, 14, 11,  4,  4, ZSTD_lazy2   },  /* level 11 */
    {  0, 14, 15, 15, 12,  4, 32, ZSTD_btopt   },  /* level 12 */
    {  0, 14, 15, 15, 12,  4, 64, ZSTD_btopt   },  /* level 13 */
    {  0, 14, 15, 15, 12,  4, 96, ZSTD_btopt   },  /* level 14 */
    {  0, 14, 15, 15, 12,  4,128, ZSTD_btopt   },  /* level 15 */
    {  0, 14, 15, 15, 12,  4,256, ZSTD_btopt   },  /* level 16 */
    {  0, 14, 15, 15, 13,  4,256, ZSTD_btopt   },  /* level 17 */
    {  0, 14, 15, 15, 14,  4,256, ZSTD_btopt   },  /* level 18 */
    {  0, 14, 15, 15, 15,  4,256, ZSTD_btopt   },  /* level 19 */
    {  0, 14, 15, 15, 16,  4,256, ZSTD_btopt   },  /* level 20 */
    {  0, 14, 15, 15, 17,  4,256, ZSTD_btopt   },  /* level 21 */
},
};

/*! ZSTD_getParams
*   @return ZSTD_parameters structure for a selected compression level and srcSize.
*   @srcSizeHint value is optional, select 0 if not known */
ZSTD_parameters ZSTD_getParams(int compressionLevel, U64 srcSizeHint)
{
    ZSTD_parameters result;
    int tableID = ((srcSizeHint-1) <= 256 KB) + ((srcSizeHint-1) <= 128 KB) + ((srcSizeHint-1) <= 16 KB);   /* intentional underflow for srcSizeHint == 0 */
    if (compressionLevel<=0) compressionLevel = 1;
    if (compressionLevel > ZSTD_MAX_CLEVEL) compressionLevel = ZSTD_MAX_CLEVEL;
#if ZSTD_OPT_DEBUG >= 1
    tableID=0;
#endif
    result = ZSTD_defaultParameters[tableID][compressionLevel];
    result.srcSize = srcSizeHint;
    return result;
}

