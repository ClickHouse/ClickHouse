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
#include "huf_static.h"
#include "zstd_internal.h"


/*-*************************************
*  Constants
***************************************/
static const U32 g_searchStrength = 8;   /* control skip over incompressible data */


/*-*************************************
*  Helper functions
***************************************/
size_t ZSTD_compressBound(size_t srcSize) { return FSE_compressBound(srcSize) + 12; }


/*-*************************************
*  Sequence storage
***************************************/
static void ZSTD_resetSeqStore(seqStore_t* ssPtr)
{
    ssPtr->offset = ssPtr->offsetStart;
    ssPtr->lit = ssPtr->litStart;
    ssPtr->litLength = ssPtr->litLengthStart;
    ssPtr->matchLength = ssPtr->matchLengthStart;
    ssPtr->longLengthID = 0;
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
    U32   nextToUpdate3;    /* index from which to continue dictionary update */
    U32   hashLog3;         /* dispatch table : larger == faster, more memory */
    U32   loadedDictEnd;
    U32   stage;            /* 0: created; 1: init,dictLoad; 2:started */
    ZSTD_parameters params;
    void* workSpace;
    size_t workSpaceSize;
    size_t blockSize;

    seqStore_t seqStore;    /* sequences storage ptrs */
    U32* hashTable;
    U32* hashTable3;
    U32* chainTable;
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

const seqStore_t* ZSTD_getSeqStore(const ZSTD_CCtx* ctx)   /* hidden interface */
{
    return &(ctx->seqStore);
}


#define CLAMP(val,min,max) { if (val<min) val=min; else if (val>max) val=max; }
#define CLAMPCHECK(val,min,max) { if ((val<min) || (val>max)) return ERROR(compressionParameter_unsupported); }

/** ZSTD_checkParams() :
    ensure param values remain within authorized range.
    @return : 0, or an error code if one value is beyond authorized range */
size_t ZSTD_checkCParams(ZSTD_compressionParameters cParams)
{
    CLAMPCHECK(cParams.windowLog, ZSTD_WINDOWLOG_MIN, ZSTD_WINDOWLOG_MAX);
    CLAMPCHECK(cParams.chainLog, ZSTD_CHAINLOG_MIN, ZSTD_CHAINLOG_MAX);
    CLAMPCHECK(cParams.hashLog, ZSTD_HASHLOG_MIN, ZSTD_HASHLOG_MAX);
    CLAMPCHECK(cParams.searchLog, ZSTD_SEARCHLOG_MIN, ZSTD_SEARCHLOG_MAX);
    { U32 const searchLengthMin = (cParams.strategy == ZSTD_fast || cParams.strategy == ZSTD_greedy) ? ZSTD_SEARCHLENGTH_MIN+1 : ZSTD_SEARCHLENGTH_MIN;
      U32 const searchLengthMax = (cParams.strategy == ZSTD_fast) ? ZSTD_SEARCHLENGTH_MAX : ZSTD_SEARCHLENGTH_MAX-1;
      CLAMPCHECK(cParams.searchLength, searchLengthMin, searchLengthMax); }
    CLAMPCHECK(cParams.targetLength, ZSTD_TARGETLENGTH_MIN, ZSTD_TARGETLENGTH_MAX);
    if ((U32)(cParams.strategy) > (U32)ZSTD_btopt) return ERROR(compressionParameter_unsupported);
    return 0;
}


static unsigned ZSTD_highbit(U32 val);

/** ZSTD_checkCParams_advanced() :
    temporary work-around, while the compressor compatibility remains limited regarding windowLog < 18 */
size_t ZSTD_checkCParams_advanced(ZSTD_compressionParameters cParams, U64 srcSize)
{
    if (srcSize > (1ULL << ZSTD_WINDOWLOG_MIN)) return ZSTD_checkCParams(cParams);
    if (cParams.windowLog < ZSTD_WINDOWLOG_ABSOLUTEMIN) return ERROR(compressionParameter_unsupported);
    if (srcSize <= (1ULL << cParams.windowLog)) cParams.windowLog = ZSTD_WINDOWLOG_MIN; /* fake value - temporary work around */
    if (srcSize <= (1ULL << cParams.chainLog)) cParams.chainLog = ZSTD_CHAINLOG_MIN;    /* fake value - temporary work around */
    if ((srcSize <= (1ULL << cParams.hashLog)) && ((U32)cParams.strategy < (U32)ZSTD_btlazy2)) cParams.hashLog = ZSTD_HASHLOG_MIN;       /* fake value - temporary work around */
    return ZSTD_checkCParams(cParams);
}


/** ZSTD_adjustParams() :
    optimize params for q given input (`srcSize` and `dictSize`).
    mostly downsizing to reduce memory consumption and initialization.
    Both `srcSize` and `dictSize` are optional (use 0 if unknown),
    but if both are 0, no optimization can be done.
    Note : params is considered validated at this stage. Use ZSTD_checkParams() to ensure that. */
void ZSTD_adjustCParams(ZSTD_compressionParameters* params, U64 srcSize, size_t dictSize)
{
    if (srcSize+dictSize == 0) return;   /* no size information available : no adjustment */

    /* resize params, to use less memory when necessary */
    {   U32 const minSrcSize = (srcSize==0) ? 500 : 0;
        U64 const rSize = srcSize + dictSize + minSrcSize;
        if (rSize < ((U64)1<<ZSTD_WINDOWLOG_MAX)) {
            U32 const srcLog = ZSTD_highbit((U32)(rSize)-1) + 1;
            if (params->windowLog > srcLog) params->windowLog = srcLog;
    }   }
    if (params->hashLog > params->windowLog) params->hashLog = params->windowLog;
    {   U32 const btPlus = (params->strategy == ZSTD_btlazy2) || (params->strategy == ZSTD_btopt);
        U32 const maxChainLog = params->windowLog+btPlus;
        if (params->chainLog > maxChainLog) params->chainLog = maxChainLog; }   /* <= ZSTD_CHAINLOG_MAX */

    if (params->windowLog  < ZSTD_WINDOWLOG_ABSOLUTEMIN) params->windowLog = ZSTD_WINDOWLOG_ABSOLUTEMIN;  /* required for frame header */
    if ((params->hashLog  < ZSTD_HASHLOG_MIN) && ((U32)params->strategy >= (U32)ZSTD_btlazy2)) params->hashLog = ZSTD_HASHLOG_MIN;  /* required to ensure collision resistance in bt */
}


size_t ZSTD_sizeofCCtx(ZSTD_compressionParameters cParams)   /* hidden interface, for paramagrill */
{
    ZSTD_CCtx* zc = ZSTD_createCCtx();
    ZSTD_parameters params;
    params.cParams = cParams;
    params.fParams.contentSizeFlag = 1;
    ZSTD_compressBegin_advanced(zc, NULL, 0, params, 0);
    { size_t const ccsize = sizeof(*zc) + zc->workSpaceSize;
      ZSTD_freeCCtx(zc);
      return ccsize; }
}

/*! ZSTD_resetCCtx_advanced() :
    note : 'params' is expected to be validated */
static size_t ZSTD_resetCCtx_advanced (ZSTD_CCtx* zc,
                                       ZSTD_parameters params, U32 reset)
{   /* note : params considered validated here */
    const size_t blockSize = MIN(ZSTD_BLOCKSIZE_MAX, (size_t)1 << params.cParams.windowLog);
    const U32    divider = (params.cParams.searchLength==3) ? 3 : 4;
    const size_t maxNbSeq = blockSize / divider;
    const size_t tokenSpace = blockSize + 11*maxNbSeq;
    const size_t chainSize = (params.cParams.strategy == ZSTD_fast) ? 0 : (1 << params.cParams.chainLog);
    const size_t hSize = ((size_t)1) << params.cParams.hashLog;
    const size_t h3Size = (zc->hashLog3) ? 1 << zc->hashLog3 : 0;
    const size_t tableSpace = (chainSize + hSize + h3Size) * sizeof(U32);

    /* Check if workSpace is large enough, alloc a new one if needed */
    {   size_t const optSpace = ((MaxML+1) + (MaxLL+1) + (MaxOff+1) + (1<<Litbits))*sizeof(U32)
                              + (ZSTD_OPT_NUM+1)*(sizeof(ZSTD_match_t) + sizeof(ZSTD_optimal_t));
        size_t const neededSpace = tableSpace + (256*sizeof(U32)) /* huffTable */ + tokenSpace
                              + ((params.cParams.strategy == ZSTD_btopt) ? optSpace : 0);
        if (zc->workSpaceSize < neededSpace) {
            free(zc->workSpace);
            zc->workSpace = malloc(neededSpace);
            if (zc->workSpace == NULL) return ERROR(memory_allocation);
            zc->workSpaceSize = neededSpace;
    }   }

    if (reset) memset(zc->workSpace, 0, tableSpace );   /* reset only tables */
    zc->hashTable3 = (U32*)(zc->workSpace);
    zc->hashTable = zc->hashTable3 + h3Size;
    zc->chainTable = zc->hashTable + hSize;
    zc->seqStore.buffer = zc->chainTable + chainSize;
    zc->hufTable = (HUF_CElt*)zc->seqStore.buffer;
    zc->flagStaticTables = 0;
    zc->seqStore.buffer = ((U32*)(zc->seqStore.buffer)) + 256;

    zc->nextToUpdate = 1;
    zc->nextSrc = NULL;
    zc->base = NULL;
    zc->dictBase = NULL;
    zc->dictLimit = 0;
    zc->lowLimit = 0;
    zc->params = params;
    zc->blockSize = blockSize;

    if (params.cParams.strategy == ZSTD_btopt) {
        zc->seqStore.litFreq = (U32*)(zc->seqStore.buffer);
        zc->seqStore.litLengthFreq = zc->seqStore.litFreq + (1<<Litbits);
        zc->seqStore.matchLengthFreq = zc->seqStore.litLengthFreq + (MaxLL+1);
        zc->seqStore.offCodeFreq = zc->seqStore.matchLengthFreq + (MaxML+1);
        zc->seqStore.matchTable = (ZSTD_match_t*)((void*)(zc->seqStore.offCodeFreq + (MaxOff+1)));
        zc->seqStore.priceTable = (ZSTD_optimal_t*)((void*)(zc->seqStore.matchTable + ZSTD_OPT_NUM+1));
        zc->seqStore.buffer = zc->seqStore.priceTable + ZSTD_OPT_NUM+1;
        zc->seqStore.litLengthSum = 0;
    }
    zc->seqStore.offsetStart = (U32*) (zc->seqStore.buffer);
    zc->seqStore.litLengthStart = (U16*) (void*)(zc->seqStore.offsetStart + maxNbSeq);
    zc->seqStore.matchLengthStart = (U16*) (void*)(zc->seqStore.litLengthStart + maxNbSeq);
    zc->seqStore.llCodeStart = (BYTE*) (zc->seqStore.matchLengthStart + maxNbSeq);
    zc->seqStore.mlCodeStart = zc->seqStore.llCodeStart + maxNbSeq;
    zc->seqStore.offCodeStart = zc->seqStore.mlCodeStart + maxNbSeq;
    zc->seqStore.litStart = zc->seqStore.offCodeStart + maxNbSeq;

    zc->stage = 1;
    zc->loadedDictEnd = 0;

    return 0;
}


/*! ZSTD_copyCCtx() :
*   Duplicate an existing context `srcCCtx` into another one `dstCCtx`.
*   Only works during stage 1 (i.e. after creation, but before first call to ZSTD_compressContinue()).
*   @return : 0, or an error code */
size_t ZSTD_copyCCtx(ZSTD_CCtx* dstCCtx, const ZSTD_CCtx* srcCCtx)
{
    if (srcCCtx->stage!=1) return ERROR(stage_wrong);

    dstCCtx->hashLog3 = srcCCtx->hashLog3; /* must be before ZSTD_resetCCtx_advanced */
    ZSTD_resetCCtx_advanced(dstCCtx, srcCCtx->params, 0);
    dstCCtx->params.fParams.contentSizeFlag = 0;   /* content size different from the one set during srcCCtx init */

    /* copy tables */
    {   const size_t chainSize = (srcCCtx->params.cParams.strategy == ZSTD_fast) ? 0 : (1 << srcCCtx->params.cParams.chainLog);
        const size_t hSize = ((size_t)1) << srcCCtx->params.cParams.hashLog;
        const size_t h3Size = (srcCCtx->hashLog3) ? 1 << srcCCtx->hashLog3 : 0;
        const size_t tableSpace = (chainSize + hSize + h3Size) * sizeof(U32);
        memcpy(dstCCtx->workSpace, srcCCtx->workSpace, tableSpace);
    }

    /* copy dictionary pointers */
    dstCCtx->nextToUpdate = srcCCtx->nextToUpdate;
    dstCCtx->nextToUpdate3= srcCCtx->nextToUpdate3;
    dstCCtx->nextSrc      = srcCCtx->nextSrc;
    dstCCtx->base         = srcCCtx->base;
    dstCCtx->dictBase     = srcCCtx->dictBase;
    dstCCtx->dictLimit    = srcCCtx->dictLimit;
    dstCCtx->lowLimit     = srcCCtx->lowLimit;
    dstCCtx->loadedDictEnd= srcCCtx->loadedDictEnd;

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


/*! ZSTD_reduceTable() :
*   reduce table indexes by `reducerValue` */
static void ZSTD_reduceTable (U32* const table, U32 const size, U32 const reducerValue)
{
    U32 u;
    for (u=0 ; u < size ; u++) {
        if (table[u] < reducerValue) table[u] = 0;
        else table[u] -= reducerValue;
    }
}

/*! ZSTD_reduceIndex() :
*   rescale all indexes to avoid future overflow (indexes are U32) */
static void ZSTD_reduceIndex (ZSTD_CCtx* zc, const U32 reducerValue)
{
    { const U32 hSize = 1 << zc->params.cParams.hashLog;
      ZSTD_reduceTable(zc->hashTable, hSize, reducerValue); }

    { const U32 chainSize = (zc->params.cParams.strategy == ZSTD_fast) ? 0 : (1 << zc->params.cParams.chainLog);
      ZSTD_reduceTable(zc->chainTable, chainSize, reducerValue); }

    { const U32 h3Size = (zc->hashLog3) ? 1 << zc->hashLog3 : 0;
      ZSTD_reduceTable(zc->hashTable3, h3Size, reducerValue); }
}


/*-*******************************************************
*  Block entropic compression
*********************************************************/

/* Frame format description
   Frame Header -  [ Block Header - Block ] - Frame End
   1) Frame Header
      - 4 bytes - Magic Number : ZSTD_MAGICNUMBER (defined within zstd_static.h)
      - 1 byte  - Frame Descriptor
   2) Block Header
      - 3 bytes, starting with a 2-bits descriptor
                 Uncompressed, Compressed, Frame End, unused
   3) Block
      See Block Format Description
   4) Frame End
      - 3 bytes, compatible with Block Header
*/


/* Frame descriptor

   1 byte, using :
   bit 0-3 : windowLog - ZSTD_WINDOWLOG_ABSOLUTEMIN   (see zstd_internal.h)
   bit 4   : minmatch 4(0) or 3(1)
   bit 5   : reserved (must be zero)
   bit 6-7 : Frame content size : unknown, 1 byte, 2 bytes, 8 bytes

   Optional : content size (0, 1, 2 or 8 bytes)
   0 : unknown
   1 : 0-255 bytes
   2 : 256 - 65535+256
   8 : up to 16 exa
*/


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

size_t ZSTD_noCompressBlock (void* dst, size_t dstCapacity, const void* src, size_t srcSize)
{
    BYTE* const ostart = (BYTE* const)dst;

    if (srcSize + ZSTD_blockHeaderSize > dstCapacity) return ERROR(dstSize_tooSmall);
    memcpy(ostart + ZSTD_blockHeaderSize, src, srcSize);

    /* Build header */
    ostart[0]  = (BYTE)(srcSize>>16);
    ostart[1]  = (BYTE)(srcSize>>8);
    ostart[2]  = (BYTE) srcSize;
    ostart[0] += (BYTE)(bt_raw<<6);   /* is a raw (uncompressed) block */

    return ZSTD_blockHeaderSize+srcSize;
}


static size_t ZSTD_noCompressLiterals (void* dst, size_t dstCapacity, const void* src, size_t srcSize)
{
    BYTE* const ostart = (BYTE* const)dst;
    U32 const flSize = 1 + (srcSize>31) + (srcSize>4095);

    if (srcSize + flSize > dstCapacity) return ERROR(dstSize_tooSmall);

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

static size_t ZSTD_compressRleLiteralsBlock (void* dst, size_t dstCapacity, const void* src, size_t srcSize)
{
    BYTE* const ostart = (BYTE* const)dst;
    U32 const flSize = 1 + (srcSize>31) + (srcSize>4095);

    (void)dstCapacity;  /* dstCapacity guaranteed to be >=4, hence large enough */

    switch(flSize)
    {
        case 1: /* 2 - 1 - 5 */
            ostart[0] = (BYTE)((IS_RLE<<6) + (0<<5) + srcSize);
            break;
        case 2: /* 2 - 2 - 12 */
            ostart[0] = (BYTE)((IS_RLE<<6) + (2<<4) + (srcSize >> 8));
            ostart[1] = (BYTE)srcSize;
            break;
        default:   /*note : should not be necessary : flSize is necessarily within {1,2,3} */
        case 3: /* 2 - 2 - 20 */
            ostart[0] = (BYTE)((IS_RLE<<6) + (3<<4) + (srcSize >> 16));
            ostart[1] = (BYTE)(srcSize>>8);
            ostart[2] = (BYTE)srcSize;
            break;
    }

    ostart[flSize] = *(const BYTE*)src;
    return flSize+1;
}


static size_t ZSTD_minGain(size_t srcSize) { return (srcSize >> 6) + 2; }

static size_t ZSTD_compressLiterals (ZSTD_CCtx* zc,
                                     void* dst, size_t dstCapacity,
                               const void* src, size_t srcSize)
{
    size_t const minGain = ZSTD_minGain(srcSize);
    size_t const lhSize = 3 + (srcSize >= 1 KB) + (srcSize >= 16 KB);
    BYTE* const ostart = (BYTE*)dst;
    U32 singleStream = srcSize < 256;
    U32 hType = IS_HUF;
    size_t cLitSize;


    /* small ? don't even attempt compression (speed opt) */
#   define LITERAL_NOENTROPY 63
    {   size_t const minLitSize = zc->flagStaticTables ? 6 : LITERAL_NOENTROPY;
        if (srcSize <= minLitSize) return ZSTD_noCompressLiterals(dst, dstCapacity, src, srcSize);
    }

    if (dstCapacity < lhSize+1) return ERROR(dstSize_tooSmall);   /* not enough space for compression */
    if (zc->flagStaticTables && (lhSize==3)) {
        hType = IS_PCH;
        singleStream = 1;
        cLitSize = HUF_compress1X_usingCTable(ostart+lhSize, dstCapacity-lhSize, src, srcSize, zc->hufTable);
    } else {
        cLitSize = singleStream ? HUF_compress1X(ostart+lhSize, dstCapacity-lhSize, src, srcSize, 255, 12)
                                : HUF_compress2 (ostart+lhSize, dstCapacity-lhSize, src, srcSize, 255, 12);
    }

    if ((cLitSize==0) || (cLitSize >= srcSize - minGain))
        return ZSTD_noCompressLiterals(dst, dstCapacity, src, srcSize);
    if (cLitSize==1)
        return ZSTD_compressRleLiteralsBlock(dst, dstCapacity, src, srcSize);

    /* Build header */
    switch(lhSize)
    {
    case 3: /* 2 - 2 - 10 - 10 */
        ostart[0] = (BYTE)((srcSize>>6) + (singleStream << 4) + (hType<<6));
        ostart[1] = (BYTE)((srcSize<<2) + (cLitSize>>8));
        ostart[2] = (BYTE)(cLitSize);
        break;
    case 4: /* 2 - 2 - 14 - 14 */
        ostart[0] = (BYTE)((srcSize>>10) + (2<<4) +  (hType<<6));
        ostart[1] = (BYTE)(srcSize>> 2);
        ostart[2] = (BYTE)((srcSize<<6) + (cLitSize>>8));
        ostart[3] = (BYTE)(cLitSize);
        break;
    default:   /* should not be necessary, lhSize is only {3,4,5} */
    case 5: /* 2 - 2 - 18 - 18 */
        ostart[0] = (BYTE)((srcSize>>14) + (3<<4) +  (hType<<6));
        ostart[1] = (BYTE)(srcSize>>6);
        ostart[2] = (BYTE)((srcSize<<2) + (cLitSize>>16));
        ostart[3] = (BYTE)(cLitSize>>8);
        ostart[4] = (BYTE)(cLitSize);
        break;
    }
    return lhSize+cLitSize;
}


void ZSTD_seqToCodes(const seqStore_t* seqStorePtr, size_t const nbSeq)
{
    /* LL codes */
    {   static const BYTE LL_Code[64] = {  0,  1,  2,  3,  4,  5,  6,  7,
                                           8,  9, 10, 11, 12, 13, 14, 15,
                                          16, 16, 17, 17, 18, 18, 19, 19,
                                          20, 20, 20, 20, 21, 21, 21, 21,
                                          22, 22, 22, 22, 22, 22, 22, 22,
                                          23, 23, 23, 23, 23, 23, 23, 23,
                                          24, 24, 24, 24, 24, 24, 24, 24,
                                          24, 24, 24, 24, 24, 24, 24, 24 };
        const BYTE LL_deltaCode = 19;
        const U16* const llTable = seqStorePtr->litLengthStart;
        BYTE* const llCodeTable = seqStorePtr->llCodeStart;
        size_t u;
        for (u=0; u<nbSeq; u++) {
            U32 const  ll = llTable[u];
            llCodeTable[u] = (ll>63) ? (BYTE)ZSTD_highbit(ll) + LL_deltaCode : LL_Code[ll];
        }
        if (seqStorePtr->longLengthID==1)
            llCodeTable[seqStorePtr->longLengthPos] = MaxLL;
    }

    /* Offset codes */
    {   const U32* const offsetTable = seqStorePtr->offsetStart;
        BYTE* const ofCodeTable = seqStorePtr->offCodeStart;
        size_t u;
        for (u=0; u<nbSeq; u++) ofCodeTable[u] = (BYTE)ZSTD_highbit(offsetTable[u]);
    }

    /* ML codes */
    {   static const BYTE ML_Code[128] = { 0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15,
                                          16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
                                          32, 32, 33, 33, 34, 34, 35, 35, 36, 36, 36, 36, 37, 37, 37, 37,
                                          38, 38, 38, 38, 38, 38, 38, 38, 39, 39, 39, 39, 39, 39, 39, 39,
                                          40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
                                          41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41,
                                          42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42,
                                          42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42 };
        const BYTE ML_deltaCode = 36;
        const U16* const mlTable = seqStorePtr->matchLengthStart;
        BYTE* const mlCodeTable = seqStorePtr->mlCodeStart;
        size_t u;
        for (u=0; u<nbSeq; u++) {
            U32 const ml = mlTable[u];
            mlCodeTable[u] = (ml>127) ? (BYTE)ZSTD_highbit(ml) + ML_deltaCode : ML_Code[ml];
        }
        if (seqStorePtr->longLengthID==2)
            mlCodeTable[seqStorePtr->longLengthPos] = MaxML;
    }
}


size_t ZSTD_compressSequences(ZSTD_CCtx* zc,
                              void* dst, size_t dstCapacity,
                              size_t srcSize)
{
    const seqStore_t* seqStorePtr = &(zc->seqStore);
    U32 count[MaxSeq+1];
    S16 norm[MaxSeq+1];
    FSE_CTable* CTable_LitLength = zc->litlengthCTable;
    FSE_CTable* CTable_OffsetBits = zc->offcodeCTable;
    FSE_CTable* CTable_MatchLength = zc->matchlengthCTable;
    U32 LLtype, Offtype, MLtype;   /* compressed, raw or rle */
    U16*  const llTable = seqStorePtr->litLengthStart;
    U16*  const mlTable = seqStorePtr->matchLengthStart;
    const U32*  const offsetTable = seqStorePtr->offsetStart;
    const U32*  const offsetTableEnd = seqStorePtr->offset;
    BYTE* const ofCodeTable = seqStorePtr->offCodeStart;
    BYTE* const llCodeTable = seqStorePtr->llCodeStart;
    BYTE* const mlCodeTable = seqStorePtr->mlCodeStart;
    BYTE* const ostart = (BYTE*)dst;
    BYTE* const oend = ostart + dstCapacity;
    BYTE* op = ostart;
    size_t const nbSeq = offsetTableEnd - offsetTable;
    BYTE* seqHead;

    /* Compress literals */
    {   const BYTE* const literals = seqStorePtr->litStart;
        size_t const litSize = seqStorePtr->lit - literals;
        size_t const cSize = ZSTD_compressLiterals(zc, op, dstCapacity, literals, litSize);
        if (ZSTD_isError(cSize)) return cSize;
        op += cSize;
    }

    /* Sequences Header */
    if ((oend-op) < 3 /*max nbSeq Size*/ + 1 /*seqHead */) return ERROR(dstSize_tooSmall);
    if (nbSeq < 0x7F) *op++ = (BYTE)nbSeq;
    else if (nbSeq < LONGNBSEQ) op[0] = (BYTE)((nbSeq>>8) + 0x80), op[1] = (BYTE)nbSeq, op+=2;
    else op[0]=0xFF, MEM_writeLE16(op+1, (U16)(nbSeq - LONGNBSEQ)), op+=3;
    if (nbSeq==0) goto _check_compressibility;

    /* seqHead : flags for FSE encoding type */
    seqHead = op++;

#define MIN_SEQ_FOR_DYNAMIC_FSE   64
#define MAX_SEQ_FOR_STATIC_FSE  1000

    /* convert length/distances into codes */
    ZSTD_seqToCodes(seqStorePtr, nbSeq);

    /* CTable for Literal Lengths */
    {   U32 max = MaxLL;
        size_t const mostFrequent = FSE_countFast(count, &max, llCodeTable, nbSeq);
        if ((mostFrequent == nbSeq) && (nbSeq > 2)) {
            *op++ = llCodeTable[0];
            FSE_buildCTable_rle(CTable_LitLength, (BYTE)max);
            LLtype = FSE_ENCODING_RLE;
        } else if ((zc->flagStaticTables) && (nbSeq < MAX_SEQ_FOR_STATIC_FSE)) {
            LLtype = FSE_ENCODING_STATIC;
        } else if ((nbSeq < MIN_SEQ_FOR_DYNAMIC_FSE) || (mostFrequent < (nbSeq >> (LL_defaultNormLog-1)))) {
            FSE_buildCTable(CTable_LitLength, LL_defaultNorm, MaxLL, LL_defaultNormLog);
            LLtype = FSE_ENCODING_RAW;
        } else {
            size_t nbSeq_1 = nbSeq;
            const U32 tableLog = FSE_optimalTableLog(LLFSELog, nbSeq, max);
            if (count[llCodeTable[nbSeq-1]]>1) { count[llCodeTable[nbSeq-1]]--; nbSeq_1--; }
            FSE_normalizeCount(norm, tableLog, count, nbSeq_1, max);
            { size_t const NCountSize = FSE_writeNCount(op, oend-op, norm, max, tableLog);   /* overflow protected */
              if (FSE_isError(NCountSize)) return ERROR(GENERIC);
              op += NCountSize; }
            FSE_buildCTable(CTable_LitLength, norm, max, tableLog);
            LLtype = FSE_ENCODING_DYNAMIC;
    }   }

    /* CTable for Offsets */
    {   U32 max = MaxOff;
        size_t const mostFrequent = FSE_countFast(count, &max, ofCodeTable, nbSeq);
        if ((mostFrequent == nbSeq) && (nbSeq > 2)) {
            *op++ = ofCodeTable[0];
            FSE_buildCTable_rle(CTable_OffsetBits, (BYTE)max);
            Offtype = FSE_ENCODING_RLE;
        } else if ((zc->flagStaticTables) && (nbSeq < MAX_SEQ_FOR_STATIC_FSE)) {
            Offtype = FSE_ENCODING_STATIC;
        } else if ((nbSeq < MIN_SEQ_FOR_DYNAMIC_FSE) || (mostFrequent < (nbSeq >> (OF_defaultNormLog-1)))) {
            FSE_buildCTable(CTable_OffsetBits, OF_defaultNorm, MaxOff, OF_defaultNormLog);
            Offtype = FSE_ENCODING_RAW;
        } else {
            size_t nbSeq_1 = nbSeq;
            const U32 tableLog = FSE_optimalTableLog(OffFSELog, nbSeq, max);
            if (count[ofCodeTable[nbSeq-1]]>1) { count[ofCodeTable[nbSeq-1]]--; nbSeq_1--; }
            FSE_normalizeCount(norm, tableLog, count, nbSeq_1, max);
            { size_t const NCountSize = FSE_writeNCount(op, oend-op, norm, max, tableLog);   /* overflow protected */
              if (FSE_isError(NCountSize)) return ERROR(GENERIC);
              op += NCountSize; }
            FSE_buildCTable(CTable_OffsetBits, norm, max, tableLog);
            Offtype = FSE_ENCODING_DYNAMIC;
    }   }

    /* CTable for MatchLengths */
    {   U32 max = MaxML;
        size_t const mostFrequent = FSE_countFast(count, &max, mlCodeTable, nbSeq);
        if ((mostFrequent == nbSeq) && (nbSeq > 2)) {
            *op++ = *mlCodeTable;
            FSE_buildCTable_rle(CTable_MatchLength, (BYTE)max);
            MLtype = FSE_ENCODING_RLE;
        } else if ((zc->flagStaticTables) && (nbSeq < MAX_SEQ_FOR_STATIC_FSE)) {
            MLtype = FSE_ENCODING_STATIC;
        } else if ((nbSeq < MIN_SEQ_FOR_DYNAMIC_FSE) || (mostFrequent < (nbSeq >> (ML_defaultNormLog-1)))) {
            FSE_buildCTable(CTable_MatchLength, ML_defaultNorm, MaxML, ML_defaultNormLog);
            MLtype = FSE_ENCODING_RAW;
        } else {
            size_t nbSeq_1 = nbSeq;
            const U32 tableLog = FSE_optimalTableLog(MLFSELog, nbSeq, max);
            if (count[mlCodeTable[nbSeq-1]]>1) { count[mlCodeTable[nbSeq-1]]--; nbSeq_1--; }
            FSE_normalizeCount(norm, tableLog, count, nbSeq_1, max);
            { size_t const NCountSize = FSE_writeNCount(op, oend-op, norm, max, tableLog);   /* overflow protected */
              if (FSE_isError(NCountSize)) return ERROR(GENERIC);
              op += NCountSize; }
            FSE_buildCTable(CTable_MatchLength, norm, max, tableLog);
            MLtype = FSE_ENCODING_DYNAMIC;
    }   }

    *seqHead = (BYTE)((LLtype<<6) + (Offtype<<4) + (MLtype<<2));
    zc->flagStaticTables = 0;

    /* Encoding Sequences */
    {   BIT_CStream_t blockStream;
        FSE_CState_t  stateMatchLength;
        FSE_CState_t  stateOffsetBits;
        FSE_CState_t  stateLitLength;

        { size_t const errorCode = BIT_initCStream(&blockStream, op, oend-op);
          if (ERR_isError(errorCode)) return ERROR(dstSize_tooSmall); }   /* not enough space remaining */

        /* first symbols */
        FSE_initCState2(&stateMatchLength, CTable_MatchLength, mlCodeTable[nbSeq-1]);
        FSE_initCState2(&stateOffsetBits,  CTable_OffsetBits,  ofCodeTable[nbSeq-1]);
        FSE_initCState2(&stateLitLength,   CTable_LitLength,   llCodeTable[nbSeq-1]);
        BIT_addBits(&blockStream, llTable[nbSeq-1], LL_bits[llCodeTable[nbSeq-1]]);
        if (MEM_32bits()) BIT_flushBits(&blockStream);
        BIT_addBits(&blockStream, mlTable[nbSeq-1], ML_bits[mlCodeTable[nbSeq-1]]);
        if (MEM_32bits()) BIT_flushBits(&blockStream);
        BIT_addBits(&blockStream, offsetTable[nbSeq-1], ofCodeTable[nbSeq-1]);
        BIT_flushBits(&blockStream);

        {   size_t n;
            for (n=nbSeq-2 ; n<nbSeq ; n--) {      /* intentional underflow */
                const BYTE ofCode = ofCodeTable[n];
                const BYTE mlCode = mlCodeTable[n];
                const BYTE llCode = llCodeTable[n];
                const U32  llBits = LL_bits[llCode];
                const U32  mlBits = ML_bits[mlCode];
                const U32  ofBits = ofCode;                                     /* 32b*/  /* 64b*/
                                                                                /* (7)*/  /* (7)*/
                FSE_encodeSymbol(&blockStream, &stateOffsetBits, ofCode);       /* 15 */  /* 15 */
                FSE_encodeSymbol(&blockStream, &stateMatchLength, mlCode);      /* 24 */  /* 24 */
                if (MEM_32bits()) BIT_flushBits(&blockStream);                  /* (7)*/
                FSE_encodeSymbol(&blockStream, &stateLitLength, llCode);        /* 16 */  /* 33 */
                if (MEM_32bits() || (ofBits+mlBits+llBits >= 64-7-(LLFSELog+MLFSELog+OffFSELog)))
                    BIT_flushBits(&blockStream);                                /* (7)*/
                BIT_addBits(&blockStream, llTable[n], llBits);
                if (MEM_32bits() && ((llBits+mlBits)>24)) BIT_flushBits(&blockStream);
                BIT_addBits(&blockStream, mlTable[n], mlBits);
                if (MEM_32bits()) BIT_flushBits(&blockStream);                  /* (7)*/
                BIT_addBits(&blockStream, offsetTable[n], ofBits);              /* 31 */
                BIT_flushBits(&blockStream);                                    /* (7)*/
        }   }

        FSE_flushCState(&blockStream, &stateMatchLength);
        FSE_flushCState(&blockStream, &stateOffsetBits);
        FSE_flushCState(&blockStream, &stateLitLength);

        {   size_t const streamSize = BIT_closeCStream(&blockStream);
            if (streamSize==0) return ERROR(dstSize_tooSmall);   /* not enough space */
            op += streamSize;
    }   }

    /* check compressibility */
_check_compressibility:
    { size_t const minGain = ZSTD_minGain(srcSize);
      size_t const maxCSize = srcSize - minGain;
      if ((size_t)(op-ostart) >= maxCSize) return 0; }

    return op - ostart;
}


/*! ZSTD_storeSeq() :
    Store a sequence (literal length, literals, offset code and match length code) into seqStore_t.
    `offsetCode` : distance to match, or 0 == repCode.
    `matchCode` : matchLength - MINMATCH
*/
MEM_STATIC void ZSTD_storeSeq(seqStore_t* seqStorePtr, size_t litLength, const BYTE* literals, size_t offsetCode, size_t matchCode)
{
#if 0  /* for debug */
    static const BYTE* g_start = NULL;
    const U32 pos = (U32)(literals - g_start);
    if (g_start==NULL) g_start = literals;
    if ((pos > 2587900) && (pos < 2588050))
        printf("Cpos %6u :%5u literals & match %3u bytes at distance %6u \n",
               pos, (U32)litLength, (U32)matchCode+MINMATCH, (U32)offsetCode);
#endif
    ZSTD_statsUpdatePrices(&seqStorePtr->stats, litLength, literals, offsetCode, matchCode);

    /* copy Literals */
    ZSTD_wildcopy(seqStorePtr->lit, literals, litLength);
    seqStorePtr->lit += litLength;

    /* literal Length */
    if (litLength>0xFFFF) { seqStorePtr->longLengthID = 1; seqStorePtr->longLengthPos = (U32)(seqStorePtr->litLength - seqStorePtr->litLengthStart); }
    *seqStorePtr->litLength++ = (U16)litLength;

    /* match offset */
    *(seqStorePtr->offset++) = (U32)offsetCode + 1;

    /* match Length */
    if (matchCode>0xFFFF) { seqStorePtr->longLengthID = 2; seqStorePtr->longLengthPos = (U32)(seqStorePtr->matchLength - seqStorePtr->matchLengthStart); }
    *seqStorePtr->matchLength++ = (U16)matchCode;
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
static const U32 prime3bytes = 506832829U;
static U32    ZSTD_hash3(U32 u, U32 h) { return ((u << (32-24)) * prime3bytes)  >> (32-h) ; }
static size_t ZSTD_hash3Ptr(const void* ptr, U32 h) { return ZSTD_hash3(MEM_readLE32(ptr), h); }

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
static void ZSTD_fillHashTable (ZSTD_CCtx* zc, const void* end, const U32 mls)
{
    U32* const hashTable = zc->hashTable;
    const U32 hBits = zc->params.cParams.hashLog;
    const BYTE* const base = zc->base;
    const BYTE* ip = base + zc->nextToUpdate;
    const BYTE* const iend = ((const BYTE*)end) - 8;
    const size_t fastHashFillStep = 3;

    while(ip <= iend) {
        hashTable[ZSTD_hashPtr(ip, hBits, mls)] = (U32)(ip - base);
        ip += fastHashFillStep;
    }
}


FORCE_INLINE
void ZSTD_compressBlock_fast_generic(ZSTD_CCtx* zc,
                                 const void* src, size_t srcSize,
                                 const U32 mls)
{
    U32* const hashTable = zc->hashTable;
    const U32 hBits = zc->params.cParams.hashLog;
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
            mlCode = ZSTD_count(ip+1+EQUAL_READ32, ip+1+EQUAL_READ32-offset_1, iend) + EQUAL_READ32;
            ip++;
            ZSTD_storeSeq(seqStorePtr, ip-anchor, anchor, 0, mlCode-MINMATCH);
       } else {
            if ( (matchIndex <= lowIndex) ||
                 (MEM_read32(match) != MEM_read32(ip)) ) {
                ip += ((ip-anchor) >> g_searchStrength) + 1;
                continue;
            }
            mlCode = ZSTD_count(ip+EQUAL_READ32, match+EQUAL_READ32, iend) + EQUAL_READ32;
            offset = ip-match;
            while ((ip>anchor) && (match>lowest) && (ip[-1] == match[-1])) { ip--; match--; mlCode++; }  /* catch up */
            offset_2 = offset_1;
            offset_1 = offset;

            ZSTD_storeSeq(seqStorePtr, ip-anchor, anchor, offset + ZSTD_REP_MOVE, mlCode-MINMATCH);
        }

        /* match found */
        ip += mlCode;
        anchor = ip;

        if (ip <= ilimit) {
            /* Fill Table */
            hashTable[ZSTD_hashPtr(base+current+2, hBits, mls)] = current+2;  /* here because current+2 could be > iend-8 */
            hashTable[ZSTD_hashPtr(ip-2, hBits, mls)] = (U32)(ip-2-base);
            /* check immediate repcode */
            while ( (ip <= ilimit)
                 && (MEM_read32(ip) == MEM_read32(ip - offset_2)) ) {
                /* store sequence */
                size_t const rlCode = ZSTD_count(ip+EQUAL_READ32, ip+EQUAL_READ32-offset_2, iend) + EQUAL_READ32;
                { size_t const tmpOff = offset_2; offset_2 = offset_1; offset_1 = tmpOff; } /* swap offset_2 <=> offset_1 */
                hashTable[ZSTD_hashPtr(ip, hBits, mls)] = (U32)(ip-base);
                ZSTD_storeSeq(seqStorePtr, 0, anchor, 0, rlCode-MINMATCH);
                ip += rlCode;
                anchor = ip;
                continue;   /* faster when present ... (?) */
    }   }   }

    /* Last Literals */
    {   size_t const lastLLSize = iend - anchor;
        memcpy(seqStorePtr->lit, anchor, lastLLSize);
        seqStorePtr->lit += lastLLSize;
    }
}


static void ZSTD_compressBlock_fast(ZSTD_CCtx* ctx,
                       const void* src, size_t srcSize)
{
    const U32 mls = ctx->params.cParams.searchLength;
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
    const U32 hBits = ctx->params.cParams.hashLog;
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

        if ( ((repIndex >= dictLimit) || (repIndex <= dictLimit-4))
          && (MEM_read32(repMatch) == MEM_read32(ip+1)) ) {
            const BYTE* repMatchEnd = repIndex < dictLimit ? dictEnd : iend;
            mlCode = ZSTD_count_2segments(ip+1+EQUAL_READ32, repMatch+EQUAL_READ32, iend, repMatchEnd, lowPrefixPtr) + EQUAL_READ32;
            ip++;
            ZSTD_storeSeq(seqStorePtr, ip-anchor, anchor, 0, mlCode-MINMATCH);
        } else {
            if ( (matchIndex < lowLimit) ||
                 (MEM_read32(match) != MEM_read32(ip)) ) {
                ip += ((ip-anchor) >> g_searchStrength) + 1;
                continue;
            }
            {   const BYTE* matchEnd = matchIndex < dictLimit ? dictEnd : iend;
                const BYTE* lowMatchPtr = matchIndex < dictLimit ? dictStart : lowPrefixPtr;
                mlCode = ZSTD_count_2segments(ip+EQUAL_READ32, match+EQUAL_READ32, iend, matchEnd, lowPrefixPtr) + EQUAL_READ32;
                while ((ip>anchor) && (match>lowMatchPtr) && (ip[-1] == match[-1])) { ip--; match--; mlCode++; }   /* catch up */
                offset = current - matchIndex;
                offset_2 = offset_1;
                offset_1 = offset;
                ZSTD_storeSeq(seqStorePtr, ip-anchor, anchor, offset + ZSTD_REP_MOVE, mlCode-MINMATCH);
        }   }

        /* found a match : store it */
        ip += mlCode;
        anchor = ip;

        if (ip <= ilimit) {
            /* Fill Table */
			hashTable[ZSTD_hashPtr(base+current+2, hBits, mls)] = current+2;
            hashTable[ZSTD_hashPtr(ip-2, hBits, mls)] = (U32)(ip-2-base);
            /* check immediate repcode */
            while (ip <= ilimit) {
                U32 const current2 = (U32)(ip-base);
                U32 const repIndex2 = current2 - offset_2;
                const BYTE* repMatch2 = repIndex2 < dictLimit ? dictBase + repIndex2 : base + repIndex2;
                if ( ((repIndex2 <= dictLimit-4) || (repIndex2 >= dictLimit))
                  && (MEM_read32(repMatch2) == MEM_read32(ip)) ) {
                    const BYTE* const repEnd2 = repIndex2 < dictLimit ? dictEnd : iend;
                    size_t repLength2 = ZSTD_count_2segments(ip+EQUAL_READ32, repMatch2+EQUAL_READ32, iend, repEnd2, lowPrefixPtr) + EQUAL_READ32;
                    U32 tmpOffset = offset_2; offset_2 = offset_1; offset_1 = tmpOffset;   /* swap offset_2 <=> offset_1 */
                    ZSTD_storeSeq(seqStorePtr, 0, anchor, 0, repLength2-MINMATCH);
                    hashTable[ZSTD_hashPtr(ip, hBits, mls)] = current2;
                    ip += repLength2;
                    anchor = ip;
                    continue;
                }
                break;
    }   }   }

    /* Last Literals */
    {   size_t const lastLLSize = iend - anchor;
        memcpy(seqStorePtr->lit, anchor, lastLLSize);
        seqStorePtr->lit += lastLLSize;
    }
}


static void ZSTD_compressBlock_fast_extDict(ZSTD_CCtx* ctx,
                         const void* src, size_t srcSize)
{
    const U32 mls = ctx->params.cParams.searchLength;
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
    const U32 hashLog = zc->params.cParams.hashLog;
    const size_t h  = ZSTD_hashPtr(ip, hashLog, mls);
    U32* const bt   = zc->chainTable;
    const U32 btLog = zc->params.cParams.chainLog - 1;
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
#if 0   /* note : can create issues when hlog small <= 11 */
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
    const U32 hashLog = zc->params.cParams.hashLog;
    const size_t h  = ZSTD_hashPtr(ip, hashLog, mls);
    U32* const bt   = zc->chainTable;
    const U32 btLog = zc->params.cParams.chainLog - 1;
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
    U32 matchEndIdx = current+8;
    U32 dummy32;   /* to be nullified at the end */
    size_t bestLength = 0;

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
                bestLength = matchLength, *offsetPtr = ZSTD_REP_MOVE + current - matchIndex;
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

/** ZSTD_BtFindBestMatch() : Tree updater, providing best match */
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
    const U32 hashLog = zc->params.cParams.hashLog;
    U32* const chainTable = zc->chainTable;
    const U32 chainMask = (1 << zc->params.cParams.chainLog) - 1;
    const BYTE* const base = zc->base;
    const U32 target = (U32)(ip - base);
    U32 idx = zc->nextToUpdate;

    while(idx < target) {
        size_t const h = ZSTD_hashPtr(base+idx, hashLog, mls);
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
    U32* const chainTable = zc->chainTable;
    const U32 chainSize = (1 << zc->params.cParams.chainLog);
    const U32 chainMask = chainSize-1;
    const BYTE* const base = zc->base;
    const BYTE* const dictBase = zc->dictBase;
    const U32 dictLimit = zc->dictLimit;
    const BYTE* const prefixStart = base + dictLimit;
    const BYTE* const dictEnd = dictBase + dictLimit;
    const U32 lowLimit = zc->lowLimit;
    const U32 current = (U32)(ip-base);
    const U32 minChain = current > chainSize ? current - chainSize : 0;
    int nbAttempts=maxNbAttempts;
    size_t ml=EQUAL_READ32-1;

    /* HC4 match finder */
    U32 matchIndex = ZSTD_insertAndFindFirstIndex (zc, ip, mls);

    for ( ; (matchIndex>lowLimit) && (nbAttempts) ; nbAttempts--) {
        const BYTE* match;
        size_t currentMl=0;
        if ((!extDict) || matchIndex >= dictLimit) {
            match = base + matchIndex;
            if (match[ml] == ip[ml])   /* potentially better */
                currentMl = ZSTD_count(ip, match, iLimit);
        } else {
            match = dictBase + matchIndex;
            if (MEM_read32(match) == MEM_read32(ip))   /* assumption : matchIndex <= dictLimit-4 (by table construction) */
                currentMl = ZSTD_count_2segments(ip+EQUAL_READ32, match+EQUAL_READ32, iLimit, dictEnd, prefixStart) + EQUAL_READ32;
        }

        /* save best solution */
        if (currentMl > ml) { ml = currentMl; *offsetPtr = ZSTD_REP_MOVE + current - matchIndex; if (ip+currentMl == iLimit) break; /* best possible, and avoid read overflow*/ }

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

    U32 const maxSearches = 1 << ctx->params.cParams.searchLog;
    U32 const mls = ctx->params.cParams.searchLength;

    typedef size_t (*searchMax_f)(ZSTD_CCtx* zc, const BYTE* ip, const BYTE* iLimit,
                        size_t* offsetPtr,
                        U32 maxNbAttempts, U32 matchLengthSearch);
    searchMax_f searchMax = searchMethod ? ZSTD_BtFindBestMatch_selectMLS : ZSTD_HcFindBestMatch_selectMLS;

    /* init */
    U32 rep[ZSTD_REP_INIT];
    { U32 i ; for (i=0; i<ZSTD_REP_INIT; i++) rep[i]=REPCODE_STARTVALUE; }

    ctx->nextToUpdate3 = ctx->nextToUpdate;
    ZSTD_resetSeqStore(seqStorePtr);
    if ((ip-base) < REPCODE_STARTVALUE) ip = base + REPCODE_STARTVALUE;

    /* Match Loop */
    while (ip < ilimit) {
        size_t matchLength=0;
        size_t offset=0;
        const BYTE* start=ip+1;

        /* check repCode */
        if (MEM_read32(ip+1) == MEM_read32(ip+1 - rep[0])) {
            /* repcode : we take it */
            matchLength = ZSTD_count(ip+1+EQUAL_READ32, ip+1+EQUAL_READ32-rep[0], iend) + EQUAL_READ32;
            if (depth==0) goto _storeSequence;
        }

        /* first search (depth 0) */
        {   size_t offsetFound = 99999999;
            size_t const ml2 = searchMax(ctx, ip, iend, &offsetFound, maxSearches, mls);
            if (ml2 > matchLength)
                matchLength = ml2, start = ip, offset=offsetFound;
        }

        if (matchLength < EQUAL_READ32) {
            ip += ((ip-anchor) >> g_searchStrength) + 1;   /* jump faster over incompressible sections */
            continue;
        }

        /* let's try to find a better solution */
        if (depth>=1)
        while (ip<ilimit) {
            ip ++;
            if ((offset) && (MEM_read32(ip) == MEM_read32(ip - rep[0]))) {
                size_t const mlRep = ZSTD_count(ip+EQUAL_READ32, ip+EQUAL_READ32-rep[0], iend) + EQUAL_READ32;
                int const gain2 = (int)(mlRep * 3);
                int const gain1 = (int)(matchLength*3 - ZSTD_highbit((U32)offset+1) + 1);
                if ((mlRep >= EQUAL_READ32) && (gain2 > gain1))
                    matchLength = mlRep, offset = 0, start = ip;
            }
            {   size_t offset2=99999999;
                size_t const ml2 = searchMax(ctx, ip, iend, &offset2, maxSearches, mls);
                int const gain2 = (int)(ml2*4 - ZSTD_highbit((U32)offset2+1));   /* raw approx */
                int const gain1 = (int)(matchLength*4 - ZSTD_highbit((U32)offset+1) + 4);
                if ((ml2 >= EQUAL_READ32) && (gain2 > gain1)) {
                    matchLength = ml2, offset = offset2, start = ip;
                    continue;   /* search a better one */
            }   }

            /* let's find an even better one */
            if ((depth==2) && (ip<ilimit)) {
                ip ++;
                if ((offset) && (MEM_read32(ip) == MEM_read32(ip - rep[0]))) {
                    size_t const ml2 = ZSTD_count(ip+EQUAL_READ32, ip+EQUAL_READ32-rep[0], iend) + EQUAL_READ32;
                    int const gain2 = (int)(ml2 * 4);
                    int const gain1 = (int)(matchLength*4 - ZSTD_highbit((U32)offset+1) + 1);
                    if ((ml2 >= EQUAL_READ32) && (gain2 > gain1))
                        matchLength = ml2, offset = 0, start = ip;
                }
                {   size_t offset2=99999999;
                    size_t const ml2 = searchMax(ctx, ip, iend, &offset2, maxSearches, mls);
                    int const gain2 = (int)(ml2*4 - ZSTD_highbit((U32)offset2+1));   /* raw approx */
                    int const gain1 = (int)(matchLength*4 - ZSTD_highbit((U32)offset+1) + 7);
                    if ((ml2 >= EQUAL_READ32) && (gain2 > gain1)) {
                        matchLength = ml2, offset = offset2, start = ip;
                        continue;
            }   }   }
            break;  /* nothing found : store previous solution */
        }

        /* catch up */
        if (offset) {
            while ((start>anchor) && (start>base+offset-ZSTD_REP_MOVE) && (start[-1] == start[-1-offset+ZSTD_REP_MOVE]))   /* only search for offset within prefix */
                { start--; matchLength++; }
            rep[1] = rep[0]; rep[0] = (U32)(offset - ZSTD_REP_MOVE);
        }

        /* store sequence */
_storeSequence:
        {   size_t const litLength = start - anchor;
            ZSTD_storeSeq(seqStorePtr, litLength, anchor, offset, matchLength-MINMATCH);
            anchor = ip = start + matchLength;
        }

        /* check immediate repcode */
        while ( (ip <= ilimit)
             && (MEM_read32(ip) == MEM_read32(ip - rep[1])) ) {
            /* store sequence */
            matchLength = ZSTD_count(ip+EQUAL_READ32, ip+EQUAL_READ32-rep[1], iend) + EQUAL_READ32;
            offset = rep[1]; rep[1] = rep[0]; rep[0] = (U32)offset; /* swap repcodes */
            ZSTD_storeSeq(seqStorePtr, 0, anchor, 0, matchLength-MINMATCH);
            ip += matchLength;
            anchor = ip;
            continue;   /* faster when present ... (?) */
    }   }

    /* Last Literals */
    {   size_t const lastLLSize = iend - anchor;
        memcpy(seqStorePtr->lit, anchor, lastLLSize);
        seqStorePtr->lit += lastLLSize;
        ZSTD_statsUpdatePrices(&seqStorePtr->stats, lastLLSize, anchor, 0, 0);
    }
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

    const U32 maxSearches = 1 << ctx->params.cParams.searchLog;
    const U32 mls = ctx->params.cParams.searchLength;

    typedef size_t (*searchMax_f)(ZSTD_CCtx* zc, const BYTE* ip, const BYTE* iLimit,
                        size_t* offsetPtr,
                        U32 maxNbAttempts, U32 matchLengthSearch);
    searchMax_f searchMax = searchMethod ? ZSTD_BtFindBestMatch_selectMLS_extDict : ZSTD_HcFindBestMatch_extDict_selectMLS;

    /* init */
    U32 rep[ZSTD_REP_INIT];
    { U32 i; for (i=0; i<ZSTD_REP_INIT; i++) rep[i]=REPCODE_STARTVALUE; }

    ctx->nextToUpdate3 = ctx->nextToUpdate;
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
            const U32 repIndex = (U32)(current+1 - rep[0]);
            const BYTE* const repBase = repIndex < dictLimit ? dictBase : base;
            const BYTE* const repMatch = repBase + repIndex;
            if ((U32)((dictLimit-1) - repIndex) >= 3)   /* intentional overflow */
            if (MEM_read32(ip+1) == MEM_read32(repMatch)) {
                /* repcode detected we should take it */
                const BYTE* const repEnd = repIndex < dictLimit ? dictEnd : iend;
                matchLength = ZSTD_count_2segments(ip+1+EQUAL_READ32, repMatch+EQUAL_READ32, iend, repEnd, prefixStart) + EQUAL_READ32;
                if (depth==0) goto _storeSequence;
        }   }

        /* first search (depth 0) */
        {   size_t offsetFound = 99999999;
            size_t const ml2 = searchMax(ctx, ip, iend, &offsetFound, maxSearches, mls);
            if (ml2 > matchLength)
                matchLength = ml2, start = ip, offset=offsetFound;
        }

         if (matchLength < EQUAL_READ32) {
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
                const U32 repIndex = (U32)(current - rep[0]);
                const BYTE* const repBase = repIndex < dictLimit ? dictBase : base;
                const BYTE* const repMatch = repBase + repIndex;
                if ((U32)((dictLimit-1) - repIndex) >= 3)   /* intentional overflow */
                if (MEM_read32(ip) == MEM_read32(repMatch)) {
                    /* repcode detected */
                    const BYTE* const repEnd = repIndex < dictLimit ? dictEnd : iend;
                    size_t const repLength = ZSTD_count_2segments(ip+EQUAL_READ32, repMatch+EQUAL_READ32, iend, repEnd, prefixStart) + EQUAL_READ32;
                    int const gain2 = (int)(repLength * 3);
                    int const gain1 = (int)(matchLength*3 - ZSTD_highbit((U32)offset+1) + 1);
                    if ((repLength >= EQUAL_READ32) && (gain2 > gain1))
                        matchLength = repLength, offset = 0, start = ip;
            }   }

            /* search match, depth 1 */
            {   size_t offset2=99999999;
                size_t const ml2 = searchMax(ctx, ip, iend, &offset2, maxSearches, mls);
                int const gain2 = (int)(ml2*4 - ZSTD_highbit((U32)offset2+1));   /* raw approx */
                int const gain1 = (int)(matchLength*4 - ZSTD_highbit((U32)offset+1) + 4);
                if ((ml2 >= EQUAL_READ32) && (gain2 > gain1)) {
                    matchLength = ml2, offset = offset2, start = ip;
                    continue;   /* search a better one */
            }   }

            /* let's find an even better one */
            if ((depth==2) && (ip<ilimit)) {
                ip ++;
                current++;
                /* check repCode */
                if (offset) {
                    const U32 repIndex = (U32)(current - rep[0]);
                    const BYTE* const repBase = repIndex < dictLimit ? dictBase : base;
                    const BYTE* const repMatch = repBase + repIndex;
                    if ((U32)((dictLimit-1) - repIndex) >= 3)   /* intentional overflow */
                    if (MEM_read32(ip) == MEM_read32(repMatch)) {
                        /* repcode detected */
                        const BYTE* const repEnd = repIndex < dictLimit ? dictEnd : iend;
                        size_t repLength = ZSTD_count_2segments(ip+EQUAL_READ32, repMatch+EQUAL_READ32, iend, repEnd, prefixStart) + EQUAL_READ32;
                        int gain2 = (int)(repLength * 4);
                        int gain1 = (int)(matchLength*4 - ZSTD_highbit((U32)offset+1) + 1);
                        if ((repLength >= EQUAL_READ32) && (gain2 > gain1))
                            matchLength = repLength, offset = 0, start = ip;
                }   }

                /* search match, depth 2 */
                {   size_t offset2=99999999;
                    size_t const ml2 = searchMax(ctx, ip, iend, &offset2, maxSearches, mls);
                    int const gain2 = (int)(ml2*4 - ZSTD_highbit((U32)offset2+1));   /* raw approx */
                    int const gain1 = (int)(matchLength*4 - ZSTD_highbit((U32)offset+1) + 7);
                    if ((ml2 >= EQUAL_READ32) && (gain2 > gain1)) {
                        matchLength = ml2, offset = offset2, start = ip;
                        continue;
            }   }   }
            break;  /* nothing found : store previous solution */
        }

        /* catch up */
        if (offset) {
            U32 matchIndex = (U32)((start-base) - (offset - ZSTD_REP_MOVE));
            const BYTE* match = (matchIndex < dictLimit) ? dictBase + matchIndex : base + matchIndex;
            const BYTE* const mStart = (matchIndex < dictLimit) ? dictStart : prefixStart;
            while ((start>anchor) && (match>mStart) && (start[-1] == match[-1])) { start--; match--; matchLength++; }  /* catch up */
            rep[1] = rep[0]; rep[0] = (U32)(offset - ZSTD_REP_MOVE);
        }

        /* store sequence */
_storeSequence:
        {   size_t const litLength = start - anchor;
            ZSTD_storeSeq(seqStorePtr, litLength, anchor, offset, matchLength-MINMATCH);
            anchor = ip = start + matchLength;
        }

        /* check immediate repcode */
        while (ip <= ilimit) {
            const U32 repIndex = (U32)((ip-base) - rep[1]);
            const BYTE* const repBase = repIndex < dictLimit ? dictBase : base;
            const BYTE* const repMatch = repBase + repIndex;
            if ((U32)((dictLimit-1) - repIndex) >= 3)   /* intentional overflow */
            if (MEM_read32(ip) == MEM_read32(repMatch)) {
                /* repcode detected we should take it */
                const BYTE* const repEnd = repIndex < dictLimit ? dictEnd : iend;
                matchLength = ZSTD_count_2segments(ip+EQUAL_READ32, repMatch+EQUAL_READ32, iend, repEnd, prefixStart) + EQUAL_READ32;
                offset = rep[1]; rep[1] = rep[0]; rep[0] = (U32)offset;   /* swap offset history */
                ZSTD_storeSeq(seqStorePtr, 0, anchor, 0, matchLength-MINMATCH);
                ip += matchLength;
                anchor = ip;
                continue;   /* faster when present ... (?) */
            }
            break;
    }   }

    /* Last Literals */
    {   size_t const lastLLSize = iend - anchor;
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



/* The optimal parser */
#include "zstd_opt.h"

static void ZSTD_compressBlock_btopt(ZSTD_CCtx* ctx, const void* src, size_t srcSize)
{
    ZSTD_compressBlock_opt_generic(ctx, src, srcSize);
}

static void ZSTD_compressBlock_btopt_extDict(ZSTD_CCtx* ctx, const void* src, size_t srcSize)
{
    ZSTD_compressBlock_opt_extDict_generic(ctx, src, srcSize);
}


typedef void (*ZSTD_blockCompressor) (ZSTD_CCtx* ctx, const void* src, size_t srcSize);

static ZSTD_blockCompressor ZSTD_selectBlockCompressor(ZSTD_strategy strat, int extDict)
{
    static const ZSTD_blockCompressor blockCompressor[2][6] = {
#if 1
        { ZSTD_compressBlock_fast, ZSTD_compressBlock_greedy, ZSTD_compressBlock_lazy, ZSTD_compressBlock_lazy2, ZSTD_compressBlock_btlazy2, ZSTD_compressBlock_btopt },
#else
        { ZSTD_compressBlock_fast_extDict, ZSTD_compressBlock_greedy_extDict, ZSTD_compressBlock_lazy_extDict,ZSTD_compressBlock_lazy2_extDict, ZSTD_compressBlock_btlazy2_extDict, ZSTD_compressBlock_btopt_extDict },
#endif
        { ZSTD_compressBlock_fast_extDict, ZSTD_compressBlock_greedy_extDict, ZSTD_compressBlock_lazy_extDict,ZSTD_compressBlock_lazy2_extDict, ZSTD_compressBlock_btlazy2_extDict, ZSTD_compressBlock_btopt_extDict }
    };

    return blockCompressor[extDict][(U32)strat];
}


static size_t ZSTD_compressBlock_internal(ZSTD_CCtx* zc, void* dst, size_t dstCapacity, const void* src, size_t srcSize)
{
    ZSTD_blockCompressor blockCompressor = ZSTD_selectBlockCompressor(zc->params.cParams.strategy, zc->lowLimit < zc->dictLimit);
    if (srcSize < MIN_CBLOCK_SIZE+ZSTD_blockHeaderSize+1) return 0;   /* don't even attempt compression below a certain srcSize */
    blockCompressor(zc, src, srcSize);
    return ZSTD_compressSequences(zc, dst, dstCapacity, srcSize);
}




static size_t ZSTD_compress_generic (ZSTD_CCtx* zc,
                                        void* dst, size_t dstCapacity,
                                  const void* src, size_t srcSize)
{
    size_t blockSize = zc->blockSize;
    size_t remaining = srcSize;
    const BYTE* ip = (const BYTE*)src;
    BYTE* const ostart = (BYTE*)dst;
    BYTE* op = ostart;
    const U32 maxDist = 1 << zc->params.cParams.windowLog;
    ZSTD_stats_t* stats = &zc->seqStore.stats;

    ZSTD_statsInit(stats);

    while (remaining) {
        size_t cSize;
        ZSTD_statsResetFreqs(stats);

        if (dstCapacity < ZSTD_blockHeaderSize + MIN_CBLOCK_SIZE) return ERROR(dstSize_tooSmall);   /* not enough space to store compressed block */
        if (remaining < blockSize) blockSize = remaining;

        if ((U32)(ip+blockSize - zc->base) > zc->loadedDictEnd + maxDist) {
            /* enforce maxDist */
            U32 const newLowLimit = (U32)(ip+blockSize - zc->base) - maxDist;
            if (zc->lowLimit < newLowLimit) zc->lowLimit = newLowLimit;
            if (zc->dictLimit < zc->lowLimit) zc->dictLimit = zc->lowLimit;
        }

        cSize = ZSTD_compressBlock_internal(zc, op+ZSTD_blockHeaderSize, dstCapacity-ZSTD_blockHeaderSize, ip, blockSize);
        if (ZSTD_isError(cSize)) return cSize;

        if (cSize == 0) {  /* block is not compressible */
            cSize = ZSTD_noCompressBlock(op, dstCapacity, ip, blockSize);
            if (ZSTD_isError(cSize)) return cSize;
        } else {
            op[0] = (BYTE)(cSize>>16);
            op[1] = (BYTE)(cSize>>8);
            op[2] = (BYTE)cSize;
            op[0] += (BYTE)(bt_compressed << 6); /* is a compressed block */
            cSize += 3;
        }

        remaining -= blockSize;
        dstCapacity -= cSize;
        ip += blockSize;
        op += cSize;
    }

    ZSTD_statsPrint(stats, zc->params.cParams.searchLength);
    return op-ostart;
}


static size_t ZSTD_writeFrameHeader(void* dst, size_t dstCapacity,
                                    ZSTD_parameters params, U64 pledgedSrcSize)
{   BYTE* const op = (BYTE*)dst;
    U32 const fcsId = params.fParams.contentSizeFlag ?
                     (pledgedSrcSize>0) + (pledgedSrcSize>=256) + (pledgedSrcSize>=65536+256) :   /* 0-3 */
                      0;
    BYTE const fdescriptor = (BYTE)((params.cParams.windowLog - ZSTD_WINDOWLOG_ABSOLUTEMIN)   /* windowLog : 4 KB - 128 MB */
                                  | (fcsId << 6) );
    size_t const hSize = ZSTD_frameHeaderSize_min + ZSTD_fcs_fieldSize[fcsId];
    if (hSize > dstCapacity) return ERROR(dstSize_tooSmall);

    MEM_writeLE32(dst, ZSTD_MAGICNUMBER);
    op[4] = fdescriptor;
    switch(fcsId)
    {
        default:   /* impossible */
        case 0 : break;
        case 1 : op[5] = (BYTE)(pledgedSrcSize); break;
        case 2 : MEM_writeLE16(op+5, (U16)(pledgedSrcSize-256)); break;
        case 3 : MEM_writeLE64(op+5, (U64)(pledgedSrcSize)); break;
    }
    return hSize;
}


static size_t ZSTD_compressContinue_internal (ZSTD_CCtx* zc,
                              void* dst, size_t dstCapacity,
                        const void* src, size_t srcSize,
                               U32 frame)
{
    const BYTE* const ip = (const BYTE*) src;
    size_t fhSize = 0;

    if (zc->stage==0) return ERROR(stage_wrong);
    if (frame && (zc->stage==1)) {   /* copy saved header */
        fhSize = ZSTD_writeFrameHeader(dst, dstCapacity, zc->params, srcSize);
        if (ZSTD_isError(fhSize)) return fhSize;
        dstCapacity -= fhSize;
        dst = (char*)dst + fhSize;
        zc->stage = 2;
    }

    /* Check if blocks follow each other */
    if (src != zc->nextSrc) {
        /* not contiguous */
        size_t const delta = zc->nextSrc - ip;
        zc->lowLimit = zc->dictLimit;
        zc->dictLimit = (U32)(zc->nextSrc - zc->base);
        zc->dictBase = zc->base;
        zc->base -= delta;
        zc->nextToUpdate = zc->dictLimit;
        if (zc->dictLimit - zc->lowLimit < 8) zc->lowLimit = zc->dictLimit;   /* too small extDict */
    }

    /* preemptive overflow correction */
    if (zc->lowLimit > (1<<30)) {
        U32 const btplus = (zc->params.cParams.strategy == ZSTD_btlazy2) || (zc->params.cParams.strategy == ZSTD_btopt);
        U32 const chainMask = (1 << (zc->params.cParams.chainLog - btplus)) - 1;
        U32 const newLowLimit = zc->lowLimit & chainMask;   /* preserve position % chainSize */
        U32 const correction = zc->lowLimit - newLowLimit;
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
    {   size_t const cSize = frame ?
                             ZSTD_compress_generic (zc, dst, dstCapacity, src, srcSize) :
                             ZSTD_compressBlock_internal (zc, dst, dstCapacity, src, srcSize);
        if (ZSTD_isError(cSize)) return cSize;
        return cSize + fhSize;
    }
}


size_t ZSTD_compressContinue (ZSTD_CCtx* zc,
                              void* dst, size_t dstCapacity,
                        const void* src, size_t srcSize)
{
    return ZSTD_compressContinue_internal(zc, dst, dstCapacity, src, srcSize, 1);
}


size_t ZSTD_compressBlock(ZSTD_CCtx* zc, void* dst, size_t dstCapacity, const void* src, size_t srcSize)
{
    if (srcSize > ZSTD_BLOCKSIZE_MAX) return ERROR(srcSize_wrong);
    ZSTD_LOG_BLOCK("%p: ZSTD_compressBlock searchLength=%d\n", zc->base, zc->params.cParams.searchLength);
    return ZSTD_compressContinue_internal(zc, dst, dstCapacity, src, srcSize, 0);
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

    switch(zc->params.cParams.strategy)
    {
    case ZSTD_fast:
        ZSTD_fillHashTable (zc, iend, zc->params.cParams.searchLength);
        break;

    case ZSTD_greedy:
    case ZSTD_lazy:
    case ZSTD_lazy2:
        ZSTD_insertAndFindFirstIndex (zc, iend-8, zc->params.cParams.searchLength);
        break;

    case ZSTD_btlazy2:
    case ZSTD_btopt:
        ZSTD_updateTree(zc, iend-8, iend, 1 << zc->params.cParams.searchLog, zc->params.cParams.searchLength);
        break;

    default:
        return ERROR(GENERIC);   /* strategy doesn't exist; impossible */
    }

    zc->nextToUpdate = zc->loadedDictEnd;
    return 0;
}


/* Dictionary format :
     Magic == ZSTD_DICT_MAGIC (4 bytes)
     HUF_writeCTable(256)
     Dictionary content
*/
/*! ZSTD_loadDictEntropyStats() :
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

    size_t const hufHeaderSize = HUF_readCTable(zc->hufTable, 255, dict, dictSize);
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

/** ZSTD_compress_insertDictionary() :
*   @return : 0, or an error code */
static size_t ZSTD_compress_insertDictionary(ZSTD_CCtx* zc, const void* dict, size_t dictSize)
{
    if ((dict==NULL) || (dictSize<=4)) return 0;

    /* default : dict is pure content */
    if (MEM_readLE32(dict) != ZSTD_DICT_MAGIC) return ZSTD_loadDictionaryContent(zc, dict, dictSize);

    /* known magic number : dict is parsed for entropy stats and content */
    {   size_t const eSize = ZSTD_loadDictEntropyStats(zc, (const char*)dict+4 /* skip magic */, dictSize-4) + 4;
        if (ZSTD_isError(eSize)) return eSize;
        return ZSTD_loadDictionaryContent(zc, (const char*)dict+eSize, dictSize-eSize);
    }
}


/*! ZSTD_compressBegin_internal() :
*   @return : 0, or an error code */
static size_t ZSTD_compressBegin_internal(ZSTD_CCtx* zc,
                             const void* dict, size_t dictSize,
                                   ZSTD_parameters params, U64 pledgedSrcSize)
{
    { U32 const hashLog3 = (pledgedSrcSize || pledgedSrcSize >= 8192) ? ZSTD_HASHLOG3_MAX : ((pledgedSrcSize >= 2048) ? ZSTD_HASHLOG3_MIN + 1 : ZSTD_HASHLOG3_MIN);
      zc->hashLog3 = (params.cParams.searchLength==3) ? hashLog3 : 0; }

    { size_t const resetError = ZSTD_resetCCtx_advanced(zc, params, 1);
      if (ZSTD_isError(resetError)) return resetError; }

    return ZSTD_compress_insertDictionary(zc, dict, dictSize);
}


/*! ZSTD_compressBegin_advanced() :
*   @return : 0, or an error code */
size_t ZSTD_compressBegin_advanced(ZSTD_CCtx* zc,
                             const void* dict, size_t dictSize,
                                   ZSTD_parameters params, U64 pledgedSrcSize)
{
    /* compression parameters verification and optimization */
    { size_t const errorCode = ZSTD_checkCParams_advanced(params.cParams, pledgedSrcSize);
      if (ZSTD_isError(errorCode)) return errorCode; }

    return ZSTD_compressBegin_internal(zc, dict, dictSize, params, pledgedSrcSize);
}


size_t ZSTD_compressBegin_usingDict(ZSTD_CCtx* zc, const void* dict, size_t dictSize, int compressionLevel)
{
    ZSTD_parameters params;
    params.cParams = ZSTD_getCParams(compressionLevel, 0, dictSize);
    params.fParams.contentSizeFlag = 0;
    ZSTD_adjustCParams(&params.cParams, 0, dictSize);
    ZSTD_LOG_BLOCK("%p: ZSTD_compressBegin_usingDict compressionLevel=%d\n", zc->base, compressionLevel);
    return ZSTD_compressBegin_internal(zc, dict, dictSize, params, 0);
}


size_t ZSTD_compressBegin(ZSTD_CCtx* zc, int compressionLevel)
{
    ZSTD_LOG_BLOCK("%p: ZSTD_compressBegin compressionLevel=%d\n", zc->base, compressionLevel);
    return ZSTD_compressBegin_usingDict(zc, NULL, 0, compressionLevel);
}


/*! ZSTD_compressEnd() :
*   Write frame epilogue.
*   @return : nb of bytes written into dst (or an error code) */
size_t ZSTD_compressEnd(ZSTD_CCtx* zc, void* dst, size_t dstCapacity)
{
    BYTE* op = (BYTE*)dst;
    size_t fhSize = 0;

    /* not even init ! */
    if (zc->stage==0) return ERROR(stage_wrong);

    /* special case : empty frame */
    if (zc->stage==1) {
        fhSize = ZSTD_writeFrameHeader(dst, dstCapacity, zc->params, 0);
        if (ZSTD_isError(fhSize)) return fhSize;
        dstCapacity -= fhSize;
        op += fhSize;
        zc->stage = 2;
    }

    /* frame epilogue */
    if (dstCapacity < 3) return ERROR(dstSize_tooSmall);
    op[0] = (BYTE)(bt_end << 6);
    op[1] = 0;
    op[2] = 0;

    zc->stage = 0;  /* return to "created by not init" status */
    return 3+fhSize;
}


size_t ZSTD_compress_usingPreparedCCtx(ZSTD_CCtx* cctx, const ZSTD_CCtx* preparedCCtx,
                                       void* dst, size_t dstCapacity,
                                 const void* src, size_t srcSize)
{
    {   size_t const errorCode = ZSTD_copyCCtx(cctx, preparedCCtx);
        if (ZSTD_isError(errorCode)) return errorCode;
    }
    {   size_t const cSize = ZSTD_compressContinue(cctx, dst, dstCapacity, src, srcSize);
        if (ZSTD_isError(cSize)) return cSize;

        {   size_t const endSize = ZSTD_compressEnd(cctx, (char*)dst+cSize, dstCapacity-cSize);
            if (ZSTD_isError(endSize)) return endSize;
            return cSize + endSize;
    }   }
}


static size_t ZSTD_compress_internal (ZSTD_CCtx* ctx,
                               void* dst, size_t dstCapacity,
                         const void* src, size_t srcSize,
                         const void* dict,size_t dictSize,
                               ZSTD_parameters params)
{
    BYTE* const ostart = (BYTE*)dst;
    BYTE* op = ostart;

    /* Init */
    { size_t const errorCode = ZSTD_compressBegin_internal(ctx, dict, dictSize, params, srcSize);
      if(ZSTD_isError(errorCode)) return errorCode; }

    /* body (compression) */
    { size_t const oSize = ZSTD_compressContinue (ctx, op,  dstCapacity, src, srcSize);
      if(ZSTD_isError(oSize)) return oSize;
      op += oSize;
      dstCapacity -= oSize; }

    /* Close frame */
    { size_t const oSize = ZSTD_compressEnd(ctx, op, dstCapacity);
      if(ZSTD_isError(oSize)) return oSize;
      op += oSize; }

    return (op - ostart);
}

size_t ZSTD_compress_advanced (ZSTD_CCtx* ctx,
                               void* dst, size_t dstCapacity,
                         const void* src, size_t srcSize,
                         const void* dict,size_t dictSize,
                               ZSTD_parameters params)
{
    size_t const errorCode = ZSTD_checkCParams_advanced(params.cParams, srcSize);
    if (ZSTD_isError(errorCode)) return errorCode;
    return ZSTD_compress_internal(ctx, dst, dstCapacity, src, srcSize, dict, dictSize, params);
}

size_t ZSTD_compress_usingDict(ZSTD_CCtx* ctx, void* dst, size_t dstCapacity, const void* src, size_t srcSize, const void* dict, size_t dictSize, int compressionLevel)
{
    ZSTD_parameters params;
    ZSTD_LOG_BLOCK("%p: ZSTD_compress_usingDict srcSize=%d dictSize=%d compressionLevel=%d\n", ctx->base, (int)srcSize, (int)dictSize, compressionLevel);
    params.cParams =  ZSTD_getCParams(compressionLevel, srcSize, dictSize);
    params.fParams.contentSizeFlag = 1;
    ZSTD_adjustCParams(&params.cParams, srcSize, dictSize);
    return ZSTD_compress_internal(ctx, dst, dstCapacity, src, srcSize, dict, dictSize, params);
}

size_t ZSTD_compressCCtx (ZSTD_CCtx* ctx, void* dst, size_t dstCapacity, const void* src, size_t srcSize, int compressionLevel)
{
    ZSTD_LOG_BLOCK("%p: ZSTD_compressCCtx srcSize=%d compressionLevel=%d\n", ctx->base, (int)srcSize, compressionLevel);
    return ZSTD_compress_usingDict(ctx, dst, dstCapacity, src, srcSize, NULL, 0, compressionLevel);
}

size_t ZSTD_compress(void* dst, size_t dstCapacity, const void* src, size_t srcSize, int compressionLevel)
{
    size_t result;
    ZSTD_CCtx ctxBody;
    memset(&ctxBody, 0, sizeof(ctxBody));
    result = ZSTD_compressCCtx(&ctxBody, dst, dstCapacity, src, srcSize, compressionLevel);
    free(ctxBody.workSpace);   /* can't free ctxBody, since it's on stack; just free heap content */
    return result;
}


/*-=====  Pre-defined compression levels  =====-*/

#define ZSTD_DEFAULT_CLEVEL 5
#define ZSTD_MAX_CLEVEL     22
unsigned ZSTD_maxCLevel(void) { return ZSTD_MAX_CLEVEL; }

static const ZSTD_compressionParameters ZSTD_defaultCParameters[4][ZSTD_MAX_CLEVEL+1] = {
{   /* "default" */
    /* W,  C,  H,  S,  L, TL, strat */
    {  0,  0,  0,  0,  0,  0, ZSTD_fast    },  /* level  0 - never used */
    { 19, 13, 14,  1,  7,  4, ZSTD_fast    },  /* level  1 */
    { 19, 15, 16,  1,  6,  4, ZSTD_fast    },  /* level  2 */
    { 20, 18, 20,  1,  6,  4, ZSTD_fast    },  /* level  3 */
    { 20, 13, 17,  2,  5,  4, ZSTD_greedy  },  /* level  4.*/
    { 20, 15, 18,  3,  5,  4, ZSTD_greedy  },  /* level  5 */
    { 21, 16, 19,  2,  5,  4, ZSTD_lazy    },  /* level  6 */
    { 21, 17, 20,  3,  5,  4, ZSTD_lazy    },  /* level  7 */
    { 21, 18, 20,  3,  5,  4, ZSTD_lazy2   },  /* level  8.*/
    { 21, 20, 20,  3,  5,  4, ZSTD_lazy2   },  /* level  9 */
    { 21, 19, 21,  4,  5,  4, ZSTD_lazy2   },  /* level 10 */
    { 22, 20, 22,  4,  5,  4, ZSTD_lazy2   },  /* level 11 */
    { 22, 20, 22,  5,  5,  4, ZSTD_lazy2   },  /* level 12 */
    { 22, 21, 22,  5,  5,  4, ZSTD_lazy2   },  /* level 13 */
    { 22, 21, 22,  6,  5,  4, ZSTD_lazy2   },  /* level 14 */
    { 22, 21, 21,  5,  5,  4, ZSTD_btlazy2 },  /* level 15 */
    { 23, 22, 22,  5,  5,  4, ZSTD_btlazy2 },  /* level 16 */
    { 23, 23, 22,  5,  5,  4, ZSTD_btlazy2 },  /* level 17.*/
    { 23, 23, 22,  6,  5, 24, ZSTD_btopt   },  /* level 18.*/
    { 23, 23, 22,  6,  3, 48, ZSTD_btopt   },  /* level 19.*/
    { 25, 26, 23,  7,  3, 64, ZSTD_btopt   },  /* level 20.*/
    { 26, 26, 23,  7,  3,256, ZSTD_btopt   },  /* level 21.*/
    { 27, 27, 25,  9,  3,512, ZSTD_btopt   },  /* level 22.*/
},
{   /* for srcSize <= 256 KB */
    /* W,  C,  H,  S,  L,  T, strat */
    {  0,  0,  0,  0,  0,  0, ZSTD_fast    },  /* level  0 */
    { 18, 13, 14,  1,  6,  4, ZSTD_fast    },  /* level  1 */
    { 18, 15, 17,  1,  5,  4, ZSTD_fast    },  /* level  2 */
    { 18, 13, 15,  1,  5,  4, ZSTD_greedy  },  /* level  3.*/
    { 18, 15, 17,  1,  5,  4, ZSTD_greedy  },  /* level  4.*/
    { 18, 16, 17,  4,  5,  4, ZSTD_greedy  },  /* level  5 */
    { 18, 17, 17,  5,  5,  4, ZSTD_greedy  },  /* level  6 */
    { 18, 17, 17,  4,  4,  4, ZSTD_lazy    },  /* level  7 */
    { 18, 17, 17,  4,  4,  4, ZSTD_lazy2   },  /* level  8 */
    { 18, 17, 17,  5,  4,  4, ZSTD_lazy2   },  /* level  9 */
    { 18, 17, 17,  6,  4,  4, ZSTD_lazy2   },  /* level 10 */
    { 18, 18, 17,  6,  4,  4, ZSTD_lazy2   },  /* level 11.*/
    { 18, 18, 17,  7,  4,  4, ZSTD_lazy2   },  /* level 12.*/
    { 18, 19, 17,  7,  4,  4, ZSTD_btlazy2 },  /* level 13 */
    { 18, 18, 18,  4,  4, 16, ZSTD_btopt   },  /* level 14.*/
    { 18, 18, 18,  8,  4, 24, ZSTD_btopt   },  /* level 15.*/
    { 18, 19, 18,  8,  3, 48, ZSTD_btopt   },  /* level 16.*/
    { 18, 19, 18,  8,  3, 96, ZSTD_btopt   },  /* level 17.*/
    { 18, 19, 18,  9,  3,128, ZSTD_btopt   },  /* level 18.*/
    { 18, 19, 18, 10,  3,256, ZSTD_btopt   },  /* level 19.*/
    { 18, 19, 18, 11,  3,512, ZSTD_btopt   },  /* level 20.*/
    { 18, 19, 18, 12,  3,512, ZSTD_btopt   },  /* level 21.*/
    { 18, 19, 18, 13,  3,512, ZSTD_btopt   },  /* level 22.*/
},
{   /* for srcSize <= 128 KB */
    /* W,  C,  H,  S,  L,  T, strat */
    {  0,  0,  0,  0,  0,  0, ZSTD_fast    },  /* level  0 - never used */
    { 17, 12, 13,  1,  6,  4, ZSTD_fast    },  /* level  1 */
    { 17, 13, 16,  1,  5,  4, ZSTD_fast    },  /* level  2 */
    { 17, 13, 14,  2,  5,  4, ZSTD_greedy  },  /* level  3 */
    { 17, 13, 15,  3,  4,  4, ZSTD_greedy  },  /* level  4 */
    { 17, 15, 17,  4,  4,  4, ZSTD_greedy  },  /* level  5 */
    { 17, 16, 17,  3,  4,  4, ZSTD_lazy    },  /* level  6 */
    { 17, 15, 17,  4,  4,  4, ZSTD_lazy2   },  /* level  7 */
    { 17, 17, 17,  4,  4,  4, ZSTD_lazy2   },  /* level  8 */
    { 17, 17, 17,  5,  4,  4, ZSTD_lazy2   },  /* level  9 */
    { 17, 17, 17,  6,  4,  4, ZSTD_lazy2   },  /* level 10 */
    { 17, 17, 17,  7,  4,  4, ZSTD_lazy2   },  /* level 11 */
    { 17, 17, 17,  8,  4,  4, ZSTD_lazy2   },  /* level 12 */
    { 17, 18, 17,  6,  4,  4, ZSTD_btlazy2 },  /* level 13.*/
    { 17, 17, 17,  7,  3,  8, ZSTD_btopt   },  /* level 14.*/
    { 17, 17, 17,  7,  3, 16, ZSTD_btopt   },  /* level 15.*/
    { 17, 18, 17,  7,  3, 32, ZSTD_btopt   },  /* level 16.*/
    { 17, 18, 17,  7,  3, 64, ZSTD_btopt   },  /* level 17.*/
    { 17, 18, 17,  7,  3,256, ZSTD_btopt   },  /* level 18.*/
    { 17, 18, 17,  8,  3,256, ZSTD_btopt   },  /* level 19.*/
    { 17, 18, 17,  9,  3,256, ZSTD_btopt   },  /* level 20.*/
    { 17, 18, 17, 10,  3,256, ZSTD_btopt   },  /* level 21.*/
    { 17, 18, 17, 11,  3,256, ZSTD_btopt   },  /* level 22.*/
},
{   /* for srcSize <= 16 KB */
    /* W,  C,  H,  S,  L,  T, strat */
    {  0,  0,  0,  0,  0,  0, ZSTD_fast    },  /* level  0 -- never used */
    { 14, 14, 14,  1,  4,  4, ZSTD_fast    },  /* level  1 */
    { 14, 14, 15,  1,  4,  4, ZSTD_fast    },  /* level  2 */
    { 14, 14, 14,  4,  4,  4, ZSTD_greedy  },  /* level  3.*/
    { 14, 14, 14,  3,  4,  4, ZSTD_lazy    },  /* level  4.*/
    { 14, 14, 14,  4,  4,  4, ZSTD_lazy2   },  /* level  5 */
    { 14, 14, 14,  5,  4,  4, ZSTD_lazy2   },  /* level  6 */
    { 14, 14, 14,  6,  4,  4, ZSTD_lazy2   },  /* level  7.*/
    { 14, 14, 14,  7,  4,  4, ZSTD_lazy2   },  /* level  8.*/
    { 14, 15, 14,  6,  4,  4, ZSTD_btlazy2 },  /* level  9.*/
    { 14, 15, 14,  3,  3,  6, ZSTD_btopt   },  /* level 10.*/
    { 14, 15, 14,  6,  3,  8, ZSTD_btopt   },  /* level 11.*/
    { 14, 15, 14,  6,  3, 16, ZSTD_btopt   },  /* level 12.*/
    { 14, 15, 14,  6,  3, 24, ZSTD_btopt   },  /* level 13.*/
    { 14, 15, 15,  6,  3, 48, ZSTD_btopt   },  /* level 14.*/
    { 14, 15, 15,  6,  3, 64, ZSTD_btopt   },  /* level 15.*/
    { 14, 15, 15,  6,  3, 96, ZSTD_btopt   },  /* level 16.*/
    { 14, 15, 15,  6,  3,128, ZSTD_btopt   },  /* level 17.*/
    { 14, 15, 15,  6,  3,256, ZSTD_btopt   },  /* level 18.*/
    { 14, 15, 15,  7,  3,256, ZSTD_btopt   },  /* level 19.*/
    { 14, 15, 15,  8,  3,256, ZSTD_btopt   },  /* level 20.*/
    { 14, 15, 15,  9,  3,256, ZSTD_btopt   },  /* level 21.*/
    { 14, 15, 15, 10,  3,256, ZSTD_btopt   },  /* level 22.*/
},
};

/*! ZSTD_getParams() :
*   @return ZSTD_parameters structure for a selected compression level and srcSize.
*   `srcSize` value is optional, select 0 if not known */
ZSTD_compressionParameters ZSTD_getCParams(int compressionLevel, U64 srcSize, size_t dictSize)
{
    ZSTD_compressionParameters cp;
    size_t const addedSize = srcSize ? 0 : 500;
    U64 const rSize = srcSize+dictSize ? srcSize+dictSize+addedSize : (U64)-1;
    U32 const tableID = (rSize <= 256 KB) + (rSize <= 128 KB) + (rSize <= 16 KB);   /* intentional underflow for srcSizeHint == 0 */
    if (compressionLevel < 0) compressionLevel = ZSTD_DEFAULT_CLEVEL;
    if (compressionLevel==0) compressionLevel = 1;
    if (compressionLevel > ZSTD_MAX_CLEVEL) compressionLevel = ZSTD_MAX_CLEVEL;
    cp = ZSTD_defaultCParameters[tableID][compressionLevel];
    if (MEM_32bits()) {   /* auto-correction, for 32-bits mode */
        if (cp.windowLog > ZSTD_WINDOWLOG_MAX) cp.windowLog = ZSTD_WINDOWLOG_MAX;
        if (cp.chainLog > ZSTD_CHAINLOG_MAX) cp.chainLog = ZSTD_CHAINLOG_MAX;
        if (cp.hashLog > ZSTD_HASHLOG_MAX) cp.hashLog = ZSTD_HASHLOG_MAX;
    }
    return cp;
}
