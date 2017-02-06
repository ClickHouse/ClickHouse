/**
 * Copyright (c) 2016-present, Yann Collet, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */


/* ***************************************************************
*  Tuning parameters
*****************************************************************/
/*!
 * HEAPMODE :
 * Select how default decompression function ZSTD_decompress() will allocate memory,
 * in memory stack (0), or in memory heap (1, requires malloc())
 */
#ifndef ZSTD_HEAPMODE
#  define ZSTD_HEAPMODE 1
#endif

/*!
*  LEGACY_SUPPORT :
*  if set to 1, ZSTD_decompress() can decode older formats (v0.1+)
*/
#ifndef ZSTD_LEGACY_SUPPORT
#  define ZSTD_LEGACY_SUPPORT 0
#endif

/*!
*  MAXWINDOWSIZE_DEFAULT :
*  maximum window size accepted by DStream, by default.
*  Frames requiring more memory will be rejected.
*/
#ifndef ZSTD_MAXWINDOWSIZE_DEFAULT
#  define ZSTD_MAXWINDOWSIZE_DEFAULT (257 << 20)   /* 257 MB */
#endif


/*-*******************************************************
*  Dependencies
*********************************************************/
#include <string.h>      /* memcpy, memmove, memset */
#include "mem.h"         /* low level memory routines */
#define XXH_STATIC_LINKING_ONLY   /* XXH64_state_t */
#include "xxhash.h"      /* XXH64_* */
#define FSE_STATIC_LINKING_ONLY
#include "fse.h"
#define HUF_STATIC_LINKING_ONLY
#include "huf.h"
#include "zstd_internal.h"

#if defined(ZSTD_LEGACY_SUPPORT) && (ZSTD_LEGACY_SUPPORT>=1)
#  include "zstd_legacy.h"
#endif


/*-*************************************
*  Macros
***************************************/
#define ZSTD_isError ERR_isError   /* for inlining */
#define FSE_isError  ERR_isError
#define HUF_isError  ERR_isError


/*_*******************************************************
*  Memory operations
**********************************************************/
static void ZSTD_copy4(void* dst, const void* src) { memcpy(dst, src, 4); }


/*-*************************************************************
*   Context management
***************************************************************/
typedef enum { ZSTDds_getFrameHeaderSize, ZSTDds_decodeFrameHeader,
               ZSTDds_decodeBlockHeader, ZSTDds_decompressBlock,
               ZSTDds_decompressLastBlock, ZSTDds_checkChecksum,
               ZSTDds_decodeSkippableHeader, ZSTDds_skipFrame } ZSTD_dStage;

struct ZSTD_DCtx_s
{
    const FSE_DTable* LLTptr;
    const FSE_DTable* MLTptr;
    const FSE_DTable* OFTptr;
    const HUF_DTable* HUFptr;
    FSE_DTable LLTable[FSE_DTABLE_SIZE_U32(LLFSELog)];
    FSE_DTable OFTable[FSE_DTABLE_SIZE_U32(OffFSELog)];
    FSE_DTable MLTable[FSE_DTABLE_SIZE_U32(MLFSELog)];
    HUF_DTable hufTable[HUF_DTABLE_SIZE(HufLog)];  /* can accommodate HUF_decompress4X */
    const void* previousDstEnd;
    const void* base;
    const void* vBase;
    const void* dictEnd;
    size_t expected;
    U32 rep[ZSTD_REP_NUM];
    ZSTD_frameParams fParams;
    blockType_e bType;   /* used in ZSTD_decompressContinue(), to transfer blockType between header decoding and block decoding stages */
    ZSTD_dStage stage;
    U32 litEntropy;
    U32 fseEntropy;
    XXH64_state_t xxhState;
    size_t headerSize;
    U32 dictID;
    const BYTE* litPtr;
    ZSTD_customMem customMem;
    size_t litBufSize;
    size_t litSize;
    size_t rleSize;
    BYTE litBuffer[ZSTD_BLOCKSIZE_ABSOLUTEMAX + WILDCOPY_OVERLENGTH];
    BYTE headerBuffer[ZSTD_FRAMEHEADERSIZE_MAX];
};  /* typedef'd to ZSTD_DCtx within "zstd.h" */

size_t ZSTD_sizeof_DCtx (const ZSTD_DCtx* dctx) { if (dctx==NULL) return 0; return sizeof(ZSTD_DCtx); }  /* support sizeof on NULL */

size_t ZSTD_estimateDCtxSize(void) { return sizeof(ZSTD_DCtx); }

size_t ZSTD_decompressBegin(ZSTD_DCtx* dctx)
{
    dctx->expected = ZSTD_frameHeaderSize_prefix;
    dctx->stage = ZSTDds_getFrameHeaderSize;
    dctx->previousDstEnd = NULL;
    dctx->base = NULL;
    dctx->vBase = NULL;
    dctx->dictEnd = NULL;
    dctx->hufTable[0] = (HUF_DTable)((HufLog)*0x1000001);  /* cover both little and big endian */
    dctx->litEntropy = dctx->fseEntropy = 0;
    dctx->dictID = 0;
    MEM_STATIC_ASSERT(sizeof(dctx->rep) == sizeof(repStartValue));
    memcpy(dctx->rep, repStartValue, sizeof(repStartValue));  /* initial repcodes */
    dctx->LLTptr = dctx->LLTable;
    dctx->MLTptr = dctx->MLTable;
    dctx->OFTptr = dctx->OFTable;
    dctx->HUFptr = dctx->hufTable;
    return 0;
}

ZSTD_DCtx* ZSTD_createDCtx_advanced(ZSTD_customMem customMem)
{
    ZSTD_DCtx* dctx;

    if (!customMem.customAlloc && !customMem.customFree) customMem = defaultCustomMem;
    if (!customMem.customAlloc || !customMem.customFree) return NULL;

    dctx = (ZSTD_DCtx*)ZSTD_malloc(sizeof(ZSTD_DCtx), customMem);
    if (!dctx) return NULL;
    memcpy(&dctx->customMem, &customMem, sizeof(customMem));
    ZSTD_decompressBegin(dctx);
    return dctx;
}

ZSTD_DCtx* ZSTD_createDCtx(void)
{
    return ZSTD_createDCtx_advanced(defaultCustomMem);
}

size_t ZSTD_freeDCtx(ZSTD_DCtx* dctx)
{
    if (dctx==NULL) return 0;   /* support free on NULL */
    ZSTD_free(dctx, dctx->customMem);
    return 0;   /* reserved as a potential error code in the future */
}

void ZSTD_copyDCtx(ZSTD_DCtx* dstDCtx, const ZSTD_DCtx* srcDCtx)
{
    size_t const workSpaceSize = (ZSTD_BLOCKSIZE_ABSOLUTEMAX+WILDCOPY_OVERLENGTH) + ZSTD_frameHeaderSize_max;
    memcpy(dstDCtx, srcDCtx, sizeof(ZSTD_DCtx) - workSpaceSize);  /* no need to copy workspace */
}

static void ZSTD_refDCtx(ZSTD_DCtx* dstDCtx, const ZSTD_DCtx* srcDCtx)
{
    ZSTD_decompressBegin(dstDCtx);  /* init */
    dstDCtx->dictEnd = srcDCtx->dictEnd;
    dstDCtx->vBase = srcDCtx->vBase;
    dstDCtx->base = srcDCtx->base;
    dstDCtx->previousDstEnd = srcDCtx->previousDstEnd;
    dstDCtx->dictID = srcDCtx->dictID;
    dstDCtx->litEntropy = srcDCtx->litEntropy;
    dstDCtx->fseEntropy = srcDCtx->fseEntropy;
    dstDCtx->LLTptr = srcDCtx->LLTable;
    dstDCtx->MLTptr = srcDCtx->MLTable;
    dstDCtx->OFTptr = srcDCtx->OFTable;
    dstDCtx->HUFptr = srcDCtx->hufTable;
    dstDCtx->rep[0] = srcDCtx->rep[0];
    dstDCtx->rep[1] = srcDCtx->rep[1];
    dstDCtx->rep[2] = srcDCtx->rep[2];
}


/*-*************************************************************
*   Decompression section
***************************************************************/

/* See compression format details in : zstd_compression_format.md */

/** ZSTD_frameHeaderSize() :
*   srcSize must be >= ZSTD_frameHeaderSize_prefix.
*   @return : size of the Frame Header */
static size_t ZSTD_frameHeaderSize(const void* src, size_t srcSize)
{
    if (srcSize < ZSTD_frameHeaderSize_prefix) return ERROR(srcSize_wrong);
    {   BYTE const fhd = ((const BYTE*)src)[4];
        U32 const dictID= fhd & 3;
        U32 const singleSegment = (fhd >> 5) & 1;
        U32 const fcsId = fhd >> 6;
        return ZSTD_frameHeaderSize_prefix + !singleSegment + ZSTD_did_fieldSize[dictID] + ZSTD_fcs_fieldSize[fcsId]
                + (singleSegment && !fcsId);
    }
}


/** ZSTD_getFrameParams() :
*   decode Frame Header, or require larger `srcSize`.
*   @return : 0, `fparamsPtr` is correctly filled,
*            >0, `srcSize` is too small, result is expected `srcSize`,
*             or an error code, which can be tested using ZSTD_isError() */
size_t ZSTD_getFrameParams(ZSTD_frameParams* fparamsPtr, const void* src, size_t srcSize)
{
    const BYTE* ip = (const BYTE*)src;

    if (srcSize < ZSTD_frameHeaderSize_prefix) return ZSTD_frameHeaderSize_prefix;
    if (MEM_readLE32(src) != ZSTD_MAGICNUMBER) {
        if ((MEM_readLE32(src) & 0xFFFFFFF0U) == ZSTD_MAGIC_SKIPPABLE_START) {
            if (srcSize < ZSTD_skippableHeaderSize) return ZSTD_skippableHeaderSize; /* magic number + skippable frame length */
            memset(fparamsPtr, 0, sizeof(*fparamsPtr));
            fparamsPtr->frameContentSize = MEM_readLE32((const char *)src + 4);
            fparamsPtr->windowSize = 0; /* windowSize==0 means a frame is skippable */
            return 0;
        }
        return ERROR(prefix_unknown);
    }

    /* ensure there is enough `srcSize` to fully read/decode frame header */
    { size_t const fhsize = ZSTD_frameHeaderSize(src, srcSize);
      if (srcSize < fhsize) return fhsize; }

    {   BYTE const fhdByte = ip[4];
        size_t pos = 5;
        U32 const dictIDSizeCode = fhdByte&3;
        U32 const checksumFlag = (fhdByte>>2)&1;
        U32 const singleSegment = (fhdByte>>5)&1;
        U32 const fcsID = fhdByte>>6;
        U32 const windowSizeMax = 1U << ZSTD_WINDOWLOG_MAX;
        U32 windowSize = 0;
        U32 dictID = 0;
        U64 frameContentSize = 0;
        if ((fhdByte & 0x08) != 0) return ERROR(frameParameter_unsupported);   /* reserved bits, which must be zero */
        if (!singleSegment) {
            BYTE const wlByte = ip[pos++];
            U32 const windowLog = (wlByte >> 3) + ZSTD_WINDOWLOG_ABSOLUTEMIN;
            if (windowLog > ZSTD_WINDOWLOG_MAX) return ERROR(frameParameter_unsupported);
            windowSize = (1U << windowLog);
            windowSize += (windowSize >> 3) * (wlByte&7);
        }

        switch(dictIDSizeCode)
        {
            default:   /* impossible */
            case 0 : break;
            case 1 : dictID = ip[pos]; pos++; break;
            case 2 : dictID = MEM_readLE16(ip+pos); pos+=2; break;
            case 3 : dictID = MEM_readLE32(ip+pos); pos+=4; break;
        }
        switch(fcsID)
        {
            default:   /* impossible */
            case 0 : if (singleSegment) frameContentSize = ip[pos]; break;
            case 1 : frameContentSize = MEM_readLE16(ip+pos)+256; break;
            case 2 : frameContentSize = MEM_readLE32(ip+pos); break;
            case 3 : frameContentSize = MEM_readLE64(ip+pos); break;
        }
        if (!windowSize) windowSize = (U32)frameContentSize;
        if (windowSize > windowSizeMax) return ERROR(frameParameter_unsupported);
        fparamsPtr->frameContentSize = frameContentSize;
        fparamsPtr->windowSize = windowSize;
        fparamsPtr->dictID = dictID;
        fparamsPtr->checksumFlag = checksumFlag;
    }
    return 0;
}


/** ZSTD_getDecompressedSize() :
*   compatible with legacy mode
*   @return : decompressed size if known, 0 otherwise
              note : 0 can mean any of the following :
                   - decompressed size is not present within frame header
                   - frame header unknown / not supported
                   - frame header not complete (`srcSize` too small) */
unsigned long long ZSTD_getDecompressedSize(const void* src, size_t srcSize)
{
#if defined(ZSTD_LEGACY_SUPPORT) && (ZSTD_LEGACY_SUPPORT==1)
    if (ZSTD_isLegacy(src, srcSize)) return ZSTD_getDecompressedSize_legacy(src, srcSize);
#endif
    {   ZSTD_frameParams fparams;
        size_t const frResult = ZSTD_getFrameParams(&fparams, src, srcSize);
        if (frResult!=0) return 0;
        return fparams.frameContentSize;
    }
}


/** ZSTD_decodeFrameHeader() :
*   `srcSize` must be the size provided by ZSTD_frameHeaderSize().
*   @return : 0 if success, or an error code, which can be tested using ZSTD_isError() */
static size_t ZSTD_decodeFrameHeader(ZSTD_DCtx* dctx, const void* src, size_t srcSize)
{
    size_t const result = ZSTD_getFrameParams(&(dctx->fParams), src, srcSize);
    if (dctx->fParams.dictID && (dctx->dictID != dctx->fParams.dictID)) return ERROR(dictionary_wrong);
    if (dctx->fParams.checksumFlag) XXH64_reset(&dctx->xxhState, 0);
    return result;
}


typedef struct
{
    blockType_e blockType;
    U32 lastBlock;
    U32 origSize;
} blockProperties_t;

/*! ZSTD_getcBlockSize() :
*   Provides the size of compressed block from block header `src` */
size_t ZSTD_getcBlockSize(const void* src, size_t srcSize, blockProperties_t* bpPtr)
{
    if (srcSize < ZSTD_blockHeaderSize) return ERROR(srcSize_wrong);
    {   U32 const cBlockHeader = MEM_readLE24(src);
        U32 const cSize = cBlockHeader >> 3;
        bpPtr->lastBlock = cBlockHeader & 1;
        bpPtr->blockType = (blockType_e)((cBlockHeader >> 1) & 3);
        bpPtr->origSize = cSize;   /* only useful for RLE */
        if (bpPtr->blockType == bt_rle) return 1;
        if (bpPtr->blockType == bt_reserved) return ERROR(corruption_detected);
        return cSize;
    }
}


static size_t ZSTD_copyRawBlock(void* dst, size_t dstCapacity, const void* src, size_t srcSize)
{
    if (srcSize > dstCapacity) return ERROR(dstSize_tooSmall);
    memcpy(dst, src, srcSize);
    return srcSize;
}


static size_t ZSTD_setRleBlock(void* dst, size_t dstCapacity, const void* src, size_t srcSize, size_t regenSize)
{
    if (srcSize != 1) return ERROR(srcSize_wrong);
    if (regenSize > dstCapacity) return ERROR(dstSize_tooSmall);
    memset(dst, *(const BYTE*)src, regenSize);
    return regenSize;
}

/*! ZSTD_decodeLiteralsBlock() :
    @return : nb of bytes read from src (< srcSize ) */
size_t ZSTD_decodeLiteralsBlock(ZSTD_DCtx* dctx,
                          const void* src, size_t srcSize)   /* note : srcSize < BLOCKSIZE */
{
    if (srcSize < MIN_CBLOCK_SIZE) return ERROR(corruption_detected);

    {   const BYTE* const istart = (const BYTE*) src;
        symbolEncodingType_e const litEncType = (symbolEncodingType_e)(istart[0] & 3);

        switch(litEncType)
        {
        case set_repeat:
            if (dctx->litEntropy==0) return ERROR(dictionary_corrupted);
            /* fall-through */
        case set_compressed:
            if (srcSize < 5) return ERROR(corruption_detected);   /* srcSize >= MIN_CBLOCK_SIZE == 3; here we need up to 5 for case 3 */
            {   size_t lhSize, litSize, litCSize;
                U32 singleStream=0;
                U32 const lhlCode = (istart[0] >> 2) & 3;
                U32 const lhc = MEM_readLE32(istart);
                switch(lhlCode)
                {
                case 0: case 1: default:   /* note : default is impossible, since lhlCode into [0..3] */
                    /* 2 - 2 - 10 - 10 */
                    singleStream = !lhlCode;
                    lhSize = 3;
                    litSize  = (lhc >> 4) & 0x3FF;
                    litCSize = (lhc >> 14) & 0x3FF;
                    break;
                case 2:
                    /* 2 - 2 - 14 - 14 */
                    lhSize = 4;
                    litSize  = (lhc >> 4) & 0x3FFF;
                    litCSize = lhc >> 18;
                    break;
                case 3:
                    /* 2 - 2 - 18 - 18 */
                    lhSize = 5;
                    litSize  = (lhc >> 4) & 0x3FFFF;
                    litCSize = (lhc >> 22) + (istart[4] << 10);
                    break;
                }
                if (litSize > ZSTD_BLOCKSIZE_ABSOLUTEMAX) return ERROR(corruption_detected);
                if (litCSize + lhSize > srcSize) return ERROR(corruption_detected);

                if (HUF_isError((litEncType==set_repeat) ?
                                    ( singleStream ?
                                        HUF_decompress1X_usingDTable(dctx->litBuffer, litSize, istart+lhSize, litCSize, dctx->HUFptr) :
                                        HUF_decompress4X_usingDTable(dctx->litBuffer, litSize, istart+lhSize, litCSize, dctx->HUFptr) ) :
                                    ( singleStream ?
                                        HUF_decompress1X2_DCtx(dctx->hufTable, dctx->litBuffer, litSize, istart+lhSize, litCSize) :
                                        HUF_decompress4X_hufOnly (dctx->hufTable, dctx->litBuffer, litSize, istart+lhSize, litCSize)) ))
                    return ERROR(corruption_detected);

                dctx->litPtr = dctx->litBuffer;
                dctx->litBufSize = ZSTD_BLOCKSIZE_ABSOLUTEMAX+WILDCOPY_OVERLENGTH;
                dctx->litSize = litSize;
                dctx->litEntropy = 1;
                if (litEncType==set_compressed) dctx->HUFptr = dctx->hufTable;
                return litCSize + lhSize;
            }

        case set_basic:
            {   size_t litSize, lhSize;
                U32 const lhlCode = ((istart[0]) >> 2) & 3;
                switch(lhlCode)
                {
                case 0: case 2: default:   /* note : default is impossible, since lhlCode into [0..3] */
                    lhSize = 1;
                    litSize = istart[0] >> 3;
                    break;
                case 1:
                    lhSize = 2;
                    litSize = MEM_readLE16(istart) >> 4;
                    break;
                case 3:
                    lhSize = 3;
                    litSize = MEM_readLE24(istart) >> 4;
                    break;
                }

                if (lhSize+litSize+WILDCOPY_OVERLENGTH > srcSize) {  /* risk reading beyond src buffer with wildcopy */
                    if (litSize+lhSize > srcSize) return ERROR(corruption_detected);
                    memcpy(dctx->litBuffer, istart+lhSize, litSize);
                    dctx->litPtr = dctx->litBuffer;
                    dctx->litBufSize = ZSTD_BLOCKSIZE_ABSOLUTEMAX+8;
                    dctx->litSize = litSize;
                    return lhSize+litSize;
                }
                /* direct reference into compressed stream */
                dctx->litPtr = istart+lhSize;
                dctx->litBufSize = srcSize-lhSize;
                dctx->litSize = litSize;
                return lhSize+litSize;
            }

        case set_rle:
            {   U32 const lhlCode = ((istart[0]) >> 2) & 3;
                size_t litSize, lhSize;
                switch(lhlCode)
                {
                case 0: case 2: default:   /* note : default is impossible, since lhlCode into [0..3] */
                    lhSize = 1;
                    litSize = istart[0] >> 3;
                    break;
                case 1:
                    lhSize = 2;
                    litSize = MEM_readLE16(istart) >> 4;
                    break;
                case 3:
                    lhSize = 3;
                    litSize = MEM_readLE24(istart) >> 4;
                    if (srcSize<4) return ERROR(corruption_detected);   /* srcSize >= MIN_CBLOCK_SIZE == 3; here we need lhSize+1 = 4 */
                    break;
                }
                if (litSize > ZSTD_BLOCKSIZE_ABSOLUTEMAX) return ERROR(corruption_detected);
                memset(dctx->litBuffer, istart[lhSize], litSize);
                dctx->litPtr = dctx->litBuffer;
                dctx->litBufSize = ZSTD_BLOCKSIZE_ABSOLUTEMAX+WILDCOPY_OVERLENGTH;
                dctx->litSize = litSize;
                return lhSize+1;
            }
        default:
            return ERROR(corruption_detected);   /* impossible */
        }
    }
}


typedef union {
    FSE_decode_t realData;
    U32 alignedBy4;
} FSE_decode_t4;

static const FSE_decode_t4 LL_defaultDTable[(1<<LL_DEFAULTNORMLOG)+1] = {
    { { LL_DEFAULTNORMLOG, 1, 1 } }, /* header : tableLog, fastMode, fastMode */
    { {  0,  0,  4 } },              /* 0 : base, symbol, bits */
    { { 16,  0,  4 } },
    { { 32,  1,  5 } },
    { {  0,  3,  5 } },
    { {  0,  4,  5 } },
    { {  0,  6,  5 } },
    { {  0,  7,  5 } },
    { {  0,  9,  5 } },
    { {  0, 10,  5 } },
    { {  0, 12,  5 } },
    { {  0, 14,  6 } },
    { {  0, 16,  5 } },
    { {  0, 18,  5 } },
    { {  0, 19,  5 } },
    { {  0, 21,  5 } },
    { {  0, 22,  5 } },
    { {  0, 24,  5 } },
    { { 32, 25,  5 } },
    { {  0, 26,  5 } },
    { {  0, 27,  6 } },
    { {  0, 29,  6 } },
    { {  0, 31,  6 } },
    { { 32,  0,  4 } },
    { {  0,  1,  4 } },
    { {  0,  2,  5 } },
    { { 32,  4,  5 } },
    { {  0,  5,  5 } },
    { { 32,  7,  5 } },
    { {  0,  8,  5 } },
    { { 32, 10,  5 } },
    { {  0, 11,  5 } },
    { {  0, 13,  6 } },
    { { 32, 16,  5 } },
    { {  0, 17,  5 } },
    { { 32, 19,  5 } },
    { {  0, 20,  5 } },
    { { 32, 22,  5 } },
    { {  0, 23,  5 } },
    { {  0, 25,  4 } },
    { { 16, 25,  4 } },
    { { 32, 26,  5 } },
    { {  0, 28,  6 } },
    { {  0, 30,  6 } },
    { { 48,  0,  4 } },
    { { 16,  1,  4 } },
    { { 32,  2,  5 } },
    { { 32,  3,  5 } },
    { { 32,  5,  5 } },
    { { 32,  6,  5 } },
    { { 32,  8,  5 } },
    { { 32,  9,  5 } },
    { { 32, 11,  5 } },
    { { 32, 12,  5 } },
    { {  0, 15,  6 } },
    { { 32, 17,  5 } },
    { { 32, 18,  5 } },
    { { 32, 20,  5 } },
    { { 32, 21,  5 } },
    { { 32, 23,  5 } },
    { { 32, 24,  5 } },
    { {  0, 35,  6 } },
    { {  0, 34,  6 } },
    { {  0, 33,  6 } },
    { {  0, 32,  6 } },
};   /* LL_defaultDTable */

static const FSE_decode_t4 ML_defaultDTable[(1<<ML_DEFAULTNORMLOG)+1] = {
    { { ML_DEFAULTNORMLOG, 1, 1 } }, /* header : tableLog, fastMode, fastMode */
    { {  0,  0,  6 } },              /* 0 : base, symbol, bits */
    { {  0,  1,  4 } },
    { { 32,  2,  5 } },
    { {  0,  3,  5 } },
    { {  0,  5,  5 } },
    { {  0,  6,  5 } },
    { {  0,  8,  5 } },
    { {  0, 10,  6 } },
    { {  0, 13,  6 } },
    { {  0, 16,  6 } },
    { {  0, 19,  6 } },
    { {  0, 22,  6 } },
    { {  0, 25,  6 } },
    { {  0, 28,  6 } },
    { {  0, 31,  6 } },
    { {  0, 33,  6 } },
    { {  0, 35,  6 } },
    { {  0, 37,  6 } },
    { {  0, 39,  6 } },
    { {  0, 41,  6 } },
    { {  0, 43,  6 } },
    { {  0, 45,  6 } },
    { { 16,  1,  4 } },
    { {  0,  2,  4 } },
    { { 32,  3,  5 } },
    { {  0,  4,  5 } },
    { { 32,  6,  5 } },
    { {  0,  7,  5 } },
    { {  0,  9,  6 } },
    { {  0, 12,  6 } },
    { {  0, 15,  6 } },
    { {  0, 18,  6 } },
    { {  0, 21,  6 } },
    { {  0, 24,  6 } },
    { {  0, 27,  6 } },
    { {  0, 30,  6 } },
    { {  0, 32,  6 } },
    { {  0, 34,  6 } },
    { {  0, 36,  6 } },
    { {  0, 38,  6 } },
    { {  0, 40,  6 } },
    { {  0, 42,  6 } },
    { {  0, 44,  6 } },
    { { 32,  1,  4 } },
    { { 48,  1,  4 } },
    { { 16,  2,  4 } },
    { { 32,  4,  5 } },
    { { 32,  5,  5 } },
    { { 32,  7,  5 } },
    { { 32,  8,  5 } },
    { {  0, 11,  6 } },
    { {  0, 14,  6 } },
    { {  0, 17,  6 } },
    { {  0, 20,  6 } },
    { {  0, 23,  6 } },
    { {  0, 26,  6 } },
    { {  0, 29,  6 } },
    { {  0, 52,  6 } },
    { {  0, 51,  6 } },
    { {  0, 50,  6 } },
    { {  0, 49,  6 } },
    { {  0, 48,  6 } },
    { {  0, 47,  6 } },
    { {  0, 46,  6 } },
};   /* ML_defaultDTable */

static const FSE_decode_t4 OF_defaultDTable[(1<<OF_DEFAULTNORMLOG)+1] = {
    { { OF_DEFAULTNORMLOG, 1, 1 } }, /* header : tableLog, fastMode, fastMode */
    { {  0,  0,  5 } },              /* 0 : base, symbol, bits */
    { {  0,  6,  4 } },
    { {  0,  9,  5 } },
    { {  0, 15,  5 } },
    { {  0, 21,  5 } },
    { {  0,  3,  5 } },
    { {  0,  7,  4 } },
    { {  0, 12,  5 } },
    { {  0, 18,  5 } },
    { {  0, 23,  5 } },
    { {  0,  5,  5 } },
    { {  0,  8,  4 } },
    { {  0, 14,  5 } },
    { {  0, 20,  5 } },
    { {  0,  2,  5 } },
    { { 16,  7,  4 } },
    { {  0, 11,  5 } },
    { {  0, 17,  5 } },
    { {  0, 22,  5 } },
    { {  0,  4,  5 } },
    { { 16,  8,  4 } },
    { {  0, 13,  5 } },
    { {  0, 19,  5 } },
    { {  0,  1,  5 } },
    { { 16,  6,  4 } },
    { {  0, 10,  5 } },
    { {  0, 16,  5 } },
    { {  0, 28,  5 } },
    { {  0, 27,  5 } },
    { {  0, 26,  5 } },
    { {  0, 25,  5 } },
    { {  0, 24,  5 } },
};   /* OF_defaultDTable */

/*! ZSTD_buildSeqTable() :
    @return : nb bytes read from src,
              or an error code if it fails, testable with ZSTD_isError()
*/
static size_t ZSTD_buildSeqTable(FSE_DTable* DTableSpace, const FSE_DTable** DTablePtr,
                                 symbolEncodingType_e type, U32 max, U32 maxLog,
                                 const void* src, size_t srcSize,
                                 const FSE_decode_t4* defaultTable, U32 flagRepeatTable)
{
    const void* const tmpPtr = defaultTable;   /* bypass strict aliasing */
    switch(type)
    {
    case set_rle :
        if (!srcSize) return ERROR(srcSize_wrong);
        if ( (*(const BYTE*)src) > max) return ERROR(corruption_detected);
        FSE_buildDTable_rle(DTableSpace, *(const BYTE*)src);
        *DTablePtr = DTableSpace;
        return 1;
    case set_basic :
        *DTablePtr = (const FSE_DTable*)tmpPtr;
        return 0;
    case set_repeat:
        if (!flagRepeatTable) return ERROR(corruption_detected);
        return 0;
    default :   /* impossible */
    case set_compressed :
        {   U32 tableLog;
            S16 norm[MaxSeq+1];
            size_t const headerSize = FSE_readNCount(norm, &max, &tableLog, src, srcSize);
            if (FSE_isError(headerSize)) return ERROR(corruption_detected);
            if (tableLog > maxLog) return ERROR(corruption_detected);
            FSE_buildDTable(DTableSpace, norm, max, tableLog);
            *DTablePtr = DTableSpace;
            return headerSize;
    }   }
}

size_t ZSTD_decodeSeqHeaders(ZSTD_DCtx* dctx, int* nbSeqPtr,
                             const void* src, size_t srcSize)
{
    const BYTE* const istart = (const BYTE* const)src;
    const BYTE* const iend = istart + srcSize;
    const BYTE* ip = istart;

    /* check */
    if (srcSize < MIN_SEQUENCES_SIZE) return ERROR(srcSize_wrong);

    /* SeqHead */
    {   int nbSeq = *ip++;
        if (!nbSeq) { *nbSeqPtr=0; return 1; }
        if (nbSeq > 0x7F) {
            if (nbSeq == 0xFF)
                nbSeq = MEM_readLE16(ip) + LONGNBSEQ, ip+=2;
            else
                nbSeq = ((nbSeq-0x80)<<8) + *ip++;
        }
        *nbSeqPtr = nbSeq;
    }

    /* FSE table descriptors */
    if (ip+4 > iend) return ERROR(srcSize_wrong); /* minimum possible size */
    {   symbolEncodingType_e const LLtype = (symbolEncodingType_e)(*ip >> 6);
        symbolEncodingType_e const OFtype = (symbolEncodingType_e)((*ip >> 4) & 3);
        symbolEncodingType_e const MLtype = (symbolEncodingType_e)((*ip >> 2) & 3);
        ip++;

        /* Build DTables */
        {   size_t const llhSize = ZSTD_buildSeqTable(dctx->LLTable, &dctx->LLTptr,
                                                      LLtype, MaxLL, LLFSELog,
                                                      ip, iend-ip, LL_defaultDTable, dctx->fseEntropy);
            if (ZSTD_isError(llhSize)) return ERROR(corruption_detected);
            ip += llhSize;
        }
        {   size_t const ofhSize = ZSTD_buildSeqTable(dctx->OFTable, &dctx->OFTptr,
                                                      OFtype, MaxOff, OffFSELog,
                                                      ip, iend-ip, OF_defaultDTable, dctx->fseEntropy);
            if (ZSTD_isError(ofhSize)) return ERROR(corruption_detected);
            ip += ofhSize;
        }
        {   size_t const mlhSize = ZSTD_buildSeqTable(dctx->MLTable, &dctx->MLTptr,
                                                      MLtype, MaxML, MLFSELog,
                                                      ip, iend-ip, ML_defaultDTable, dctx->fseEntropy);
            if (ZSTD_isError(mlhSize)) return ERROR(corruption_detected);
            ip += mlhSize;
        }
    }

    return ip-istart;
}


typedef struct {
    size_t litLength;
    size_t matchLength;
    size_t offset;
} seq_t;

typedef struct {
    BIT_DStream_t DStream;
    FSE_DState_t stateLL;
    FSE_DState_t stateOffb;
    FSE_DState_t stateML;
    size_t prevOffset[ZSTD_REP_NUM];
} seqState_t;


static seq_t ZSTD_decodeSequence(seqState_t* seqState)
{
    seq_t seq;

    U32 const llCode = FSE_peekSymbol(&seqState->stateLL);
    U32 const mlCode = FSE_peekSymbol(&seqState->stateML);
    U32 const ofCode = FSE_peekSymbol(&seqState->stateOffb);   /* <= maxOff, by table construction */

    U32 const llBits = LL_bits[llCode];
    U32 const mlBits = ML_bits[mlCode];
    U32 const ofBits = ofCode;
    U32 const totalBits = llBits+mlBits+ofBits;

    static const U32 LL_base[MaxLL+1] = {
                             0,  1,  2,  3,  4,  5,  6,  7,  8,  9,   10,    11,    12,    13,    14,     15,
                            16, 18, 20, 22, 24, 28, 32, 40, 48, 64, 0x80, 0x100, 0x200, 0x400, 0x800, 0x1000,
                            0x2000, 0x4000, 0x8000, 0x10000 };

    static const U32 ML_base[MaxML+1] = {
                             3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13,   14,    15,    16,    17,    18,
                            19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29,   30,    31,    32,    33,    34,
                            35, 37, 39, 41, 43, 47, 51, 59, 67, 83, 99, 0x83, 0x103, 0x203, 0x403, 0x803,
                            0x1003, 0x2003, 0x4003, 0x8003, 0x10003 };

    static const U32 OF_base[MaxOff+1] = {
                 0,        1,       1,       5,     0xD,     0x1D,     0x3D,     0x7D,
                 0xFD,   0x1FD,   0x3FD,   0x7FD,   0xFFD,   0x1FFD,   0x3FFD,   0x7FFD,
                 0xFFFD, 0x1FFFD, 0x3FFFD, 0x7FFFD, 0xFFFFD, 0x1FFFFD, 0x3FFFFD, 0x7FFFFD,
                 0xFFFFFD, 0x1FFFFFD, 0x3FFFFFD, 0x7FFFFFD, 0xFFFFFFD };

    /* sequence */
    {   size_t offset;
        if (!ofCode)
            offset = 0;
        else {
            offset = OF_base[ofCode] + BIT_readBits(&seqState->DStream, ofBits);   /* <=  (ZSTD_WINDOWLOG_MAX-1) bits */
            if (MEM_32bits()) BIT_reloadDStream(&seqState->DStream);
        }

        if (ofCode <= 1) {
            offset += (llCode==0);
            if (offset) {
                size_t const temp = (offset==3) ? seqState->prevOffset[0] - 1 : seqState->prevOffset[offset];
                if (offset != 1) seqState->prevOffset[2] = seqState->prevOffset[1];
                seqState->prevOffset[1] = seqState->prevOffset[0];
                seqState->prevOffset[0] = offset = temp;
            } else {
                offset = seqState->prevOffset[0];
            }
        } else {
            seqState->prevOffset[2] = seqState->prevOffset[1];
            seqState->prevOffset[1] = seqState->prevOffset[0];
            seqState->prevOffset[0] = offset;
        }
        seq.offset = offset;
    }

    seq.matchLength = ML_base[mlCode] + ((mlCode>31) ? BIT_readBits(&seqState->DStream, mlBits) : 0);  /* <=  16 bits */
    if (MEM_32bits() && (mlBits+llBits>24)) BIT_reloadDStream(&seqState->DStream);

    seq.litLength = LL_base[llCode] + ((llCode>15) ? BIT_readBits(&seqState->DStream, llBits) : 0);    /* <=  16 bits */
    if (MEM_32bits() ||
       (totalBits > 64 - 7 - (LLFSELog+MLFSELog+OffFSELog)) ) BIT_reloadDStream(&seqState->DStream);

    /* ANS state update */
    FSE_updateState(&seqState->stateLL, &seqState->DStream);    /* <=  9 bits */
    FSE_updateState(&seqState->stateML, &seqState->DStream);    /* <=  9 bits */
    if (MEM_32bits()) BIT_reloadDStream(&seqState->DStream);    /* <= 18 bits */
    FSE_updateState(&seqState->stateOffb, &seqState->DStream);  /* <=  8 bits */

    return seq;
}


FORCE_INLINE
size_t ZSTD_execSequence(BYTE* op,
                                BYTE* const oend, seq_t sequence,
                                const BYTE** litPtr, const BYTE* const litLimit_w,
                                const BYTE* const base, const BYTE* const vBase, const BYTE* const dictEnd)
{
    BYTE* const oLitEnd = op + sequence.litLength;
    size_t const sequenceLength = sequence.litLength + sequence.matchLength;
    BYTE* const oMatchEnd = op + sequenceLength;   /* risk : address space overflow (32-bits) */
    BYTE* const oend_w = oend - WILDCOPY_OVERLENGTH;
    const BYTE* const iLitEnd = *litPtr + sequence.litLength;
    const BYTE* match = oLitEnd - sequence.offset;

    /* check */
    if ((oLitEnd>oend_w) | (oMatchEnd>oend)) return ERROR(dstSize_tooSmall); /* last match must start at a minimum distance of WILDCOPY_OVERLENGTH from oend */
    if (iLitEnd > litLimit_w) return ERROR(corruption_detected);   /* over-read beyond lit buffer */

    /* copy Literals */
    ZSTD_copy8(op, *litPtr);
    if (sequence.litLength > 8)
        ZSTD_wildcopy(op+8, (*litPtr)+8, sequence.litLength - 8);   /* note : since oLitEnd <= oend-WILDCOPY_OVERLENGTH, no risk of overwrite beyond oend */
    op = oLitEnd;
    *litPtr = iLitEnd;   /* update for next sequence */

    /* copy Match */
    if (sequence.offset > (size_t)(oLitEnd - base)) {
        /* offset beyond prefix */
        if (sequence.offset > (size_t)(oLitEnd - vBase)) return ERROR(corruption_detected);
        match = dictEnd - (base-match);
        if (match + sequence.matchLength <= dictEnd) {
            memmove(oLitEnd, match, sequence.matchLength);
            return sequenceLength;
        }
        /* span extDict & currentPrefixSegment */
        {   size_t const length1 = dictEnd - match;
            memmove(oLitEnd, match, length1);
            op = oLitEnd + length1;
            sequence.matchLength -= length1;
            match = base;
    }   }

    /* match within prefix */
    if (sequence.offset < 8) {
        /* close range match, overlap */
        static const U32 dec32table[] = { 0, 1, 2, 1, 4, 4, 4, 4 };   /* added */
        static const int dec64table[] = { 8, 8, 8, 7, 8, 9,10,11 };   /* substracted */
        int const sub2 = dec64table[sequence.offset];
        op[0] = match[0];
        op[1] = match[1];
        op[2] = match[2];
        op[3] = match[3];
        match += dec32table[sequence.offset];
        ZSTD_copy4(op+4, match);
        match -= sub2;
    } else {
        ZSTD_copy8(op, match);
    }
    op += 8; match += 8;

    if (oMatchEnd > oend-(16-MINMATCH)) {
        if (op < oend_w) {
            ZSTD_wildcopy(op, match, oend_w - op);
            match += oend_w - op;
            op = oend_w;
        }
        while (op < oMatchEnd) *op++ = *match++;
    } else {
        ZSTD_wildcopy(op, match, sequence.matchLength-8);   /* works even if matchLength < 8 */
    }
    return sequenceLength;
}


static size_t ZSTD_decompressSequences(
                               ZSTD_DCtx* dctx,
                               void* dst, size_t maxDstSize,
                         const void* seqStart, size_t seqSize)
{
    const BYTE* ip = (const BYTE*)seqStart;
    const BYTE* const iend = ip + seqSize;
    BYTE* const ostart = (BYTE* const)dst;
    BYTE* const oend = ostart + maxDstSize;
    BYTE* op = ostart;
    const BYTE* litPtr = dctx->litPtr;
    const BYTE* const litLimit_w = litPtr + dctx->litBufSize - WILDCOPY_OVERLENGTH;
    const BYTE* const litEnd = litPtr + dctx->litSize;
    const BYTE* const base = (const BYTE*) (dctx->base);
    const BYTE* const vBase = (const BYTE*) (dctx->vBase);
    const BYTE* const dictEnd = (const BYTE*) (dctx->dictEnd);
    int nbSeq;

    /* Build Decoding Tables */
    {   size_t const seqHSize = ZSTD_decodeSeqHeaders(dctx, &nbSeq, ip, seqSize);
        if (ZSTD_isError(seqHSize)) return seqHSize;
        ip += seqHSize;
    }

    /* Regen sequences */
    if (nbSeq) {
        seqState_t seqState;
        dctx->fseEntropy = 1;
        { U32 i; for (i=0; i<ZSTD_REP_NUM; i++) seqState.prevOffset[i] = dctx->rep[i]; }
        CHECK_E(BIT_initDStream(&seqState.DStream, ip, iend-ip), corruption_detected);
        FSE_initDState(&seqState.stateLL, &seqState.DStream, dctx->LLTptr);
        FSE_initDState(&seqState.stateOffb, &seqState.DStream, dctx->OFTptr);
        FSE_initDState(&seqState.stateML, &seqState.DStream, dctx->MLTptr);

        for ( ; (BIT_reloadDStream(&(seqState.DStream)) <= BIT_DStream_completed) && nbSeq ; ) {
            nbSeq--;
            {   seq_t const sequence = ZSTD_decodeSequence(&seqState);
                size_t const oneSeqSize = ZSTD_execSequence(op, oend, sequence, &litPtr, litLimit_w, base, vBase, dictEnd);
                if (ZSTD_isError(oneSeqSize)) return oneSeqSize;
                op += oneSeqSize;
        }   }

        /* check if reached exact end */
        if (nbSeq) return ERROR(corruption_detected);
        /* save reps for next block */
        { U32 i; for (i=0; i<ZSTD_REP_NUM; i++) dctx->rep[i] = (U32)(seqState.prevOffset[i]); }
    }

    /* last literal segment */
    {   size_t const lastLLSize = litEnd - litPtr;
        if (lastLLSize > (size_t)(oend-op)) return ERROR(dstSize_tooSmall);
        memcpy(op, litPtr, lastLLSize);
        op += lastLLSize;
    }

    return op-ostart;
}


static void ZSTD_checkContinuity(ZSTD_DCtx* dctx, const void* dst)
{
    if (dst != dctx->previousDstEnd) {   /* not contiguous */
        dctx->dictEnd = dctx->previousDstEnd;
        dctx->vBase = (const char*)dst - ((const char*)(dctx->previousDstEnd) - (const char*)(dctx->base));
        dctx->base = dst;
        dctx->previousDstEnd = dst;
    }
}


static size_t ZSTD_decompressBlock_internal(ZSTD_DCtx* dctx,
                            void* dst, size_t dstCapacity,
                      const void* src, size_t srcSize)
{   /* blockType == blockCompressed */
    const BYTE* ip = (const BYTE*)src;

    if (srcSize >= ZSTD_BLOCKSIZE_ABSOLUTEMAX) return ERROR(srcSize_wrong);

    /* Decode literals sub-block */
    {   size_t const litCSize = ZSTD_decodeLiteralsBlock(dctx, src, srcSize);
        if (ZSTD_isError(litCSize)) return litCSize;
        ip += litCSize;
        srcSize -= litCSize;
    }
    return ZSTD_decompressSequences(dctx, dst, dstCapacity, ip, srcSize);
}


size_t ZSTD_decompressBlock(ZSTD_DCtx* dctx,
                            void* dst, size_t dstCapacity,
                      const void* src, size_t srcSize)
{
    size_t dSize;
    ZSTD_checkContinuity(dctx, dst);
    dSize = ZSTD_decompressBlock_internal(dctx, dst, dstCapacity, src, srcSize);
    dctx->previousDstEnd = (char*)dst + dSize;
    return dSize;
}


/** ZSTD_insertBlock() :
    insert `src` block into `dctx` history. Useful to track uncompressed blocks. */
ZSTDLIB_API size_t ZSTD_insertBlock(ZSTD_DCtx* dctx, const void* blockStart, size_t blockSize)
{
    ZSTD_checkContinuity(dctx, blockStart);
    dctx->previousDstEnd = (const char*)blockStart + blockSize;
    return blockSize;
}


size_t ZSTD_generateNxBytes(void* dst, size_t dstCapacity, BYTE byte, size_t length)
{
    if (length > dstCapacity) return ERROR(dstSize_tooSmall);
    memset(dst, byte, length);
    return length;
}


/*! ZSTD_decompressFrame() :
*   `dctx` must be properly initialized */
static size_t ZSTD_decompressFrame(ZSTD_DCtx* dctx,
                                 void* dst, size_t dstCapacity,
                                 const void* src, size_t srcSize)
{
    const BYTE* ip = (const BYTE*)src;
    BYTE* const ostart = (BYTE* const)dst;
    BYTE* const oend = ostart + dstCapacity;
    BYTE* op = ostart;
    size_t remainingSize = srcSize;

    /* check */
    if (srcSize < ZSTD_frameHeaderSize_min+ZSTD_blockHeaderSize) return ERROR(srcSize_wrong);

    /* Frame Header */
    {   size_t const frameHeaderSize = ZSTD_frameHeaderSize(src, ZSTD_frameHeaderSize_prefix);
        if (ZSTD_isError(frameHeaderSize)) return frameHeaderSize;
        if (srcSize < frameHeaderSize+ZSTD_blockHeaderSize) return ERROR(srcSize_wrong);
        CHECK_F(ZSTD_decodeFrameHeader(dctx, src, frameHeaderSize));
        ip += frameHeaderSize; remainingSize -= frameHeaderSize;
    }

    /* Loop on each block */
    while (1) {
        size_t decodedSize;
        blockProperties_t blockProperties;
        size_t const cBlockSize = ZSTD_getcBlockSize(ip, remainingSize, &blockProperties);
        if (ZSTD_isError(cBlockSize)) return cBlockSize;

        ip += ZSTD_blockHeaderSize;
        remainingSize -= ZSTD_blockHeaderSize;
        if (cBlockSize > remainingSize) return ERROR(srcSize_wrong);

        switch(blockProperties.blockType)
        {
        case bt_compressed:
            decodedSize = ZSTD_decompressBlock_internal(dctx, op, oend-op, ip, cBlockSize);
            break;
        case bt_raw :
            decodedSize = ZSTD_copyRawBlock(op, oend-op, ip, cBlockSize);
            break;
        case bt_rle :
            decodedSize = ZSTD_generateNxBytes(op, oend-op, *ip, blockProperties.origSize);
            break;
        case bt_reserved :
        default:
            return ERROR(corruption_detected);
        }

        if (ZSTD_isError(decodedSize)) return decodedSize;
        if (dctx->fParams.checksumFlag) XXH64_update(&dctx->xxhState, op, decodedSize);
        op += decodedSize;
        ip += cBlockSize;
        remainingSize -= cBlockSize;
        if (blockProperties.lastBlock) break;
    }

    if (dctx->fParams.checksumFlag) {   /* Frame content checksum verification */
        U32 const checkCalc = (U32)XXH64_digest(&dctx->xxhState);
        U32 checkRead;
        if (remainingSize<4) return ERROR(checksum_wrong);
        checkRead = MEM_readLE32(ip);
        if (checkRead != checkCalc) return ERROR(checksum_wrong);
        remainingSize -= 4;
    }

    if (remainingSize) return ERROR(srcSize_wrong);
    return op-ostart;
}


size_t ZSTD_decompress_usingDict(ZSTD_DCtx* dctx,
                                 void* dst, size_t dstCapacity,
                           const void* src, size_t srcSize,
                           const void* dict, size_t dictSize)
{
#if defined(ZSTD_LEGACY_SUPPORT) && (ZSTD_LEGACY_SUPPORT==1)
    if (ZSTD_isLegacy(src, srcSize)) return ZSTD_decompressLegacy(dst, dstCapacity, src, srcSize, dict, dictSize);
#endif
    ZSTD_decompressBegin_usingDict(dctx, dict, dictSize);
    ZSTD_checkContinuity(dctx, dst);
    return ZSTD_decompressFrame(dctx, dst, dstCapacity, src, srcSize);
}


size_t ZSTD_decompressDCtx(ZSTD_DCtx* dctx, void* dst, size_t dstCapacity, const void* src, size_t srcSize)
{
    return ZSTD_decompress_usingDict(dctx, dst, dstCapacity, src, srcSize, NULL, 0);
}


size_t ZSTD_decompress(void* dst, size_t dstCapacity, const void* src, size_t srcSize)
{
#if defined(ZSTD_HEAPMODE) && (ZSTD_HEAPMODE==1)
    size_t regenSize;
    ZSTD_DCtx* const dctx = ZSTD_createDCtx();
    if (dctx==NULL) return ERROR(memory_allocation);
    regenSize = ZSTD_decompressDCtx(dctx, dst, dstCapacity, src, srcSize);
    ZSTD_freeDCtx(dctx);
    return regenSize;
#else   /* stack mode */
    ZSTD_DCtx dctx;
    return ZSTD_decompressDCtx(&dctx, dst, dstCapacity, src, srcSize);
#endif
}


/*-**************************************
*   Advanced Streaming Decompression API
*   Bufferless and synchronous
****************************************/
size_t ZSTD_nextSrcSizeToDecompress(ZSTD_DCtx* dctx) { return dctx->expected; }

ZSTD_nextInputType_e ZSTD_nextInputType(ZSTD_DCtx* dctx) {
    switch(dctx->stage)
    {
    default:   /* should not happen */
    case ZSTDds_getFrameHeaderSize:
    case ZSTDds_decodeFrameHeader:
        return ZSTDnit_frameHeader;
    case ZSTDds_decodeBlockHeader:
        return ZSTDnit_blockHeader;
    case ZSTDds_decompressBlock:
        return ZSTDnit_block;
    case ZSTDds_decompressLastBlock:
        return ZSTDnit_lastBlock;
    case ZSTDds_checkChecksum:
        return ZSTDnit_checksum;
    case ZSTDds_decodeSkippableHeader:
    case ZSTDds_skipFrame:
        return ZSTDnit_skippableFrame;
    }
}

int ZSTD_isSkipFrame(ZSTD_DCtx* dctx) { return dctx->stage == ZSTDds_skipFrame; }   /* for zbuff */

/** ZSTD_decompressContinue() :
*   @return : nb of bytes generated into `dst` (necessarily <= `dstCapacity)
*             or an error code, which can be tested using ZSTD_isError() */
size_t ZSTD_decompressContinue(ZSTD_DCtx* dctx, void* dst, size_t dstCapacity, const void* src, size_t srcSize)
{
    /* Sanity check */
    if (srcSize != dctx->expected) return ERROR(srcSize_wrong);
    if (dstCapacity) ZSTD_checkContinuity(dctx, dst);

    switch (dctx->stage)
    {
    case ZSTDds_getFrameHeaderSize :
        if (srcSize != ZSTD_frameHeaderSize_prefix) return ERROR(srcSize_wrong);      /* impossible */
        if ((MEM_readLE32(src) & 0xFFFFFFF0U) == ZSTD_MAGIC_SKIPPABLE_START) {        /* skippable frame */
            memcpy(dctx->headerBuffer, src, ZSTD_frameHeaderSize_prefix);
            dctx->expected = ZSTD_skippableHeaderSize - ZSTD_frameHeaderSize_prefix;  /* magic number + skippable frame length */
            dctx->stage = ZSTDds_decodeSkippableHeader;
            return 0;
        }
        dctx->headerSize = ZSTD_frameHeaderSize(src, ZSTD_frameHeaderSize_prefix);
        if (ZSTD_isError(dctx->headerSize)) return dctx->headerSize;
        memcpy(dctx->headerBuffer, src, ZSTD_frameHeaderSize_prefix);
        if (dctx->headerSize > ZSTD_frameHeaderSize_prefix) {
            dctx->expected = dctx->headerSize - ZSTD_frameHeaderSize_prefix;
            dctx->stage = ZSTDds_decodeFrameHeader;
            return 0;
        }
        dctx->expected = 0;   /* not necessary to copy more */

    case ZSTDds_decodeFrameHeader:
        memcpy(dctx->headerBuffer + ZSTD_frameHeaderSize_prefix, src, dctx->expected);
        CHECK_F(ZSTD_decodeFrameHeader(dctx, dctx->headerBuffer, dctx->headerSize));
        dctx->expected = ZSTD_blockHeaderSize;
        dctx->stage = ZSTDds_decodeBlockHeader;
        return 0;

    case ZSTDds_decodeBlockHeader:
        {   blockProperties_t bp;
            size_t const cBlockSize = ZSTD_getcBlockSize(src, ZSTD_blockHeaderSize, &bp);
            if (ZSTD_isError(cBlockSize)) return cBlockSize;
            dctx->expected = cBlockSize;
            dctx->bType = bp.blockType;
            dctx->rleSize = bp.origSize;
            if (cBlockSize) {
                dctx->stage = bp.lastBlock ? ZSTDds_decompressLastBlock : ZSTDds_decompressBlock;
                return 0;
            }
            /* empty block */
            if (bp.lastBlock) {
                if (dctx->fParams.checksumFlag) {
                    dctx->expected = 4;
                    dctx->stage = ZSTDds_checkChecksum;
                } else {
                    dctx->expected = 0; /* end of frame */
                    dctx->stage = ZSTDds_getFrameHeaderSize;
                }
            } else {
                dctx->expected = 3;  /* go directly to next header */
                dctx->stage = ZSTDds_decodeBlockHeader;
            }
            return 0;
        }
    case ZSTDds_decompressLastBlock:
    case ZSTDds_decompressBlock:
        {   size_t rSize;
            switch(dctx->bType)
            {
            case bt_compressed:
                rSize = ZSTD_decompressBlock_internal(dctx, dst, dstCapacity, src, srcSize);
                break;
            case bt_raw :
                rSize = ZSTD_copyRawBlock(dst, dstCapacity, src, srcSize);
                break;
            case bt_rle :
                rSize = ZSTD_setRleBlock(dst, dstCapacity, src, srcSize, dctx->rleSize);
                break;
            case bt_reserved :   /* should never happen */
            default:
                return ERROR(corruption_detected);
            }
            if (ZSTD_isError(rSize)) return rSize;
            if (dctx->fParams.checksumFlag) XXH64_update(&dctx->xxhState, dst, rSize);

            if (dctx->stage == ZSTDds_decompressLastBlock) {   /* end of frame */
                if (dctx->fParams.checksumFlag) {  /* another round for frame checksum */
                    dctx->expected = 4;
                    dctx->stage = ZSTDds_checkChecksum;
                } else {
                    dctx->expected = 0;   /* ends here */
                    dctx->stage = ZSTDds_getFrameHeaderSize;
                }
            } else {
                dctx->stage = ZSTDds_decodeBlockHeader;
                dctx->expected = ZSTD_blockHeaderSize;
                dctx->previousDstEnd = (char*)dst + rSize;
            }
            return rSize;
        }
    case ZSTDds_checkChecksum:
        {   U32 const h32 = (U32)XXH64_digest(&dctx->xxhState);
            U32 const check32 = MEM_readLE32(src);   /* srcSize == 4, guaranteed by dctx->expected */
            if (check32 != h32) return ERROR(checksum_wrong);
            dctx->expected = 0;
            dctx->stage = ZSTDds_getFrameHeaderSize;
            return 0;
        }
    case ZSTDds_decodeSkippableHeader:
        {   memcpy(dctx->headerBuffer + ZSTD_frameHeaderSize_prefix, src, dctx->expected);
            dctx->expected = MEM_readLE32(dctx->headerBuffer + 4);
            dctx->stage = ZSTDds_skipFrame;
            return 0;
        }
    case ZSTDds_skipFrame:
        {   dctx->expected = 0;
            dctx->stage = ZSTDds_getFrameHeaderSize;
            return 0;
        }
    default:
        return ERROR(GENERIC);   /* impossible */
    }
}


static size_t ZSTD_refDictContent(ZSTD_DCtx* dctx, const void* dict, size_t dictSize)
{
    dctx->dictEnd = dctx->previousDstEnd;
    dctx->vBase = (const char*)dict - ((const char*)(dctx->previousDstEnd) - (const char*)(dctx->base));
    dctx->base = dict;
    dctx->previousDstEnd = (const char*)dict + dictSize;
    return 0;
}

static size_t ZSTD_loadEntropy(ZSTD_DCtx* dctx, const void* const dict, size_t const dictSize)
{
    const BYTE* dictPtr = (const BYTE*)dict;
    const BYTE* const dictEnd = dictPtr + dictSize;

    {   size_t const hSize = HUF_readDTableX4(dctx->hufTable, dict, dictSize);
        if (HUF_isError(hSize)) return ERROR(dictionary_corrupted);
        dictPtr += hSize;
    }

    {   short offcodeNCount[MaxOff+1];
        U32 offcodeMaxValue=MaxOff, offcodeLog=OffFSELog;
        size_t const offcodeHeaderSize = FSE_readNCount(offcodeNCount, &offcodeMaxValue, &offcodeLog, dictPtr, dictEnd-dictPtr);
        if (FSE_isError(offcodeHeaderSize)) return ERROR(dictionary_corrupted);
        CHECK_E(FSE_buildDTable(dctx->OFTable, offcodeNCount, offcodeMaxValue, offcodeLog), dictionary_corrupted);
        dictPtr += offcodeHeaderSize;
    }

    {   short matchlengthNCount[MaxML+1];
        unsigned matchlengthMaxValue = MaxML, matchlengthLog = MLFSELog;
        size_t const matchlengthHeaderSize = FSE_readNCount(matchlengthNCount, &matchlengthMaxValue, &matchlengthLog, dictPtr, dictEnd-dictPtr);
        if (FSE_isError(matchlengthHeaderSize)) return ERROR(dictionary_corrupted);
        CHECK_E(FSE_buildDTable(dctx->MLTable, matchlengthNCount, matchlengthMaxValue, matchlengthLog), dictionary_corrupted);
        dictPtr += matchlengthHeaderSize;
    }

    {   short litlengthNCount[MaxLL+1];
        unsigned litlengthMaxValue = MaxLL, litlengthLog = LLFSELog;
        size_t const litlengthHeaderSize = FSE_readNCount(litlengthNCount, &litlengthMaxValue, &litlengthLog, dictPtr, dictEnd-dictPtr);
        if (FSE_isError(litlengthHeaderSize)) return ERROR(dictionary_corrupted);
        CHECK_E(FSE_buildDTable(dctx->LLTable, litlengthNCount, litlengthMaxValue, litlengthLog), dictionary_corrupted);
        dictPtr += litlengthHeaderSize;
    }

    if (dictPtr+12 > dictEnd) return ERROR(dictionary_corrupted);
    dctx->rep[0] = MEM_readLE32(dictPtr+0); if (dctx->rep[0] >= dictSize) return ERROR(dictionary_corrupted);
    dctx->rep[1] = MEM_readLE32(dictPtr+4); if (dctx->rep[1] >= dictSize) return ERROR(dictionary_corrupted);
    dctx->rep[2] = MEM_readLE32(dictPtr+8); if (dctx->rep[2] >= dictSize) return ERROR(dictionary_corrupted);
    dictPtr += 12;

    dctx->litEntropy = dctx->fseEntropy = 1;
    return dictPtr - (const BYTE*)dict;
}

static size_t ZSTD_decompress_insertDictionary(ZSTD_DCtx* dctx, const void* dict, size_t dictSize)
{
    if (dictSize < 8) return ZSTD_refDictContent(dctx, dict, dictSize);
    {   U32 const magic = MEM_readLE32(dict);
        if (magic != ZSTD_DICT_MAGIC) {
            return ZSTD_refDictContent(dctx, dict, dictSize);   /* pure content mode */
    }   }
    dctx->dictID = MEM_readLE32((const char*)dict + 4);

    /* load entropy tables */
    dict = (const char*)dict + 8;
    dictSize -= 8;
    {   size_t const eSize = ZSTD_loadEntropy(dctx, dict, dictSize);
        if (ZSTD_isError(eSize)) return ERROR(dictionary_corrupted);
        dict = (const char*)dict + eSize;
        dictSize -= eSize;
    }

    /* reference dictionary content */
    return ZSTD_refDictContent(dctx, dict, dictSize);
}

size_t ZSTD_decompressBegin_usingDict(ZSTD_DCtx* dctx, const void* dict, size_t dictSize)
{
    CHECK_F(ZSTD_decompressBegin(dctx));
    if (dict && dictSize) CHECK_E(ZSTD_decompress_insertDictionary(dctx, dict, dictSize), dictionary_corrupted);
    return 0;
}


/* ======   ZSTD_DDict   ====== */

struct ZSTD_DDict_s {
    void* dict;
    size_t dictSize;
    ZSTD_DCtx* refContext;
};  /* typedef'd to ZSTD_DDict within "zstd.h" */

ZSTD_DDict* ZSTD_createDDict_advanced(const void* dict, size_t dictSize, ZSTD_customMem customMem)
{
    if (!customMem.customAlloc && !customMem.customFree) customMem = defaultCustomMem;
    if (!customMem.customAlloc || !customMem.customFree) return NULL;

    {   ZSTD_DDict* const ddict = (ZSTD_DDict*) ZSTD_malloc(sizeof(ZSTD_DDict), customMem);
        void* const dictContent = ZSTD_malloc(dictSize, customMem);
        ZSTD_DCtx* const dctx = ZSTD_createDCtx_advanced(customMem);

        if (!dictContent || !ddict || !dctx) {
            ZSTD_free(dictContent, customMem);
            ZSTD_free(ddict, customMem);
            ZSTD_free(dctx, customMem);
            return NULL;
        }

        memcpy(dictContent, dict, dictSize);
        {   size_t const errorCode = ZSTD_decompressBegin_usingDict(dctx, dictContent, dictSize);
            if (ZSTD_isError(errorCode)) {
                ZSTD_free(dictContent, customMem);
                ZSTD_free(ddict, customMem);
                ZSTD_free(dctx, customMem);
                return NULL;
        }   }

        ddict->dict = dictContent;
        ddict->dictSize = dictSize;
        ddict->refContext = dctx;
        return ddict;
    }
}

/*! ZSTD_createDDict() :
*   Create a digested dictionary, ready to start decompression without startup delay.
*   `dict` can be released after `ZSTD_DDict` creation */
ZSTD_DDict* ZSTD_createDDict(const void* dict, size_t dictSize)
{
    ZSTD_customMem const allocator = { NULL, NULL, NULL };
    return ZSTD_createDDict_advanced(dict, dictSize, allocator);
}

size_t ZSTD_freeDDict(ZSTD_DDict* ddict)
{
    if (ddict==NULL) return 0;   /* support free on NULL */
    {   ZSTD_customMem const cMem = ddict->refContext->customMem;
        ZSTD_freeDCtx(ddict->refContext);
        ZSTD_free(ddict->dict, cMem);
        ZSTD_free(ddict, cMem);
        return 0;
    }
}

size_t ZSTD_sizeof_DDict(const ZSTD_DDict* ddict)
{
    if (ddict==NULL) return 0;   /* support sizeof on NULL */
    return sizeof(*ddict) + sizeof(ddict->refContext) + ddict->dictSize;
}


/*! ZSTD_decompress_usingDDict() :
*   Decompression using a pre-digested Dictionary
*   Use dictionary without significant overhead. */
size_t ZSTD_decompress_usingDDict(ZSTD_DCtx* dctx,
                                  void* dst, size_t dstCapacity,
                            const void* src, size_t srcSize,
                            const ZSTD_DDict* ddict)
{
#if defined(ZSTD_LEGACY_SUPPORT) && (ZSTD_LEGACY_SUPPORT==1)
    if (ZSTD_isLegacy(src, srcSize)) return ZSTD_decompressLegacy(dst, dstCapacity, src, srcSize, ddict->dict, ddict->dictSize);
#endif
    ZSTD_refDCtx(dctx, ddict->refContext);
    ZSTD_checkContinuity(dctx, dst);
    return ZSTD_decompressFrame(dctx, dst, dstCapacity, src, srcSize);
}


/*=====================================
*   Streaming decompression
*====================================*/

typedef enum { zdss_init, zdss_loadHeader,
               zdss_read, zdss_load, zdss_flush } ZSTD_dStreamStage;

/* *** Resource management *** */
struct ZSTD_DStream_s {
    ZSTD_DCtx* dctx;
    ZSTD_DDict* ddict;
    ZSTD_frameParams fParams;
    ZSTD_dStreamStage stage;
    char*  inBuff;
    size_t inBuffSize;
    size_t inPos;
    size_t maxWindowSize;
    char*  outBuff;
    size_t outBuffSize;
    size_t outStart;
    size_t outEnd;
    size_t blockSize;
    BYTE headerBuffer[ZSTD_FRAMEHEADERSIZE_MAX];   /* tmp buffer to store frame header */
    size_t lhSize;
    ZSTD_customMem customMem;
    void* legacyContext;
    U32 previousLegacyVersion;
    U32 legacyVersion;
    U32 hostageByte;
};   /* typedef'd to ZSTD_DStream within "zstd.h" */


ZSTD_DStream* ZSTD_createDStream(void)
{
    return ZSTD_createDStream_advanced(defaultCustomMem);
}

ZSTD_DStream* ZSTD_createDStream_advanced(ZSTD_customMem customMem)
{
    ZSTD_DStream* zds;

    if (!customMem.customAlloc && !customMem.customFree) customMem = defaultCustomMem;
    if (!customMem.customAlloc || !customMem.customFree) return NULL;

    zds = (ZSTD_DStream*) ZSTD_malloc(sizeof(ZSTD_DStream), customMem);
    if (zds==NULL) return NULL;
    memset(zds, 0, sizeof(ZSTD_DStream));
    memcpy(&zds->customMem, &customMem, sizeof(ZSTD_customMem));
    zds->dctx = ZSTD_createDCtx_advanced(customMem);
    if (zds->dctx == NULL) { ZSTD_freeDStream(zds); return NULL; }
    zds->stage = zdss_init;
    zds->maxWindowSize = ZSTD_MAXWINDOWSIZE_DEFAULT;
    return zds;
}

size_t ZSTD_freeDStream(ZSTD_DStream* zds)
{
    if (zds==NULL) return 0;   /* support free on null */
    {   ZSTD_customMem const cMem = zds->customMem;
        ZSTD_freeDCtx(zds->dctx);
        ZSTD_freeDDict(zds->ddict);
        ZSTD_free(zds->inBuff, cMem);
        ZSTD_free(zds->outBuff, cMem);
#if defined(ZSTD_LEGACY_SUPPORT) && (ZSTD_LEGACY_SUPPORT >= 1)
        if (zds->legacyContext)
            ZSTD_freeLegacyStreamContext(zds->legacyContext, zds->previousLegacyVersion);
#endif
        ZSTD_free(zds, cMem);
        return 0;
    }
}


/* *** Initialization *** */

size_t ZSTD_DStreamInSize(void)  { return ZSTD_BLOCKSIZE_ABSOLUTEMAX + ZSTD_blockHeaderSize; }
size_t ZSTD_DStreamOutSize(void) { return ZSTD_BLOCKSIZE_ABSOLUTEMAX; }

size_t ZSTD_initDStream_usingDict(ZSTD_DStream* zds, const void* dict, size_t dictSize)
{
    zds->stage = zdss_loadHeader;
    zds->lhSize = zds->inPos = zds->outStart = zds->outEnd = 0;
    ZSTD_freeDDict(zds->ddict);
    zds->ddict = ZSTD_createDDict(dict, dictSize);
    if (zds->ddict == NULL) return ERROR(memory_allocation);
    zds->legacyVersion = 0;
    zds->hostageByte = 0;
    return ZSTD_frameHeaderSize_prefix;
}

size_t ZSTD_initDStream(ZSTD_DStream* zds)
{
    return ZSTD_initDStream_usingDict(zds, NULL, 0);
}

size_t ZSTD_resetDStream(ZSTD_DStream* zds)
{
    if (zds->ddict == NULL) return ERROR(stage_wrong);  /* must be init at least once */
    zds->stage = zdss_loadHeader;
    zds->lhSize = zds->inPos = zds->outStart = zds->outEnd = 0;
    zds->legacyVersion = 0;
    zds->hostageByte = 0;
    return ZSTD_frameHeaderSize_prefix;
}

size_t ZSTD_setDStreamParameter(ZSTD_DStream* zds,
                                ZSTD_DStreamParameter_e paramType, unsigned paramValue)
{
    switch(paramType)
    {
        default : return ERROR(parameter_unknown);
        case ZSTDdsp_maxWindowSize : zds->maxWindowSize = paramValue; break;
    }
    return 0;
}


size_t ZSTD_sizeof_DStream(const ZSTD_DStream* zds)
{
    if (zds==NULL) return 0;   /* support sizeof on NULL */
    return sizeof(*zds) + ZSTD_sizeof_DCtx(zds->dctx) + ZSTD_sizeof_DDict(zds->ddict) + zds->inBuffSize + zds->outBuffSize;
}


/* *****   Decompression   ***** */

MEM_STATIC size_t ZSTD_limitCopy(void* dst, size_t dstCapacity, const void* src, size_t srcSize)
{
    size_t const length = MIN(dstCapacity, srcSize);
    memcpy(dst, src, length);
    return length;
}


size_t ZSTD_decompressStream(ZSTD_DStream* zds, ZSTD_outBuffer* output, ZSTD_inBuffer* input)
{
    const char* const istart = (const char*)(input->src) + input->pos;
    const char* const iend = (const char*)(input->src) + input->size;
    const char* ip = istart;
    char* const ostart = (char*)(output->dst) + output->pos;
    char* const oend = (char*)(output->dst) + output->size;
    char* op = ostart;
    U32 someMoreWork = 1;

#if defined(ZSTD_LEGACY_SUPPORT) && (ZSTD_LEGACY_SUPPORT>=1)
    if (zds->legacyVersion)
        return ZSTD_decompressLegacyStream(zds->legacyContext, zds->legacyVersion, output, input);
#endif

    while (someMoreWork) {
        switch(zds->stage)
        {
        case zdss_init :
            return ERROR(init_missing);

        case zdss_loadHeader :
            {   size_t const hSize = ZSTD_getFrameParams(&zds->fParams, zds->headerBuffer, zds->lhSize);
                if (ZSTD_isError(hSize))
#if defined(ZSTD_LEGACY_SUPPORT) && (ZSTD_LEGACY_SUPPORT>=1)
                {   U32 const legacyVersion = ZSTD_isLegacy(istart, iend-istart);
                    if (legacyVersion) {
                        CHECK_F(ZSTD_initLegacyStream(&zds->legacyContext, zds->previousLegacyVersion, legacyVersion,
                                                       zds->ddict->dict, zds->ddict->dictSize));
                        zds->legacyVersion = zds->previousLegacyVersion = legacyVersion;
                        return ZSTD_decompressLegacyStream(zds->legacyContext, zds->legacyVersion, output, input);
                    } else {
                        return hSize; /* error */
                }   }
#else
                    return hSize;
#endif
                if (hSize != 0) {   /* need more input */
                    size_t const toLoad = hSize - zds->lhSize;   /* if hSize!=0, hSize > zds->lhSize */
                    if (toLoad > (size_t)(iend-ip)) {   /* not enough input to load full header */
                        memcpy(zds->headerBuffer + zds->lhSize, ip, iend-ip);
                        zds->lhSize += iend-ip;
                        input->pos = input->size;
                        return (MAX(ZSTD_frameHeaderSize_min, hSize) - zds->lhSize) + ZSTD_blockHeaderSize;   /* remaining header bytes + next block header */
                    }
                    memcpy(zds->headerBuffer + zds->lhSize, ip, toLoad); zds->lhSize = hSize; ip += toLoad;
                    break;
            }   }

            /* Consume header */
            ZSTD_refDCtx(zds->dctx, zds->ddict->refContext);
            {   size_t const h1Size = ZSTD_nextSrcSizeToDecompress(zds->dctx);  /* == ZSTD_frameHeaderSize_prefix */
                CHECK_F(ZSTD_decompressContinue(zds->dctx, NULL, 0, zds->headerBuffer, h1Size));
                {   size_t const h2Size = ZSTD_nextSrcSizeToDecompress(zds->dctx);
                    CHECK_F(ZSTD_decompressContinue(zds->dctx, NULL, 0, zds->headerBuffer+h1Size, h2Size));
            }   }

            zds->fParams.windowSize = MAX(zds->fParams.windowSize, 1U << ZSTD_WINDOWLOG_ABSOLUTEMIN);
            if (zds->fParams.windowSize > zds->maxWindowSize) return ERROR(frameParameter_unsupported);

            /* Adapt buffer sizes to frame header instructions */
            {   size_t const blockSize = MIN(zds->fParams.windowSize, ZSTD_BLOCKSIZE_ABSOLUTEMAX);
                size_t const neededOutSize = zds->fParams.windowSize + blockSize;
                zds->blockSize = blockSize;
                if (zds->inBuffSize < blockSize) {
                    ZSTD_free(zds->inBuff, zds->customMem);
                    zds->inBuffSize = blockSize;
                    zds->inBuff = (char*)ZSTD_malloc(blockSize, zds->customMem);
                    if (zds->inBuff == NULL) return ERROR(memory_allocation);
                }
                if (zds->outBuffSize < neededOutSize) {
                    ZSTD_free(zds->outBuff, zds->customMem);
                    zds->outBuffSize = neededOutSize;
                    zds->outBuff = (char*)ZSTD_malloc(neededOutSize, zds->customMem);
                    if (zds->outBuff == NULL) return ERROR(memory_allocation);
            }   }
            zds->stage = zdss_read;
            /* pass-through */

        case zdss_read:
            {   size_t const neededInSize = ZSTD_nextSrcSizeToDecompress(zds->dctx);
                if (neededInSize==0) {  /* end of frame */
                    zds->stage = zdss_init;
                    someMoreWork = 0;
                    break;
                }
                if ((size_t)(iend-ip) >= neededInSize) {  /* decode directly from src */
                    const int isSkipFrame = ZSTD_isSkipFrame(zds->dctx);
                    size_t const decodedSize = ZSTD_decompressContinue(zds->dctx,
                        zds->outBuff + zds->outStart, (isSkipFrame ? 0 : zds->outBuffSize - zds->outStart),
                        ip, neededInSize);
                    if (ZSTD_isError(decodedSize)) return decodedSize;
                    ip += neededInSize;
                    if (!decodedSize && !isSkipFrame) break;   /* this was just a header */
                    zds->outEnd = zds->outStart + decodedSize;
                    zds->stage = zdss_flush;
                    break;
                }
                if (ip==iend) { someMoreWork = 0; break; }   /* no more input */
                zds->stage = zdss_load;
                /* pass-through */
            }

        case zdss_load:
            {   size_t const neededInSize = ZSTD_nextSrcSizeToDecompress(zds->dctx);
                size_t const toLoad = neededInSize - zds->inPos;   /* should always be <= remaining space within inBuff */
                size_t loadedSize;
                if (toLoad > zds->inBuffSize - zds->inPos) return ERROR(corruption_detected);   /* should never happen */
                loadedSize = ZSTD_limitCopy(zds->inBuff + zds->inPos, toLoad, ip, iend-ip);
                ip += loadedSize;
                zds->inPos += loadedSize;
                if (loadedSize < toLoad) { someMoreWork = 0; break; }   /* not enough input, wait for more */

                /* decode loaded input */
                {  const int isSkipFrame = ZSTD_isSkipFrame(zds->dctx);
                   size_t const decodedSize = ZSTD_decompressContinue(zds->dctx,
                        zds->outBuff + zds->outStart, zds->outBuffSize - zds->outStart,
                        zds->inBuff, neededInSize);
                    if (ZSTD_isError(decodedSize)) return decodedSize;
                    zds->inPos = 0;   /* input is consumed */
                    if (!decodedSize && !isSkipFrame) { zds->stage = zdss_read; break; }   /* this was just a header */
                    zds->outEnd = zds->outStart +  decodedSize;
                    zds->stage = zdss_flush;
                    /* pass-through */
            }   }

        case zdss_flush:
            {   size_t const toFlushSize = zds->outEnd - zds->outStart;
                size_t const flushedSize = ZSTD_limitCopy(op, oend-op, zds->outBuff + zds->outStart, toFlushSize);
                op += flushedSize;
                zds->outStart += flushedSize;
                if (flushedSize == toFlushSize) {  /* flush completed */
                    zds->stage = zdss_read;
                    if (zds->outStart + zds->blockSize > zds->outBuffSize)
                        zds->outStart = zds->outEnd = 0;
                    break;
                }
                /* cannot complete flush */
                someMoreWork = 0;
                break;
            }
        default: return ERROR(GENERIC);   /* impossible */
    }   }

    /* result */
    input->pos += (size_t)(ip-istart);
    output->pos += (size_t)(op-ostart);
    {   size_t nextSrcSizeHint = ZSTD_nextSrcSizeToDecompress(zds->dctx);
        if (!nextSrcSizeHint) {   /* frame fully decoded */
            if (zds->outEnd == zds->outStart) {  /* output fully flushed */
                if (zds->hostageByte) {
                    if (input->pos >= input->size) { zds->stage = zdss_read; return 1; }  /* can't release hostage (not present) */
                    input->pos++;  /* release hostage */
                }
                return 0;
            }
            if (!zds->hostageByte) { /* output not fully flushed; keep last byte as hostage; will be released when all output is flushed */
                input->pos--;   /* note : pos > 0, otherwise, impossible to finish reading last block */
                zds->hostageByte=1;
            }
            return 1;
        }
        nextSrcSizeHint += ZSTD_blockHeaderSize * (ZSTD_nextInputType(zds->dctx) == ZSTDnit_block);   /* preload header of next block */
        if (zds->inPos > nextSrcSizeHint) return ERROR(GENERIC);   /* should never happen */
        nextSrcSizeHint -= zds->inPos;   /* already loaded*/
        return nextSrcSizeHint;
    }
}
