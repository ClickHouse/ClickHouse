/*
    zstd - standard compression library
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
    - zstd source repository : https://github.com/Cyan4973/zstd
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


/*-*******************************************************
*  Dependencies
*********************************************************/
#include <stdlib.h>      /* calloc */
#include <string.h>      /* memcpy, memmove */
#include <stdio.h>       /* debug only : printf */
#include "mem.h"         /* low level memory routines */
#include "zstd_internal.h"
#include "fse_static.h"
#include "huff0_static.h"

#if defined(ZSTD_LEGACY_SUPPORT) && (ZSTD_LEGACY_SUPPORT==1)
#  include "zstd_legacy.h"
#endif


/*-*******************************************************
*  Compiler specifics
*********************************************************/
#ifdef _MSC_VER    /* Visual Studio */
#  define FORCE_INLINE static __forceinline
#  include <intrin.h>                    /* For Visual 2005 */
#  pragma warning(disable : 4127)        /* disable: C4127: conditional expression is constant */
#  pragma warning(disable : 4324)        /* disable: C4324: padded structure */
#else
#  define GCC_VERSION (__GNUC__ * 100 + __GNUC_MINOR__)
#  ifdef __GNUC__
#    define FORCE_INLINE static inline __attribute__((always_inline))
#  else
#    define FORCE_INLINE static inline
#  endif
#endif


/*-*************************************
*  Local types
***************************************/
typedef struct
{
    blockType_t blockType;
    U32 origSize;
} blockProperties_t;


/* *******************************************************
*  Memory operations
**********************************************************/
static void ZSTD_copy4(void* dst, const void* src) { memcpy(dst, src, 4); }


/* *************************************
*  Error Management
***************************************/
unsigned ZSTD_versionNumber (void) { return ZSTD_VERSION_NUMBER; }

/*! ZSTD_isError() :
*   tells if a return value is an error code */
unsigned ZSTD_isError(size_t code) { return ERR_isError(code); }

/*! ZSTD_getError() :
*   convert a `size_t` function result into a proper ZSTD_errorCode enum */
ZSTD_ErrorCode ZSTD_getError(size_t code) { return ERR_getError(code); }

/*! ZSTD_getErrorName() :
*   provides error code string (useful for debugging) */
const char* ZSTD_getErrorName(size_t code) { return ERR_getErrorName(code); }


/* *************************************************************
*   Context management
***************************************************************/
typedef enum { ZSTDds_getFrameHeaderSize, ZSTDds_decodeFrameHeader,
               ZSTDds_decodeBlockHeader, ZSTDds_decompressBlock } ZSTD_dStage;

struct ZSTD_DCtx_s
{
    FSE_DTable LLTable[FSE_DTABLE_SIZE_U32(LLFSELog)];
    FSE_DTable OffTable[FSE_DTABLE_SIZE_U32(OffFSELog)];
    FSE_DTable MLTable[FSE_DTABLE_SIZE_U32(MLFSELog)];
    unsigned   hufTableX4[HUF_DTABLE_SIZE(HufLog)];
    const void* previousDstEnd;
    const void* base;
    const void* vBase;
    const void* dictEnd;
    size_t expected;
    size_t headerSize;
    ZSTD_parameters params;
    blockType_t bType;   /* used in ZSTD_decompressContinue(), to transfer blockType between header decoding and block decoding stages */
    ZSTD_dStage stage;
    U32 flagStaticTables;
    const BYTE* litPtr;
    size_t litBufSize;
    size_t litSize;
    BYTE litBuffer[BLOCKSIZE + WILDCOPY_OVERLENGTH];
    BYTE headerBuffer[ZSTD_frameHeaderSize_max];
};  /* typedef'd to ZSTD_DCtx within "zstd_static.h" */

size_t sizeofDCtx (void) { return sizeof(ZSTD_DCtx); }

size_t ZSTD_decompressBegin(ZSTD_DCtx* dctx)
{
    dctx->expected = ZSTD_frameHeaderSize_min;
    dctx->stage = ZSTDds_getFrameHeaderSize;
    dctx->previousDstEnd = NULL;
    dctx->base = NULL;
    dctx->vBase = NULL;
    dctx->dictEnd = NULL;
    dctx->hufTableX4[0] = HufLog;
    dctx->flagStaticTables = 0;
    return 0;
}

ZSTD_DCtx* ZSTD_createDCtx(void)
{
    ZSTD_DCtx* dctx = (ZSTD_DCtx*)malloc(sizeof(ZSTD_DCtx));
    if (dctx==NULL) return NULL;
    ZSTD_decompressBegin(dctx);
    return dctx;
}

size_t ZSTD_freeDCtx(ZSTD_DCtx* dctx)
{
    free(dctx);
    return 0;   /* reserved as a potential error code in the future */
}

void ZSTD_copyDCtx(ZSTD_DCtx* dstDCtx, const ZSTD_DCtx* srcDCtx)
{
    memcpy(dstDCtx, srcDCtx,
           sizeof(ZSTD_DCtx) - (BLOCKSIZE+WILDCOPY_OVERLENGTH + ZSTD_frameHeaderSize_max));  /* no need to copy workspace */
}


/* *************************************************************
*   Decompression section
***************************************************************/

/* Frame format description
   Frame Header -  [ Block Header - Block ] - Frame End
   1) Frame Header
      - 4 bytes - Magic Number : ZSTD_MAGICNUMBER (defined within zstd_internal.h)
      - 1 byte  - Window Descriptor
   2) Block Header
      - 3 bytes, starting with a 2-bits descriptor
                 Uncompressed, Compressed, Frame End, unused
   3) Block
      See Block Format Description
   4) Frame End
      - 3 bytes, compatible with Block Header
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
      TO DO
*/


/** ZSTD_decodeFrameHeader_Part1() :
*   decode the 1st part of the Frame Header, which tells Frame Header size.
*   srcSize must be == ZSTD_frameHeaderSize_min.
*   @return : the full size of the Frame Header */
static size_t ZSTD_decodeFrameHeader_Part1(ZSTD_DCtx* zc, const void* src, size_t srcSize)
{
    U32 magicNumber;
    if (srcSize != ZSTD_frameHeaderSize_min)
        return ERROR(srcSize_wrong);
    magicNumber = MEM_readLE32(src);
    if (magicNumber != ZSTD_MAGICNUMBER) return ERROR(prefix_unknown);
    zc->headerSize = ZSTD_frameHeaderSize_min;
    return zc->headerSize;
}


size_t ZSTD_getFrameParams(ZSTD_parameters* params, const void* src, size_t srcSize)
{
    U32 magicNumber;
    if (srcSize < ZSTD_frameHeaderSize_min) return ZSTD_frameHeaderSize_max;
    magicNumber = MEM_readLE32(src);
    if (magicNumber != ZSTD_MAGICNUMBER) return ERROR(prefix_unknown);
    memset(params, 0, sizeof(*params));
    params->windowLog = (((const BYTE*)src)[4] & 15) + ZSTD_WINDOWLOG_ABSOLUTEMIN;
    if ((((const BYTE*)src)[4] >> 4) != 0) return ERROR(frameParameter_unsupported);   /* reserved bits */
    return 0;
}

/** ZSTD_decodeFrameHeader_Part2() :
*   decode the full Frame Header.
*   srcSize must be the size provided by ZSTD_decodeFrameHeader_Part1().
*   @return : 0, or an error code, which can be tested using ZSTD_isError() */
static size_t ZSTD_decodeFrameHeader_Part2(ZSTD_DCtx* zc, const void* src, size_t srcSize)
{
    size_t result;
    if (srcSize != zc->headerSize)
        return ERROR(srcSize_wrong);
    result = ZSTD_getFrameParams(&(zc->params), src, srcSize);
    if ((MEM_32bits()) && (zc->params.windowLog > 25)) return ERROR(frameParameter_unsupportedBy32bits);
    return result;
}


size_t ZSTD_getcBlockSize(const void* src, size_t srcSize, blockProperties_t* bpPtr)
{
    const BYTE* const in = (const BYTE* const)src;
    BYTE headerFlags;
    U32 cSize;

    if (srcSize < 3)
        return ERROR(srcSize_wrong);

    headerFlags = *in;
    cSize = in[2] + (in[1]<<8) + ((in[0] & 7)<<16);

    bpPtr->blockType = (blockType_t)(headerFlags >> 6);
    bpPtr->origSize = (bpPtr->blockType == bt_rle) ? cSize : 0;

    if (bpPtr->blockType == bt_end) return 0;
    if (bpPtr->blockType == bt_rle) return 1;
    return cSize;
}


static size_t ZSTD_copyRawBlock(void* dst, size_t maxDstSize, const void* src, size_t srcSize)
{
    if (srcSize > maxDstSize) return ERROR(dstSize_tooSmall);
    memcpy(dst, src, srcSize);
    return srcSize;
}


/*! ZSTD_decodeLiteralsBlock() :
    @return : nb of bytes read from src (< srcSize ) */
size_t ZSTD_decodeLiteralsBlock(ZSTD_DCtx* dctx,
                          const void* src, size_t srcSize)   /* note : srcSize < BLOCKSIZE */
{
    const BYTE* const istart = (const BYTE*) src;

    /* any compressed block with literals segment must be at least this size */
    if (srcSize < MIN_CBLOCK_SIZE) return ERROR(corruption_detected);

    switch(istart[0]>> 6)
    {
    case IS_HUF:
        {
            size_t litSize, litCSize, singleStream=0;
            U32 lhSize = ((istart[0]) >> 4) & 3;
            switch(lhSize)
            {
            case 0: case 1: default:   /* note : default is impossible, since lhSize into [0..3] */
                /* 2 - 2 - 10 - 10 */
                lhSize=3;
                singleStream = istart[0] & 16;
                litSize  = ((istart[0] & 15) << 6) + (istart[1] >> 2);
                litCSize = ((istart[1] &  3) << 8) + istart[2];
                break;
            case 2:
                /* 2 - 2 - 14 - 14 */
                lhSize=4;
                litSize  = ((istart[0] & 15) << 10) + (istart[1] << 2) + (istart[2] >> 6);
                litCSize = ((istart[2] & 63) <<  8) + istart[3];
                break;
            case 3:
                /* 2 - 2 - 18 - 18 */
                lhSize=5;
                litSize  = ((istart[0] & 15) << 14) + (istart[1] << 6) + (istart[2] >> 2);
                litCSize = ((istart[2] &  3) << 16) + (istart[3] << 8) + istart[4];
                break;
            }
            if (litSize > BLOCKSIZE) return ERROR(corruption_detected);

            if (HUF_isError(singleStream ?
                            HUF_decompress1X2(dctx->litBuffer, litSize, istart+lhSize, litCSize) :
                            HUF_decompress   (dctx->litBuffer, litSize, istart+lhSize, litCSize) ))
                return ERROR(corruption_detected);

            dctx->litPtr = dctx->litBuffer;
            dctx->litBufSize = BLOCKSIZE+8;
            dctx->litSize = litSize;
            return litCSize + lhSize;
        }
    case IS_PCH:
        {
            size_t errorCode;
            size_t litSize, litCSize;
            U32 lhSize = ((istart[0]) >> 4) & 3;
            if (lhSize != 1)  /* only case supported for now : small litSize, single stream */
                return ERROR(corruption_detected);
            if (!dctx->flagStaticTables)
                return ERROR(dictionary_corrupted);

            /* 2 - 2 - 10 - 10 */
            lhSize=3;
            litSize  = ((istart[0] & 15) << 6) + (istart[1] >> 2);
            litCSize = ((istart[1] &  3) << 8) + istart[2];

            errorCode = HUF_decompress1X4_usingDTable(dctx->litBuffer, litSize, istart+lhSize, litCSize, dctx->hufTableX4);
            if (HUF_isError(errorCode)) return ERROR(corruption_detected);

            dctx->litPtr = dctx->litBuffer;
            dctx->litBufSize = BLOCKSIZE+WILDCOPY_OVERLENGTH;
            dctx->litSize = litSize;
            return litCSize + lhSize;
        }
    case IS_RAW:
        {
            size_t litSize;
            U32 lhSize = ((istart[0]) >> 4) & 3;
            switch(lhSize)
            {
            case 0: case 1: default:   /* note : default is impossible, since lhSize into [0..3] */
                lhSize=1;
                litSize = istart[0] & 31;
                break;
            case 2:
                litSize = ((istart[0] & 15) << 8) + istart[1];
                break;
            case 3:
                litSize = ((istart[0] & 15) << 16) + (istart[1] << 8) + istart[2];
                break;
            }

            if (lhSize+litSize+WILDCOPY_OVERLENGTH > srcSize) {  /* risk reading beyond src buffer with wildcopy */
                if (litSize+lhSize > srcSize) return ERROR(corruption_detected);
                memcpy(dctx->litBuffer, istart+lhSize, litSize);
                dctx->litPtr = dctx->litBuffer;
                dctx->litBufSize = BLOCKSIZE+8;
                dctx->litSize = litSize;
                return lhSize+litSize;
            }
            /* direct reference into compressed stream */
            dctx->litPtr = istart+lhSize;
            dctx->litBufSize = srcSize-lhSize;
            dctx->litSize = litSize;
            return lhSize+litSize;
        }
    case IS_RLE:
        {
            size_t litSize;
            U32 lhSize = ((istart[0]) >> 4) & 3;
            switch(lhSize)
            {
            case 0: case 1: default:   /* note : default is impossible, since lhSize into [0..3] */
                lhSize = 1;
                litSize = istart[0] & 31;
                break;
            case 2:
                litSize = ((istart[0] & 15) << 8) + istart[1];
                break;
            case 3:
                litSize = ((istart[0] & 15) << 16) + (istart[1] << 8) + istart[2];
                break;
            }
            if (litSize > BLOCKSIZE) return ERROR(corruption_detected);
            memset(dctx->litBuffer, istart[lhSize], litSize);
            dctx->litPtr = dctx->litBuffer;
            dctx->litBufSize = BLOCKSIZE+WILDCOPY_OVERLENGTH;
            dctx->litSize = litSize;
            return lhSize+1;
        }
    default:
        return ERROR(corruption_detected);   /* impossible */
    }
}


size_t ZSTD_decodeSeqHeaders(int* nbSeq, const BYTE** dumpsPtr, size_t* dumpsLengthPtr,
                         FSE_DTable* DTableLL, FSE_DTable* DTableML, FSE_DTable* DTableOffb,
                         const void* src, size_t srcSize)
{
    const BYTE* const istart = (const BYTE* const)src;
    const BYTE* ip = istart;
    const BYTE* const iend = istart + srcSize;
    U32 LLtype, Offtype, MLtype;
    U32 LLlog, Offlog, MLlog;
    size_t dumpsLength;

    /* check */
    if (srcSize < MIN_SEQUENCES_SIZE)
        return ERROR(srcSize_wrong);

    /* SeqHead */
    *nbSeq = *ip++;
    if (*nbSeq==0) return 1;
    if (*nbSeq >= 128)
        *nbSeq = ((nbSeq[0]-128)<<8) + *ip++;

    LLtype  = *ip >> 6;
    Offtype = (*ip >> 4) & 3;
    MLtype  = (*ip >> 2) & 3;
    if (*ip & 2) {
        dumpsLength  = ip[2];
        dumpsLength += ip[1] << 8;
        ip += 3;
    } else {
        dumpsLength  = ip[1];
        dumpsLength += (ip[0] & 1) << 8;
        ip += 2;
    }
    *dumpsPtr = ip;
    ip += dumpsLength;
    *dumpsLengthPtr = dumpsLength;

    /* check */
    if (ip > iend-3) return ERROR(srcSize_wrong); /* min : all 3 are "raw", hence no header, but at least xxLog bits per type */

    /* sequences */
    {
        S16 norm[MaxML+1];    /* assumption : MaxML >= MaxLL >= MaxOff */
        size_t headerSize;

        /* Build DTables */
        switch(LLtype)
        {
        U32 max;
        case FSE_ENCODING_RLE :
            LLlog = 0;
            FSE_buildDTable_rle(DTableLL, *ip++);
            break;
        case FSE_ENCODING_RAW :
            LLlog = LLbits;
            FSE_buildDTable_raw(DTableLL, LLbits);
            break;
        case FSE_ENCODING_STATIC:
            break;
        case FSE_ENCODING_DYNAMIC :
        default :   /* impossible */
            max = MaxLL;
            headerSize = FSE_readNCount(norm, &max, &LLlog, ip, iend-ip);
            if (FSE_isError(headerSize)) return ERROR(GENERIC);
            if (LLlog > LLFSELog) return ERROR(corruption_detected);
            ip += headerSize;
            FSE_buildDTable(DTableLL, norm, max, LLlog);
        }

        switch(Offtype)
        {
        U32 max;
        case FSE_ENCODING_RLE :
            Offlog = 0;
            if (ip > iend-2) return ERROR(srcSize_wrong);   /* min : "raw", hence no header, but at least xxLog bits */
            FSE_buildDTable_rle(DTableOffb, *ip++ & MaxOff); /* if *ip > MaxOff, data is corrupted */
            break;
        case FSE_ENCODING_RAW :
            Offlog = Offbits;
            FSE_buildDTable_raw(DTableOffb, Offbits);
            break;
        case FSE_ENCODING_STATIC:
            break;
        case FSE_ENCODING_DYNAMIC :
        default :   /* impossible */
            max = MaxOff;
            headerSize = FSE_readNCount(norm, &max, &Offlog, ip, iend-ip);
            if (FSE_isError(headerSize)) return ERROR(GENERIC);
            if (Offlog > OffFSELog) return ERROR(corruption_detected);
            ip += headerSize;
            FSE_buildDTable(DTableOffb, norm, max, Offlog);
        }

        switch(MLtype)
        {
        U32 max;
        case FSE_ENCODING_RLE :
            MLlog = 0;
            if (ip > iend-2) return ERROR(srcSize_wrong); /* min : "raw", hence no header, but at least xxLog bits */
            FSE_buildDTable_rle(DTableML, *ip++);
            break;
        case FSE_ENCODING_RAW :
            MLlog = MLbits;
            FSE_buildDTable_raw(DTableML, MLbits);
            break;
        case FSE_ENCODING_STATIC:
            break;
        case FSE_ENCODING_DYNAMIC :
        default :   /* impossible */
            max = MaxML;
            headerSize = FSE_readNCount(norm, &max, &MLlog, ip, iend-ip);
            if (FSE_isError(headerSize)) return ERROR(GENERIC);
            if (MLlog > MLFSELog) return ERROR(corruption_detected);
            ip += headerSize;
            FSE_buildDTable(DTableML, norm, max, MLlog);
    }   }

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
    size_t prevOffset;
    const BYTE* dumps;
    const BYTE* dumpsEnd;
} seqState_t;



static void ZSTD_decodeSequence(seq_t* seq, seqState_t* seqState)
{
    size_t litLength;
    size_t prevOffset;
    size_t offset;
    size_t matchLength;
    const BYTE* dumps = seqState->dumps;
    const BYTE* const de = seqState->dumpsEnd;

    /* Literal length */
    litLength = FSE_peakSymbol(&(seqState->stateLL));
    prevOffset = litLength ? seq->offset : seqState->prevOffset;
    if (litLength == MaxLL) {
        U32 add = *dumps++;
        if (add < 255) litLength += add;
        else {
            litLength = MEM_readLE32(dumps) & 0xFFFFFF;  /* no risk : dumps is always followed by seq tables > 1 byte */
            if (litLength&1) litLength>>=1, dumps += 3;
            else litLength = (U16)(litLength)>>1, dumps += 2;
        }
        if (dumps >= de) dumps = de-1;   /* late correction, to avoid read overflow (data is now corrupted anyway) */
    }

    /* Offset */
    {
        static const U32 offsetPrefix[MaxOff+1] = {
                1 /*fake*/, 1, 2, 4, 8, 16, 32, 64, 128, 256,
                512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144,
                524288, 1048576, 2097152, 4194304, 8388608, 16777216, 33554432, /*fake*/ 1, 1, 1, 1, 1 };
        U32 offsetCode = FSE_peakSymbol(&(seqState->stateOffb));   /* <= maxOff, by table construction */
        U32 nbBits = offsetCode - 1;
        if (offsetCode==0) nbBits = 0;   /* cmove */
        offset = offsetPrefix[offsetCode] + BIT_readBits(&(seqState->DStream), nbBits);
        if (MEM_32bits()) BIT_reloadDStream(&(seqState->DStream));
        if (offsetCode==0) offset = prevOffset;   /* repcode, cmove */
        if (offsetCode | !litLength) seqState->prevOffset = seq->offset;   /* cmove */
        FSE_decodeSymbol(&(seqState->stateOffb), &(seqState->DStream));    /* update */
    }

    /* Literal length update */
    FSE_decodeSymbol(&(seqState->stateLL), &(seqState->DStream));   /* update */
    if (MEM_32bits()) BIT_reloadDStream(&(seqState->DStream));

    /* MatchLength */
    matchLength = FSE_decodeSymbol(&(seqState->stateML), &(seqState->DStream));
    if (matchLength == MaxML) {
        U32 add = *dumps++;
        if (add < 255) matchLength += add;
        else {
            matchLength = MEM_readLE32(dumps) & 0xFFFFFF;  /* no pb : dumps is always followed by seq tables > 1 byte */
            if (matchLength&1) matchLength>>=1, dumps += 3;
            else matchLength = (U16)(matchLength)>>1, dumps += 2;
        }
        if (dumps >= de) dumps = de-1;   /* late correction, to avoid read overflow (data is now corrupted anyway) */
    }
    matchLength += MINMATCH;

    /* save result */
    seq->litLength = litLength;
    seq->offset = offset;
    seq->matchLength = matchLength;
    seqState->dumps = dumps;

#if 0   /* debug */
    {
        static U64 totalDecoded = 0;
        printf("pos %6u : %3u literals & match %3u bytes at distance %6u \n",
           (U32)(totalDecoded), (U32)litLength, (U32)matchLength, (U32)offset);
        totalDecoded += litLength + matchLength;
    }
#endif
}


FORCE_INLINE size_t ZSTD_execSequence(BYTE* op,
                                BYTE* const oend, seq_t sequence,
                                const BYTE** litPtr, const BYTE* const litLimit_8,
                                const BYTE* const base, const BYTE* const vBase, const BYTE* const dictEnd)
{
    static const int dec32table[] = { 0, 1, 2, 1, 4, 4, 4, 4 };   /* added */
    static const int dec64table[] = { 8, 8, 8, 7, 8, 9,10,11 };   /* substracted */
    BYTE* const oLitEnd = op + sequence.litLength;
    const size_t sequenceLength = sequence.litLength + sequence.matchLength;
    BYTE* const oMatchEnd = op + sequenceLength;   /* risk : address space overflow (32-bits) */
    BYTE* const oend_8 = oend-8;
    const BYTE* const litEnd = *litPtr + sequence.litLength;
    const BYTE* match = oLitEnd - sequence.offset;

    /* check */
    if (oLitEnd > oend_8) return ERROR(dstSize_tooSmall);   /* last match must start at a minimum distance of 8 from oend */
    if (oMatchEnd > oend) return ERROR(dstSize_tooSmall);   /* overwrite beyond dst buffer */
    if (litEnd > litLimit_8) return ERROR(corruption_detected);   /* risk read beyond lit buffer */

    /* copy Literals */
    ZSTD_wildcopy(op, *litPtr, sequence.litLength);   /* note : oLitEnd <= oend-8 : no risk of overwrite beyond oend */
    op = oLitEnd;
    *litPtr = litEnd;   /* update for next sequence */

    /* copy Match */
    if (sequence.offset > (size_t)(oLitEnd - base)) {
        /* offset beyond prefix */
        if (sequence.offset > (size_t)(oLitEnd - vBase))
            return ERROR(corruption_detected);
        match = dictEnd - (base-match);
        if (match + sequence.matchLength <= dictEnd) {
            memmove(oLitEnd, match, sequence.matchLength);
            return sequenceLength;
        }
        /* span extDict & currentPrefixSegment */
        {
            size_t length1 = dictEnd - match;
            memmove(oLitEnd, match, length1);
            op = oLitEnd + length1;
            sequence.matchLength -= length1;
            match = base;
    }   }

    /* match within prefix */
    if (sequence.offset < 8) {
        /* close range match, overlap */
        const int sub2 = dec64table[sequence.offset];
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

    if (oMatchEnd > oend-12) {
        if (op < oend_8) {
            ZSTD_wildcopy(op, match, oend_8 - op);
            match += oend_8 - op;
            op = oend_8;
        }
        while (op < oMatchEnd)
            *op++ = *match++;
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
    BYTE* op = ostart;
    BYTE* const oend = ostart + maxDstSize;
    size_t errorCode, dumpsLength;
    const BYTE* litPtr = dctx->litPtr;
    const BYTE* const litLimit_8 = litPtr + dctx->litBufSize - 8;
    const BYTE* const litEnd = litPtr + dctx->litSize;
    int nbSeq;
    const BYTE* dumps;
    U32* DTableLL = dctx->LLTable;
    U32* DTableML = dctx->MLTable;
    U32* DTableOffb = dctx->OffTable;
    const BYTE* const base = (const BYTE*) (dctx->base);
    const BYTE* const vBase = (const BYTE*) (dctx->vBase);
    const BYTE* const dictEnd = (const BYTE*) (dctx->dictEnd);

    /* Build Decoding Tables */
    errorCode = ZSTD_decodeSeqHeaders(&nbSeq, &dumps, &dumpsLength,
                                      DTableLL, DTableML, DTableOffb,
                                      ip, seqSize);
    if (ZSTD_isError(errorCode)) return errorCode;
    ip += errorCode;

    /* Regen sequences */
    if (nbSeq) {
        seq_t sequence;
        seqState_t seqState;

        memset(&sequence, 0, sizeof(sequence));
        sequence.offset = REPCODE_STARTVALUE;
        seqState.dumps = dumps;
        seqState.dumpsEnd = dumps + dumpsLength;
        seqState.prevOffset = REPCODE_STARTVALUE;
        errorCode = BIT_initDStream(&(seqState.DStream), ip, iend-ip);
        if (ERR_isError(errorCode)) return ERROR(corruption_detected);
        FSE_initDState(&(seqState.stateLL), &(seqState.DStream), DTableLL);
        FSE_initDState(&(seqState.stateOffb), &(seqState.DStream), DTableOffb);
        FSE_initDState(&(seqState.stateML), &(seqState.DStream), DTableML);

        for ( ; (BIT_reloadDStream(&(seqState.DStream)) <= BIT_DStream_completed) && nbSeq ; ) {
            size_t oneSeqSize;
            nbSeq--;
            ZSTD_decodeSequence(&sequence, &seqState);
            oneSeqSize = ZSTD_execSequence(op, oend, sequence, &litPtr, litLimit_8, base, vBase, dictEnd);
            if (ZSTD_isError(oneSeqSize)) return oneSeqSize;
            op += oneSeqSize;
        }

        /* check if reached exact end */
        if (nbSeq) return ERROR(corruption_detected);
    }

    /* last literal segment */
    {
        size_t lastLLSize = litEnd - litPtr;
        if (litPtr > litEnd) return ERROR(corruption_detected);   /* too many literals already used */
        if (op+lastLLSize > oend) return ERROR(dstSize_tooSmall);
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
    size_t litCSize;

    if (srcSize >= BLOCKSIZE) return ERROR(srcSize_wrong);

    /* Decode literals sub-block */
    litCSize = ZSTD_decodeLiteralsBlock(dctx, src, srcSize);
    if (ZSTD_isError(litCSize)) return litCSize;
    ip += litCSize;
    srcSize -= litCSize;

    return ZSTD_decompressSequences(dctx, dst, dstCapacity, ip, srcSize);
}


size_t ZSTD_decompressBlock(ZSTD_DCtx* dctx,
                            void* dst, size_t dstCapacity,
                      const void* src, size_t srcSize)
{
    ZSTD_checkContinuity(dctx, dst);
    return ZSTD_decompressBlock_internal(dctx, dst, dstCapacity, src, srcSize);
}


/*! ZSTD_decompress_continueDCtx
*   dctx must have been properly initialized */
static size_t ZSTD_decompress_continueDCtx(ZSTD_DCtx* dctx,
                                 void* dst, size_t maxDstSize,
                                 const void* src, size_t srcSize)
{
    const BYTE* ip = (const BYTE*)src;
    const BYTE* iend = ip + srcSize;
    BYTE* const ostart = (BYTE* const)dst;
    BYTE* op = ostart;
    BYTE* const oend = ostart + maxDstSize;
    size_t remainingSize = srcSize;
    blockProperties_t blockProperties;

    /* Frame Header */
    {
        size_t frameHeaderSize;
        if (srcSize < ZSTD_frameHeaderSize_min+ZSTD_blockHeaderSize) return ERROR(srcSize_wrong);
#if defined(ZSTD_LEGACY_SUPPORT) && (ZSTD_LEGACY_SUPPORT==1)
        {
            const U32 magicNumber = MEM_readLE32(src);
            if (ZSTD_isLegacy(magicNumber))
                return ZSTD_decompressLegacy(dst, maxDstSize, src, srcSize, magicNumber);
        }
#endif
        frameHeaderSize = ZSTD_decodeFrameHeader_Part1(dctx, src, ZSTD_frameHeaderSize_min);
        if (ZSTD_isError(frameHeaderSize)) return frameHeaderSize;
        if (srcSize < frameHeaderSize+ZSTD_blockHeaderSize) return ERROR(srcSize_wrong);
        ip += frameHeaderSize; remainingSize -= frameHeaderSize;
        frameHeaderSize = ZSTD_decodeFrameHeader_Part2(dctx, src, frameHeaderSize);
        if (ZSTD_isError(frameHeaderSize)) return frameHeaderSize;
    }

    /* Loop on each block */
    while (1)
    {
        size_t decodedSize=0;
        size_t cBlockSize = ZSTD_getcBlockSize(ip, iend-ip, &blockProperties);
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
            return ERROR(GENERIC);   /* not yet supported */
            break;
        case bt_end :
            /* end of frame */
            if (remainingSize) return ERROR(srcSize_wrong);
            break;
        default:
            return ERROR(GENERIC);   /* impossible */
        }
        if (cBlockSize == 0) break;   /* bt_end */

        if (ZSTD_isError(decodedSize)) return decodedSize;
        op += decodedSize;
        ip += cBlockSize;
        remainingSize -= cBlockSize;
    }

    return op-ostart;
}


size_t ZSTD_decompress_usingPreparedDCtx(ZSTD_DCtx* dctx, const ZSTD_DCtx* refDCtx,
                                         void* dst, size_t maxDstSize,
                                   const void* src, size_t srcSize)
{
    ZSTD_copyDCtx(dctx, refDCtx);
    ZSTD_checkContinuity(dctx, dst);
    return ZSTD_decompress_continueDCtx(dctx, dst, maxDstSize, src, srcSize);
}


size_t ZSTD_decompress_usingDict(ZSTD_DCtx* dctx,
                                 void* dst, size_t maxDstSize,
                                 const void* src, size_t srcSize,
                                 const void* dict, size_t dictSize)
{
    ZSTD_decompressBegin_usingDict(dctx, dict, dictSize);
    ZSTD_checkContinuity(dctx, dst);
    return ZSTD_decompress_continueDCtx(dctx, dst, maxDstSize, src, srcSize);
}


size_t ZSTD_decompressDCtx(ZSTD_DCtx* dctx, void* dst, size_t maxDstSize, const void* src, size_t srcSize)
{
    return ZSTD_decompress_usingDict(dctx, dst, maxDstSize, src, srcSize, NULL, 0);
}

size_t ZSTD_decompress(void* dst, size_t maxDstSize, const void* src, size_t srcSize)
{
#if defined(ZSTD_HEAPMODE) && (ZSTD_HEAPMODE==1)
    size_t regenSize;
    ZSTD_DCtx* dctx = ZSTD_createDCtx();
    if (dctx==NULL) return ERROR(memory_allocation);
    regenSize = ZSTD_decompressDCtx(dctx, dst, maxDstSize, src, srcSize);
    ZSTD_freeDCtx(dctx);
    return regenSize;
#else
    ZSTD_DCtx dctx;
    return ZSTD_decompressDCtx(&dctx, dst, maxDstSize, src, srcSize);
#endif
}


/* ******************************
*  Streaming Decompression API
********************************/
size_t ZSTD_nextSrcSizeToDecompress(ZSTD_DCtx* dctx)
{
    return dctx->expected;
}

size_t ZSTD_decompressContinue(ZSTD_DCtx* dctx, void* dst, size_t maxDstSize, const void* src, size_t srcSize)
{
    /* Sanity check */
    if (srcSize != dctx->expected) return ERROR(srcSize_wrong);
    ZSTD_checkContinuity(dctx, dst);

    /* Decompress : frame header; part 1 */
    switch (dctx->stage)
    {
    case ZSTDds_getFrameHeaderSize :
        {
            /* get frame header size */
            if (srcSize != ZSTD_frameHeaderSize_min) return ERROR(srcSize_wrong);   /* impossible */
            dctx->headerSize = ZSTD_decodeFrameHeader_Part1(dctx, src, ZSTD_frameHeaderSize_min);
            if (ZSTD_isError(dctx->headerSize)) return dctx->headerSize;
            memcpy(dctx->headerBuffer, src, ZSTD_frameHeaderSize_min);
            if (dctx->headerSize > ZSTD_frameHeaderSize_min) {
                dctx->expected = dctx->headerSize - ZSTD_frameHeaderSize_min;
                dctx->stage = ZSTDds_decodeFrameHeader;
                return 0;
            }
            dctx->expected = 0;   /* not necessary to copy more */
        }
    case ZSTDds_decodeFrameHeader:
        {
            /* get frame header */
            size_t result;
            memcpy(dctx->headerBuffer + ZSTD_frameHeaderSize_min, src, dctx->expected);
            result = ZSTD_decodeFrameHeader_Part2(dctx, dctx->headerBuffer, dctx->headerSize);
            if (ZSTD_isError(result)) return result;
            dctx->expected = ZSTD_blockHeaderSize;
            dctx->stage = ZSTDds_decodeBlockHeader;
            return 0;
        }
    case ZSTDds_decodeBlockHeader:
        {
            /* Decode block header */
            blockProperties_t bp;
            size_t blockSize = ZSTD_getcBlockSize(src, ZSTD_blockHeaderSize, &bp);
            if (ZSTD_isError(blockSize)) return blockSize;
            if (bp.blockType == bt_end) {
                dctx->expected = 0;
                dctx->stage = ZSTDds_getFrameHeaderSize;
            }
            else {
                dctx->expected = blockSize;
                dctx->bType = bp.blockType;
                dctx->stage = ZSTDds_decompressBlock;
            }
            return 0;
        }
    case ZSTDds_decompressBlock:
        {
            /* Decompress : block content */
            size_t rSize;
            switch(dctx->bType)
            {
            case bt_compressed:
                rSize = ZSTD_decompressBlock_internal(dctx, dst, maxDstSize, src, srcSize);
                break;
            case bt_raw :
                rSize = ZSTD_copyRawBlock(dst, maxDstSize, src, srcSize);
                break;
            case bt_rle :
                return ERROR(GENERIC);   /* not yet handled */
                break;
            case bt_end :   /* should never happen (filtered at phase 1) */
                rSize = 0;
                break;
            default:
                return ERROR(GENERIC);   /* impossible */
            }
            dctx->stage = ZSTDds_decodeBlockHeader;
            dctx->expected = ZSTD_blockHeaderSize;
            dctx->previousDstEnd = (char*)dst + rSize;
            return rSize;
        }
    default:
        return ERROR(GENERIC);   /* impossible */
    }
}


static void ZSTD_refDictContent(ZSTD_DCtx* dctx, const void* dict, size_t dictSize)
{
    dctx->dictEnd = dctx->previousDstEnd;
    dctx->vBase = (const char*)dict - ((const char*)(dctx->previousDstEnd) - (const char*)(dctx->base));
    dctx->base = dict;
    dctx->previousDstEnd = (const char*)dict + dictSize;
}

static size_t ZSTD_loadEntropy(ZSTD_DCtx* dctx, const void* dict, size_t dictSize)
{
    size_t hSize, offcodeHeaderSize, matchlengthHeaderSize, errorCode, litlengthHeaderSize;
    short offcodeNCount[MaxOff+1];
    U32 offcodeMaxValue=MaxOff, offcodeLog=OffFSELog;
    short matchlengthNCount[MaxML+1];
    unsigned matchlengthMaxValue = MaxML, matchlengthLog = MLFSELog;
    short litlengthNCount[MaxLL+1];
    unsigned litlengthMaxValue = MaxLL, litlengthLog = LLFSELog;

    hSize = HUF_readDTableX4(dctx->hufTableX4, dict, dictSize);
    if (HUF_isError(hSize)) return ERROR(dictionary_corrupted);
    dict = (const char*)dict + hSize;
    dictSize -= hSize;

    offcodeHeaderSize = FSE_readNCount(offcodeNCount, &offcodeMaxValue, &offcodeLog, dict, dictSize);
    if (FSE_isError(offcodeHeaderSize)) return ERROR(dictionary_corrupted);
    errorCode = FSE_buildDTable(dctx->OffTable, offcodeNCount, offcodeMaxValue, offcodeLog);
    if (FSE_isError(errorCode)) return ERROR(dictionary_corrupted);
    dict = (const char*)dict + offcodeHeaderSize;
    dictSize -= offcodeHeaderSize;

    matchlengthHeaderSize = FSE_readNCount(matchlengthNCount, &matchlengthMaxValue, &matchlengthLog, dict, dictSize);
    if (FSE_isError(matchlengthHeaderSize)) return ERROR(dictionary_corrupted);
    errorCode = FSE_buildDTable(dctx->MLTable, matchlengthNCount, matchlengthMaxValue, matchlengthLog);
    if (FSE_isError(errorCode)) return ERROR(dictionary_corrupted);
    dict = (const char*)dict + matchlengthHeaderSize;
    dictSize -= matchlengthHeaderSize;

    litlengthHeaderSize = FSE_readNCount(litlengthNCount, &litlengthMaxValue, &litlengthLog, dict, dictSize);
    if (FSE_isError(litlengthHeaderSize)) return ERROR(dictionary_corrupted);
    errorCode = FSE_buildDTable(dctx->LLTable, litlengthNCount, litlengthMaxValue, litlengthLog);
    if (FSE_isError(errorCode)) return ERROR(dictionary_corrupted);

    dctx->flagStaticTables = 1;
    return hSize + offcodeHeaderSize + matchlengthHeaderSize + litlengthHeaderSize;
}

static size_t ZSTD_decompress_insertDictionary(ZSTD_DCtx* dctx, const void* dict, size_t dictSize)
{
    size_t eSize;
    U32 magic = MEM_readLE32(dict);
    if (magic != ZSTD_DICT_MAGIC) {
        /* pure content mode */
        ZSTD_refDictContent(dctx, dict, dictSize);
        return 0;
    }
    /* load entropy tables */
    dict = (const char*)dict + 4;
    dictSize -= 4;
    eSize = ZSTD_loadEntropy(dctx, dict, dictSize);
    if (ZSTD_isError(eSize)) return ERROR(dictionary_corrupted);

    /* reference dictionary content */
    dict = (const char*)dict + eSize;
    dictSize -= eSize;
    ZSTD_refDictContent(dctx, dict, dictSize);

    return 0;
}


size_t ZSTD_decompressBegin_usingDict(ZSTD_DCtx* dctx, const void* dict, size_t dictSize)
{
    size_t errorCode;
    errorCode = ZSTD_decompressBegin(dctx);
    if (ZSTD_isError(errorCode)) return errorCode;

    if (dict && dictSize) {
        errorCode = ZSTD_decompress_insertDictionary(dctx, dict, dictSize);
        if (ZSTD_isError(errorCode)) return ERROR(dictionary_corrupted);
    }

    return 0;
}

