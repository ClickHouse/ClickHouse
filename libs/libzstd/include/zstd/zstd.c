/*
    zstd - standard compression library
    Copyright (C) 2014-2015, Yann Collet.

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
    - ztsd public forum : https://groups.google.com/forum/#!forum/lz4c
*/

/****************************************************************
*  Tuning parameters
*****************************************************************/
/* MEMORY_USAGE :
*  Memory usage formula : N->2^N Bytes (examples : 10 -> 1KB; 12 -> 4KB ; 16 -> 64KB; 20 -> 1MB; etc.)
*  Increasing memory usage improves compression ratio
*  Reduced memory usage can improve speed, due to cache effect */
#define ZSTD_MEMORY_USAGE 17


/**************************************
   CPU Feature Detection
**************************************/
/*
 * Automated efficient unaligned memory access detection
 * Based on known hardware architectures
 * This list will be updated thanks to feedbacks
 */
#if defined(CPU_HAS_EFFICIENT_UNALIGNED_MEMORY_ACCESS) \
    || defined(__ARM_FEATURE_UNALIGNED) \
    || defined(__i386__) || defined(__x86_64__) \
    || defined(_M_IX86) || defined(_M_X64) \
    || defined(__ARM_ARCH_7__) || defined(__ARM_ARCH_8__) \
    || (defined(_M_ARM) && (_M_ARM >= 7))
#  define ZSTD_UNALIGNED_ACCESS 1
#else
#  define ZSTD_UNALIGNED_ACCESS 0
#endif


/********************************************************
*  Includes
*********************************************************/
#include <stdlib.h>      /* calloc */
#include <string.h>      /* memcpy, memmove */
#include <stdio.h>       /* debug : printf */
#include "zstd_static.h"
#if defined(__clang__) || defined(__GNUC__)
#  include "fse.c"       /* due to GCC/Clang inlining limitations, including *.c runs noticeably faster */
#else
#  include "fse_static.h"
#endif


/********************************************************
*  Compiler specifics
*********************************************************/
#ifdef __AVX2__
#  include <immintrin.h>   /* AVX2 intrinsics */
#endif

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


#ifndef MEM_ACCESS_MODULE
#define MEM_ACCESS_MODULE
/********************************************************
*  Basic Types
*********************************************************/
#if defined (__STDC_VERSION__) && __STDC_VERSION__ >= 199901L   /* C99 */
# include <stdint.h>
typedef  uint8_t BYTE;
typedef uint16_t U16;
typedef  int16_t S16;
typedef uint32_t U32;
typedef  int32_t S32;
typedef uint64_t U64;
#else
typedef unsigned char       BYTE;
typedef unsigned short      U16;
typedef   signed short      S16;
typedef unsigned int        U32;
typedef   signed int        S32;
typedef unsigned long long  U64;
#endif

#endif   /* MEM_ACCESS_MODULE */


/********************************************************
*  Constants
*********************************************************/
static const U32 ZSTD_magicNumber = 0xFD2FB51C;   /* Initial (limited) frame format */

#define HASH_LOG (ZSTD_MEMORY_USAGE - 2)
#define HASH_TABLESIZE (1 << HASH_LOG)
#define HASH_MASK (HASH_TABLESIZE - 1)

#define KNUTH 2654435761

#define BIT7 128
#define BIT6  64
#define BIT5  32
#define BIT4  16

#define KB *(1 <<10)
#define MB *(1 <<20)
#define GB *(1U<<30)

#define BLOCKSIZE (128 KB)                 /* define, for static allocation */
static const U32 g_maxDistance = 4 * BLOCKSIZE;
static const U32 g_maxLimit = 1 GB;
static const U32 g_searchStrength = 8;

#define WORKPLACESIZE (BLOCKSIZE*11/4)
#define MINMATCH 4
#define MLbits   7
#define LLbits   6
#define Offbits  5
#define MaxML  ((1<<MLbits )-1)
#define MaxLL  ((1<<LLbits )-1)
#define MaxOff ((1<<Offbits)-1)
#define LitFSELog  11
#define MLFSELog   10
#define LLFSELog   10
#define OffFSELog   9

#define LITERAL_NOENTROPY 63
#define COMMAND_NOENTROPY 7   /* to remove */

static const size_t ZSTD_blockHeaderSize = 3;
static const size_t ZSTD_frameHeaderSize = 4;


/********************************************************
*  Memory operations
*********************************************************/
static unsigned ZSTD_32bits(void) { return sizeof(void*)==4; }
static unsigned ZSTD_64bits(void) { return sizeof(void*)==8; }

static unsigned ZSTD_isLittleEndian(void)
{
    const union { U32 i; BYTE c[4]; } one = { 1 };   /* don't use static : performance detrimental  */
    return one.c[0];
}

static U16    ZSTD_read16(const void* p) { return *(U16*)p; }

static U32    ZSTD_read32(const void* p) { return *(U32*)p; }

static size_t ZSTD_read_ARCH(const void* p) { return *(size_t*)p; }

static void   ZSTD_copy4(void* dst, const void* src) { memcpy(dst, src, 4); }

static void   ZSTD_copy8(void* dst, const void* src) { memcpy(dst, src, 8); }

#define COPY8(d,s)    { ZSTD_copy8(d,s); d+=8; s+=8; }

static void ZSTD_wildcopy(void* dst, const void* src, size_t length)
{
    const BYTE* ip = (const BYTE*)src;
    BYTE* op = (BYTE*)dst;
    BYTE* const oend = op + length;
    while (op < oend) COPY8(op, ip);
}

static U32 ZSTD_readLE32(const void* memPtr)
{
    if (ZSTD_isLittleEndian())
        return ZSTD_read32(memPtr);
    else
    {
        const BYTE* p = (const BYTE*)memPtr;
        return (U32)((U32)p[0] + ((U32)p[1]<<8) + ((U32)p[2]<<16) + ((U32)p[3]<<24));
    }
}

static void ZSTD_writeLE32(void* memPtr, U32 val32)
{
    if (ZSTD_isLittleEndian())
    {
        memcpy(memPtr, &val32, 4);
    }
    else
    {
        BYTE* p = (BYTE*)memPtr;
        p[0] = (BYTE)val32;
        p[1] = (BYTE)(val32>>8);
        p[2] = (BYTE)(val32>>16);
        p[3] = (BYTE)(val32>>24);
    }
}

static U32 ZSTD_readBE32(const void* memPtr)
{
    const BYTE* p = (const BYTE*)memPtr;
    return (U32)(((U32)p[0]<<24) + ((U32)p[1]<<16) + ((U32)p[2]<<8) + ((U32)p[3]<<0));
}

static void ZSTD_writeBE32(void* memPtr, U32 value)
{
    BYTE* const p = (BYTE* const) memPtr;
    p[0] = (BYTE)(value>>24);
    p[1] = (BYTE)(value>>16);
    p[2] = (BYTE)(value>>8);
    p[3] = (BYTE)(value>>0);
}

static size_t ZSTD_writeProgressive(void* ptr, size_t value)
{
    BYTE* const bStart = (BYTE* const)ptr;
    BYTE* byte = bStart;

    do
    {
        BYTE l = value & 127;
        value >>= 7;
        if (value) l += 128;
        *byte++ = l;
    } while (value);

    return byte - bStart;
}


static size_t ZSTD_readProgressive(size_t* result, const void* ptr)
{
    const BYTE* const bStart = (const BYTE* const)ptr;
    const BYTE* byte = bStart;
    size_t r = 0;
    U32 shift = 0;

    do
    {
        r += (*byte & 127) << shift;
        shift += 7;
    } while (*byte++ & 128);

    *result = r;
    return byte - bStart;
}


/**************************************
*  Local structures
***************************************/
typedef enum { bt_compressed, bt_raw, bt_rle, bt_end } blockType_t;

typedef struct
{
    blockType_t blockType;
    U32 origSize;
} blockProperties_t;

typedef struct {
    void* buffer;
    U32*  offsetStart;
    U32*  offset;
    BYTE* litStart;
    BYTE* lit;
    BYTE* litLengthStart;
    BYTE* litLength;
    BYTE* matchLengthStart;
    BYTE* matchLength;
    BYTE* dumpsStart;
    BYTE* dumps;
} seqStore_t;

void ZSTD_resetSeqStore(seqStore_t* ssPtr)
{
    ssPtr->offset = ssPtr->offsetStart;
    ssPtr->lit = ssPtr->litStart;
    ssPtr->litLength = ssPtr->litLengthStart;
    ssPtr->matchLength = ssPtr->matchLengthStart;
    ssPtr->dumps = ssPtr->dumpsStart;
}


typedef struct
{
    const BYTE* base;
    U32 current;
    U32 nextUpdate;
    seqStore_t seqStore;
#ifdef __AVX2__
    __m256i hashTable[HASH_TABLESIZE>>3];
#else
    U32 hashTable[HASH_TABLESIZE];
#endif
} cctxi_t;


ZSTD_cctx_t ZSTD_createCCtx(void)
{
    cctxi_t* ctx = (cctxi_t*) malloc( sizeof(cctxi_t) );
    ctx->seqStore.buffer = malloc(WORKPLACESIZE);
    ctx->seqStore.offsetStart = (U32*) (ctx->seqStore.buffer);
    ctx->seqStore.litStart = (BYTE*) (ctx->seqStore.offsetStart + (BLOCKSIZE>>2));
    ctx->seqStore.litLengthStart =  ctx->seqStore.litStart + BLOCKSIZE;
    ctx->seqStore.matchLengthStart = ctx->seqStore.litLengthStart + (BLOCKSIZE>>2);
    ctx->seqStore.dumpsStart = ctx->seqStore.matchLengthStart + (BLOCKSIZE>>2);
    return (ZSTD_cctx_t)ctx;
}

void ZSTD_resetCCtx(ZSTD_cctx_t cctx)
{
    cctxi_t* ctx = (cctxi_t*)cctx;
    ctx->base = NULL;
    memset(ctx->hashTable, 0, HASH_TABLESIZE*4);
}

size_t ZSTD_freeCCtx(ZSTD_cctx_t cctx)
{
    cctxi_t* ctx = (cctxi_t*) (cctx);
    free(ctx->seqStore.buffer);
    free(ctx);
    return 0;
}


/**************************************
*  Error Management
**************************************/
/* tells if a return value is an error code */
unsigned ZSTD_isError(size_t code)
{
    return (code > (size_t)(-ZSTD_ERROR_maxCode));
}

#define ZSTD_GENERATE_STRING(STRING) #STRING,
static const char* ZSTD_errorStrings[] = { ZSTD_LIST_ERRORS(ZSTD_GENERATE_STRING) };

/* provides error code string (useful for debugging) */
const char* ZSTD_getErrorName(size_t code)
{
    static const char* codeError = "Unspecified error code";
    if (ZSTD_isError(code)) return ZSTD_errorStrings[-(int)(code)];
    return codeError;
}


/**************************************
*  Tool functions
**************************************/
unsigned ZSTD_versionNumber (void) { return ZSTD_VERSION_NUMBER; }

static unsigned ZSTD_highbit(U32 val)
{
#   if defined(_MSC_VER)   /* Visual */
    unsigned long r;
    _BitScanReverse(&r, val);
    return (unsigned)r;
#   elif defined(__GNUC__) && (GCC_VERSION >= 304)   /* GCC Intrinsic */
    return 31 - __builtin_clz(val);
#   else   /* Software version */
    static const int DeBruijnClz[32] = { 0, 9, 1, 10, 13, 21, 2, 29, 11, 14, 16, 18, 22, 25, 3, 30, 8, 12, 20, 28, 15, 17, 24, 7, 19, 27, 23, 6, 26, 5, 4, 31 };
    U32 v = val;
    int r;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    r = DeBruijnClz[(U32)(v * 0x07C4ACDDU) >> 27];
    return r;
#   endif
}

static unsigned ZSTD_NbCommonBytes (register size_t val)
{
    if (ZSTD_isLittleEndian())
    {
        if (ZSTD_64bits())
        {
#       if defined(_MSC_VER) && defined(_WIN64) && !defined(LZ4_FORCE_SW_BITCOUNT)
            unsigned long r = 0;
            _BitScanForward64( &r, (U64)val );
            return (int)(r>>3);
#       elif defined(__GNUC__) && (GCC_VERSION >= 304) && !defined(LZ4_FORCE_SW_BITCOUNT)
            return (__builtin_ctzll((U64)val) >> 3);
#       else
            static const int DeBruijnBytePos[64] = { 0, 0, 0, 0, 0, 1, 1, 2, 0, 3, 1, 3, 1, 4, 2, 7, 0, 2, 3, 6, 1, 5, 3, 5, 1, 3, 4, 4, 2, 5, 6, 7, 7, 0, 1, 2, 3, 3, 4, 6, 2, 6, 5, 5, 3, 4, 5, 6, 7, 1, 2, 4, 6, 4, 4, 5, 7, 2, 6, 5, 7, 6, 7, 7 };
            return DeBruijnBytePos[((U64)((val & -(long long)val) * 0x0218A392CDABBD3FULL)) >> 58];
#       endif
        }
        else /* 32 bits */
        {
#       if defined(_MSC_VER) && !defined(LZ4_FORCE_SW_BITCOUNT)
            unsigned long r;
            _BitScanForward( &r, (U32)val );
            return (int)(r>>3);
#       elif defined(__GNUC__) && (GCC_VERSION >= 304) && !defined(LZ4_FORCE_SW_BITCOUNT)
            return (__builtin_ctz((U32)val) >> 3);
#       else
            static const int DeBruijnBytePos[32] = { 0, 0, 3, 0, 3, 1, 3, 0, 3, 2, 2, 1, 3, 2, 0, 1, 3, 3, 1, 2, 2, 2, 2, 0, 3, 1, 2, 0, 1, 0, 1, 1 };
            return DeBruijnBytePos[((U32)((val & -(S32)val) * 0x077CB531U)) >> 27];
#       endif
        }
    }
    else   /* Big Endian CPU */
    {
        if (ZSTD_64bits())
        {
#       if defined(_MSC_VER) && defined(_WIN64) && !defined(LZ4_FORCE_SW_BITCOUNT)
            unsigned long r = 0;
            _BitScanReverse64( &r, val );
            return (unsigned)(r>>3);
#       elif defined(__GNUC__) && (GCC_VERSION >= 304) && !defined(LZ4_FORCE_SW_BITCOUNT)
            return (__builtin_clzll(val) >> 3);
#       else
            unsigned r;
            const unsigned n32 = sizeof(size_t)*4;   /* calculate this way due to compiler complaining in 32-bits mode */
            if (!(val>>n32)) { r=4; } else { r=0; val>>=n32; }
            if (!(val>>16)) { r+=2; val>>=8; } else { val>>=24; }
            r += (!val);
            return r;
#       endif
        }
        else /* 32 bits */
        {
#       if defined(_MSC_VER) && !defined(LZ4_FORCE_SW_BITCOUNT)
            unsigned long r = 0;
            _BitScanReverse( &r, (unsigned long)val );
            return (unsigned)(r>>3);
#       elif defined(__GNUC__) && (GCC_VERSION >= 304) && !defined(LZ4_FORCE_SW_BITCOUNT)
            return (__builtin_clz(val) >> 3);
#       else
            unsigned r;
            if (!(val>>16)) { r=2; val>>=8; } else { r=0; val>>=24; }
            r += (!val);
            return r;
#       endif
        }
    }
}

static unsigned ZSTD_count(const BYTE* pIn, const BYTE* pMatch, const BYTE* pInLimit)
{
    const BYTE* const pStart = pIn;

    while ((pIn<pInLimit-(sizeof(size_t)-1)))
    {
        size_t diff = ZSTD_read_ARCH(pMatch) ^ ZSTD_read_ARCH(pIn);
        if (!diff) { pIn+=sizeof(size_t); pMatch+=sizeof(size_t); continue; }
        pIn += ZSTD_NbCommonBytes(diff);
        return (unsigned)(pIn - pStart);
    }

    if (ZSTD_64bits()) if ((pIn<(pInLimit-3)) && (ZSTD_read32(pMatch) == ZSTD_read32(pIn))) { pIn+=4; pMatch+=4; }
    if ((pIn<(pInLimit-1)) && (ZSTD_read16(pMatch) == ZSTD_read16(pIn))) { pIn+=2; pMatch+=2; }
    if ((pIn<pInLimit) && (*pMatch == *pIn)) pIn++;
    return (unsigned)(pIn - pStart);
}


/********************************************************
*  Compression
*********************************************************/
size_t ZSTD_compressBound(size_t srcSize)   /* maximum compressed size */
{
    return FSE_compressBound(srcSize) + 12;
}


static size_t ZSTD_compressRle (void* dst, size_t maxDstSize, const void* src, size_t srcSize)
{
    BYTE* const ostart = (BYTE* const)dst;

    /* at this stage : dstSize >= FSE_compressBound(srcSize) > (ZSTD_blockHeaderSize+1) (checked by ZSTD_compressLiterals()) */
    (void)maxDstSize;

    ostart[ZSTD_blockHeaderSize] = *(BYTE*)src;

    /* Build header */
    ostart[0]  = (BYTE)(srcSize>>16);
    ostart[1]  = (BYTE)(srcSize>>8);
    ostart[2]  = (BYTE)srcSize;
    ostart[0] += (BYTE)(bt_rle<<6);

    return ZSTD_blockHeaderSize+1;
}


static size_t ZSTD_noCompressBlock (void* dst, size_t maxDstSize, const void* src, size_t srcSize)
{
    BYTE* const ostart = (BYTE* const)dst;

    if (srcSize + ZSTD_blockHeaderSize > maxDstSize) return (size_t)-ZSTD_ERROR_maxDstSize_tooSmall;
    memcpy(ostart + ZSTD_blockHeaderSize, src, srcSize);

    /* Build header */
    ostart[0] = (BYTE)(srcSize>>16);
    ostart[1] = (BYTE)(srcSize>>8);
    ostart[2] = (BYTE)srcSize;
    ostart[0] += (BYTE)(bt_raw<<6);   /* is a raw (uncompressed) block */

    return ZSTD_blockHeaderSize+srcSize;
}


/* return : size of CStream in bits */
static size_t ZSTD_compressLiterals_usingCTable(void* dst, size_t dstSize,
                                          const void* src, size_t srcSize,
                                          const void* CTable)
{
    const BYTE* const istart = (const BYTE*)src;
    const BYTE* ip = istart;
    const BYTE* const iend = istart + srcSize;
    FSE_CStream_t bitC;
    FSE_CState_t CState1, CState2;

    /* init */
    (void)dstSize;   // objective : ensure it fits into dstBuffer (Todo)
    FSE_initCStream(&bitC, dst);
    FSE_initCState(&CState1, CTable);
    CState2 = CState1;

    /* Note : at this stage, srcSize > LITERALS_NOENTROPY (checked by ZSTD_compressLiterals()) */
    // join to mod 2
    if (srcSize & 1)
    {
        FSE_encodeByte(&bitC, &CState1, *ip++);
        FSE_flushBits(&bitC);
    }

    // join to mod 4
    if ((sizeof(size_t)*8 > LitFSELog*4+7 ) && (srcSize & 2))   // test bit 2
    {
        FSE_encodeByte(&bitC, &CState2, *ip++);
        FSE_encodeByte(&bitC, &CState1, *ip++);
        FSE_flushBits(&bitC);
    }

    // 2 or 4 encoding per loop
    while (ip<iend)
    {
        FSE_encodeByte(&bitC, &CState2, *ip++);

        if (sizeof(size_t)*8 < LitFSELog*2+7 )   // this test must be static
            FSE_flushBits(&bitC);

        FSE_encodeByte(&bitC, &CState1, *ip++);

        if (sizeof(size_t)*8 > LitFSELog*4+7 )   // this test must be static
        {
            FSE_encodeByte(&bitC, &CState2, *ip++);
            FSE_encodeByte(&bitC, &CState1, *ip++);
        }

        FSE_flushBits(&bitC);
    }

    FSE_flushCState(&bitC, &CState2);
    FSE_flushCState(&bitC, &CState1);
    return FSE_closeCStream(&bitC);
}


size_t ZSTD_minGain(size_t srcSize)
{
    return (srcSize >> 6) + 1;
}


static size_t ZSTD_compressLiterals (void* dst, size_t dstSize,
                                     const void* src, size_t srcSize)
{
    const BYTE* const istart = (const BYTE*) src;
    const BYTE* ip = istart;

    BYTE* const ostart = (BYTE*) dst;
    BYTE* op = ostart + ZSTD_blockHeaderSize;
    BYTE* const oend = ostart + dstSize;

    U32 maxSymbolValue = 256;
    U32 tableLog = LitFSELog;
    U32 count[256];
    S16 norm[256];
    U32 CTable[ FSE_CTABLE_SIZE_U32(LitFSELog, 256) ];
    size_t errorCode;
    const size_t minGain = ZSTD_minGain(srcSize);

    /* early out */
    if (dstSize < FSE_compressBound(srcSize)) return (size_t)-ZSTD_ERROR_maxDstSize_tooSmall;

    /* Scan input and build symbol stats */
    errorCode = FSE_count (count, ip, srcSize, &maxSymbolValue);
    if (FSE_isError(errorCode)) return (size_t)-ZSTD_ERROR_GENERIC;
    if (errorCode == srcSize) return 1;
    //if (errorCode < ((srcSize * 7) >> 10)) return 0;
    //if (errorCode < (srcSize >> 7)) return 0;
    if (errorCode < (srcSize >> 6)) return 0;   /* heuristic : probably not compressible enough */

    tableLog = FSE_optimalTableLog(tableLog, srcSize, maxSymbolValue);
    errorCode = (int)FSE_normalizeCount (norm, tableLog, count, srcSize, maxSymbolValue);
    if (FSE_isError(errorCode)) return (size_t)-ZSTD_ERROR_GENERIC;

    /* Write table description header */
    errorCode = FSE_writeHeader (op, FSE_MAX_HEADERSIZE, norm, maxSymbolValue, tableLog);
    if (FSE_isError(errorCode)) return (size_t)-ZSTD_ERROR_GENERIC;
    op += errorCode;

    /* Compress */
    errorCode = FSE_buildCTable (&CTable, norm, maxSymbolValue, tableLog);
    if (FSE_isError(errorCode)) return (size_t)-ZSTD_ERROR_GENERIC;
    errorCode = ZSTD_compressLiterals_usingCTable(op, oend - op, ip, srcSize, &CTable);
    if (ZSTD_isError(errorCode)) return errorCode;
    op += errorCode;

    /* check compressibility */
    if ( (size_t)(op-ostart) >= srcSize-minGain)
        return 0;

    /* Build header */
    {
        size_t totalSize;
        totalSize  = op - ostart - ZSTD_blockHeaderSize;
        ostart[0]  = (BYTE)(totalSize>>16);
        ostart[1]  = (BYTE)(totalSize>>8);
        ostart[2]  = (BYTE)totalSize;
        ostart[0] += (BYTE)(bt_compressed<<6); /* is a block, is compressed */
    }

    return op-ostart;
}


static size_t ZSTD_compressSequences(BYTE* dst, size_t maxDstSize,
                                     const seqStore_t* seqStorePtr,
                                     size_t lastLLSize, size_t srcSize)
{
    FSE_CStream_t blockStream;
    U32 count[256];
    S16 norm[256];
    size_t mostFrequent;
    U32 max = 255;
    U32 tableLog = 11;
    U32 CTable_LitLength  [FSE_CTABLE_SIZE_U32(LLFSELog, MaxLL )];
    U32 CTable_OffsetBits [FSE_CTABLE_SIZE_U32(OffFSELog,MaxOff)];
    U32 CTable_MatchLength[FSE_CTABLE_SIZE_U32(MLFSELog, MaxML )];
    U32 LLtype, Offtype, MLtype;
    const BYTE* const op_lit_start = seqStorePtr->litStart;
    const BYTE* op_lit = seqStorePtr->lit;
    const BYTE* const op_litLength_start = seqStorePtr->litLengthStart;
    const BYTE* op_litLength = seqStorePtr->litLength;
    const U32*  op_offset = seqStorePtr->offset;
    const BYTE* op_matchLength = seqStorePtr->matchLength;
    const size_t nbSeq = op_litLength - op_litLength_start;
    BYTE* op;
    BYTE offsetBits_start[BLOCKSIZE / 4];
    BYTE* offsetBitsPtr = offsetBits_start;
    const size_t minGain = ZSTD_minGain(srcSize);
    const size_t maxCSize = srcSize - minGain;
    const size_t minSeqSize = 1 /*lastL*/ + 2 /*dHead*/ + 2 /*dumpsIn*/ + 5 /*SeqHead*/ + 3 /*SeqIn*/ + 1 /*margin*/ + ZSTD_blockHeaderSize;
    const size_t maxLSize = maxCSize > minSeqSize ? maxCSize - minSeqSize : 0;
    BYTE* seqHead;


    /* init */
    op = dst;

    /* Encode literals */
    {
        size_t cSize;
        size_t litSize = op_lit - op_lit_start;
        if (litSize <= LITERAL_NOENTROPY) cSize = ZSTD_noCompressBlock (op, maxDstSize, op_lit_start, litSize);
        else
        {
            cSize = ZSTD_compressLiterals(op, maxDstSize, op_lit_start, litSize);
            if (cSize == 1) cSize = ZSTD_compressRle (op, maxDstSize, op_lit_start, litSize);
            else if (cSize == 0)
            {
                if (litSize >= maxLSize) return 0;   /* block not compressible enough */
                cSize = ZSTD_noCompressBlock (op, maxDstSize, op_lit_start, litSize);
            }
        }
        if (ZSTD_isError(cSize)) return cSize;
        op += cSize;
    }

    /* Encode Sequences */

    /* seqHeader */
    op += ZSTD_writeProgressive(op, lastLLSize);
    seqHead = op;

    /* dumps */
    {
        size_t dumpsLength = seqStorePtr->dumps - seqStorePtr->dumpsStart;
        if (dumpsLength < 512)
        {
            op[0] = (BYTE)(dumpsLength >> 8);
            op[1] = (BYTE)(dumpsLength);
            op += 2;
        }
        else
        {
            op[0] = 2;
            op[1] = (BYTE)(dumpsLength>>8);
            op[2] = (BYTE)(dumpsLength);
            op += 3;
        }
        memcpy(op, seqStorePtr->dumpsStart, dumpsLength);
        op += dumpsLength;
    }

    /* Encoding table of Literal Lengths */
    max = MaxLL;
    mostFrequent = FSE_countFast(count, seqStorePtr->litLengthStart, nbSeq, &max);
    if (mostFrequent == nbSeq)
    {
        *op++ = *(seqStorePtr->litLengthStart);
        FSE_buildCTable_rle(CTable_LitLength, (BYTE)max);
        LLtype = bt_rle;
    }
    else if ((nbSeq < 64) || (mostFrequent < (nbSeq >> (LLbits-1))))
    {
        FSE_buildCTable_raw(CTable_LitLength, LLbits);
        LLtype = bt_raw;
    }
    else
    {
        tableLog = FSE_optimalTableLog(LLFSELog, nbSeq, max);
        FSE_normalizeCount(norm, tableLog, count, nbSeq, max);
        op += FSE_writeHeader(op, maxDstSize, norm, max, tableLog);
        FSE_buildCTable(CTable_LitLength, norm, max, tableLog);
        LLtype = bt_compressed;
    }

    /* Encoding table of Offsets */
    {
        /* create OffsetBits */
        size_t i;
        const U32* const op_offset_start = seqStorePtr->offsetStart;
        max = MaxOff;
        for (i=0; i<nbSeq; i++)
        {
            offsetBits_start[i] = (BYTE)ZSTD_highbit(op_offset_start[i]) + 1;
            if (op_offset_start[i]==0) offsetBits_start[i]=0;
        }
        offsetBitsPtr += nbSeq;
        mostFrequent = FSE_countFast(count, offsetBits_start, nbSeq, &max);
    }
    if (mostFrequent == nbSeq)
    {
        *op++ = *offsetBits_start;
        FSE_buildCTable_rle(CTable_OffsetBits, (BYTE)max);
        Offtype = bt_rle;
    }
    else if ((nbSeq < 64) || (mostFrequent < (nbSeq >> (Offbits-1))))
    {
        FSE_buildCTable_raw(CTable_OffsetBits, Offbits);
        Offtype = bt_raw;
    }
    else
    {
        tableLog = FSE_optimalTableLog(OffFSELog, nbSeq, max);
        FSE_normalizeCount(norm, tableLog, count, nbSeq, max);
        op += FSE_writeHeader(op, maxDstSize, norm, max, tableLog);
        FSE_buildCTable(CTable_OffsetBits, norm, max, tableLog);
        Offtype = bt_compressed;
    }

    /* Encoding Table of MatchLengths */
    max = MaxML;
    mostFrequent = FSE_countFast(count, seqStorePtr->matchLengthStart, nbSeq, &max);
    if (mostFrequent == nbSeq)
    {
        *op++ = *seqStorePtr->matchLengthStart;
        FSE_buildCTable_rle(CTable_MatchLength, (BYTE)max);
        MLtype = bt_rle;
    }
    else if ((nbSeq < 64) || (mostFrequent < (nbSeq >> (MLbits-1))))
    {
        FSE_buildCTable_raw(CTable_MatchLength, MLbits);
        MLtype = bt_raw;
    }
    else
    {
        tableLog = FSE_optimalTableLog(MLFSELog, nbSeq, max);
        FSE_normalizeCount(norm, tableLog, count, nbSeq, max);
        op += FSE_writeHeader(op, maxDstSize, norm, max, tableLog);
        FSE_buildCTable(CTable_MatchLength, norm, max, tableLog);
        MLtype = bt_compressed;
    }

    seqHead[0] += (BYTE)((LLtype<<6) + (Offtype<<4) + (MLtype<<2));

    /* Encoding */
    {
        FSE_CState_t stateMatchLength;
        FSE_CState_t stateOffsetBits;
        FSE_CState_t stateLitLength;

        FSE_initCStream(&blockStream, op);
        FSE_initCState(&stateMatchLength, CTable_MatchLength);
        FSE_initCState(&stateOffsetBits, CTable_OffsetBits);
        FSE_initCState(&stateLitLength, CTable_LitLength);

        while (op_litLength > op_litLength_start)
        {
            BYTE matchLength = *(--op_matchLength);
            U32  offset = *(--op_offset);
            BYTE offCode = *(--offsetBitsPtr);                              /* 32b*/  /* 64b*/
            U32 nbBits = (offCode-1) * (!!offCode);
            BYTE litLength = *(--op_litLength);                             /* (7)*/  /* (7)*/
            FSE_encodeByte(&blockStream, &stateMatchLength, matchLength);   /* 17 */  /* 17 */
            if (ZSTD_32bits()) FSE_flushBits(&blockStream);                 /*  7 */
            FSE_addBits(&blockStream, offset, nbBits);                      /* 32 */  /* 42 */
            if (ZSTD_32bits()) FSE_flushBits(&blockStream);                 /*  7 */
            FSE_encodeByte(&blockStream, &stateOffsetBits, offCode);        /* 16 */  /* 51 */
            FSE_encodeByte(&blockStream, &stateLitLength, litLength);       /* 26 */  /* 61 */
            FSE_flushBits(&blockStream);                                    /*  7 */  /*  7 */
        }

        FSE_flushCState(&blockStream, &stateMatchLength);
        FSE_flushCState(&blockStream, &stateOffsetBits);
        FSE_flushCState(&blockStream, &stateLitLength);
    }

    op += FSE_closeCStream(&blockStream);

    /* check compressibility */
    if ((size_t)(op-dst) >= maxCSize) return 0;

    return op - dst;
}


static void ZSTD_storeSeq(seqStore_t* seqStorePtr, size_t litLength, const BYTE* literals, size_t offset, size_t matchLength)
{
    BYTE* op_lit = seqStorePtr->lit;
    BYTE* const l_end = op_lit + litLength;

    /* copy Literals */
    while (op_lit<l_end) COPY8(op_lit, literals);
    seqStorePtr->lit += litLength;

    /* literal Length */
    if (litLength >= MaxLL)
    {
        *(seqStorePtr->litLength++) = MaxLL;
        if (litLength<255 + MaxLL)
            *(seqStorePtr->dumps++) = (BYTE)(litLength - MaxLL);
        else
        {
            *(seqStorePtr->dumps++) = 255;
            ZSTD_writeLE32(seqStorePtr->dumps, (U32)litLength); seqStorePtr->dumps += 3;
        }
    }
    else *(seqStorePtr->litLength++) = (BYTE)litLength;

    /* match offset */
    *(seqStorePtr->offset++) = (U32)offset;

    /* match Length */
    if (matchLength >= MaxML)
    {
        *(seqStorePtr->matchLength++) = MaxML;
        if (matchLength < 255+MaxML)
            *(seqStorePtr->dumps++) = (BYTE)(matchLength - MaxML);
        else
        {
            *(seqStorePtr->dumps++) = 255;
            ZSTD_writeLE32(seqStorePtr->dumps, (U32)matchLength); seqStorePtr->dumps+=3;
        }
    }
    else *(seqStorePtr->matchLength++) = (BYTE)matchLength;
}


//static const U32 hashMask = (1<<HASH_LOG)-1;
//static const U64 prime5bytes =         889523592379ULL;
//static const U64 prime6bytes =      227718039650203ULL;
static const U64 prime7bytes =    58295818150454627ULL;
//static const U64 prime8bytes = 14923729446516375013ULL;

//static U32   ZSTD_hashPtr(const void* p) { return (U32) _bextr_u64(*(U64*)p * prime7bytes, (56-HASH_LOG), HASH_LOG); }
//static U32   ZSTD_hashPtr(const void* p) { return ( (*(U64*)p * prime7bytes) << 8 >> (64-HASH_LOG)); }
//static U32   ZSTD_hashPtr(const void* p) { return ( (*(U64*)p * prime7bytes) >> (56-HASH_LOG)) & ((1<<HASH_LOG)-1); }
//static U32   ZSTD_hashPtr(const void* p) { return ( ((*(U64*)p & 0xFFFFFFFFFFFFFF) * prime7bytes) >> (64-HASH_LOG)); }

//static U32   ZSTD_hashPtr(const void* p) { return ( (*(U64*)p * prime8bytes) >> (64-HASH_LOG)); }
static U32   ZSTD_hashPtr(const void* p) { return ( (*(U64*)p * prime7bytes) >> (56-HASH_LOG)) & HASH_MASK; }
//static U32   ZSTD_hashPtr(const void* p) { return ( (*(U64*)p * prime6bytes) >> (48-HASH_LOG)) & HASH_MASK; }
//static U32   ZSTD_hashPtr(const void* p) { return ( (*(U64*)p * prime5bytes) >> (40-HASH_LOG)) & HASH_MASK; }
//static U32   ZSTD_hashPtr(const void* p) { return ( (*(U32*)p * KNUTH) >> (32-HASH_LOG)); }

static void  ZSTD_addPtr(U32* table, const BYTE* p, const BYTE* start) { table[ZSTD_hashPtr(p)] = (U32)(p-start); }

static const BYTE* ZSTD_updateMatch(U32* table, const BYTE* p, const BYTE* start)
{
    U32 h = ZSTD_hashPtr(p);
    const BYTE* r;
    r = table[h] + start;
    //table[h] = (U32)(p - start);
    ZSTD_addPtr(table, p, start);
    return r;
}

static int ZSTD_checkMatch(const BYTE* match, const BYTE* ip)
{
    return ZSTD_read32(match) == ZSTD_read32(ip);
}


static size_t ZSTD_compressBlock(void* cctx, void* dst, size_t maxDstSize, const void* src, size_t srcSize)
{
    cctxi_t* ctx = (cctxi_t*) cctx;
    U32*  HashTable = (U32*)(ctx->hashTable);
    seqStore_t* seqStorePtr = &(ctx->seqStore);
    const BYTE* const base = ctx->base;

    const BYTE* const istart = (const BYTE*)src;
    const BYTE* ip = istart + 1;
    const BYTE* anchor = istart;
    const BYTE* const iend = istart + srcSize;
    const BYTE* const ilimit = iend - 16;

    size_t prevOffset=0, offset=0;
    size_t lastLLSize;


    /* init */
    ZSTD_resetSeqStore(seqStorePtr);

    /* Main Search Loop */
    while (ip < ilimit)
    {
        const BYTE* match = (BYTE*) ZSTD_updateMatch(HashTable, ip, base);

        if (!ZSTD_checkMatch(match,ip)) { ip += ((ip-anchor) >> g_searchStrength) + 1; continue; }

        /* catch up */
        while ((ip>anchor) && (match>base) && (ip[-1] == match[-1])) { ip--; match--; }

        {
            size_t litLength = ip-anchor;
            size_t matchLength = ZSTD_count(ip+MINMATCH, match+MINMATCH, iend);
            size_t offsetCode;
            if (litLength) prevOffset = offset;
            offsetCode = ip-match;
            if (offsetCode == prevOffset) offsetCode = 0;
            prevOffset = offset;
            offset = ip-match;
            ZSTD_storeSeq(seqStorePtr, litLength, anchor, offsetCode, matchLength);

            /* Fill Table */
            ZSTD_addPtr(HashTable, ip+1, base);
            ip += matchLength + MINMATCH;
            if (ip<=iend-8) ZSTD_addPtr(HashTable, ip-2, base);
            anchor = ip;
        }
    }

    /* Last Literals */
    lastLLSize = iend - anchor;
    memcpy(seqStorePtr->lit, anchor, lastLLSize);
    seqStorePtr->lit += lastLLSize;

    /* Finale compression stage */
    return ZSTD_compressSequences((BYTE*)dst, maxDstSize,
                                  seqStorePtr, lastLLSize, srcSize);
}


size_t ZSTD_compressBegin(ZSTD_cctx_t ctx, void* dst, size_t maxDstSize)
{
    /* Sanity check */
    if (maxDstSize < ZSTD_frameHeaderSize) return (size_t)-ZSTD_ERROR_maxDstSize_tooSmall;

    /* Init */
    ZSTD_resetCCtx(ctx);

    /* Write Header */
    ZSTD_writeBE32(dst, ZSTD_magicNumber);

    return ZSTD_frameHeaderSize;
}


/* this should be auto-vectorized by compiler */
static void ZSTD_scaleDownCtx(void* cctx, const U32 limit)
{
    cctxi_t* ctx = (cctxi_t*) cctx;
    int i;

#if defined(__AVX2__)   /* <immintrin.h> */
    /* AVX2 version */
    __m256i* h = ctx->hashTable;
    const __m256i limit8 = _mm256_set1_epi32(limit);
    for (i=0; i<(HASH_TABLESIZE>>3); i++)
    {
        __m256i src =_mm256_loadu_si256((const __m256i*)(h+i));
  const __m256i dec = _mm256_min_epu32(src, limit8);
                src = _mm256_sub_epi32(src, dec);
        _mm256_storeu_si256((__m256i*)(h+i), src);
    }
#else
    U32* h = ctx->hashTable;
    for (i=0; i<HASH_TABLESIZE; ++i)
    {
        U32 dec;
        if (h[i] > limit) dec = limit; else dec = h[i];
        h[i] -= dec;
    }
#endif
}


/* this should be auto-vectorized by compiler */
static void ZSTD_limitCtx(void* cctx, const U32 limit)
{
    cctxi_t* ctx = (cctxi_t*) cctx;
    int i;

    if (limit > g_maxLimit)
    {
        ZSTD_scaleDownCtx(cctx, limit);
        ctx->base += limit;
        ctx->current -= limit;
        ctx->nextUpdate -= limit;
        return;
    }

#if defined(__AVX2__)   /* <immintrin.h> */
    /* AVX2 version */
    {
        __m256i* h = ctx->hashTable;
        const __m256i limit8 = _mm256_set1_epi32(limit);
        //printf("Address h : %0X\n", (U32)h);    // address test
        for (i=0; i<(HASH_TABLESIZE>>3); i++)
        {
            __m256i src =_mm256_loadu_si256((const __m256i*)(h+i));   // Unfortunately, clang doesn't guarantee 32-bytes alignment
                    src = _mm256_max_epu32(src, limit8);
            _mm256_storeu_si256((__m256i*)(h+i), src);
        }
    }
#else
    {
        U32* h = (U32*)(ctx->hashTable);
        for (i=0; i<HASH_TABLESIZE; ++i)
        {
            if (h[i] < limit) h[i] = limit;
        }
    }
#endif
}


size_t ZSTD_compressContinue(ZSTD_cctx_t cctx, void* dst, size_t maxDstSize, const void* src, size_t srcSize)
{
    cctxi_t* ctx = (cctxi_t*) cctx;
    const BYTE* const istart = (const BYTE* const)src;
    const BYTE* ip = istart;
    BYTE* const ostart = (BYTE* const)dst;
    BYTE* op = ostart;
    const U32 updateRate = 2 * BLOCKSIZE;

    /*  Init */
    if (ctx->base==NULL)
        ctx->base = (const BYTE*)src, ctx->current=0, ctx->nextUpdate = g_maxDistance;
    if (src != ctx->base + ctx->current)   /* not contiguous */
    {
            ZSTD_resetCCtx(ctx);
            ctx->base = (const BYTE*)src;
            ctx->current = 0;
    }
    ctx->current += (U32)srcSize;

    while (srcSize)
    {
        size_t cSize;
        size_t blockSize = BLOCKSIZE;
        if (blockSize > srcSize) blockSize = srcSize;

        /* update hash table */
        if (g_maxDistance <= BLOCKSIZE)   /* static test => all blocks are independent */
        {
            ZSTD_resetCCtx(ctx);
            ctx->base = ip;
            ctx->current=0;
        }
        else if (ip >= ctx->base + ctx->nextUpdate)
        {
            ctx->nextUpdate += updateRate;
            ZSTD_limitCtx(ctx, ctx->nextUpdate - g_maxDistance);
        }

        /* compress */
        if (maxDstSize < ZSTD_blockHeaderSize) return (size_t)-ZSTD_ERROR_maxDstSize_tooSmall;
        cSize = ZSTD_compressBlock(ctx, op+ZSTD_blockHeaderSize, maxDstSize-ZSTD_blockHeaderSize, ip, blockSize);
        if (cSize == 0)
        {
            cSize = ZSTD_noCompressBlock(op, maxDstSize, ip, blockSize);   /* block is not compressible */
            if (ZSTD_isError(cSize)) return cSize;
        }
        else
        {
            if (ZSTD_isError(cSize)) return cSize;
            op[0] = (BYTE)(cSize>>16);
            op[1] = (BYTE)(cSize>>8);
            op[2] = (BYTE)cSize;
            op[0] += (BYTE)(bt_compressed << 6); /* is a compressed block */
            cSize += 3;
        }
        op += cSize;
        maxDstSize -= cSize;
        ip += blockSize;
        srcSize -= blockSize;
    }

    return op-ostart;
}


size_t ZSTD_compressEnd(ZSTD_cctx_t ctx, void* dst, size_t maxDstSize)
{
    BYTE* op = (BYTE*)dst;

    /* Sanity check */
    (void)ctx;
    if (maxDstSize < ZSTD_blockHeaderSize) return (size_t)-ZSTD_ERROR_maxDstSize_tooSmall;

    /* End of frame */
    op[0] = (BYTE)(bt_end << 6);
    op[1] = 0;
    op[2] = 0;

    return 3;
}


static size_t ZSTD_compressCCtx(void* ctx, void* dst, size_t maxDstSize, const void* src, size_t srcSize)
{
    BYTE* const ostart = (BYTE* const)dst;
    BYTE* op = ostart;

    /* Header */
    {
        size_t headerSize = ZSTD_compressBegin(ctx, dst, maxDstSize);
        if(ZSTD_isError(headerSize)) return headerSize;
        op += headerSize;
        maxDstSize -= headerSize;
    }

    /* Compression */
    {
        size_t cSize = ZSTD_compressContinue(ctx, op, maxDstSize, src, srcSize);
        if (ZSTD_isError(cSize)) return cSize;
        op += cSize;
        maxDstSize -= cSize;
    }

    /* Close frame */
    {
        size_t endSize = ZSTD_compressEnd(ctx, op, maxDstSize);
        if(ZSTD_isError(endSize)) return endSize;
        op += endSize;
    }

    return (op - ostart);
}


size_t ZSTD_compress(void* dst, size_t maxDstSize, const void* src, size_t srcSize)
{
    void* ctx;
    size_t r;

    ctx = ZSTD_createCCtx();
    r = ZSTD_compressCCtx(ctx, dst, maxDstSize, src, srcSize);
    ZSTD_freeCCtx(ctx);
    return r;
}


/**************************************************************
*   Decompression code
**************************************************************/

size_t ZSTD_getcBlockSize(const void* src, size_t srcSize, blockProperties_t* bpPtr)
{
    const BYTE* const in = (const BYTE* const)src;
    BYTE headerFlags;
    U32 cSize;

    if (srcSize < 3) return (size_t)-ZSTD_ERROR_wrongSrcSize;

    headerFlags = *in;
    cSize = in[2] + (in[1]<<8) + ((in[0] & 7)<<16);

    bpPtr->blockType = (blockType_t)(headerFlags >> 6);
    bpPtr->origSize = (bpPtr->blockType == bt_rle) ? cSize : 0;

    if (bpPtr->blockType == bt_end) return 0;
    if (bpPtr->blockType == bt_rle) return 1;
    return cSize;
}


static size_t ZSTD_copyUncompressedBlock(void* dst, size_t maxDstSize, const void* src, size_t srcSize)
{
    if (srcSize > maxDstSize) return (size_t)-ZSTD_ERROR_maxDstSize_tooSmall;
    memcpy(dst, src, srcSize);
    return srcSize;
}


/* force inline : 'fast' really needs to be evaluated at compile time */
FORCE_INLINE size_t ZSTD_decompressLiterals_usingDTable_generic(
                       void* const dst, size_t maxDstSize,
                 const void* src, size_t srcSize,
                 const void* DTable, U32 fast)
{
    BYTE* op = (BYTE*) dst;
    BYTE* const olimit = op;
    BYTE* const oend = op + maxDstSize;
    FSE_DStream_t bitD;
    FSE_DState_t state1, state2;
    size_t errorCode;

    /* Init */
    errorCode = FSE_initDStream(&bitD, src, srcSize);
    if (FSE_isError(errorCode)) return (size_t)-ZSTD_ERROR_GENERIC;

    FSE_initDState(&state1, &bitD, DTable);
    FSE_initDState(&state2, &bitD, DTable);
    op = oend;

    /* 2-4 symbols per loop */
    while (!FSE_reloadDStream(&bitD) && (op>olimit+3))
    {
        *--op = fast ? FSE_decodeSymbolFast(&state1, &bitD) : FSE_decodeSymbol(&state1, &bitD);

        if (LitFSELog*2+7 > sizeof(size_t)*8)    /* This test must be static */
            FSE_reloadDStream(&bitD);

        *--op = fast ? FSE_decodeSymbolFast(&state2, &bitD) : FSE_decodeSymbol(&state2, &bitD);

        if (LitFSELog*4+7 < sizeof(size_t)*8)    /* This test must be static */
        {
            *--op = fast ? FSE_decodeSymbolFast(&state1, &bitD) : FSE_decodeSymbol(&state1, &bitD);
            *--op = fast ? FSE_decodeSymbolFast(&state2, &bitD) : FSE_decodeSymbol(&state2, &bitD);
        }
    }

    /* tail */
    while (1)
    {
        if ( (FSE_reloadDStream(&bitD)>2) || (op==olimit) || (FSE_endOfDState(&state1) && FSE_endOfDStream(&bitD)) )
            break;

        *--op = fast ? FSE_decodeSymbolFast(&state1, &bitD) : FSE_decodeSymbol(&state1, &bitD);

        if ( (FSE_reloadDStream(&bitD)>2) || (op==olimit) || (FSE_endOfDState(&state2) && FSE_endOfDStream(&bitD)) )
            break;

        *--op = fast ? FSE_decodeSymbolFast(&state2, &bitD) : FSE_decodeSymbol(&state2, &bitD);
    }

    /* end ? */
    if (FSE_endOfDStream(&bitD) && FSE_endOfDState(&state1) && FSE_endOfDState(&state2) )
        return oend-op;

    if (op==olimit) return (size_t)-ZSTD_ERROR_maxDstSize_tooSmall;   /* dst buffer is full, but cSrc unfinished */

    return (size_t)-ZSTD_ERROR_GENERIC;
}

static size_t ZSTD_decompressLiterals_usingDTable(
                       void* const dst, size_t maxDstSize,
                 const void* src, size_t srcSize,
                 const void* DTable, U32 fast)
{
    if (fast) return ZSTD_decompressLiterals_usingDTable_generic(dst, maxDstSize, src, srcSize, DTable, 1);
    return ZSTD_decompressLiterals_usingDTable_generic(dst, maxDstSize, src, srcSize, DTable, 0);
}

static size_t ZSTD_decompressLiterals(void* ctx, void* dst, size_t maxDstSize,
                                const void* src, size_t srcSize)
{
    /* assumed : blockType == blockCompressed */
    const BYTE* ip = (const BYTE*)src;
    short norm[256];
    void* DTable = ctx;
    U32 maxSymbolValue = 255;
    U32 tableLog;
    U32 fastMode;
    size_t errorCode;

    if (srcSize < 2) return (size_t)-ZSTD_ERROR_wrongLBlockSize;   /* too small input size */

    errorCode = FSE_readHeader (norm, &maxSymbolValue, &tableLog, ip, srcSize);
    if (FSE_isError(errorCode)) return (size_t)-ZSTD_ERROR_GENERIC;
    ip += errorCode;
    srcSize -= errorCode;

    errorCode = FSE_buildDTable (DTable, norm, maxSymbolValue, tableLog);
    if (FSE_isError(errorCode)) return (size_t)-ZSTD_ERROR_GENERIC;
    fastMode = (U32)errorCode;

    return ZSTD_decompressLiterals_usingDTable (dst, maxDstSize, ip, srcSize, DTable, fastMode);
}


size_t ZSTD_decodeLiteralsBlock(void* ctx,
                                void* dst, size_t maxDstSize,
                          const BYTE** litPtr,
                          const void* src, size_t srcSize)
{
    const BYTE* const istart = (const BYTE* const)src;
    const BYTE* ip = istart;
    BYTE* const ostart = (BYTE* const)dst;
    BYTE* const oend = ostart + maxDstSize;
    blockProperties_t litbp;

    size_t litcSize = ZSTD_getcBlockSize(src, srcSize, &litbp);
    if (ZSTD_isError(litcSize)) return litcSize;
    if (litcSize > srcSize - ZSTD_blockHeaderSize) return (size_t)-ZSTD_ERROR_wrongLBlockSize;
    ip += ZSTD_blockHeaderSize;

    switch(litbp.blockType)
    {
    case bt_raw: *litPtr = ip; ip+= litcSize; break;
    case bt_rle:
        {
            size_t rleSize = litbp.origSize;
            memset(oend - rleSize, *ip, rleSize);
            *litPtr = oend - rleSize;
            ip++;
            break;
        }
    case bt_compressed:
        {
            size_t cSize = ZSTD_decompressLiterals(ctx, dst, maxDstSize, ip, litcSize);
            if (ZSTD_isError(cSize)) return cSize;
            *litPtr = oend - cSize;
            ip += litcSize;
            break;
        }
    default:
        return (size_t)-ZSTD_ERROR_GENERIC;
    }

    return ip-istart;
}


size_t ZSTD_decodeSeqHeaders(size_t* lastLLPtr, const BYTE** dumpsPtr,
                               void* DTableLL, void* DTableML, void* DTableOffb,
                         const void* src, size_t srcSize)
{
    const BYTE* const istart = (const BYTE* const)src;
    const BYTE* ip = istart;
    const BYTE* const iend = istart + srcSize;
    U32 LLtype, Offtype, MLtype;
    U32 LLlog, Offlog, MLlog;
    size_t dumpsLength;

    /* SeqHead */
    ip += ZSTD_readProgressive(lastLLPtr, ip);
    LLtype  = *ip >> 6;
    Offtype = (*ip >> 4) & 3;
    MLtype  = (*ip >> 2) & 3;
    if (*ip & 2)
    {
        dumpsLength  = ip[2];
        dumpsLength += ip[1] << 8;
        ip += 3;
    }
    else
    {
        dumpsLength  = ip[1];
        dumpsLength += (ip[0] & 1) << 8;
        ip += 2;
    }
    *dumpsPtr = ip;
    ip += dumpsLength;

    /* sequences */
    {
        S16 norm[MaxML+1];    /* assumption : MaxML >= MaxLL and MaxOff */
        size_t headerSize;

        /* Build DTables */
        switch(LLtype)
        {
        U32 max;
        case bt_rle :
            LLlog = 0;
            FSE_buildDTable_rle(DTableLL, *ip++); break;
        case bt_raw :
            LLlog = LLbits;
            FSE_buildDTable_raw(DTableLL, LLbits); break;
        default :
            max = MaxLL;
            headerSize = FSE_readHeader(norm, &max, &LLlog, ip, iend-ip);
            if (FSE_isError(headerSize)) return (size_t)-ZSTD_ERROR_GENERIC;
            ip += headerSize;
            FSE_buildDTable(DTableLL, norm, max, LLlog);
        }

        switch(Offtype)
        {
        U32 max;
        case bt_rle :
            Offlog = 0;
            FSE_buildDTable_rle(DTableOffb, *ip++); break;
        case bt_raw :
            Offlog = Offbits;
            FSE_buildDTable_raw(DTableOffb, Offbits); break;
        default :
            max = MaxOff;
            headerSize = FSE_readHeader(norm, &max, &Offlog, ip, iend-ip);
            if (FSE_isError(headerSize)) return (size_t)-ZSTD_ERROR_GENERIC;
            ip += headerSize;
            FSE_buildDTable(DTableOffb, norm, max, Offlog);
        }

        switch(MLtype)
        {
        U32 max;
        case bt_rle :
            MLlog = 0;
            FSE_buildDTable_rle(DTableML, *ip++); break;
        case bt_raw :
            MLlog = MLbits;
            FSE_buildDTable_raw(DTableML, MLbits); break;
        default :
            max = MaxML;
            headerSize = FSE_readHeader(norm, &max, &MLlog, ip, iend-ip);
            if (FSE_isError(headerSize)) return (size_t)-ZSTD_ERROR_GENERIC;
            ip += headerSize;
            FSE_buildDTable(DTableML, norm, max, MLlog);
        }
    }

    return ip-istart;
}


#define ZSTD_prefetch(p) { const BYTE pByte = *(volatile const BYTE*)p; }

FORCE_INLINE size_t ZSTD_decompressBlock(void* ctx, void* dst, size_t maxDstSize,
                             const void* src, size_t srcSize)
{
    const BYTE* ip = (const BYTE*)src;
    const BYTE* const iend = ip + srcSize;
    BYTE* const ostart = (BYTE* const)dst;
    BYTE* op = ostart;
    BYTE* const oend = ostart + maxDstSize;
    size_t errorCode;
    size_t lastLLSize;
    const BYTE* dumps;
    const BYTE* litPtr;
    const BYTE* litEnd;
    const size_t dec32table[] = {4, 1, 2, 1, 4, 4, 4, 4};   /* added */
    const size_t dec64table[] = {8, 8, 8, 7, 8, 9,10,11};   /* substracted */
    void* DTableML = ctx;
    void* DTableLL = ((U32*)ctx) + FSE_DTABLE_SIZE_U32(MLFSELog);
    void* DTableOffb = ((U32*)DTableLL) + FSE_DTABLE_SIZE_U32(LLFSELog);

    /* blockType == blockCompressed, srcSize is trusted */

    /* literal sub-block */
    errorCode = ZSTD_decodeLiteralsBlock(ctx, dst, maxDstSize, &litPtr, src, srcSize);
    if (ZSTD_isError(errorCode)) return errorCode;
    ip += errorCode;

    /* Build Decoding Tables */
    errorCode = ZSTD_decodeSeqHeaders(&lastLLSize, &dumps,
                                      DTableLL, DTableML, DTableOffb,
                                      ip, iend-ip);
    if (ZSTD_isError(errorCode)) return errorCode;
    /* end pos */
    if ((litPtr>=ostart) && (litPtr<=oend))
        litEnd = oend - lastLLSize;
    else
        litEnd = ip - lastLLSize;
    ip += errorCode;

    /* decompression */
    {
        FSE_DStream_t DStream;
        FSE_DState_t stateLL, stateOffb, stateML;
        size_t prevOffset = 0, offset = 0;
        size_t qutt=0;

        FSE_initDStream(&DStream, ip, iend-ip);
        FSE_initDState(&stateLL, &DStream, DTableLL);
        FSE_initDState(&stateOffb, &DStream, DTableOffb);
        FSE_initDState(&stateML, &DStream, DTableML);

        while (FSE_reloadDStream(&DStream)<2)
        {
            U32 nbBits, offsetCode;
            const BYTE* match;
            size_t litLength;
            size_t matchLength;
            size_t newOffset;

_another_round:

            /* Literals */
            litLength = FSE_decodeSymbol(&stateLL, &DStream);
            if (litLength) prevOffset = offset;
            if (litLength == MaxLL)
            {
                BYTE add = *dumps++;
                if (add < 255) litLength += add;
                else
                {
                    //litLength = (*(U32*)dumps) & 0xFFFFFF;
                    litLength = ZSTD_readLE32(dumps) & 0xFFFFFF;
                    dumps += 3;
                }
            }
            if (((size_t)(litPtr - op) < 8) || ((size_t)(oend-(litPtr+litLength)) < 8))
                memmove(op, litPtr, litLength);   /* overwrite risk */
            else
                ZSTD_wildcopy(op, litPtr, litLength);
            op += litLength;
            litPtr += litLength;

            /* Offset */
            offsetCode = FSE_decodeSymbol(&stateOffb, &DStream);
            if (ZSTD_32bits()) FSE_reloadDStream(&DStream);
            nbBits = offsetCode - 1;
            if (offsetCode==0) nbBits = 0;   /* cmove */
            newOffset = FSE_readBits(&DStream, nbBits);
            if (ZSTD_32bits()) FSE_reloadDStream(&DStream);
            newOffset += (size_t)1 << nbBits;
            if (offsetCode==0) newOffset = prevOffset;
            match = op - newOffset;
            prevOffset = offset;
            offset = newOffset;

            /* MatchLength */
            matchLength = FSE_decodeSymbol(&stateML, &DStream);
            if (matchLength == MaxML)
            {
                BYTE add = *dumps++;
                if (add < 255) matchLength += add;
                else
                {
                    matchLength = ZSTD_readLE32(dumps) & 0xFFFFFF;
                    dumps += 3;
                }
            }
            matchLength += MINMATCH;

            /* copy Match */
            {
                BYTE* const endMatch = op + matchLength;
                U64 saved[2];

                if ((size_t)(litPtr - endMatch) < 12)
                {
                    qutt = endMatch + 12 - litPtr;
                    if ((litPtr + qutt) > oend) qutt = oend-litPtr;
                    memcpy(saved, litPtr, qutt);
                }

                if (offset < 8)
                {
                    const size_t dec64 = dec64table[offset];
                    op[0] = match[0];
                    op[1] = match[1];
                    op[2] = match[2];
                    op[3] = match[3];
                    match += dec32table[offset];
                    ZSTD_copy4(op+4, match);
                    match -= dec64;
                } else { ZSTD_copy8(op, match); }

                if (endMatch > oend-12)
                {
                    if (op < oend-16)
                    {
                        ZSTD_wildcopy(op+8, match+8, (oend-8) - (op+8));
                        match += (oend-8) - op;
                        op = oend-8;
                    }
                    while (op<endMatch) *op++ = *match++;
                }
                else
                    ZSTD_wildcopy(op+8, match+8, matchLength-8);   /* works even if matchLength < 8 */

                op = endMatch;

                if ((size_t)(litPtr - endMatch) < 12)
                    memcpy((void*)litPtr, saved, qutt);
            }
        }

        /* check if reached exact end */
        if (FSE_reloadDStream(&DStream) > 2) return (size_t)-ZSTD_ERROR_GENERIC;   /* requested too much : data is corrupted */
        if (!FSE_endOfDState(&stateLL) && !FSE_endOfDState(&stateML) && !FSE_endOfDState(&stateOffb)) goto _another_round;   /* some ultra-compressible sequence remain ! */
        if (litPtr != litEnd) goto _another_round;   /* literals not entirely spent */

        /* last literal segment */
        if (op != litPtr) memmove(op, litPtr, lastLLSize);
        op += lastLLSize;
    }

    return op-ostart;
}


static size_t ZSTD_decompressDCtx(void* ctx, void* dst, size_t maxDstSize, const void* src, size_t srcSize)
{
    const BYTE* ip = (const BYTE*)src;
    const BYTE* iend = ip + srcSize;
    BYTE* const ostart = (BYTE* const)dst;
    BYTE* op = ostart;
    BYTE* const oend = ostart + maxDstSize;
    size_t remainingSize = srcSize;
    U32 magicNumber;
    size_t errorCode=0;
    blockProperties_t blockProperties;

    /* Header */
    if (srcSize < ZSTD_frameHeaderSize) return (size_t)-ZSTD_ERROR_wrongSrcSize;
    magicNumber = ZSTD_readBE32(src);
    if (magicNumber != ZSTD_magicNumber) return (size_t)-ZSTD_ERROR_wrongMagicNumber;
    ip += ZSTD_frameHeaderSize; remainingSize -= ZSTD_frameHeaderSize;

    while (1)
    {
        size_t blockSize = ZSTD_getcBlockSize(ip, iend-ip, &blockProperties);
        if (ZSTD_isError(blockSize))
            return blockSize;

        ip += ZSTD_blockHeaderSize;
        remainingSize -= ZSTD_blockHeaderSize;
        if (ip+blockSize > iend)
            return (size_t)-ZSTD_ERROR_wrongSrcSize;

        switch(blockProperties.blockType)
        {
        case bt_compressed:
            errorCode = ZSTD_decompressBlock(ctx, op, oend-op, ip, blockSize);
            break;
        case bt_raw :
            errorCode = ZSTD_copyUncompressedBlock(op, oend-op, ip, blockSize);
            break;
        case bt_rle :
            return (size_t)-ZSTD_ERROR_GENERIC;   /* not yet handled */
            break;
        case bt_end :
            /* end of frame */
            if (remainingSize) return (size_t)-ZSTD_ERROR_wrongSrcSize;
            break;
        default:
            return (size_t)-ZSTD_ERROR_GENERIC;
        }
        if (blockSize == 0) break;   /* bt_end */

        if (ZSTD_isError(errorCode)) return errorCode;
        op += errorCode;
        ip += blockSize;
        remainingSize -= blockSize;
    }

    return op-ostart;
}


size_t ZSTD_decompress(void* dst, size_t maxDstSize, const void* src, size_t srcSize)
{
    U32 ctx[FSE_DTABLE_SIZE_U32(LLFSELog) + FSE_DTABLE_SIZE_U32(OffFSELog) + FSE_DTABLE_SIZE_U32(MLFSELog)];
    return ZSTD_decompressDCtx(ctx, dst, maxDstSize, src, srcSize);
}


/*******************************
*  Streaming Decompression API
*******************************/

typedef struct
{
    U32 ctx[FSE_DTABLE_SIZE_U32(LLFSELog) + FSE_DTABLE_SIZE_U32(OffFSELog) + FSE_DTABLE_SIZE_U32(MLFSELog)];
    size_t expected;
    blockType_t bType;
    U32 phase;
} dctx_t;


ZSTD_dctx_t ZSTD_createDCtx(void)
{
    dctx_t* dctx = (dctx_t*)malloc(sizeof(dctx_t));
    dctx->expected = ZSTD_frameHeaderSize;
    dctx->phase = 0;
    return (ZSTD_dctx_t)dctx;
}

size_t ZSTD_freeDCtx(ZSTD_dctx_t dctx)
{
    free(dctx);
    return 0;
}


size_t ZSTD_nextSrcSizeToDecompress(ZSTD_dctx_t dctx)
{
    return ((dctx_t*)dctx)->expected;
}

size_t ZSTD_decompressContinue(ZSTD_dctx_t dctx, void* dst, size_t maxDstSize, const void* src, size_t srcSize)
{
    dctx_t* ctx = (dctx_t*)dctx;

    /* Sanity check */
    if (srcSize != ctx->expected) return (size_t)-ZSTD_ERROR_wrongSrcSize;

    /* Decompress : frame header */
    if (ctx->phase == 0)
    {
        /* Check frame magic header */
        U32 magicNumber = ZSTD_readBE32(src);
        if (magicNumber != ZSTD_magicNumber) return (size_t)-ZSTD_ERROR_wrongMagicNumber;
        ctx->phase = 1;
        ctx->expected = ZSTD_blockHeaderSize;
        return 0;
    }

    /* Decompress : block header */
    if (ctx->phase == 1)
    {
        blockProperties_t bp;
        size_t blockSize = ZSTD_getcBlockSize(src, ZSTD_blockHeaderSize, &bp);
        if (ZSTD_isError(blockSize)) return blockSize;
        if (bp.blockType == bt_end)
        {
            ctx->expected = 0;
            ctx->phase = 0;
        }
        else
        {
            ctx->expected = blockSize;
            ctx->bType = bp.blockType;
            ctx->phase = 2;
        }

        return 0;
    }

    /* Decompress : block content */
    {
        size_t rSize;
        switch(ctx->bType)
        {
        case bt_compressed:
            rSize = ZSTD_decompressBlock(ctx, dst, maxDstSize, src, srcSize);
            break;
        case bt_raw :
            rSize = ZSTD_copyUncompressedBlock(dst, maxDstSize, src, srcSize);
            break;
        case bt_rle :
            return (size_t)-ZSTD_ERROR_GENERIC;   /* not yet handled */
            break;
        case bt_end :   /* should never happen (filtered at phase 1) */
            rSize = 0;
            break;
        default:
            return (size_t)-ZSTD_ERROR_GENERIC;
        }
        ctx->phase = 1;
        ctx->expected = ZSTD_blockHeaderSize;
        return rSize;
    }

}


