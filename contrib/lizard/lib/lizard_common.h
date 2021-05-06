/*
    Lizard - Fast LZ compression algorithm 
    Copyright (C) 2011-2015, Yann Collet
    Copyright (C) 2016-2017, Przemyslaw Skibinski <inikep@gmail.com>

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
       - Lizard source repository : https://github.com/inikep/lizard
*/

#ifndef LIZARD_COMMON_H_2983
#define LIZARD_COMMON_H_2983

#if defined (__cplusplus)
extern "C" {
#endif


/*-************************************
*  Memory routines
**************************************/
#include <stdlib.h>   /* malloc, calloc, free */
#include <string.h>   /* memset, memcpy */
#include <stdint.h>   /* intptr_t */
#include "entropy/mem.h"
#include "lizard_compress.h"      /* LIZARD_GCC_VERSION */

//#define LIZARD_USE_LOGS
#define LIZARD_LOG_COMPRESS(...) //printf(__VA_ARGS__)
#define LIZARD_LOG_DECOMPRESS(...) //printf(__VA_ARGS__)

#define LIZARD_LOG_COMPRESS_LZ4(...) //printf(__VA_ARGS__)
#define COMPLOG_CODEWORDS_LZ4(...) //printf(__VA_ARGS__)
#define LIZARD_LOG_DECOMPRESS_LZ4(...) //printf(__VA_ARGS__)
#define DECOMPLOG_CODEWORDS_LZ4(...) //printf(__VA_ARGS__)

#define LIZARD_LOG_COMPRESS_LIZv1(...) //printf(__VA_ARGS__)
#define COMPLOG_CODEWORDS_LIZv1(...) //printf(__VA_ARGS__)
#define LIZARD_LOG_DECOMPRESS_LIZv1(...) //printf(__VA_ARGS__)
#define DECOMPLOG_CODEWORDS_LIZv1(...) //printf(__VA_ARGS__)




/*-************************************
*  Common Constants
**************************************/
#define MINMATCH 4
//#define USE_LZ4_ONLY
//#define LIZARD_USE_TEST

#define LIZARD_DICT_SIZE       (1<<24)
#define WILDCOPYLENGTH      16
#define LASTLITERALS WILDCOPYLENGTH
#define MFLIMIT (WILDCOPYLENGTH+MINMATCH)

#define LIZARD_MAX_PRICE           (1<<28)
#define LIZARD_INIT_LAST_OFFSET    0
#define LIZARD_MAX_16BIT_OFFSET    (1<<16)
#define MM_LONGOFF              16
#define LIZARD_BLOCK_SIZE_PAD      (LIZARD_BLOCK_SIZE+32)
#define LIZARD_COMPRESS_ADD_BUF    (5*LIZARD_BLOCK_SIZE_PAD)
#ifndef LIZARD_NO_HUFFMAN
    #define LIZARD_COMPRESS_ADD_HUF    HUF_compressBound(LIZARD_BLOCK_SIZE_PAD)
    #define LIZARD_HUF_BLOCK_SIZE      LIZARD_BLOCK_SIZE
#else
    #define LIZARD_COMPRESS_ADD_HUF    0
    #define LIZARD_HUF_BLOCK_SIZE      1
#endif

/* LZ4 codewords */
#define ML_BITS_LZ4  4
#define ML_MASK_LZ4  ((1U<<ML_BITS_LZ4)-1)
#define RUN_BITS_LZ4 (8-ML_BITS_LZ4)
#define RUN_MASK_LZ4 ((1U<<RUN_BITS_LZ4)-1)

/* LIZv1 codewords */
#define ML_BITS_LIZv1       4
#define RUN_BITS_LIZv1      3
#define ML_RUN_BITS         (ML_BITS_LIZv1 + RUN_BITS_LIZv1)
#define MAX_SHORT_LITLEN    7
#define MAX_SHORT_MATCHLEN  15
#define LIZARD_LAST_LONG_OFF   31

/* header byte */
#define LIZARD_FLAG_LITERALS       1
#define LIZARD_FLAG_FLAGS          2
#define LIZARD_FLAG_OFFSET16       4
#define LIZARD_FLAG_OFFSET24       8
#define LIZARD_FLAG_LEN            16
#define LIZARD_FLAG_UNCOMPRESSED   128

/* stream numbers */
#define LIZARD_STREAM_LITERALS       0
#define LIZARD_STREAM_FLAGS          1
#define LIZARD_STREAM_OFFSET16       2
#define LIZARD_STREAM_OFFSET24       3
#define LIZARD_STREAM_LEN            4
#define LIZARD_STREAM_UNCOMPRESSED   5




typedef enum { Lizard_parser_fastSmall, Lizard_parser_fast, Lizard_parser_fastBig, Lizard_parser_noChain, Lizard_parser_hashChain, Lizard_parser_priceFast, Lizard_parser_lowestPrice, Lizard_parser_optimalPrice, Lizard_parser_optimalPriceBT } Lizard_parser_type;   /* from faster to stronger */ 
typedef enum { Lizard_coderwords_LZ4, Lizard_coderwords_LIZv1 } Lizard_decompress_type;
typedef struct
{
    U32 windowLog;     /* largest match distance : impact decompression buffer size */
    U32 contentLog;    /* full search segment : larger == more compression, slower, more memory (useless for fast) */
    U32 hashLog;       /* dispatch table : larger == more memory, faster*/
    U32 hashLog3;      /* dispatch table : larger == more memory, faster*/
    U32 searchNum;     /* nb of searches : larger == more compression, slower*/
    U32 searchLength;  /* size of matches : larger == faster decompression */
    U32 minMatchLongOff;  /* min match size with offsets >= 1<<16 */ 
    U32 sufficientLength;  /* used only by optimal parser: size of matches which is acceptable: larger == more compression, slower */
    U32 fullSearch;    /* used only by optimal parser: perform full search of matches: 1 == more compression, slower */
    Lizard_parser_type parserType;
    Lizard_decompress_type decompressType;
} Lizard_parameters; 


struct Lizard_stream_s
{
    const BYTE* end;        /* next block here to continue on current prefix */
    const BYTE* base;       /* All index relative to this position */
    const BYTE* dictBase;   /* alternate base for extDict */
    U32   dictLimit;        /* below that point, need extDict */
    U32   lowLimit;         /* below that point, no more dict */
    U32   nextToUpdate;     /* index from which to continue dictionary update */
    U32   allocatedMemory;
    int   compressionLevel;
    Lizard_parameters params;
    U32   hashTableSize;
    U32   chainTableSize;
    U32*  chainTable;
    U32*  hashTable;
    int   last_off;
    const BYTE* off24pos;
    U32   huffType;
    U32   comprStreamLen;

    BYTE*  huffBase;
    BYTE*  huffEnd;
    BYTE*  offset16Base;
    BYTE*  offset24Base;
    BYTE*  lenBase;
    BYTE*  literalsBase;
    BYTE*  flagsBase;
    BYTE*  offset16Ptr;
    BYTE*  offset24Ptr;
    BYTE*  lenPtr;
    BYTE*  literalsPtr;
    BYTE*  flagsPtr;
    BYTE*  offset16End;
    BYTE*  offset24End;
    BYTE*  lenEnd;
    BYTE*  literalsEnd;
    BYTE*  flagsEnd;
    U32 flagFreq[256];
    U32 litFreq[256];
    U32 litSum, flagSum;
    U32 litPriceSum, log2LitSum, log2FlagSum;
    U32  cachedPrice;
    U32  cachedLitLength;
    const BYTE* cachedLiterals; 
    const BYTE* diffBase;
    const BYTE* srcBase;
    const BYTE* destBase;
};

struct Lizard_streamDecode_s {
    const BYTE* externalDict;
    size_t extDictSize;
    const BYTE* prefixEnd;
    size_t prefixSize;
};

struct Lizard_dstream_s
{
    const BYTE*  offset16Ptr;
    const BYTE*  offset24Ptr;
    const BYTE*  lenPtr;
    const BYTE*  literalsPtr;
    const BYTE*  flagsPtr;
    const BYTE*  offset16End;
    const BYTE*  offset24End;
    const BYTE*  lenEnd;
    const BYTE*  literalsEnd;
    const BYTE*  flagsEnd;
    const BYTE*  diffBase;
    intptr_t last_off;
};

typedef struct Lizard_dstream_s Lizard_dstream_t;

/* *************************************
*  HC Pre-defined compression levels
***************************************/
#define LIZARD_WINDOWLOG_LZ4   16
#define LIZARD_CHAINLOG_LZ4    LIZARD_WINDOWLOG_LZ4
#define LIZARD_HASHLOG_LZ4     18
#define LIZARD_HASHLOG_LZ4SM   12

#define LIZARD_WINDOWLOG_LIZv1 22
#define LIZARD_CHAINLOG_LIZv1  LIZARD_WINDOWLOG_LIZv1
#define LIZARD_HASHLOG_LIZv1   18



static const Lizard_parameters Lizard_defaultParameters[LIZARD_MAX_CLEVEL+1-LIZARD_MIN_CLEVEL] =
{
    /*               windLog,              contentLog,               HashLog,  H3,  Snum, SL,   MMLongOff, SuffL, FS, Parser function,           Decompressor type  */
    {   LIZARD_WINDOWLOG_LZ4,                       0,  LIZARD_HASHLOG_LZ4SM,   0,     0,  0,           0,     0,  0, Lizard_parser_fastSmall,      Lizard_coderwords_LZ4   }, // level 10
    {   LIZARD_WINDOWLOG_LZ4,                       0,    LIZARD_HASHLOG_LZ4,   0,     0,  0,           0,     0,  0, Lizard_parser_fast,           Lizard_coderwords_LZ4   }, // level 11
    {   LIZARD_WINDOWLOG_LZ4,                       0,    LIZARD_HASHLOG_LZ4,   0,     0,  0,           0,     0,  0, Lizard_parser_noChain,        Lizard_coderwords_LZ4   }, // level 12
    {   LIZARD_WINDOWLOG_LZ4,      LIZARD_CHAINLOG_LZ4,   LIZARD_HASHLOG_LZ4,   0,     2,  5,           0,     0,  0, Lizard_parser_hashChain,      Lizard_coderwords_LZ4   }, // level 13
    {   LIZARD_WINDOWLOG_LZ4,      LIZARD_CHAINLOG_LZ4,   LIZARD_HASHLOG_LZ4,   0,     4,  5,           0,     0,  0, Lizard_parser_hashChain,      Lizard_coderwords_LZ4   }, // level 14
    {   LIZARD_WINDOWLOG_LZ4,      LIZARD_CHAINLOG_LZ4,   LIZARD_HASHLOG_LZ4,   0,     8,  5,           0,     0,  0, Lizard_parser_hashChain,      Lizard_coderwords_LZ4   }, // level 15
    {   LIZARD_WINDOWLOG_LZ4,      LIZARD_CHAINLOG_LZ4,   LIZARD_HASHLOG_LZ4,   0,    16,  4,           0,     0,  0, Lizard_parser_hashChain,      Lizard_coderwords_LZ4   }, // level 16
    {   LIZARD_WINDOWLOG_LZ4,      LIZARD_CHAINLOG_LZ4,   LIZARD_HASHLOG_LZ4,   0,   256,  4,           0,     0,  0, Lizard_parser_hashChain,      Lizard_coderwords_LZ4   }, // level 17
    {   LIZARD_WINDOWLOG_LZ4,   LIZARD_WINDOWLOG_LZ4+1,   LIZARD_HASHLOG_LZ4,  16,    16,  4,           0, 1<<10,  1, Lizard_parser_optimalPriceBT, Lizard_coderwords_LZ4   }, // level 18
    {   LIZARD_WINDOWLOG_LZ4,   LIZARD_WINDOWLOG_LZ4+1,                   23,  16,   256,  4,           0, 1<<10,  1, Lizard_parser_optimalPriceBT, Lizard_coderwords_LZ4   }, // level 19
    /*              windLog,                contentLog,              HashLog,  H3,  Snum, SL,   MMLongOff, SuffL, FS, Parser function,           Decompressor type  */
    { LIZARD_WINDOWLOG_LIZv1,                        0,                   14,   0,     1,  5,  MM_LONGOFF,     0,  0, Lizard_parser_fastBig,        Lizard_coderwords_LIZv1 }, // level 20
    { LIZARD_WINDOWLOG_LIZv1,    LIZARD_CHAINLOG_LIZv1,                   14,  13,     1,  5,  MM_LONGOFF,     0,  0, Lizard_parser_priceFast,      Lizard_coderwords_LIZv1 }, // level 21
    { LIZARD_WINDOWLOG_LIZv1,    LIZARD_CHAINLOG_LIZv1, LIZARD_HASHLOG_LIZv1,  13,     1,  5,  MM_LONGOFF,     0,  0, Lizard_parser_priceFast,      Lizard_coderwords_LIZv1 }, // level 22
    { LIZARD_WINDOWLOG_LIZv1,    LIZARD_CHAINLOG_LIZv1, LIZARD_HASHLOG_LIZv1,  13,     1,  5,  MM_LONGOFF,    64,  0, Lizard_parser_lowestPrice,    Lizard_coderwords_LIZv1 }, // level 23
    { LIZARD_WINDOWLOG_LIZv1,    LIZARD_CHAINLOG_LIZv1,                   23,  16,     2,  5,  MM_LONGOFF,    64,  0, Lizard_parser_lowestPrice,    Lizard_coderwords_LIZv1 }, // level 24
    { LIZARD_WINDOWLOG_LIZv1,    LIZARD_CHAINLOG_LIZv1,                   23,  16,     8,  4,  MM_LONGOFF,    64,  0, Lizard_parser_lowestPrice,    Lizard_coderwords_LIZv1 }, // level 25
    { LIZARD_WINDOWLOG_LIZv1,  LIZARD_CHAINLOG_LIZv1+1,                   23,  16,     8,  4,  MM_LONGOFF,    64,  1, Lizard_parser_optimalPriceBT, Lizard_coderwords_LIZv1 }, // level 26
    { LIZARD_WINDOWLOG_LIZv1,  LIZARD_CHAINLOG_LIZv1+1,                   23,  16,   128,  4,  MM_LONGOFF,    64,  1, Lizard_parser_optimalPriceBT, Lizard_coderwords_LIZv1 }, // level 27
    { LIZARD_WINDOWLOG_LIZv1,  LIZARD_CHAINLOG_LIZv1+1,                   23,  24, 1<<10,  4,  MM_LONGOFF, 1<<10,  1, Lizard_parser_optimalPriceBT, Lizard_coderwords_LIZv1 }, // level 28
    {                     24,                       25,                   23,  24, 1<<10,  4,  MM_LONGOFF, 1<<10,  1, Lizard_parser_optimalPriceBT, Lizard_coderwords_LIZv1 }, // level 29
#ifndef LIZARD_NO_HUFFMAN
    /*               windLog,               contentLog,              HashLog,  H3,  Snum, SL,   MMLongOff, SuffL, FS, Parser function,           Decompressor type  */
    {   LIZARD_WINDOWLOG_LZ4,                        0, LIZARD_HASHLOG_LZ4SM,   0,     0,  0,           0,     0,  0, Lizard_parser_fastSmall,      Lizard_coderwords_LZ4   }, // level 30
    {   LIZARD_WINDOWLOG_LZ4,                        0,   LIZARD_HASHLOG_LZ4,   0,     0,  0,           0,     0,  0, Lizard_parser_fast,           Lizard_coderwords_LZ4   }, // level 31
    {   LIZARD_WINDOWLOG_LZ4,                        0,                   14,   0,     0,  0,           0,     0,  0, Lizard_parser_noChain,        Lizard_coderwords_LZ4   }, // level 32
    {   LIZARD_WINDOWLOG_LZ4,                        0,   LIZARD_HASHLOG_LZ4,   0,     0,  0,           0,     0,  0, Lizard_parser_noChain,        Lizard_coderwords_LZ4   }, // level 33
    {   LIZARD_WINDOWLOG_LZ4,      LIZARD_CHAINLOG_LZ4,   LIZARD_HASHLOG_LZ4,   0,     2,  5,           0,     0,  0, Lizard_parser_hashChain,      Lizard_coderwords_LZ4   }, // level 34
    {   LIZARD_WINDOWLOG_LZ4,      LIZARD_CHAINLOG_LZ4,   LIZARD_HASHLOG_LZ4,   0,     4,  5,           0,     0,  0, Lizard_parser_hashChain,      Lizard_coderwords_LZ4   }, // level 35
    {   LIZARD_WINDOWLOG_LZ4,      LIZARD_CHAINLOG_LZ4,   LIZARD_HASHLOG_LZ4,   0,     8,  5,           0,     0,  0, Lizard_parser_hashChain,      Lizard_coderwords_LZ4   }, // level 36
    {   LIZARD_WINDOWLOG_LZ4,      LIZARD_CHAINLOG_LZ4,   LIZARD_HASHLOG_LZ4,   0,    16,  4,           0,     0,  0, Lizard_parser_hashChain,      Lizard_coderwords_LZ4   }, // level 37
    {   LIZARD_WINDOWLOG_LZ4,      LIZARD_CHAINLOG_LZ4,   LIZARD_HASHLOG_LZ4,   0,   256,  4,           0,     0,  0, Lizard_parser_hashChain,      Lizard_coderwords_LZ4   }, // level 38
    {   LIZARD_WINDOWLOG_LZ4,   LIZARD_WINDOWLOG_LZ4+1,                   23,  16,   256,  4,           0, 1<<10,  1, Lizard_parser_optimalPriceBT, Lizard_coderwords_LZ4   }, // level 39
    /*               windLog,               contentLog,              HashLog,  H3,  Snum, SL,   MMLongOff, SuffL, FS, Parser function,           Decompressor type  */
    { LIZARD_WINDOWLOG_LIZv1,                        0,                   14,   0,     1,  5,  MM_LONGOFF,     0,  0, Lizard_parser_fastBig,        Lizard_coderwords_LIZv1 }, // level 40
    { LIZARD_WINDOWLOG_LIZv1,    LIZARD_CHAINLOG_LIZv1,                   14,  13,     1,  5,  MM_LONGOFF,     0,  0, Lizard_parser_priceFast,      Lizard_coderwords_LIZv1 }, // level 41
    { LIZARD_WINDOWLOG_LIZv1,    LIZARD_CHAINLOG_LIZv1, LIZARD_HASHLOG_LIZv1,  13,     1,  5,  MM_LONGOFF,     0,  0, Lizard_parser_priceFast,      Lizard_coderwords_LIZv1 }, // level 42
    { LIZARD_WINDOWLOG_LIZv1,    LIZARD_CHAINLOG_LIZv1, LIZARD_HASHLOG_LIZv1,  13,     1,  5,  MM_LONGOFF,    64,  0, Lizard_parser_lowestPrice,    Lizard_coderwords_LIZv1 }, // level 43
    { LIZARD_WINDOWLOG_LIZv1,    LIZARD_CHAINLOG_LIZv1,                   23,  16,     2,  5,  MM_LONGOFF,    64,  0, Lizard_parser_lowestPrice,    Lizard_coderwords_LIZv1 }, // level 44
    { LIZARD_WINDOWLOG_LIZv1,    LIZARD_CHAINLOG_LIZv1,                   23,  16,     8,  4,  MM_LONGOFF,    64,  0, Lizard_parser_lowestPrice,    Lizard_coderwords_LIZv1 }, // level 45
    { LIZARD_WINDOWLOG_LIZv1,    LIZARD_CHAINLOG_LIZv1,                   23,  16,     8,  4,  MM_LONGOFF,    64,  0, Lizard_parser_optimalPrice,   Lizard_coderwords_LIZv1 }, // level 46
    { LIZARD_WINDOWLOG_LIZv1,  LIZARD_CHAINLOG_LIZv1+1,                   23,  16,     8,  4,  MM_LONGOFF,    64,  1, Lizard_parser_optimalPriceBT, Lizard_coderwords_LIZv1 }, // level 47
    { LIZARD_WINDOWLOG_LIZv1,  LIZARD_CHAINLOG_LIZv1+1,                   23,  16,   128,  4,  MM_LONGOFF,    64,  1, Lizard_parser_optimalPriceBT, Lizard_coderwords_LIZv1 }, // level 48
    {                     24,                       25,                   23,  24, 1<<10,  4,  MM_LONGOFF, 1<<10,  1, Lizard_parser_optimalPriceBT, Lizard_coderwords_LIZv1 }, // level 49
#endif
//  {                     10,                       10,                   10,   0,     0,  4,           0,     0,  0, Lizard_fast          }, // min values
//  {                     24,                       24,                   28,  24, 1<<24,  7,           0, 1<<24,  2, Lizard_optimal_price }, // max values
};



/*-************************************
*  Compiler Options
**************************************/
#ifdef _MSC_VER    /* Visual Studio */
#  define FORCE_INLINE static __forceinline
#  include <intrin.h>
#  pragma warning(disable : 4127)        /* disable: C4127: conditional expression is constant */
#  pragma warning(disable : 4293)        /* disable: C4293: too large shift (32-bits) */
#else
#  if defined(__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L)   /* C99 */
#    if defined(__GNUC__) || defined(__clang__)
#      define FORCE_INLINE static inline __attribute__((always_inline))
#    else
#      define FORCE_INLINE static inline
#    endif
#  else
#    define FORCE_INLINE static
#  endif   /* __STDC_VERSION__ */
#endif  /* _MSC_VER */

#define LIZARD_GCC_VERSION (__GNUC__ * 100 + __GNUC_MINOR__)
#if (LIZARD_GCC_VERSION >= 302) || (__INTEL_COMPILER >= 800) || defined(__clang__)
#  define expect(expr,value)    (__builtin_expect ((expr),(value)) )
#else
#  define expect(expr,value)    (expr)
#endif

#define likely(expr)     expect((expr) != 0, 1)
#define unlikely(expr)   expect((expr) != 0, 0)

#define KB *(1 <<10)
#define MB *(1 <<20)
#define GB *(1U<<30)

#define ALLOCATOR(n,s) calloc(n,s)
#define FREEMEM        free
#define MEM_INIT       memset
#ifndef MAX
    #define MAX(a,b) ((a)>(b))?(a):(b)
#endif
#ifndef MIN
	#define MIN(a,b) ((a)<(b)?(a):(b))
#endif

#if MINMATCH == 3
    #define MEM_readMINMATCH(ptr) (U32)(MEM_read32(ptr)<<8) 
#else
    #define MEM_readMINMATCH(ptr) (U32)(MEM_read32(ptr)) 
#endif 




/*-************************************
*  Reading and writing into memory
**************************************/
#define STEPSIZE sizeof(size_t)


MEM_STATIC void Lizard_copy8(void* dst, const void* src)
{
    memcpy(dst,src,8);
}

/* customized variant of memcpy, which can overwrite up to 7 bytes beyond dstEnd */
MEM_STATIC void Lizard_wildCopy(void* dstPtr, const void* srcPtr, void* dstEnd)
{
    BYTE* d = (BYTE*)dstPtr;
    const BYTE* s = (const BYTE*)srcPtr;
    BYTE* const e = (BYTE*)dstEnd;

#if 0
    const size_t l2 = 8 - (((size_t)d) & (sizeof(void*)-1));
    Lizard_copy8(d,s); if (d>e-9) return;
    d+=l2; s+=l2;
#endif /* join to align */

    do { Lizard_copy8(d,s); d+=8; s+=8; } while (d<e);
}

MEM_STATIC void Lizard_wildCopy16(BYTE* dstPtr, const BYTE* srcPtr, BYTE* dstEnd)
{
    do {
        Lizard_copy8(dstPtr, srcPtr);
        Lizard_copy8(dstPtr+8, srcPtr+8);
        dstPtr += 16;
        srcPtr += 16;
    }
    while (dstPtr < dstEnd);
}

/*
 * LIZARD_FORCE_SW_BITCOUNT
 * Define this parameter if your target system or compiler does not support hardware bit count
 */
#if defined(_MSC_VER) && defined(_WIN32_WCE)   /* Visual Studio for Windows CE does not support Hardware bit count */
#  define LIZARD_FORCE_SW_BITCOUNT
#endif


/* **************************************
*  Function body to include for inlining
****************************************/
MEM_STATIC U32 Lizard_highbit32(U32 val)
{
#   if defined(_MSC_VER)   /* Visual */
    unsigned long r=0;
    _BitScanReverse(&r, val);
    return (unsigned)r;
#   elif defined(__GNUC__) && (__GNUC__ >= 3)   /* GCC Intrinsic */
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


/*-************************************
*  Common functions
**************************************/
MEM_STATIC unsigned Lizard_NbCommonBytes (register size_t val)
{
    if (MEM_isLittleEndian()) {
        if (MEM_64bits()) {
#       if defined(_MSC_VER) && defined(_WIN64) && !defined(LIZARD_FORCE_SW_BITCOUNT)
            unsigned long r = 0;
            _BitScanForward64( &r, (U64)val );
            return (int)(r>>3);
#       elif (defined(__clang__) || (LIZARD_GCC_VERSION >= 304)) && !defined(LIZARD_FORCE_SW_BITCOUNT)
            return (__builtin_ctzll((U64)val) >> 3);
#       else
            static const int DeBruijnBytePos[64] = { 0, 0, 0, 0, 0, 1, 1, 2, 0, 3, 1, 3, 1, 4, 2, 7, 0, 2, 3, 6, 1, 5, 3, 5, 1, 3, 4, 4, 2, 5, 6, 7, 7, 0, 1, 2, 3, 3, 4, 6, 2, 6, 5, 5, 3, 4, 5, 6, 7, 1, 2, 4, 6, 4, 4, 5, 7, 2, 6, 5, 7, 6, 7, 7 };
            return DeBruijnBytePos[((U64)((val & -(long long)val) * 0x0218A392CDABBD3FULL)) >> 58];
#       endif
        } else /* 32 bits */ {
#       if defined(_MSC_VER) && !defined(LIZARD_FORCE_SW_BITCOUNT)
            unsigned long r;
            _BitScanForward( &r, (U32)val );
            return (int)(r>>3);
#       elif (defined(__clang__) || (LIZARD_GCC_VERSION >= 304)) && !defined(LIZARD_FORCE_SW_BITCOUNT)
            return (__builtin_ctz((U32)val) >> 3);
#       else
            static const int DeBruijnBytePos[32] = { 0, 0, 3, 0, 3, 1, 3, 0, 3, 2, 2, 1, 3, 2, 0, 1, 3, 3, 1, 2, 2, 2, 2, 0, 3, 1, 2, 0, 1, 0, 1, 1 };
            return DeBruijnBytePos[((U32)((val & -(S32)val) * 0x077CB531U)) >> 27];
#       endif
        }
    } else   /* Big Endian CPU */ {
        if (MEM_64bits()) {
#       if defined(_MSC_VER) && defined(_WIN64) && !defined(LIZARD_FORCE_SW_BITCOUNT)
            unsigned long r = 0;
            _BitScanReverse64( &r, val );
            return (unsigned)(r>>3);
#       elif (defined(__clang__) || (LIZARD_GCC_VERSION >= 304)) && !defined(LIZARD_FORCE_SW_BITCOUNT)
            return (__builtin_clzll((U64)val) >> 3);
#       else
            unsigned r;
            if (!(val>>32)) { r=4; } else { r=0; val>>=32; }
            if (!(val>>16)) { r+=2; val>>=8; } else { val>>=24; }
            r += (!val);
            return r;
#       endif
        } else /* 32 bits */ {
#       if defined(_MSC_VER) && !defined(LIZARD_FORCE_SW_BITCOUNT)
            unsigned long r = 0;
            _BitScanReverse( &r, (unsigned long)val );
            return (unsigned)(r>>3);
#       elif (defined(__clang__) || (LIZARD_GCC_VERSION >= 304)) && !defined(LIZARD_FORCE_SW_BITCOUNT)
            return (__builtin_clz((U32)val) >> 3);
#       else
            unsigned r;
            if (!(val>>16)) { r=2; val>>=8; } else { r=0; val>>=24; }
            r += (!val);
            return r;
#       endif
        }
    }
}

MEM_STATIC unsigned Lizard_count(const BYTE* pIn, const BYTE* pMatch, const BYTE* pInLimit)
{
    const BYTE* const pStart = pIn;

    while (likely(pIn<pInLimit-(STEPSIZE-1))) {
        size_t diff = MEM_readST(pMatch) ^ MEM_readST(pIn);
        if (!diff) { pIn+=STEPSIZE; pMatch+=STEPSIZE; continue; }
        pIn += Lizard_NbCommonBytes(diff);
        return (unsigned)(pIn - pStart);
    }

    if (MEM_64bits()) if ((pIn<(pInLimit-3)) && (MEM_read32(pMatch) == MEM_read32(pIn))) { pIn+=4; pMatch+=4; }
    if ((pIn<(pInLimit-1)) && (MEM_read16(pMatch) == MEM_read16(pIn))) { pIn+=2; pMatch+=2; }
    if ((pIn<pInLimit) && (*pMatch == *pIn)) pIn++;
    return (unsigned)(pIn - pStart);
}

/* alias to functions with compressionLevel=1 */
int Lizard_sizeofState_MinLevel(void);
int Lizard_compress_MinLevel(const char* source, char* dest, int sourceSize, int maxDestSize);
int Lizard_compress_extState_MinLevel (void* state, const char* source, char* dest, int inputSize, int maxDestSize);
Lizard_stream_t* Lizard_resetStream_MinLevel (Lizard_stream_t* streamPtr);
Lizard_stream_t* Lizard_createStream_MinLevel(void);


#if defined (__cplusplus)
}
#endif

#endif /* LIZARD_COMMON_H_2983827168210 */
