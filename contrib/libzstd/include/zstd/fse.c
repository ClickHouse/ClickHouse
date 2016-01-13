/* ******************************************************************
   FSE : Finite State Entropy coder
   Copyright (C) 2013-2015, Yann Collet.

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
    - FSE source repository : https://github.com/Cyan4973/FiniteStateEntropy
    - Public forum : https://groups.google.com/forum/#!forum/lz4c
****************************************************************** */

#ifndef FSE_COMMONDEFS_ONLY

/****************************************************************
*  Tuning parameters
****************************************************************/
/* MEMORY_USAGE :
*  Memory usage formula : N->2^N Bytes (examples : 10 -> 1KB; 12 -> 4KB ; 16 -> 64KB; 20 -> 1MB; etc.)
*  Increasing memory usage improves compression ratio
*  Reduced memory usage can improve speed, due to cache effect
*  Recommended max value is 14, for 16KB, which nicely fits into Intel x86 L1 cache */
#define FSE_MAX_MEMORY_USAGE 14
#define FSE_DEFAULT_MEMORY_USAGE 13

/* FSE_MAX_SYMBOL_VALUE :
*  Maximum symbol value authorized.
*  Required for proper stack allocation */
#define FSE_MAX_SYMBOL_VALUE 255


/****************************************************************
*  template functions type & suffix
****************************************************************/
#define FSE_FUNCTION_TYPE BYTE
#define FSE_FUNCTION_EXTENSION


/****************************************************************
*  Byte symbol type
****************************************************************/
typedef struct
{
    unsigned short newState;
    unsigned char  symbol;
    unsigned char  nbBits;
} FSE_decode_t;   /* size == U32 */

#endif   /* !FSE_COMMONDEFS_ONLY */


/****************************************************************
*  Compiler specifics
****************************************************************/
#ifdef _MSC_VER    /* Visual Studio */
#  define FORCE_INLINE static __forceinline
#  include <intrin.h>                    /* For Visual 2005 */
#  pragma warning(disable : 4127)        /* disable: C4127: conditional expression is constant */
#  pragma warning(disable : 4214)        /* disable: C4214: non-int bitfields */
#else
#  define GCC_VERSION (__GNUC__ * 100 + __GNUC_MINOR__)
#  ifdef __GNUC__
#    define FORCE_INLINE static inline __attribute__((always_inline))
#  else
#    define FORCE_INLINE static inline
#  endif
#endif


/****************************************************************
*  Includes
****************************************************************/
#include <stdlib.h>     /* malloc, free, qsort */
#include <string.h>     /* memcpy, memset */
#include <stdio.h>      /* printf (debug) */
#include "fse_static.h"


#ifndef MEM_ACCESS_MODULE
#define MEM_ACCESS_MODULE
/****************************************************************
*  Basic Types
*****************************************************************/
#if defined (__STDC_VERSION__) && __STDC_VERSION__ >= 199901L   /* C99 */
# include <stdint.h>
typedef  uint8_t BYTE;
typedef uint16_t U16;
typedef  int16_t S16;
typedef uint32_t U32;
typedef  int32_t S32;
typedef uint64_t U64;
typedef  int64_t S64;
#else
typedef unsigned char       BYTE;
typedef unsigned short      U16;
typedef   signed short      S16;
typedef unsigned int        U32;
typedef   signed int        S32;
typedef unsigned long long  U64;
typedef   signed long long  S64;
#endif

#endif   /* MEM_ACCESS_MODULE */

/****************************************************************
*  Memory I/O
*****************************************************************/
/* FSE_FORCE_MEMORY_ACCESS
 * By default, access to unaligned memory is controlled by `memcpy()`, which is safe and portable.
 * Unfortunately, on some target/compiler combinations, the generated assembly is sub-optimal.
 * The below switch allow to select different access method for improved performance.
 * Method 0 (default) : use `memcpy()`. Safe and portable.
 * Method 1 : `__packed` statement. It depends on compiler extension (ie, not portable).
 *            This method is safe if your compiler supports it, and *generally* as fast or faster than `memcpy`.
 * Method 2 : direct access. This method is portable but violate C standard.
 *            It can generate buggy code on targets generating assembly depending on alignment.
 *            But in some circumstances, it's the only known way to get the most performance (ie GCC + ARMv6)
 * See http://fastcompression.blogspot.fr/2015/08/accessing-unaligned-memory.html for details.
 * Prefer these methods in priority order (0 > 1 > 2)
 */
#ifndef FSE_FORCE_MEMORY_ACCESS   /* can be defined externally, on command line for example */
#  if defined(__GNUC__) && ( defined(__ARM_ARCH_6__) || defined(__ARM_ARCH_6J__) || defined(__ARM_ARCH_6K__) || defined(__ARM_ARCH_6Z__) || defined(__ARM_ARCH_6ZK__) || defined(__ARM_ARCH_6T2__) )
#    define FSE_FORCE_MEMORY_ACCESS 2
#  elif defined(__INTEL_COMPILER) || \
  (defined(__GNUC__) && ( defined(__ARM_ARCH_7__) || defined(__ARM_ARCH_7A__) || defined(__ARM_ARCH_7R__) || defined(__ARM_ARCH_7M__) || defined(__ARM_ARCH_7S__) ))
#    define FSE_FORCE_MEMORY_ACCESS 1
#  endif
#endif


static unsigned FSE_32bits(void)
{
    return sizeof(void*)==4;
}

static unsigned FSE_isLittleEndian(void)
{
    const union { U32 i; BYTE c[4]; } one = { 1 };   /* don't use static : performance detrimental  */
    return one.c[0];
}

#if defined(FSE_FORCE_MEMORY_ACCESS) && (FSE_FORCE_MEMORY_ACCESS==2)

static U16 FSE_read16(const void* memPtr) { return *(const U16*) memPtr; }
static U32 FSE_read32(const void* memPtr) { return *(const U32*) memPtr; }
static U64 FSE_read64(const void* memPtr) { return *(const U64*) memPtr; }

static void FSE_write16(void* memPtr, U16 value) { *(U16*)memPtr = value; }
static void FSE_write32(void* memPtr, U32 value) { *(U32*)memPtr = value; }
static void FSE_write64(void* memPtr, U64 value) { *(U64*)memPtr = value; }

#elif defined(FSE_FORCE_MEMORY_ACCESS) && (FSE_FORCE_MEMORY_ACCESS==1)

/* __pack instructions are safer, but compiler specific, hence potentially problematic for some compilers */
/* currently only defined for gcc and icc */
typedef union { U16 u16; U32 u32; U64 u64; } __attribute__((packed)) unalign;

static U16 FSE_read16(const void* ptr) { return ((const unalign*)ptr)->u16; }
static U32 FSE_read32(const void* ptr) { return ((const unalign*)ptr)->u32; }
static U64 FSE_read64(const void* ptr) { return ((const unalign*)ptr)->u64; }

static void FSE_write16(void* memPtr, U16 value) { ((unalign*)memPtr)->u16 = value; }
static void FSE_write32(void* memPtr, U32 value) { ((unalign*)memPtr)->u32 = value; }
static void FSE_write64(void* memPtr, U64 value) { ((unalign*)memPtr)->u64 = value; }

#else

static U16 FSE_read16(const void* memPtr)
{
    U16 val; memcpy(&val, memPtr, sizeof(val)); return val;
}

static U32 FSE_read32(const void* memPtr)
{
    U32 val; memcpy(&val, memPtr, sizeof(val)); return val;
}

static U64 FSE_read64(const void* memPtr)
{
    U64 val; memcpy(&val, memPtr, sizeof(val)); return val;
}

static void FSE_write16(void* memPtr, U16 value)
{
    memcpy(memPtr, &value, sizeof(value));
}

static void FSE_write32(void* memPtr, U32 value)
{
    memcpy(memPtr, &value, sizeof(value));
}

static void FSE_write64(void* memPtr, U64 value)
{
    memcpy(memPtr, &value, sizeof(value));
}

#endif // FSE_FORCE_MEMORY_ACCESS

static U16 FSE_readLE16(const void* memPtr)
{
    if (FSE_isLittleEndian())
        return FSE_read16(memPtr);
    else
    {
        const BYTE* p = (const BYTE*)memPtr;
        return (U16)(p[0] + (p[1]<<8));
    }
}

static void FSE_writeLE16(void* memPtr, U16 val)
{
    if (FSE_isLittleEndian())
    {
        FSE_write16(memPtr, val);
    }
    else
    {
        BYTE* p = (BYTE*)memPtr;
        p[0] = (BYTE)val;
        p[1] = (BYTE)(val>>8);
    }
}

static U32 FSE_readLE32(const void* memPtr)
{
    if (FSE_isLittleEndian())
        return FSE_read32(memPtr);
    else
    {
        const BYTE* p = (const BYTE*)memPtr;
        return (U32)((U32)p[0] + ((U32)p[1]<<8) + ((U32)p[2]<<16) + ((U32)p[3]<<24));
    }
}

static void FSE_writeLE32(void* memPtr, U32 val32)
{
    if (FSE_isLittleEndian())
    {
        FSE_write32(memPtr, val32);
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

static U64 FSE_readLE64(const void* memPtr)
{
    if (FSE_isLittleEndian())
        return FSE_read64(memPtr);
    else
    {
        const BYTE* p = (const BYTE*)memPtr;
        return (U64)((U64)p[0] + ((U64)p[1]<<8) + ((U64)p[2]<<16) + ((U64)p[3]<<24)
                     + ((U64)p[4]<<32) + ((U64)p[5]<<40) + ((U64)p[6]<<48) + ((U64)p[7]<<56));
    }
}

static void FSE_writeLE64(void* memPtr, U64 val64)
{
    if (FSE_isLittleEndian())
    {
        FSE_write64(memPtr, val64);
    }
    else
    {
        BYTE* p = (BYTE*)memPtr;
        p[0] = (BYTE)val64;
        p[1] = (BYTE)(val64>>8);
        p[2] = (BYTE)(val64>>16);
        p[3] = (BYTE)(val64>>24);
        p[4] = (BYTE)(val64>>32);
        p[5] = (BYTE)(val64>>40);
        p[6] = (BYTE)(val64>>48);
        p[7] = (BYTE)(val64>>56);
    }
}

static size_t FSE_readLEST(const void* memPtr)
{
    if (FSE_32bits())
        return (size_t)FSE_readLE32(memPtr);
    else
        return (size_t)FSE_readLE64(memPtr);
}

static void FSE_writeLEST(void* memPtr, size_t val)
{
    if (FSE_32bits())
        FSE_writeLE32(memPtr, (U32)val);
    else
        FSE_writeLE64(memPtr, (U64)val);
}


/****************************************************************
*  Constants
*****************************************************************/
#define FSE_MAX_TABLELOG  (FSE_MAX_MEMORY_USAGE-2)
#define FSE_MAX_TABLESIZE (1U<<FSE_MAX_TABLELOG)
#define FSE_MAXTABLESIZE_MASK (FSE_MAX_TABLESIZE-1)
#define FSE_DEFAULT_TABLELOG (FSE_DEFAULT_MEMORY_USAGE-2)
#define FSE_MIN_TABLELOG 5

#define FSE_TABLELOG_ABSOLUTE_MAX 15
#if FSE_MAX_TABLELOG > FSE_TABLELOG_ABSOLUTE_MAX
#error "FSE_MAX_TABLELOG > FSE_TABLELOG_ABSOLUTE_MAX is not supported"
#endif


/****************************************************************
*  Error Management
****************************************************************/
#define FSE_STATIC_ASSERT(c) { enum { FSE_static_assert = 1/(int)(!!(c)) }; }   /* use only *after* variable declarations */


/****************************************************************
*  Complex types
****************************************************************/
typedef struct
{
    int deltaFindState;
    U32 deltaNbBits;
} FSE_symbolCompressionTransform; /* total 8 bytes */

typedef U32 CTable_max_t[FSE_CTABLE_SIZE_U32(FSE_MAX_TABLELOG, FSE_MAX_SYMBOL_VALUE)];
typedef U32 DTable_max_t[FSE_DTABLE_SIZE_U32(FSE_MAX_TABLELOG)];

/****************************************************************
*  Internal functions
****************************************************************/
FORCE_INLINE unsigned FSE_highbit32 (register U32 val)
{
#   if defined(_MSC_VER)   /* Visual */
    unsigned long r;
    _BitScanReverse ( &r, val );
    return (unsigned) r;
#   elif defined(__GNUC__) && (GCC_VERSION >= 304)   /* GCC Intrinsic */
    return 31 - __builtin_clz (val);
#   else   /* Software version */
    static const unsigned DeBruijnClz[32] = { 0, 9, 1, 10, 13, 21, 2, 29, 11, 14, 16, 18, 22, 25, 3, 30, 8, 12, 20, 28, 15, 17, 24, 7, 19, 27, 23, 6, 26, 5, 4, 31 };
    U32 v = val;
    unsigned r;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    r = DeBruijnClz[ (U32) (v * 0x07C4ACDDU) >> 27];
    return r;
#   endif
}


/****************************************************************
*  Templates
****************************************************************/
/*
  designed to be included
  for type-specific functions (template emulation in C)
  Objective is to write these functions only once, for improved maintenance
*/

/* safety checks */
#ifndef FSE_FUNCTION_EXTENSION
#  error "FSE_FUNCTION_EXTENSION must be defined"
#endif
#ifndef FSE_FUNCTION_TYPE
#  error "FSE_FUNCTION_TYPE must be defined"
#endif

/* Function names */
#define FSE_CAT(X,Y) X##Y
#define FSE_FUNCTION_NAME(X,Y) FSE_CAT(X,Y)
#define FSE_TYPE_NAME(X,Y) FSE_CAT(X,Y)


/* Function templates */
size_t FSE_FUNCTION_NAME(FSE_count_generic, FSE_FUNCTION_EXTENSION)
(unsigned* count, unsigned* maxSymbolValuePtr, const FSE_FUNCTION_TYPE* source, size_t sourceSize, unsigned safe)
{
    const FSE_FUNCTION_TYPE* ip = source;
    const FSE_FUNCTION_TYPE* const iend = ip+sourceSize;
    unsigned maxSymbolValue = *maxSymbolValuePtr;
    unsigned max=0;
    int s;

    U32 Counting1[FSE_MAX_SYMBOL_VALUE+1] = { 0 };
    U32 Counting2[FSE_MAX_SYMBOL_VALUE+1] = { 0 };
    U32 Counting3[FSE_MAX_SYMBOL_VALUE+1] = { 0 };
    U32 Counting4[FSE_MAX_SYMBOL_VALUE+1] = { 0 };

    /* safety checks */
    if (!sourceSize)
    {
        memset(count, 0, (maxSymbolValue + 1) * sizeof(FSE_FUNCTION_TYPE));
        *maxSymbolValuePtr = 0;
        return 0;
    }
    if (maxSymbolValue > FSE_MAX_SYMBOL_VALUE) return (size_t)-FSE_ERROR_GENERIC;   /* maxSymbolValue too large : unsupported */
    if (!maxSymbolValue) maxSymbolValue = FSE_MAX_SYMBOL_VALUE;            /* 0 == default */

    if ((safe) || (sizeof(FSE_FUNCTION_TYPE)>1))
    {
        /* check input values, to avoid count table overflow */
        while (ip < iend-3)
        {
            if (*ip>maxSymbolValue) return (size_t)-FSE_ERROR_GENERIC; Counting1[*ip++]++;
            if (*ip>maxSymbolValue) return (size_t)-FSE_ERROR_GENERIC; Counting2[*ip++]++;
            if (*ip>maxSymbolValue) return (size_t)-FSE_ERROR_GENERIC; Counting3[*ip++]++;
            if (*ip>maxSymbolValue) return (size_t)-FSE_ERROR_GENERIC; Counting4[*ip++]++;
        }
    }
    else
    {
        U32 cached = FSE_read32(ip); ip += 4;
        while (ip < iend-15)
        {
            U32 c = cached; cached = FSE_read32(ip); ip += 4;
            Counting1[(BYTE) c     ]++;
            Counting2[(BYTE)(c>>8) ]++;
            Counting3[(BYTE)(c>>16)]++;
            Counting4[       c>>24 ]++;
            c = cached; cached = FSE_read32(ip); ip += 4;
            Counting1[(BYTE) c     ]++;
            Counting2[(BYTE)(c>>8) ]++;
            Counting3[(BYTE)(c>>16)]++;
            Counting4[       c>>24 ]++;
            c = cached; cached = FSE_read32(ip); ip += 4;
            Counting1[(BYTE) c     ]++;
            Counting2[(BYTE)(c>>8) ]++;
            Counting3[(BYTE)(c>>16)]++;
            Counting4[       c>>24 ]++;
            c = cached; cached = FSE_read32(ip); ip += 4;
            Counting1[(BYTE) c     ]++;
            Counting2[(BYTE)(c>>8) ]++;
            Counting3[(BYTE)(c>>16)]++;
            Counting4[       c>>24 ]++;
        }
        ip-=4;
    }

    /* finish last symbols */
    while (ip<iend) { if ((safe) && (*ip>maxSymbolValue)) return (size_t)-FSE_ERROR_GENERIC; Counting1[*ip++]++; }

    for (s=0; s<=(int)maxSymbolValue; s++)
    {
        count[s] = Counting1[s] + Counting2[s] + Counting3[s] + Counting4[s];
        if (count[s] > max) max = count[s];
    }

    while (!count[maxSymbolValue]) maxSymbolValue--;
    *maxSymbolValuePtr = maxSymbolValue;
    return (size_t)max;
}

/* hidden fast variant (unsafe) */
size_t FSE_FUNCTION_NAME(FSE_countFast, FSE_FUNCTION_EXTENSION)
(unsigned* count, unsigned* maxSymbolValuePtr, const FSE_FUNCTION_TYPE* source, size_t sourceSize)
{
    return FSE_FUNCTION_NAME(FSE_count_generic, FSE_FUNCTION_EXTENSION) (count, maxSymbolValuePtr, source, sourceSize, 0);
}

size_t FSE_FUNCTION_NAME(FSE_count, FSE_FUNCTION_EXTENSION)
(unsigned* count, unsigned* maxSymbolValuePtr, const FSE_FUNCTION_TYPE* source, size_t sourceSize)
{
    if ((sizeof(FSE_FUNCTION_TYPE)==1) && (*maxSymbolValuePtr >= 255))
    {
        *maxSymbolValuePtr = 255;
        return FSE_FUNCTION_NAME(FSE_count_generic, FSE_FUNCTION_EXTENSION) (count, maxSymbolValuePtr, source, sourceSize, 0);
    }
    return FSE_FUNCTION_NAME(FSE_count_generic, FSE_FUNCTION_EXTENSION) (count, maxSymbolValuePtr, source, sourceSize, 1);
}


static U32 FSE_tableStep(U32 tableSize) { return (tableSize>>1) + (tableSize>>3) + 3; }

size_t FSE_FUNCTION_NAME(FSE_buildCTable, FSE_FUNCTION_EXTENSION)
(FSE_CTable* ct, const short* normalizedCounter, unsigned maxSymbolValue, unsigned tableLog)
{
    const unsigned tableSize = 1 << tableLog;
    const unsigned tableMask = tableSize - 1;
    U16* tableU16 = ( (U16*) ct) + 2;
    FSE_symbolCompressionTransform* symbolTT = (FSE_symbolCompressionTransform*) (((U32*)ct) + 1 + (tableLog ? tableSize>>1 : 1) );
    const unsigned step = FSE_tableStep(tableSize);
    unsigned cumul[FSE_MAX_SYMBOL_VALUE+2];
    U32 position = 0;
    FSE_FUNCTION_TYPE tableSymbol[FSE_MAX_TABLESIZE]; /* init not necessary, but analyzer complain about it */
    U32 highThreshold = tableSize-1;
    unsigned symbol;
    unsigned i;

    /* header */
    tableU16[-2] = (U16) tableLog;
    tableU16[-1] = (U16) maxSymbolValue;

    /* For explanations on how to distribute symbol values over the table :
    *  http://fastcompression.blogspot.fr/2014/02/fse-distributing-symbol-values.html */

    /* symbol start positions */
    cumul[0] = 0;
    for (i=1; i<=maxSymbolValue+1; i++)
    {
        if (normalizedCounter[i-1]==-1)   /* Low prob symbol */
        {
            cumul[i] = cumul[i-1] + 1;
            tableSymbol[highThreshold--] = (FSE_FUNCTION_TYPE)(i-1);
        }
        else
            cumul[i] = cumul[i-1] + normalizedCounter[i-1];
    }
    cumul[maxSymbolValue+1] = tableSize+1;

    /* Spread symbols */
    for (symbol=0; symbol<=maxSymbolValue; symbol++)
    {
        int nbOccurences;
        for (nbOccurences=0; nbOccurences<normalizedCounter[symbol]; nbOccurences++)
        {
            tableSymbol[position] = (FSE_FUNCTION_TYPE)symbol;
            position = (position + step) & tableMask;
            while (position > highThreshold) position = (position + step) & tableMask;   /* Lowprob area */
        }
    }

    if (position!=0) return (size_t)-FSE_ERROR_GENERIC;   /* Must have gone through all positions */

    /* Build table */
    for (i=0; i<tableSize; i++)
    {
        FSE_FUNCTION_TYPE s = tableSymbol[i];   /* static analyzer doesn't understand tableSymbol is properly initialized */
        tableU16[cumul[s]++] = (U16) (tableSize+i);   /* TableU16 : sorted by symbol order; gives next state value */
    }

    /* Build Symbol Transformation Table */
    {
        unsigned s;
        unsigned total = 0;
        for (s=0; s<=maxSymbolValue; s++)
        {
            switch (normalizedCounter[s])
            {
            case  0:
                break;
            case -1:
            case  1:
                symbolTT[s].deltaNbBits = tableLog << 16;
                symbolTT[s].deltaFindState = total - 1;
                total ++;
                break;
            default :
                {
                    U32 maxBitsOut = tableLog - FSE_highbit32 (normalizedCounter[s]-1);
                    U32 minStatePlus = normalizedCounter[s] << maxBitsOut;
                    symbolTT[s].deltaNbBits = (maxBitsOut << 16) - minStatePlus;
                    symbolTT[s].deltaFindState = total - normalizedCounter[s];
                    total +=  normalizedCounter[s];
                }
            }
        }
    }

    return 0;
}


#define FSE_DECODE_TYPE FSE_TYPE_NAME(FSE_decode_t, FSE_FUNCTION_EXTENSION)

FSE_DTable* FSE_FUNCTION_NAME(FSE_createDTable, FSE_FUNCTION_EXTENSION) (unsigned tableLog)
{
    if (tableLog > FSE_TABLELOG_ABSOLUTE_MAX) tableLog = FSE_TABLELOG_ABSOLUTE_MAX;
    return (FSE_DTable*)malloc( FSE_DTABLE_SIZE_U32(tableLog) * sizeof (U32) );
}

void FSE_FUNCTION_NAME(FSE_freeDTable, FSE_FUNCTION_EXTENSION) (FSE_DTable* dt)
{
    free(dt);
}

typedef struct {
    U16 tableLog;
    U16 fastMode;
} FSE_DTableHeader;   /* sizeof U32 */

size_t FSE_FUNCTION_NAME(FSE_buildDTable, FSE_FUNCTION_EXTENSION)
(FSE_DTable* dt, const short* normalizedCounter, unsigned maxSymbolValue, unsigned tableLog)
{
    FSE_DTableHeader* const DTableH = (FSE_DTableHeader*)dt;
    FSE_DECODE_TYPE* const tableDecode = (FSE_DECODE_TYPE*) (dt+1);   /* because dt is unsigned, 32-bits aligned on 32-bits */
    const U32 tableSize = 1 << tableLog;
    const U32 tableMask = tableSize-1;
    const U32 step = FSE_tableStep(tableSize);
    U16 symbolNext[FSE_MAX_SYMBOL_VALUE+1];
    U32 position = 0;
    U32 highThreshold = tableSize-1;
    const S16 largeLimit= (S16)(1 << (tableLog-1));
    U32 noLarge = 1;
    U32 s;

    /* Sanity Checks */
    if (maxSymbolValue > FSE_MAX_SYMBOL_VALUE) return (size_t)-FSE_ERROR_maxSymbolValue_tooLarge;
    if (tableLog > FSE_MAX_TABLELOG) return (size_t)-FSE_ERROR_tableLog_tooLarge;

    /* Init, lay down lowprob symbols */
    DTableH[0].tableLog = (U16)tableLog;
    for (s=0; s<=maxSymbolValue; s++)
    {
        if (normalizedCounter[s]==-1)
        {
            tableDecode[highThreshold--].symbol = (FSE_FUNCTION_TYPE)s;
            symbolNext[s] = 1;
        }
        else
        {
            if (normalizedCounter[s] >= largeLimit) noLarge=0;
            symbolNext[s] = normalizedCounter[s];
        }
    }

    /* Spread symbols */
    for (s=0; s<=maxSymbolValue; s++)
    {
        int i;
        for (i=0; i<normalizedCounter[s]; i++)
        {
            tableDecode[position].symbol = (FSE_FUNCTION_TYPE)s;
            position = (position + step) & tableMask;
            while (position > highThreshold) position = (position + step) & tableMask;   /* lowprob area */
        }
    }

    if (position!=0) return (size_t)-FSE_ERROR_GENERIC;   /* position must reach all cells once, otherwise normalizedCounter is incorrect */

    /* Build Decoding table */
    {
        U32 i;
        for (i=0; i<tableSize; i++)
        {
            FSE_FUNCTION_TYPE symbol = (FSE_FUNCTION_TYPE)(tableDecode[i].symbol);
            U16 nextState = symbolNext[symbol]++;
            tableDecode[i].nbBits = (BYTE) (tableLog - FSE_highbit32 ((U32)nextState) );
            tableDecode[i].newState = (U16) ( (nextState << tableDecode[i].nbBits) - tableSize);
        }
    }

    DTableH->fastMode = (U16)noLarge;
    return 0;
}


/******************************************
*  FSE byte symbol
******************************************/
#ifndef FSE_COMMONDEFS_ONLY

unsigned FSE_isError(size_t code) { return (code > (size_t)(-FSE_ERROR_maxCode)); }

#define FSE_GENERATE_STRING(STRING) #STRING,
static const char* FSE_errorStrings[] = { FSE_LIST_ERRORS(FSE_GENERATE_STRING) };

const char* FSE_getErrorName(size_t code)
{
    static const char* codeError = "Unspecified error code";
    if (FSE_isError(code)) return FSE_errorStrings[-(int)(code)];
    return codeError;
}

static short FSE_abs(short a)
{
    return a<0? -a : a;
}


/****************************************************************
*  Header bitstream management
****************************************************************/
size_t FSE_NCountWriteBound(unsigned maxSymbolValue, unsigned tableLog)
{
    size_t maxHeaderSize = (((maxSymbolValue+1) * tableLog) >> 3) + 3;
    return maxSymbolValue ? maxHeaderSize : FSE_NCOUNTBOUND;  /* maxSymbolValue==0 ? use default */
}

static size_t FSE_writeNCount_generic (void* header, size_t headerBufferSize,
                                       const short* normalizedCounter, unsigned maxSymbolValue, unsigned tableLog,
                                       unsigned writeIsSafe)
{
    BYTE* const ostart = (BYTE*) header;
    BYTE* out = ostart;
    BYTE* const oend = ostart + headerBufferSize;
    int nbBits;
    const int tableSize = 1 << tableLog;
    int remaining;
    int threshold;
    U32 bitStream;
    int bitCount;
    unsigned charnum = 0;
    int previous0 = 0;

    bitStream = 0;
    bitCount  = 0;
    /* Table Size */
    bitStream += (tableLog-FSE_MIN_TABLELOG) << bitCount;
    bitCount  += 4;

    /* Init */
    remaining = tableSize+1;   /* +1 for extra accuracy */
    threshold = tableSize;
    nbBits = tableLog+1;

    while (remaining>1)   /* stops at 1 */
    {
        if (previous0)
        {
            unsigned start = charnum;
            while (!normalizedCounter[charnum]) charnum++;
            while (charnum >= start+24)
            {
                start+=24;
                bitStream += 0xFFFFU << bitCount;
                if ((!writeIsSafe) && (out > oend-2)) return (size_t)-FSE_ERROR_dstSize_tooSmall;   /* Buffer overflow */
                out[0] = (BYTE) bitStream;
                out[1] = (BYTE)(bitStream>>8);
                out+=2;
                bitStream>>=16;
            }
            while (charnum >= start+3)
            {
                start+=3;
                bitStream += 3 << bitCount;
                bitCount += 2;
            }
            bitStream += (charnum-start) << bitCount;
            bitCount += 2;
            if (bitCount>16)
            {
                if ((!writeIsSafe) && (out > oend - 2)) return (size_t)-FSE_ERROR_dstSize_tooSmall;   /* Buffer overflow */
                out[0] = (BYTE)bitStream;
                out[1] = (BYTE)(bitStream>>8);
                out += 2;
                bitStream >>= 16;
                bitCount -= 16;
            }
        }
        {
            short count = normalizedCounter[charnum++];
            const short max = (short)((2*threshold-1)-remaining);
            remaining -= FSE_abs(count);
            if (remaining<1) return (size_t)-FSE_ERROR_GENERIC;
            count++;   /* +1 for extra accuracy */
            if (count>=threshold) count += max;   /* [0..max[ [max..threshold[ (...) [threshold+max 2*threshold[ */
            bitStream += count << bitCount;
            bitCount  += nbBits;
            bitCount  -= (count<max);
            previous0 = (count==1);
            while (remaining<threshold) nbBits--, threshold>>=1;
        }
        if (bitCount>16)
        {
            if ((!writeIsSafe) && (out > oend - 2)) return (size_t)-FSE_ERROR_dstSize_tooSmall;   /* Buffer overflow */
            out[0] = (BYTE)bitStream;
            out[1] = (BYTE)(bitStream>>8);
            out += 2;
            bitStream >>= 16;
            bitCount -= 16;
        }
    }

    /* flush remaining bitStream */
    if ((!writeIsSafe) && (out > oend - 2)) return (size_t)-FSE_ERROR_dstSize_tooSmall;   /* Buffer overflow */
    out[0] = (BYTE)bitStream;
    out[1] = (BYTE)(bitStream>>8);
    out+= (bitCount+7) /8;

    if (charnum > maxSymbolValue + 1) return (size_t)-FSE_ERROR_GENERIC;

    return (out-ostart);
}


size_t FSE_writeNCount (void* buffer, size_t bufferSize, const short* normalizedCounter, unsigned maxSymbolValue, unsigned tableLog)
{
    if (tableLog > FSE_MAX_TABLELOG) return (size_t)-FSE_ERROR_GENERIC;   /* Unsupported */
    if (tableLog < FSE_MIN_TABLELOG) return (size_t)-FSE_ERROR_GENERIC;   /* Unsupported */

    if (bufferSize < FSE_NCountWriteBound(maxSymbolValue, tableLog))
        return FSE_writeNCount_generic(buffer, bufferSize, normalizedCounter, maxSymbolValue, tableLog, 0);

    return FSE_writeNCount_generic(buffer, bufferSize, normalizedCounter, maxSymbolValue, tableLog, 1);
}


size_t FSE_readNCount (short* normalizedCounter, unsigned* maxSVPtr, unsigned* tableLogPtr,
                 const void* headerBuffer, size_t hbSize)
{
    const BYTE* const istart = (const BYTE*) headerBuffer;
    const BYTE* const iend = istart + hbSize;
    const BYTE* ip = istart;
    int nbBits;
    int remaining;
    int threshold;
    U32 bitStream;
    int bitCount;
    unsigned charnum = 0;
    int previous0 = 0;

    if (hbSize < 4) return (size_t)-FSE_ERROR_srcSize_wrong;
    bitStream = FSE_readLE32(ip);
    nbBits = (bitStream & 0xF) + FSE_MIN_TABLELOG;   /* extract tableLog */
    if (nbBits > FSE_TABLELOG_ABSOLUTE_MAX) return (size_t)-FSE_ERROR_tableLog_tooLarge;
    bitStream >>= 4;
    bitCount = 4;
    *tableLogPtr = nbBits;
    remaining = (1<<nbBits)+1;
    threshold = 1<<nbBits;
    nbBits++;

    while ((remaining>1) && (charnum<=*maxSVPtr))
    {
        if (previous0)
        {
            unsigned n0 = charnum;
            while ((bitStream & 0xFFFF) == 0xFFFF)
            {
                n0+=24;
                if (ip < iend-5)
                {
                    ip+=2;
                    bitStream = FSE_readLE32(ip) >> bitCount;
                }
                else
                {
                    bitStream >>= 16;
                    bitCount+=16;
                }
            }
            while ((bitStream & 3) == 3)
            {
                n0+=3;
                bitStream>>=2;
                bitCount+=2;
            }
            n0 += bitStream & 3;
            bitCount += 2;
            if (n0 > *maxSVPtr) return (size_t)-FSE_ERROR_maxSymbolValue_tooSmall;
            while (charnum < n0) normalizedCounter[charnum++] = 0;
            if ((ip <= iend-7) || (ip + (bitCount>>3) <= iend-4))
            {
                ip += bitCount>>3;
                bitCount &= 7;
                bitStream = FSE_readLE32(ip) >> bitCount;
            }
            else
                bitStream >>= 2;
        }
        {
            const short max = (short)((2*threshold-1)-remaining);
            short count;

            if ((bitStream & (threshold-1)) < (U32)max)
            {
                count = (short)(bitStream & (threshold-1));
                bitCount   += nbBits-1;
            }
            else
            {
                count = (short)(bitStream & (2*threshold-1));
                if (count >= threshold) count -= max;
                bitCount   += nbBits;
            }

            count--;   /* extra accuracy */
            remaining -= FSE_abs(count);
            normalizedCounter[charnum++] = count;
            previous0 = !count;
            while (remaining < threshold)
            {
                nbBits--;
                threshold >>= 1;
            }

            {
                if ((ip <= iend-7) || (ip + (bitCount>>3) <= iend-4))
                {
                    ip += bitCount>>3;
                    bitCount &= 7;
                }
                else
                {
                    bitCount -= (int)(8 * (iend - 4 - ip));
					ip = iend - 4;
				}
                bitStream = FSE_readLE32(ip) >> (bitCount & 31);
            }
        }
    }
    if (remaining != 1) return (size_t)-FSE_ERROR_GENERIC;
    *maxSVPtr = charnum-1;

    ip += (bitCount+7)>>3;
    if ((size_t)(ip-istart) > hbSize) return (size_t)-FSE_ERROR_srcSize_wrong;
    return ip-istart;
}


/****************************************************************
*  FSE Compression Code
****************************************************************/
/*
FSE_CTable[0] is a variable size structure which contains :
    U16 tableLog;
    U16 maxSymbolValue;
    U16 nextStateNumber[1 << tableLog];                         // This size is variable
    FSE_symbolCompressionTransform symbolTT[maxSymbolValue+1];  // This size is variable
Allocation is manual, since C standard does not support variable-size structures.
*/

size_t FSE_sizeof_CTable (unsigned maxSymbolValue, unsigned tableLog)
{
    size_t size;
    FSE_STATIC_ASSERT((size_t)FSE_CTABLE_SIZE_U32(FSE_MAX_TABLELOG, FSE_MAX_SYMBOL_VALUE)*4 >= sizeof(CTable_max_t));   /* A compilation error here means FSE_CTABLE_SIZE_U32 is not large enough */
    if (tableLog > FSE_MAX_TABLELOG) return (size_t)-FSE_ERROR_GENERIC;
    size = FSE_CTABLE_SIZE_U32 (tableLog, maxSymbolValue) * sizeof(U32);
    return size;
}

FSE_CTable* FSE_createCTable (unsigned maxSymbolValue, unsigned tableLog)
{
    size_t size;
    if (tableLog > FSE_TABLELOG_ABSOLUTE_MAX) tableLog = FSE_TABLELOG_ABSOLUTE_MAX;
    size = FSE_CTABLE_SIZE_U32 (tableLog, maxSymbolValue) * sizeof(U32);
    return (FSE_CTable*)malloc(size);
}

void  FSE_freeCTable (FSE_CTable* ct)
{
    free(ct);
}


/* provides the minimum logSize to safely represent a distribution */
static unsigned FSE_minTableLog(size_t srcSize, unsigned maxSymbolValue)
{
	U32 minBitsSrc = FSE_highbit32((U32)(srcSize - 1)) + 1;
	U32 minBitsSymbols = FSE_highbit32(maxSymbolValue) + 2;
	U32 minBits = minBitsSrc < minBitsSymbols ? minBitsSrc : minBitsSymbols;
	return minBits;
}

unsigned FSE_optimalTableLog(unsigned maxTableLog, size_t srcSize, unsigned maxSymbolValue)
{
	U32 maxBitsSrc = FSE_highbit32((U32)(srcSize - 1)) - 2;
    U32 tableLog = maxTableLog;
	U32 minBits = FSE_minTableLog(srcSize, maxSymbolValue);
    if (tableLog==0) tableLog = FSE_DEFAULT_TABLELOG;
	if (maxBitsSrc < tableLog) tableLog = maxBitsSrc;   /* Accuracy can be reduced */
	if (minBits > tableLog) tableLog = minBits;   /* Need a minimum to safely represent all symbol values */
    if (tableLog < FSE_MIN_TABLELOG) tableLog = FSE_MIN_TABLELOG;
    if (tableLog > FSE_MAX_TABLELOG) tableLog = FSE_MAX_TABLELOG;
    return tableLog;
}


/* Secondary normalization method.
   To be used when primary method fails. */

static size_t FSE_normalizeM2(short* norm, U32 tableLog, const unsigned* count, size_t total, U32 maxSymbolValue)
{
    U32 s;
    U32 distributed = 0;
    U32 ToDistribute;

    /* Init */
    U32 lowThreshold = (U32)(total >> tableLog);
    U32 lowOne = (U32)((total * 3) >> (tableLog + 1));

    for (s=0; s<=maxSymbolValue; s++)
    {
        if (count[s] == 0)
        {
            norm[s]=0;
            continue;
        }
        if (count[s] <= lowThreshold)
        {
            norm[s] = -1;
            distributed++;
            total -= count[s];
            continue;
        }
        if (count[s] <= lowOne)
        {
            norm[s] = 1;
            distributed++;
            total -= count[s];
            continue;
        }
        norm[s]=-2;
    }
    ToDistribute = (1 << tableLog) - distributed;

    if ((total / ToDistribute) > lowOne)
    {
        /* risk of rounding to zero */
        lowOne = (U32)((total * 3) / (ToDistribute * 2));
        for (s=0; s<=maxSymbolValue; s++)
        {
            if ((norm[s] == -2) && (count[s] <= lowOne))
            {
                norm[s] = 1;
                distributed++;
                total -= count[s];
                continue;
            }
        }
        ToDistribute = (1 << tableLog) - distributed;
    }

    if (distributed == maxSymbolValue+1)
    {
        /* all values are pretty poor;
           probably incompressible data (should have already been detected);
           find max, then give all remaining points to max */
        U32 maxV = 0, maxC =0;
        for (s=0; s<=maxSymbolValue; s++)
            if (count[s] > maxC) maxV=s, maxC=count[s];
        norm[maxV] += (short)ToDistribute;
        return 0;
    }

    {
        U64 const vStepLog = 62 - tableLog;
        U64 const mid = (1ULL << (vStepLog-1)) - 1;
        U64 const rStep = ((((U64)1<<vStepLog) * ToDistribute) + mid) / total;   /* scale on remaining */
        U64 tmpTotal = mid;
        for (s=0; s<=maxSymbolValue; s++)
        {
            if (norm[s]==-2)
            {
                U64 end = tmpTotal + (count[s] * rStep);
                U32 sStart = (U32)(tmpTotal >> vStepLog);
                U32 sEnd = (U32)(end >> vStepLog);
                U32 weight = sEnd - sStart;
                if (weight < 1)
                    return (size_t)-FSE_ERROR_GENERIC;
                norm[s] = (short)weight;
                tmpTotal = end;
            }
        }
    }

    return 0;
}


size_t FSE_normalizeCount (short* normalizedCounter, unsigned tableLog,
                           const unsigned* count, size_t total,
                           unsigned maxSymbolValue)
{
    /* Sanity checks */
    if (tableLog==0) tableLog = FSE_DEFAULT_TABLELOG;
    if (tableLog < FSE_MIN_TABLELOG) return (size_t)-FSE_ERROR_GENERIC;   /* Unsupported size */
    if (tableLog > FSE_MAX_TABLELOG) return (size_t)-FSE_ERROR_GENERIC;   /* Unsupported size */
    if (tableLog < FSE_minTableLog(total, maxSymbolValue)) return (size_t)-FSE_ERROR_GENERIC;   /* Too small tableLog, compression potentially impossible */

    {
        U32 const rtbTable[] = {     0, 473195, 504333, 520860, 550000, 700000, 750000, 830000 };
        U64 const scale = 62 - tableLog;
        U64 const step = ((U64)1<<62) / total;   /* <== here, one division ! */
        U64 const vStep = 1ULL<<(scale-20);
        int stillToDistribute = 1<<tableLog;
        unsigned s;
        unsigned largest=0;
        short largestP=0;
        U32 lowThreshold = (U32)(total >> tableLog);

        for (s=0; s<=maxSymbolValue; s++)
        {
            if (count[s] == total) return 0;
            if (count[s] == 0)
            {
                normalizedCounter[s]=0;
                continue;
            }
            if (count[s] <= lowThreshold)
            {
                normalizedCounter[s] = -1;
                stillToDistribute--;
            }
            else
            {
                short proba = (short)((count[s]*step) >> scale);
                if (proba<8)
                {
                    U64 restToBeat = vStep * rtbTable[proba];
                    proba += (count[s]*step) - ((U64)proba<<scale) > restToBeat;
                }
                if (proba > largestP)
                {
                    largestP=proba;
                    largest=s;
                }
                normalizedCounter[s] = proba;
                stillToDistribute -= proba;
            }
        }
        if (-stillToDistribute >= (normalizedCounter[largest] >> 1))
        {
            /* corner case, need another normalization method */
            size_t errorCode = FSE_normalizeM2(normalizedCounter, tableLog, count, total, maxSymbolValue);
            if (FSE_isError(errorCode)) return errorCode;
        }
        else normalizedCounter[largest] += (short)stillToDistribute;
    }

#if 0
    {   /* Print Table (debug) */
        U32 s;
        U32 nTotal = 0;
        for (s=0; s<=maxSymbolValue; s++)
            printf("%3i: %4i \n", s, normalizedCounter[s]);
        for (s=0; s<=maxSymbolValue; s++)
            nTotal += abs(normalizedCounter[s]);
        if (nTotal != (1U<<tableLog))
            printf("Warning !!! Total == %u != %u !!!", nTotal, 1U<<tableLog);
        getchar();
    }
#endif

    return tableLog;
}


/* fake FSE_CTable, for raw (uncompressed) input */
size_t FSE_buildCTable_raw (FSE_CTable* ct, unsigned nbBits)
{
    const unsigned tableSize = 1 << nbBits;
    const unsigned tableMask = tableSize - 1;
    const unsigned maxSymbolValue = tableMask;
    U16* tableU16 = ( (U16*) ct) + 2;
    FSE_symbolCompressionTransform* symbolTT = (FSE_symbolCompressionTransform*) ((((U32*)ct)+1) + (tableSize>>1));
    unsigned s;

    /* Sanity checks */
    if (nbBits < 1) return (size_t)-FSE_ERROR_GENERIC;             /* min size */

    /* header */
    tableU16[-2] = (U16) nbBits;
    tableU16[-1] = (U16) maxSymbolValue;

    /* Build table */
    for (s=0; s<tableSize; s++)
        tableU16[s] = (U16)(tableSize + s);

    /* Build Symbol Transformation Table */
    for (s=0; s<=maxSymbolValue; s++)
    {
        symbolTT[s].deltaNbBits = nbBits << 16;
        symbolTT[s].deltaFindState = s-1;
    }

    return 0;
}


/* fake FSE_CTable, for rle (100% always same symbol) input */
size_t FSE_buildCTable_rle (FSE_CTable* ct, BYTE symbolValue)
{
    U16* tableU16 = ( (U16*) ct) + 2;
    FSE_symbolCompressionTransform* symbolTT = (FSE_symbolCompressionTransform*) ((U32*)ct + 2);

    /* header */
    tableU16[-2] = (U16) 0;
    tableU16[-1] = (U16) symbolValue;

    /* Build table */
    tableU16[0] = 0;
    tableU16[1] = 0;   /* just in case */

    /* Build Symbol Transformation Table */
    {
        symbolTT[symbolValue].deltaNbBits = 0;
        symbolTT[symbolValue].deltaFindState = 0;
    }

    return 0;
}


size_t FSE_initCStream(FSE_CStream_t* bitC, void* start, size_t maxSize)
{
    if (maxSize < sizeof(bitC->ptr)) return (size_t)-FSE_ERROR_dstSize_tooSmall;
    bitC->bitContainer = 0;
    bitC->bitPos = 0;
    bitC->startPtr = (char*)start;
    bitC->ptr = bitC->startPtr;
    bitC->endPtr = bitC->startPtr + maxSize - sizeof(bitC->ptr);
    return 0;
}

void FSE_initCState(FSE_CState_t* statePtr, const FSE_CTable* ct)
{
    const U32 tableLog = ( (const U16*) ct) [0];
    statePtr->value = (ptrdiff_t)1<<tableLog;
    statePtr->stateTable = ((const U16*) ct) + 2;
    statePtr->symbolTT = (const FSE_symbolCompressionTransform*)((const U32*)ct + 1 + (tableLog ? (1<<(tableLog-1)) : 1));
    statePtr->stateLog = tableLog;
}

void FSE_addBitsFast(FSE_CStream_t* bitC, size_t value, unsigned nbBits)   /* only use if upper bits are clean 0 */
{
    bitC->bitContainer |= value << bitC->bitPos;
    bitC->bitPos += nbBits;
}

void FSE_addBits(FSE_CStream_t* bitC, size_t value, unsigned nbBits)
{
    static const unsigned mask[] = { 0, 1, 3, 7, 0xF, 0x1F, 0x3F, 0x7F, 0xFF, 0x1FF, 0x3FF, 0x7FF, 0xFFF, 0x1FFF, 0x3FFF, 0x7FFF, 0xFFFF, 0x1FFFF, 0x3FFFF, 0x7FFFF, 0xFFFFF, 0x1FFFFF, 0x3FFFFF, 0x7FFFFF,  0xFFFFFF, 0x1FFFFFF };   /* up to 25 bits */
    bitC->bitContainer |= (value & mask[nbBits]) << bitC->bitPos;
    bitC->bitPos += nbBits;
}

void FSE_encodeSymbol(FSE_CStream_t* bitC, FSE_CState_t* statePtr, U32 symbol)
{
    const FSE_symbolCompressionTransform symbolTT = ((const FSE_symbolCompressionTransform*)(statePtr->symbolTT))[symbol];
    const U16* const stateTable = (const U16*)(statePtr->stateTable);
    U32 nbBitsOut  = (U32)((statePtr->value + symbolTT.deltaNbBits) >> 16);
    FSE_addBits(bitC, statePtr->value, nbBitsOut);
    statePtr->value = stateTable[ (statePtr->value >> nbBitsOut) + symbolTT.deltaFindState];
}

void FSE_flushBitsFast(FSE_CStream_t* bitC)  /* only if dst buffer is large enough ( >= FSE_compressBound()) */
{
    size_t nbBytes = bitC->bitPos >> 3;
    FSE_writeLEST(bitC->ptr, bitC->bitContainer);
    bitC->ptr += nbBytes;
    bitC->bitPos &= 7;
    bitC->bitContainer >>= nbBytes*8;
}

void FSE_flushBits(FSE_CStream_t* bitC)
{
    size_t nbBytes = bitC->bitPos >> 3;
    FSE_writeLEST(bitC->ptr, bitC->bitContainer);
    bitC->ptr += nbBytes;
    if (bitC->ptr > bitC->endPtr) bitC->ptr = bitC->endPtr;
    bitC->bitPos &= 7;
    bitC->bitContainer >>= nbBytes*8;
}

void FSE_flushCState(FSE_CStream_t* bitC, const FSE_CState_t* statePtr)
{
    FSE_addBits(bitC, statePtr->value, statePtr->stateLog);
    FSE_flushBits(bitC);
}


size_t FSE_closeCStream(FSE_CStream_t* bitC)
{
    char* endPtr;

    FSE_addBitsFast(bitC, 1, 1);
    FSE_flushBits(bitC);

    if (bitC->ptr >= bitC->endPtr)   /* too close to buffer's end */
        return 0;   /* not compressible */

    endPtr = bitC->ptr;
    endPtr += bitC->bitPos > 0;

    return (endPtr - bitC->startPtr);
}


static size_t FSE_compress_usingCTable_generic (void* dst, size_t dstSize,
                           const void* src, size_t srcSize,
                           const FSE_CTable* ct, const unsigned fast)
{
    const BYTE* const istart = (const BYTE*) src;
    const BYTE* ip;
    const BYTE* const iend = istart + srcSize;

    size_t errorCode;
    FSE_CStream_t bitC;
    FSE_CState_t CState1, CState2;


    /* init */
    errorCode = FSE_initCStream(&bitC, dst, dstSize);
    if (FSE_isError(errorCode)) return 0;
    FSE_initCState(&CState1, ct);
    CState2 = CState1;

    ip=iend;

#define FSE_FLUSHBITS(s)  (fast ? FSE_flushBitsFast(s) : FSE_flushBits(s))

    /* join to even */
    if (srcSize & 1)
    {
        FSE_encodeSymbol(&bitC, &CState1, *--ip);
        FSE_FLUSHBITS(&bitC);
    }

    /* join to mod 4 */
    if ((sizeof(bitC.bitContainer)*8 > FSE_MAX_TABLELOG*4+7 ) && (srcSize & 2))   /* test bit 2 */
    {
        FSE_encodeSymbol(&bitC, &CState2, *--ip);
        FSE_encodeSymbol(&bitC, &CState1, *--ip);
        FSE_FLUSHBITS(&bitC);
    }

    /* 2 or 4 encoding per loop */
    for ( ; ip>istart ; )
    {
        FSE_encodeSymbol(&bitC, &CState2, *--ip);

        if (sizeof(bitC.bitContainer)*8 < FSE_MAX_TABLELOG*2+7 )   /* this test must be static */
            FSE_FLUSHBITS(&bitC);

        FSE_encodeSymbol(&bitC, &CState1, *--ip);

        if (sizeof(bitC.bitContainer)*8 > FSE_MAX_TABLELOG*4+7 )   /* this test must be static */
        {
            FSE_encodeSymbol(&bitC, &CState2, *--ip);
            FSE_encodeSymbol(&bitC, &CState1, *--ip);
        }

        FSE_FLUSHBITS(&bitC);
    }

    FSE_flushCState(&bitC, &CState2);
    FSE_flushCState(&bitC, &CState1);
    return FSE_closeCStream(&bitC);
}

size_t FSE_compress_usingCTable (void* dst, size_t dstSize,
                           const void* src, size_t srcSize,
                           const FSE_CTable* ct)
{
    const unsigned fast = (dstSize >= FSE_BLOCKBOUND(srcSize));

    if (fast)
        return FSE_compress_usingCTable_generic(dst, dstSize, src, srcSize, ct, 1);
    else
        return FSE_compress_usingCTable_generic(dst, dstSize, src, srcSize, ct, 0);
}


size_t FSE_compressBound(size_t size) { return FSE_COMPRESSBOUND(size); }

size_t FSE_compress2 (void* dst, size_t dstSize, const void* src, size_t srcSize, unsigned maxSymbolValue, unsigned tableLog)
{
    const BYTE* const istart = (const BYTE*) src;
    const BYTE* ip = istart;

    BYTE* const ostart = (BYTE*) dst;
    BYTE* op = ostart;
    BYTE* const oend = ostart + dstSize;

    U32   count[FSE_MAX_SYMBOL_VALUE+1];
    S16   norm[FSE_MAX_SYMBOL_VALUE+1];
    CTable_max_t ct;
    size_t errorCode;

    /* init conditions */
    if (srcSize <= 1) return 0;  /* Uncompressible */
    if (!maxSymbolValue) maxSymbolValue = FSE_MAX_SYMBOL_VALUE;
    if (!tableLog) tableLog = FSE_DEFAULT_TABLELOG;

    /* Scan input and build symbol stats */
    errorCode = FSE_count (count, &maxSymbolValue, ip, srcSize);
    if (FSE_isError(errorCode)) return errorCode;
    if (errorCode == srcSize) return 1;
    if (errorCode == 1) return 0;   /* each symbol only present once */
    if (errorCode < (srcSize >> 7)) return 0;   /* Heuristic : not compressible enough */

    tableLog = FSE_optimalTableLog(tableLog, srcSize, maxSymbolValue);
    errorCode = FSE_normalizeCount (norm, tableLog, count, srcSize, maxSymbolValue);
    if (FSE_isError(errorCode)) return errorCode;

    /* Write table description header */
    errorCode = FSE_writeNCount (op, oend-op, norm, maxSymbolValue, tableLog);
    if (FSE_isError(errorCode)) return errorCode;
    op += errorCode;

    /* Compress */
    errorCode = FSE_buildCTable (ct, norm, maxSymbolValue, tableLog);
    if (FSE_isError(errorCode)) return errorCode;
    errorCode = FSE_compress_usingCTable(op, oend - op, ip, srcSize, ct);
    if (errorCode == 0) return 0;   /* not enough space for compressed data */
    op += errorCode;

    /* check compressibility */
    if ( (size_t)(op-ostart) >= srcSize-1 )
        return 0;

    return op-ostart;
}

size_t FSE_compress (void* dst, size_t dstSize, const void* src, size_t srcSize)
{
    return FSE_compress2(dst, dstSize, src, (U32)srcSize, FSE_MAX_SYMBOL_VALUE, FSE_DEFAULT_TABLELOG);
}


/*********************************************************
*  Decompression (Byte symbols)
*********************************************************/
size_t FSE_buildDTable_rle (FSE_DTable* dt, BYTE symbolValue)
{
    FSE_DTableHeader* const DTableH = (FSE_DTableHeader*)dt;
    FSE_decode_t* const cell = (FSE_decode_t*)(dt + 1);   /* because dt is unsigned */

    DTableH->tableLog = 0;
    DTableH->fastMode = 0;

    cell->newState = 0;
    cell->symbol = symbolValue;
    cell->nbBits = 0;

    return 0;
}


size_t FSE_buildDTable_raw (FSE_DTable* dt, unsigned nbBits)
{
    FSE_DTableHeader* const DTableH = (FSE_DTableHeader*)dt;
    FSE_decode_t* const dinfo = (FSE_decode_t*)(dt + 1);   /* because dt is unsigned */
    const unsigned tableSize = 1 << nbBits;
    const unsigned tableMask = tableSize - 1;
    const unsigned maxSymbolValue = tableMask;
    unsigned s;

    /* Sanity checks */
    if (nbBits < 1) return (size_t)-FSE_ERROR_GENERIC;             /* min size */

    /* Build Decoding Table */
    DTableH->tableLog = (U16)nbBits;
    DTableH->fastMode = 1;
    for (s=0; s<=maxSymbolValue; s++)
    {
        dinfo[s].newState = 0;
        dinfo[s].symbol = (BYTE)s;
        dinfo[s].nbBits = (BYTE)nbBits;
    }

    return 0;
}


/* FSE_initDStream
 * Initialize a FSE_DStream_t.
 * srcBuffer must point at the beginning of an FSE block.
 * The function result is the size of the FSE_block (== srcSize).
 * If srcSize is too small, the function will return an errorCode;
 */
size_t FSE_initDStream(FSE_DStream_t* bitD, const void* srcBuffer, size_t srcSize)
{
    if (srcSize < 1) return (size_t)-FSE_ERROR_srcSize_wrong;

    if (srcSize >=  sizeof(size_t))
    {
        U32 contain32;
        bitD->start = (const char*)srcBuffer;
        bitD->ptr   = (const char*)srcBuffer + srcSize - sizeof(size_t);
        bitD->bitContainer = FSE_readLEST(bitD->ptr);
        contain32 = ((const BYTE*)srcBuffer)[srcSize-1];
        if (contain32 == 0) return (size_t)-FSE_ERROR_GENERIC;   /* stop bit not present */
        bitD->bitsConsumed = 8 - FSE_highbit32(contain32);
    }
    else
    {
        U32 contain32;
        bitD->start = (const char*)srcBuffer;
        bitD->ptr   = bitD->start;
        bitD->bitContainer = *(const BYTE*)(bitD->start);
        switch(srcSize)
        {
            case 7: bitD->bitContainer += (size_t)(((const BYTE*)(bitD->start))[6]) << (sizeof(size_t)*8 - 16);
            case 6: bitD->bitContainer += (size_t)(((const BYTE*)(bitD->start))[5]) << (sizeof(size_t)*8 - 24);
            case 5: bitD->bitContainer += (size_t)(((const BYTE*)(bitD->start))[4]) << (sizeof(size_t)*8 - 32);
            case 4: bitD->bitContainer += (size_t)(((const BYTE*)(bitD->start))[3]) << 24;
            case 3: bitD->bitContainer += (size_t)(((const BYTE*)(bitD->start))[2]) << 16;
            case 2: bitD->bitContainer += (size_t)(((const BYTE*)(bitD->start))[1]) <<  8;
            default:;
        }
        contain32 = ((const BYTE*)srcBuffer)[srcSize-1];
        if (contain32 == 0) return (size_t)-FSE_ERROR_GENERIC;   /* stop bit not present */
        bitD->bitsConsumed = 8 - FSE_highbit32(contain32);
        bitD->bitsConsumed += (U32)(sizeof(size_t) - srcSize)*8;
    }

    return srcSize;
}


/* FSE_lookBits
 * Provides next n bits from the bitContainer.
 * bitContainer is not modified (bits are still present for next read/look)
 * On 32-bits, maxNbBits==25
 * On 64-bits, maxNbBits==57
 * return : value extracted.
 */
static size_t FSE_lookBits(FSE_DStream_t* bitD, U32 nbBits)
{
    const U32 bitMask = sizeof(bitD->bitContainer)*8 - 1;
    return ((bitD->bitContainer << (bitD->bitsConsumed & bitMask)) >> 1) >> ((bitMask-nbBits) & bitMask);
}

static size_t FSE_lookBitsFast(FSE_DStream_t* bitD, U32 nbBits)   /* only if nbBits >= 1 !! */
{
    const U32 bitMask = sizeof(bitD->bitContainer)*8 - 1;
    return (bitD->bitContainer << (bitD->bitsConsumed & bitMask)) >> (((bitMask+1)-nbBits) & bitMask);
}

static void FSE_skipBits(FSE_DStream_t* bitD, U32 nbBits)
{
    bitD->bitsConsumed += nbBits;
}


/* FSE_readBits
 * Read next n bits from the bitContainer.
 * On 32-bits, don't read more than maxNbBits==25
 * On 64-bits, don't read more than maxNbBits==57
 * Use the fast variant *only* if n >= 1.
 * return : value extracted.
 */
size_t FSE_readBits(FSE_DStream_t* bitD, U32 nbBits)
{
    size_t value = FSE_lookBits(bitD, nbBits);
    FSE_skipBits(bitD, nbBits);
    return value;
}

size_t FSE_readBitsFast(FSE_DStream_t* bitD, U32 nbBits)   /* only if nbBits >= 1 !! */
{
    size_t value = FSE_lookBitsFast(bitD, nbBits);
    FSE_skipBits(bitD, nbBits);
    return value;
}

unsigned FSE_reloadDStream(FSE_DStream_t* bitD)
{
	if (bitD->bitsConsumed > (sizeof(bitD->bitContainer)*8))  /* should never happen */
		return FSE_DStream_tooFar;

    if (bitD->ptr >= bitD->start + sizeof(bitD->bitContainer))
    {
        bitD->ptr -= bitD->bitsConsumed >> 3;
        bitD->bitsConsumed &= 7;
        bitD->bitContainer = FSE_readLEST(bitD->ptr);
        return FSE_DStream_unfinished;
    }
    if (bitD->ptr == bitD->start)
    {
        if (bitD->bitsConsumed < sizeof(bitD->bitContainer)*8) return FSE_DStream_endOfBuffer;
        return FSE_DStream_completed;
    }
    {
        U32 nbBytes = bitD->bitsConsumed >> 3;
        U32 result = FSE_DStream_unfinished;
        if (bitD->ptr - nbBytes < bitD->start)
        {
            nbBytes = (U32)(bitD->ptr - bitD->start);  /* ptr > start */
            result = FSE_DStream_endOfBuffer;
        }
        bitD->ptr -= nbBytes;
        bitD->bitsConsumed -= nbBytes*8;
        bitD->bitContainer = FSE_readLEST(bitD->ptr);   /* reminder : srcSize > sizeof(bitD) */
        return result;
    }
}


void FSE_initDState(FSE_DState_t* DStatePtr, FSE_DStream_t* bitD, const FSE_DTable* dt)
{
    const FSE_DTableHeader* const DTableH = (const FSE_DTableHeader*)dt;
    DStatePtr->state = FSE_readBits(bitD, DTableH->tableLog);
    FSE_reloadDStream(bitD);
    DStatePtr->table = dt + 1;
}

BYTE FSE_decodeSymbol(FSE_DState_t* DStatePtr, FSE_DStream_t* bitD)
{
    const FSE_decode_t DInfo = ((const FSE_decode_t*)(DStatePtr->table))[DStatePtr->state];
    const U32  nbBits = DInfo.nbBits;
    BYTE symbol = DInfo.symbol;
    size_t lowBits = FSE_readBits(bitD, nbBits);

    DStatePtr->state = DInfo.newState + lowBits;
    return symbol;
}

BYTE FSE_decodeSymbolFast(FSE_DState_t* DStatePtr, FSE_DStream_t* bitD)
{
    const FSE_decode_t DInfo = ((const FSE_decode_t*)(DStatePtr->table))[DStatePtr->state];
    const U32 nbBits = DInfo.nbBits;
    BYTE symbol = DInfo.symbol;
    size_t lowBits = FSE_readBitsFast(bitD, nbBits);

    DStatePtr->state = DInfo.newState + lowBits;
    return symbol;
}

/* FSE_endOfDStream
   Tells if bitD has reached end of bitStream or not */

unsigned FSE_endOfDStream(const FSE_DStream_t* bitD)
{
    return ((bitD->ptr == bitD->start) && (bitD->bitsConsumed == sizeof(bitD->bitContainer)*8));
}

unsigned FSE_endOfDState(const FSE_DState_t* DStatePtr)
{
    return DStatePtr->state == 0;
}


FORCE_INLINE size_t FSE_decompress_usingDTable_generic(
          void* dst, size_t maxDstSize,
    const void* cSrc, size_t cSrcSize,
    const FSE_DTable* dt, const unsigned fast)
{
    BYTE* const ostart = (BYTE*) dst;
    BYTE* op = ostart;
    BYTE* const omax = op + maxDstSize;
    BYTE* const olimit = omax-3;

    FSE_DStream_t bitD;
    FSE_DState_t state1;
    FSE_DState_t state2;
    size_t errorCode;

    /* Init */
    errorCode = FSE_initDStream(&bitD, cSrc, cSrcSize);   /* replaced last arg by maxCompressed Size */
    if (FSE_isError(errorCode)) return errorCode;

    FSE_initDState(&state1, &bitD, dt);
    FSE_initDState(&state2, &bitD, dt);

#define FSE_GETSYMBOL(statePtr) fast ? FSE_decodeSymbolFast(statePtr, &bitD) : FSE_decodeSymbol(statePtr, &bitD)

    /* 4 symbols per loop */
    for ( ; (FSE_reloadDStream(&bitD)==FSE_DStream_unfinished) && (op<olimit) ; op+=4)
    {
        op[0] = FSE_GETSYMBOL(&state1);

        if (FSE_MAX_TABLELOG*2+7 > sizeof(bitD.bitContainer)*8)    /* This test must be static */
            FSE_reloadDStream(&bitD);

        op[1] = FSE_GETSYMBOL(&state2);

        if (FSE_MAX_TABLELOG*4+7 > sizeof(bitD.bitContainer)*8)    /* This test must be static */
            { if (FSE_reloadDStream(&bitD) > FSE_DStream_unfinished) { op+=2; break; } }

        op[2] = FSE_GETSYMBOL(&state1);

        if (FSE_MAX_TABLELOG*2+7 > sizeof(bitD.bitContainer)*8)    /* This test must be static */
            FSE_reloadDStream(&bitD);

        op[3] = FSE_GETSYMBOL(&state2);
    }

    /* tail */
    /* note : FSE_reloadDStream(&bitD) >= FSE_DStream_partiallyFilled; Ends at exactly FSE_DStream_completed */
    while (1)
    {
        if ( (FSE_reloadDStream(&bitD)>FSE_DStream_completed) || (op==omax) || (FSE_endOfDStream(&bitD) && (fast || FSE_endOfDState(&state1))) )
            break;

        *op++ = FSE_GETSYMBOL(&state1);

        if ( (FSE_reloadDStream(&bitD)>FSE_DStream_completed) || (op==omax) || (FSE_endOfDStream(&bitD) && (fast || FSE_endOfDState(&state2))) )
            break;

        *op++ = FSE_GETSYMBOL(&state2);
    }

    /* end ? */
    if (FSE_endOfDStream(&bitD) && FSE_endOfDState(&state1) && FSE_endOfDState(&state2))
        return op-ostart;

    if (op==omax) return (size_t)-FSE_ERROR_dstSize_tooSmall;   /* dst buffer is full, but cSrc unfinished */

    return (size_t)-FSE_ERROR_corruptionDetected;
}


size_t FSE_decompress_usingDTable(void* dst, size_t originalSize,
                            const void* cSrc, size_t cSrcSize,
                            const FSE_DTable* dt)
{
    const FSE_DTableHeader* DTableH = (const FSE_DTableHeader*)dt;
    const U32 fastMode = DTableH->fastMode;

    /* select fast mode (static) */
    if (fastMode) return FSE_decompress_usingDTable_generic(dst, originalSize, cSrc, cSrcSize, dt, 1);
    return FSE_decompress_usingDTable_generic(dst, originalSize, cSrc, cSrcSize, dt, 0);
}


size_t FSE_decompress(void* dst, size_t maxDstSize, const void* cSrc, size_t cSrcSize)
{
    const BYTE* const istart = (const BYTE*)cSrc;
    const BYTE* ip = istart;
    short counting[FSE_MAX_SYMBOL_VALUE+1];
    DTable_max_t dt;   /* Static analyzer seems unable to understand this table will be properly initialized later */
    unsigned tableLog;
    unsigned maxSymbolValue = FSE_MAX_SYMBOL_VALUE;
    size_t errorCode;

    if (cSrcSize<2) return (size_t)-FSE_ERROR_srcSize_wrong;   /* too small input size */

    /* normal FSE decoding mode */
    errorCode = FSE_readNCount (counting, &maxSymbolValue, &tableLog, istart, cSrcSize);
    if (FSE_isError(errorCode)) return errorCode;
    if (errorCode >= cSrcSize) return (size_t)-FSE_ERROR_srcSize_wrong;   /* too small input size */
    ip += errorCode;
    cSrcSize -= errorCode;

    errorCode = FSE_buildDTable (dt, counting, maxSymbolValue, tableLog);
    if (FSE_isError(errorCode)) return errorCode;

    /* always return, even if it is an error code */
    return FSE_decompress_usingDTable (dst, maxDstSize, ip, cSrcSize, dt);
}



/*********************************************************
*  Huff0 : Huffman block compression
*********************************************************/
#define HUF_MAX_SYMBOL_VALUE 255
#define HUF_DEFAULT_TABLELOG  12       /* used by default, when not specified */
#define HUF_MAX_TABLELOG  12           /* max possible tableLog; for allocation purpose; can be modified */
#define HUF_ABSOLUTEMAX_TABLELOG  16   /* absolute limit of HUF_MAX_TABLELOG. Beyond that value, code does not work */
#if (HUF_MAX_TABLELOG > HUF_ABSOLUTEMAX_TABLELOG)
#  error "HUF_MAX_TABLELOG is too large !"
#endif

typedef struct HUF_CElt_s {
  U16  val;
  BYTE nbBits;
} HUF_CElt ;

typedef struct nodeElt_s {
    U32 count;
    U16 parent;
    BYTE byte;
    BYTE nbBits;
} nodeElt;

/* HUF_writeCTable() :
   return : size of saved CTable */
size_t HUF_writeCTable (void* dst, size_t maxDstSize, const HUF_CElt* tree, U32 maxSymbolValue, U32 huffLog)
{
    BYTE bitsToWeight[HUF_ABSOLUTEMAX_TABLELOG + 1];
    BYTE huffWeight[HUF_MAX_SYMBOL_VALUE + 1];
    U32 n;
    BYTE* op = (BYTE*)dst;
    size_t size;

     /* check conditions */
    if (maxSymbolValue > HUF_MAX_SYMBOL_VALUE + 1)
        return (size_t)-FSE_ERROR_GENERIC;

    /* convert to weight */
    bitsToWeight[0] = 0;
    for (n=1; n<=huffLog; n++)
        bitsToWeight[n] = (BYTE)(huffLog + 1 - n);
    for (n=0; n<maxSymbolValue; n++)
        huffWeight[n] = bitsToWeight[tree[n].nbBits];

    size = FSE_compress(op+1, maxDstSize-1, huffWeight, maxSymbolValue);   /* don't need last symbol stat : implied */
    if (FSE_isError(size)) return size;
    if (size >= 128) return (size_t)-FSE_ERROR_GENERIC;   /* should never happen, since maxSymbolValue <= 255 */
    if ((size <= 1) || (size >= maxSymbolValue/2))
    {
        if (size==1)   /* RLE */
        {
            /* only possible case : serie of 1 (because there are at least 2) */
            /* can only be 2^n or (2^n-1), otherwise not an huffman tree */
            BYTE code;
            switch(maxSymbolValue)
            {
            case 1: code = 0; break;
            case 2: code = 1; break;
            case 3: code = 2; break;
            case 4: code = 3; break;
            case 7: code = 4; break;
            case 8: code = 5; break;
            case 15: code = 6; break;
            case 16: code = 7; break;
            case 31: code = 8; break;
            case 32: code = 9; break;
            case 63: code = 10; break;
            case 64: code = 11; break;
            case 127: code = 12; break;
            case 128: code = 13; break;
            default : return (size_t)-FSE_ERROR_corruptionDetected;
            }
            op[0] = (BYTE)(255-13 + code);
            return 1;
        }
         /* Not compressible */
        if (maxSymbolValue > (241-128)) return (size_t)-FSE_ERROR_GENERIC;   /* not implemented (not possible with current format) */
        if (((maxSymbolValue+1)/2) + 1 > maxDstSize) return (size_t)-FSE_ERROR_dstSize_tooSmall;   /* not enough space within dst buffer */
        op[0] = (BYTE)(128 /*special case*/ + 0 /* Not Compressible */ + (maxSymbolValue-1));
		huffWeight[maxSymbolValue] = 0;   /* to be sure it doesn't cause issue in final combination */
        for (n=0; n<maxSymbolValue; n+=2)
            op[(n/2)+1] = (BYTE)((huffWeight[n] << 4) + huffWeight[n+1]);
        return ((maxSymbolValue+1)/2) + 1;
    }

    /* normal header case */
    op[0] = (BYTE)size;
    return size+1;
}


static U32 HUF_setMaxHeight(nodeElt* huffNode, U32 lastNonNull, U32 maxNbBits)
{
    int totalCost = 0;
    const U32 largestBits = huffNode[lastNonNull].nbBits;

    /* early exit : all is fine */
    if (largestBits <= maxNbBits) return largestBits;

    // now we have a few too large elements (at least >= 2)
    {
        const U32 baseCost = 1 << (largestBits - maxNbBits);
        U32 n = lastNonNull;

        while (huffNode[n].nbBits > maxNbBits)
        {
            totalCost += baseCost - (1 << (largestBits - huffNode[n].nbBits));
            huffNode[n].nbBits = (BYTE)maxNbBits;
            n --;
        }

        /* renorm totalCost */
        totalCost >>= (largestBits - maxNbBits);  /* note : totalCost necessarily multiple of baseCost */

        // repay cost
        while (huffNode[n].nbBits == maxNbBits) n--;   // n at last of rank (maxNbBits-1)

        {
            const U32 noOne = 0xF0F0F0F0;
            // Get pos of last (smallest) symbol per rank
            U32 rankLast[HUF_MAX_TABLELOG];
            U32 currentNbBits = maxNbBits;
            int pos;
			memset(rankLast, 0xF0, sizeof(rankLast));
            for (pos=n ; pos >= 0; pos--)
            {
                if (huffNode[pos].nbBits >= currentNbBits) continue;
                currentNbBits = huffNode[pos].nbBits;
                rankLast[maxNbBits-currentNbBits] = pos;
            }

            while (totalCost > 0)
            {
                U32 nBitsToDecrease = FSE_highbit32(totalCost) + 1;
                for ( ; nBitsToDecrease > 1; nBitsToDecrease--)
                {
                    U32 highPos = rankLast[nBitsToDecrease];
                    U32 lowPos = rankLast[nBitsToDecrease-1];
                    if (highPos == noOne) continue;
                    if (lowPos == noOne) break;
                    {
                        U32 highTotal = huffNode[highPos].count;
                        U32 lowTotal = 2 * huffNode[lowPos].count;
                        if (highTotal <= lowTotal) break;
                    }
                }
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
                while (rankLast[nBitsToDecrease] == noOne)
                    nBitsToDecrease ++;   // In some rare cases, no more rank 1 left => overshoot to closest
#pragma GCC diagnostic pop
                totalCost -= 1 << (nBitsToDecrease-1);
                if (rankLast[nBitsToDecrease-1] == noOne)
                    rankLast[nBitsToDecrease-1] = rankLast[nBitsToDecrease];   // now there is one elt
                huffNode[rankLast[nBitsToDecrease]].nbBits ++;
                if (rankLast[nBitsToDecrease] == 0)
                    rankLast[nBitsToDecrease] = noOne;
                else
                {
                    rankLast[nBitsToDecrease]--;
                    if (huffNode[rankLast[nBitsToDecrease]].nbBits != maxNbBits-nBitsToDecrease)
                        rankLast[nBitsToDecrease] = noOne;   // rank list emptied
                }
            }

			while (totalCost < 0)   /* Sometimes, cost correction overshoot */
			{
				if (rankLast[1] == noOne)   /* special case, no weight 1, let's find it back at n */
				{
					while (huffNode[n].nbBits == maxNbBits) n--;
					huffNode[n+1].nbBits--;
					rankLast[1] = n+1;
					totalCost++;
					continue;
				}
				huffNode[ rankLast[1] + 1 ].nbBits--;
				rankLast[1]++;
				totalCost ++;
			}
        }
    }

    return maxNbBits;
}


typedef struct {
    U32 base;
    U32 current;
} rankPos;

static void HUF_sort(nodeElt* huffNode, const U32* count, U32 maxSymbolValue)
{
    rankPos rank[32];
    U32 n;

    memset(rank, 0, sizeof(rank));
    for (n=0; n<=maxSymbolValue; n++)
    {
        U32 r = FSE_highbit32(count[n] + 1);
        rank[r].base ++;
    }
    for (n=30; n>0; n--) rank[n-1].base += rank[n].base;
    for (n=0; n<32; n++) rank[n].current = rank[n].base;
    for (n=0; n<=maxSymbolValue; n++)
    {
        U32 c = count[n];
        U32 r = FSE_highbit32(c+1) + 1;
        U32 pos = rank[r].current++;
        while ((pos > rank[r].base) && (c > huffNode[pos-1].count)) huffNode[pos]=huffNode[pos-1], pos--;
        huffNode[pos].count = c;
        huffNode[pos].byte  = (BYTE)n;
    }
}


#define STARTNODE (HUF_MAX_SYMBOL_VALUE+1)
size_t HUF_buildCTable (HUF_CElt* tree, const U32* count, U32 maxSymbolValue, U32 maxNbBits)
{
    nodeElt huffNode0[2*HUF_MAX_SYMBOL_VALUE+1 +1];
    nodeElt* huffNode = huffNode0 + 1;
    U32 n, nonNullRank;
    int lowS, lowN;
    U16 nodeNb = STARTNODE;
    U32 nodeRoot;

    /* safety checks */
    if (maxNbBits == 0) maxNbBits = HUF_DEFAULT_TABLELOG;
    if (maxSymbolValue > HUF_MAX_SYMBOL_VALUE) return (size_t)-FSE_ERROR_GENERIC;
	memset(huffNode0, 0, sizeof(huffNode0));

    // sort, decreasing order
    HUF_sort(huffNode, count, maxSymbolValue);

    // init for parents
    nonNullRank = maxSymbolValue;
    while(huffNode[nonNullRank].count == 0) nonNullRank--;
    lowS = nonNullRank; nodeRoot = nodeNb + lowS - 1; lowN = nodeNb;
    huffNode[nodeNb].count = huffNode[lowS].count + huffNode[lowS-1].count;
    huffNode[lowS].parent = huffNode[lowS-1].parent = nodeNb;
    nodeNb++; lowS-=2;
    for (n=nodeNb; n<=nodeRoot; n++) huffNode[n].count = (U32)(1U<<30);
    huffNode0[0].count = (U32)(1U<<31);

    // create parents
    while (nodeNb <= nodeRoot)
    {
        U32 n1 = (huffNode[lowS].count < huffNode[lowN].count) ? lowS-- : lowN++;
        U32 n2 = (huffNode[lowS].count < huffNode[lowN].count) ? lowS-- : lowN++;
        huffNode[nodeNb].count = huffNode[n1].count + huffNode[n2].count;
        huffNode[n1].parent = huffNode[n2].parent = nodeNb;
        nodeNb++;
    }

    // distribute weights (unlimited tree height)
    huffNode[nodeRoot].nbBits = 0;
    for (n=nodeRoot-1; n>=STARTNODE; n--)
        huffNode[n].nbBits = huffNode[ huffNode[n].parent ].nbBits + 1;
    for (n=0; n<=nonNullRank; n++)
        huffNode[n].nbBits = huffNode[ huffNode[n].parent ].nbBits + 1;

    // enforce maxTableLog
    maxNbBits = HUF_setMaxHeight(huffNode, nonNullRank, maxNbBits);

    // fill result into tree (val, nbBits)
    {
        U16 nbPerRank[HUF_ABSOLUTEMAX_TABLELOG+1] = {0};
        U16 valPerRank[HUF_ABSOLUTEMAX_TABLELOG+1];
        if (maxNbBits > HUF_ABSOLUTEMAX_TABLELOG) return (size_t)-FSE_ERROR_GENERIC;   // check
        for (n=0; n<=nonNullRank; n++)
            nbPerRank[huffNode[n].nbBits]++;
        {
            // determine stating value per rank
            U16 min = 0;
            for (n=maxNbBits; n>0; n--)
            {
                valPerRank[n] = min;      // get starting value within each rank
                min += nbPerRank[n];
                min >>= 1;
            }
        }
        for (n=0; n<=maxSymbolValue; n++)
            tree[huffNode[n].byte].nbBits = huffNode[n].nbBits;   // push nbBits per symbol, symbol order
        for (n=0; n<=maxSymbolValue; n++)
            tree[n].val = valPerRank[tree[n].nbBits]++;   // assign value within rank, symbol order
    }

    return maxNbBits;
}

static void HUF_encodeSymbol(FSE_CStream_t* bitCPtr, U32 symbol, const HUF_CElt* CTable)
{
    FSE_addBitsFast(bitCPtr, CTable[symbol].val, CTable[symbol].nbBits);
}

#define FSE_FLUSHBITS_1(stream) \
    if (sizeof((stream)->bitContainer)*8 < HUF_MAX_TABLELOG*2+7) FSE_FLUSHBITS(stream)

#define FSE_FLUSHBITS_2(stream) \
    if (sizeof((stream)->bitContainer)*8 < HUF_MAX_TABLELOG*4+7) FSE_FLUSHBITS(stream)

size_t HUF_compress_usingCTable(void* dst, size_t dstSize, const void* src, size_t srcSize, HUF_CElt* CTable)
{
    const BYTE* ip = (const BYTE*) src;
    BYTE* const ostart = (BYTE*)dst;
    BYTE* op = (BYTE*) ostart;
    BYTE* const oend = ostart + dstSize;
    U16* jumpTable = (U16*) dst;
    size_t n, streamSize;
    const unsigned fast = (dstSize >= HUF_BLOCKBOUND(srcSize));
    size_t errorCode;
    FSE_CStream_t bitC;

    /* init */
	if (dstSize < 8) return 0;
    op += 6;   /* jump Table -- could be optimized by delta / deviation */
    errorCode = FSE_initCStream(&bitC, op, oend-op);
    if (FSE_isError(errorCode)) return 0;

    n = srcSize & ~15;  // mod 16
    switch (srcSize & 15)
    {
        case 15: HUF_encodeSymbol(&bitC, ip[n+14], CTable);
                 FSE_FLUSHBITS_1(&bitC);
        case 14: HUF_encodeSymbol(&bitC, ip[n+13], CTable);
                 FSE_FLUSHBITS_2(&bitC);
        case 13: HUF_encodeSymbol(&bitC, ip[n+12], CTable);
                 FSE_FLUSHBITS_1(&bitC);
        case 12: HUF_encodeSymbol(&bitC, ip[n+11], CTable);
                 FSE_FLUSHBITS(&bitC);
        case 11: HUF_encodeSymbol(&bitC, ip[n+10], CTable);
                 FSE_FLUSHBITS_1(&bitC);
        case 10: HUF_encodeSymbol(&bitC, ip[n+ 9], CTable);
                 FSE_FLUSHBITS_2(&bitC);
        case 9 : HUF_encodeSymbol(&bitC, ip[n+ 8], CTable);
                 FSE_FLUSHBITS_1(&bitC);
        case 8 : HUF_encodeSymbol(&bitC, ip[n+ 7], CTable);
                 FSE_FLUSHBITS(&bitC);
        case 7 : HUF_encodeSymbol(&bitC, ip[n+ 6], CTable);
                 FSE_FLUSHBITS_1(&bitC);
        case 6 : HUF_encodeSymbol(&bitC, ip[n+ 5], CTable);
                 FSE_FLUSHBITS_2(&bitC);
        case 5 : HUF_encodeSymbol(&bitC, ip[n+ 4], CTable);
                 FSE_FLUSHBITS_1(&bitC);
        case 4 : HUF_encodeSymbol(&bitC, ip[n+ 3], CTable);
                 FSE_FLUSHBITS(&bitC);
        case 3 : HUF_encodeSymbol(&bitC, ip[n+ 2], CTable);
                 FSE_FLUSHBITS_2(&bitC);
        case 2 : HUF_encodeSymbol(&bitC, ip[n+ 1], CTable);
                 FSE_FLUSHBITS_1(&bitC);
        case 1 : HUF_encodeSymbol(&bitC, ip[n+ 0], CTable);
                 FSE_FLUSHBITS(&bitC);
        case 0 :
        default: ;
    }

    for (; n>0; n-=16)
    {
        HUF_encodeSymbol(&bitC, ip[n- 4], CTable);
        FSE_FLUSHBITS_1(&bitC);
        HUF_encodeSymbol(&bitC, ip[n- 8], CTable);
        FSE_FLUSHBITS_2(&bitC);
        HUF_encodeSymbol(&bitC, ip[n-12], CTable);
        FSE_FLUSHBITS_1(&bitC);
        HUF_encodeSymbol(&bitC, ip[n-16], CTable);
        FSE_FLUSHBITS(&bitC);
    }
    streamSize = FSE_closeCStream(&bitC);
    if (streamSize==0) return 0;   /* not enough space within dst buffer == uncompressible */
    FSE_writeLE16(jumpTable, (U16)streamSize);
    op += streamSize;

    errorCode = FSE_initCStream(&bitC, op, oend-op);
    if (FSE_isError(errorCode)) return 0;
    n = srcSize & ~15;  // mod 16
    for (; n>0; n-=16)
    {
        HUF_encodeSymbol(&bitC, ip[n- 3], CTable);
        FSE_FLUSHBITS_1(&bitC);
        HUF_encodeSymbol(&bitC, ip[n- 7], CTable);
        FSE_FLUSHBITS_2(&bitC);
        HUF_encodeSymbol(&bitC, ip[n-11], CTable);
        FSE_FLUSHBITS_1(&bitC);
        HUF_encodeSymbol(&bitC, ip[n-15], CTable);
        FSE_FLUSHBITS(&bitC);
    }
    streamSize = FSE_closeCStream(&bitC);
    if (streamSize==0) return 0;   /* not enough space within dst buffer == uncompressible */
    FSE_writeLE16(jumpTable+1, (U16)streamSize);
    op += streamSize;

    errorCode = FSE_initCStream(&bitC, op, oend-op);
    if (FSE_isError(errorCode)) return 0;
    n = srcSize & ~15;  // mod 16
    for (; n>0; n-=16)
    {
        HUF_encodeSymbol(&bitC, ip[n- 2], CTable);
        FSE_FLUSHBITS_1(&bitC);
        HUF_encodeSymbol(&bitC, ip[n- 6], CTable);
        FSE_FLUSHBITS_2(&bitC);
        HUF_encodeSymbol(&bitC, ip[n-10], CTable);
        FSE_FLUSHBITS_1(&bitC);
        HUF_encodeSymbol(&bitC, ip[n-14], CTable);
        FSE_FLUSHBITS(&bitC);
    }
    streamSize = FSE_closeCStream(&bitC);
    if (streamSize==0) return 0;   /* not enough space within dst buffer == uncompressible */
    FSE_writeLE16(jumpTable+2, (U16)streamSize);
    op += streamSize;

    errorCode = FSE_initCStream(&bitC, op, oend-op);
    if (FSE_isError(errorCode)) return 0;
    n = srcSize & ~15;  // mod 16
    for (; n>0; n-=16)
    {
        HUF_encodeSymbol(&bitC, ip[n- 1], CTable);
        FSE_FLUSHBITS_1(&bitC);
        HUF_encodeSymbol(&bitC, ip[n- 5], CTable);
        FSE_FLUSHBITS_2(&bitC);
        HUF_encodeSymbol(&bitC, ip[n- 9], CTable);
        FSE_FLUSHBITS_1(&bitC);
        HUF_encodeSymbol(&bitC, ip[n-13], CTable);
        FSE_FLUSHBITS(&bitC);
    }
    streamSize = FSE_closeCStream(&bitC);
    if (streamSize==0) return 0;   /* not enough space within dst buffer == uncompressible */
    op += streamSize;

    return op-ostart;
}


size_t HUF_compress2 (void* dst, size_t dstSize, const void* src, size_t srcSize, unsigned maxSymbolValue, unsigned huffLog)
{
    BYTE* const ostart = (BYTE*)dst;
    BYTE* op = ostart;
    BYTE* const oend = ostart + dstSize;

    U32 count[HUF_MAX_SYMBOL_VALUE+1];
    HUF_CElt CTable[HUF_MAX_SYMBOL_VALUE+1];
    size_t errorCode;

    /* early out */
    if (srcSize <= 1) return srcSize;  /* Uncompressed or RLE */
    if (!maxSymbolValue) maxSymbolValue = HUF_MAX_SYMBOL_VALUE;
    if (!huffLog) huffLog = HUF_DEFAULT_TABLELOG;

    /* Scan input and build symbol stats */
    errorCode = FSE_count (count, &maxSymbolValue, (const BYTE*)src, srcSize);
    if (FSE_isError(errorCode)) return errorCode;
    if (errorCode == srcSize) return 1;
    if (errorCode < (srcSize >> 7)) return 0;   /* Heuristic : not compressible enough */

    /* Build Huffman Tree */
    errorCode = HUF_buildCTable (CTable, count, maxSymbolValue, huffLog);
    if (FSE_isError(errorCode)) return errorCode;
    huffLog = (U32)errorCode;

    /* Write table description header */
    errorCode = HUF_writeCTable (op, dstSize, CTable, maxSymbolValue, huffLog);  /* don't write last symbol, implied */
    if (FSE_isError(errorCode)) return errorCode;
    op += errorCode;

    /* Compress */
    errorCode = HUF_compress_usingCTable(op, oend - op, src, srcSize, CTable);
    if (FSE_isError(errorCode)) return errorCode;
    if (errorCode==0) return 0;
    op += errorCode;

    /* check compressibility */
    if ((size_t)(op-ostart) >= srcSize-1)
        return op-ostart;

    return op-ostart;
}

size_t HUF_compress (void* dst, size_t maxDstSize, const void* src, size_t srcSize)
{
    return HUF_compress2(dst, maxDstSize, src, (U32)srcSize, 255, HUF_DEFAULT_TABLELOG);
}


/*********************************************************
*  Huff0 : Huffman block decompression
*********************************************************/
typedef struct {
    BYTE byte;
    BYTE nbBits;
} HUF_DElt;

size_t HUF_readDTable (U16* DTable, const void* src, size_t srcSize)
{
    BYTE huffWeight[HUF_MAX_SYMBOL_VALUE + 1];
    U32 rankVal[HUF_ABSOLUTEMAX_TABLELOG + 1];  /* large enough for values from 0 to 16 */
    U32 weightTotal;
    U32 maxBits;
    const BYTE* ip = (const BYTE*) src;
    size_t iSize = ip[0];
    size_t oSize;
    U32 n;
    U32 nextRankStart;
    HUF_DElt* const dt = (HUF_DElt*)(DTable + 1);

    FSE_STATIC_ASSERT(sizeof(HUF_DElt) == sizeof(U16));   /* if compilation fails here, assertion is false */
    //memset(huffWeight, 0, sizeof(huffWeight));   /* should not be necessary, but some analyzer complain ... */
    if (iSize >= 128)  /* special header */
    {
        if (iSize >= (242))   /* RLE */
        {
            static int l[14] = { 1, 2, 3, 4, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128 };
            oSize = l[iSize-242];
            memset(huffWeight, 1, oSize);
            iSize = 0;
        }
        else   /* Incompressible */
        {
            oSize = iSize - 127;
            iSize = ((oSize+1)/2);
            if (iSize+1 > srcSize) return (size_t)-FSE_ERROR_srcSize_wrong;
            ip += 1;
            for (n=0; n<oSize; n+=2)
            {
                huffWeight[n]   = ip[n/2] >> 4;
                huffWeight[n+1] = ip[n/2] & 15;
            }
        }
    }
    else  /* header compressed with FSE (normal case) */
    {
        if (iSize+1 > srcSize) return (size_t)-FSE_ERROR_srcSize_wrong;
        oSize = FSE_decompress(huffWeight, HUF_MAX_SYMBOL_VALUE, ip+1, iSize);   /* max 255 values decoded, last one is implied */
        if (FSE_isError(oSize)) return oSize;
    }

    /* collect weight stats */
    memset(rankVal, 0, sizeof(rankVal));
    weightTotal = 0;
    for (n=0; n<oSize; n++)
    {
        if (huffWeight[n] >= HUF_ABSOLUTEMAX_TABLELOG) return (size_t)-FSE_ERROR_corruptionDetected;
        rankVal[huffWeight[n]]++;
        weightTotal += (1 << huffWeight[n]) >> 1;
    }

    /* get last non-null symbol weight (implied, total must be 2^n) */
    maxBits = FSE_highbit32(weightTotal) + 1;
    if (maxBits > DTable[0]) return (size_t)-FSE_ERROR_tableLog_tooLarge;   /* DTable is too small */
    DTable[0] = (U16)maxBits;
    {
        U32 total = 1 << maxBits;
        U32 rest = total - weightTotal;
        U32 verif = 1 << FSE_highbit32(rest);
        U32 lastWeight = FSE_highbit32(rest) + 1;
        if (verif != rest) return (size_t)-FSE_ERROR_corruptionDetected;    /* last value must be a clean power of 2 */
        huffWeight[oSize] = (BYTE)lastWeight;
        rankVal[lastWeight]++;
    }

    /* check tree construction validity */
    if ((rankVal[1] < 2) || (rankVal[1] & 1)) return (size_t)-FSE_ERROR_corruptionDetected;   /* by construction : at least 2 elts of rank 1, must be even */

    /* Prepare ranks */
    nextRankStart = 0;
    for (n=1; n<=maxBits; n++)
    {
        U32 current = nextRankStart;
        nextRankStart += (rankVal[n] << (n-1));
        rankVal[n] = current;
    }

    /* fill DTable */
    for (n=0; n<=oSize; n++)
    {
        const U32 w = huffWeight[n];
        const U32 length = (1 << w) >> 1;
        U32 i;
        HUF_DElt D;
        D.byte = (BYTE)n; D.nbBits = (BYTE)(maxBits + 1 - w);
        for (i = rankVal[w]; i < rankVal[w] + length; i++)
            dt[i] = D;
        rankVal[w] += length;
    }

    return iSize+1;
}


static BYTE HUF_decodeSymbol(FSE_DStream_t* Dstream, const HUF_DElt* dt, const U32 dtLog)
{
        const size_t val = FSE_lookBitsFast(Dstream, dtLog); /* note : dtLog >= 1 */
        const BYTE c = dt[val].byte;
        FSE_skipBits(Dstream, dt[val].nbBits);
        return c;
}

static size_t HUF_decompress_usingDTable(   /* -3% slower when non static */
          void* dst, size_t maxDstSize,
    const void* cSrc, size_t cSrcSize,
    const U16* DTable)
{
    BYTE* const ostart = (BYTE*) dst;
    BYTE* op = ostart;
    BYTE* const omax = op + maxDstSize;
    BYTE* const olimit = omax-15;

    const HUF_DElt* const dt = (const HUF_DElt*)(DTable+1);
    const U32 dtLog = DTable[0];
    size_t errorCode;
    U32 reloadStatus;

    /* Init */

    const U16* jumpTable = (const U16*)cSrc;
    const size_t length1 = FSE_readLE16(jumpTable);
    const size_t length2 = FSE_readLE16(jumpTable+1);
    const size_t length3 = FSE_readLE16(jumpTable+2);
    const size_t length4 = cSrcSize - 6 - length1 - length2 - length3;   // check coherency !!
    const char* const start1 = (const char*)(cSrc) + 6;
    const char* const start2 = start1 + length1;
    const char* const start3 = start2 + length2;
    const char* const start4 = start3 + length3;
    FSE_DStream_t bitD1, bitD2, bitD3, bitD4;

    if (length1+length2+length3+6 >= cSrcSize) return (size_t)-FSE_ERROR_srcSize_wrong;

    errorCode = FSE_initDStream(&bitD1, start1, length1);
    if (FSE_isError(errorCode)) return errorCode;
    errorCode = FSE_initDStream(&bitD2, start2, length2);
    if (FSE_isError(errorCode)) return errorCode;
    errorCode = FSE_initDStream(&bitD3, start3, length3);
    if (FSE_isError(errorCode)) return errorCode;
    errorCode = FSE_initDStream(&bitD4, start4, length4);
    if (FSE_isError(errorCode)) return errorCode;

    reloadStatus=FSE_reloadDStream(&bitD2);

    /* 16 symbols per loop */
    for ( ; (reloadStatus<FSE_DStream_completed) && (op<olimit);  /* D2-3-4 are supposed to be synchronized and finish together */
        op+=16, reloadStatus = FSE_reloadDStream(&bitD2) | FSE_reloadDStream(&bitD3) | FSE_reloadDStream(&bitD4), FSE_reloadDStream(&bitD1))
    {
#define HUF_DECODE_SYMBOL_0(n, Dstream) \
        op[n] = HUF_decodeSymbol(&Dstream, dt, dtLog);

#define HUF_DECODE_SYMBOL_1(n, Dstream) \
        op[n] = HUF_decodeSymbol(&Dstream, dt, dtLog); \
        if (FSE_32bits() && (HUF_MAX_TABLELOG>12)) FSE_reloadDStream(&Dstream)

#define HUF_DECODE_SYMBOL_2(n, Dstream) \
        op[n] = HUF_decodeSymbol(&Dstream, dt, dtLog); \
        if (FSE_32bits()) FSE_reloadDStream(&Dstream)

        HUF_DECODE_SYMBOL_1( 0, bitD1);
        HUF_DECODE_SYMBOL_1( 1, bitD2);
        HUF_DECODE_SYMBOL_1( 2, bitD3);
        HUF_DECODE_SYMBOL_1( 3, bitD4);
        HUF_DECODE_SYMBOL_2( 4, bitD1);
        HUF_DECODE_SYMBOL_2( 5, bitD2);
        HUF_DECODE_SYMBOL_2( 6, bitD3);
        HUF_DECODE_SYMBOL_2( 7, bitD4);
        HUF_DECODE_SYMBOL_1( 8, bitD1);
        HUF_DECODE_SYMBOL_1( 9, bitD2);
        HUF_DECODE_SYMBOL_1(10, bitD3);
        HUF_DECODE_SYMBOL_1(11, bitD4);
        HUF_DECODE_SYMBOL_0(12, bitD1);
        HUF_DECODE_SYMBOL_0(13, bitD2);
        HUF_DECODE_SYMBOL_0(14, bitD3);
        HUF_DECODE_SYMBOL_0(15, bitD4);
    }

    if (reloadStatus!=FSE_DStream_completed)   /* not complete : some bitStream might be FSE_DStream_unfinished */
        return (size_t)-FSE_ERROR_corruptionDetected;

    /* tail */
    {
        // bitTail = bitD1;   // *much* slower : -20% !??!
        FSE_DStream_t bitTail;
        bitTail.ptr = bitD1.ptr;
        bitTail.bitsConsumed = bitD1.bitsConsumed;
        bitTail.bitContainer = bitD1.bitContainer;   // required in case of FSE_DStream_endOfBuffer
        bitTail.start = start1;
        for ( ; (FSE_reloadDStream(&bitTail) < FSE_DStream_completed) && (op<omax) ; op++)
        {
            HUF_DECODE_SYMBOL_0(0, bitTail);
        }

        if (FSE_endOfDStream(&bitTail))
            return op-ostart;
    }

    if (op==omax) return (size_t)-FSE_ERROR_dstSize_tooSmall;   /* dst buffer is full, but cSrc unfinished */

    return (size_t)-FSE_ERROR_corruptionDetected;
}


size_t HUF_decompress (void* dst, size_t maxDstSize, const void* cSrc, size_t cSrcSize)
{
    HUF_CREATE_STATIC_DTABLE(DTable, HUF_MAX_TABLELOG);
    const BYTE* ip = (const BYTE*) cSrc;
    size_t errorCode;

    errorCode = HUF_readDTable (DTable, cSrc, cSrcSize);
    if (FSE_isError(errorCode)) return errorCode;
    if (errorCode >= cSrcSize) return (size_t)-FSE_ERROR_srcSize_wrong;
    ip += errorCode;
    cSrcSize -= errorCode;

    return HUF_decompress_usingDTable (dst, maxDstSize, ip, cSrcSize, DTable);
}


#endif   /* FSE_COMMONDEFS_ONLY */
