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
*  Generic function type & suffix (C template emulation)
****************************************************************/
#define FSE_FUNCTION_TYPE BYTE
#define FSE_FUNCTION_EXTENSION

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
static unsigned FSE_isLittleEndian(void)
{
    const union { U32 i; BYTE c[4]; } one = { 1 };   /* don't use static : performance detrimental  */
    return one.c[0];
}

static U32 FSE_read32(const void* memPtr)
{
    U32 val32;
    memcpy(&val32, memPtr, 4);
    return val32;
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

static U64 FSE_read64(const void* memPtr)
{
    U64 val64;
    memcpy(&val64, memPtr, 8);
    return val64;
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
        memcpy(memPtr, &val64, 8);
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
    if (sizeof(size_t)==4)
        return (size_t)FSE_readLE32(memPtr);
    else
        return (size_t)FSE_readLE64(memPtr);
}

static void FSE_writeLEST(void* memPtr, size_t val)
{
    if (sizeof(size_t)==4)
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
    int  deltaFindState;
    U16  maxState;
    BYTE minBitsOut;
    /* one byte padding */
} FSE_symbolCompressionTransform;

typedef struct
{
    U32 fakeTable[FSE_CTABLE_SIZE_U32(FSE_MAX_TABLELOG, FSE_MAX_SYMBOL_VALUE)];   /* compatible with FSE_compressU16() */
} CTable_max_t;


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
size_t FSE_headerBound(unsigned maxSymbolValue, unsigned tableLog)
{
    size_t maxHeaderSize = (((maxSymbolValue+1) * tableLog) >> 3) + 1;
    return maxSymbolValue ? maxHeaderSize : FSE_MAX_HEADERSIZE;
}

static size_t FSE_writeHeader_generic (void* header, size_t headerBufferSize,
                                       const short* normalizedCounter, unsigned maxSymbolValue, unsigned tableLog,
                                       unsigned safeWrite)
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
                bitStream += 0xFFFF<<bitCount;
                if ((!safeWrite) && (out > oend-2)) return (size_t)-FSE_ERROR_GENERIC;   /* Buffer overflow */
                out[0] = (BYTE)bitStream;
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
                if ((!safeWrite) && (out > oend - 2)) return (size_t)-FSE_ERROR_GENERIC;   /* Buffer overflow */
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
            if (remaining<0) return (size_t)-FSE_ERROR_GENERIC;
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
            if ((!safeWrite) && (out > oend - 2)) return (size_t)-FSE_ERROR_GENERIC;   /* Buffer overflow */
            out[0] = (BYTE)bitStream;
            out[1] = (BYTE)(bitStream>>8);
            out += 2;
            bitStream >>= 16;
            bitCount -= 16;
        }
    }

    /* flush remaining bitStream */
    if ((!safeWrite) && (out > oend - 2)) return (size_t)-FSE_ERROR_GENERIC;   /* Buffer overflow */
    out[0] = (BYTE)bitStream;
    out[1] = (BYTE)(bitStream>>8);
    out+= (bitCount+7) /8;

    if (charnum > maxSymbolValue + 1) return (size_t)-FSE_ERROR_GENERIC;   /* Too many symbols written (a bit too late?) */

    return (out-ostart);
}


size_t FSE_writeHeader (void* header, size_t headerBufferSize, const short* normalizedCounter, unsigned maxSymbolValue, unsigned tableLog)
{
    if (tableLog > FSE_MAX_TABLELOG) return (size_t)-FSE_ERROR_GENERIC;   /* Unsupported */
    if (tableLog < FSE_MIN_TABLELOG) return (size_t)-FSE_ERROR_GENERIC;   /* Unsupported */

    if (headerBufferSize < FSE_headerBound(maxSymbolValue, tableLog))
        return FSE_writeHeader_generic(header, headerBufferSize, normalizedCounter, maxSymbolValue, tableLog, 0);

    return FSE_writeHeader_generic(header, headerBufferSize, normalizedCounter, maxSymbolValue, tableLog, 1);
}


size_t FSE_readHeader (short* normalizedCounter, unsigned* maxSVPtr, unsigned* tableLogPtr,
                 const void* headerBuffer, size_t hbSize)
{
    const BYTE* const istart = (const BYTE*) headerBuffer;
    const BYTE* ip = istart;
    int nbBits;
    int remaining;
    int threshold;
    U32 bitStream;
    int bitCount;
    unsigned charnum = 0;
    int previous0 = 0;

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
                ip+=2;
                bitStream = FSE_readLE32(ip) >> bitCount;
            }
            while ((bitStream & 3) == 3)
            {
                n0+=3;
                bitStream>>=2;
                bitCount+=2;
            }
            n0 += bitStream & 3;
            bitCount += 2;
            if (n0 > *maxSVPtr) return (size_t)-FSE_ERROR_GENERIC;
            while (charnum < n0) normalizedCounter[charnum++] = 0;
            ip += bitCount>>3;
            bitCount &= 7;
            bitStream = FSE_readLE32(ip) >> bitCount;
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

            ip += bitCount>>3;
            bitCount &= 7;
            bitStream = FSE_readLE32(ip) >> bitCount;
        }
    }
    if (remaining != 1) return (size_t)-FSE_ERROR_GENERIC;
    *maxSVPtr = charnum-1;

    ip += bitCount>0;
    if ((size_t)(ip-istart) >= hbSize) return (size_t)-FSE_ERROR_srcSize_wrong;   /* arguably a bit late , tbd */
    return ip-istart;
}


/****************************************************************
*  FSE Compression Code
****************************************************************/
/*
CTable is a variable size structure which contains :
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

void* FSE_createCTable (unsigned maxSymbolValue, unsigned tableLog)
{
    size_t size;
    if (tableLog > FSE_TABLELOG_ABSOLUTE_MAX) tableLog = FSE_TABLELOG_ABSOLUTE_MAX;
    size = FSE_CTABLE_SIZE_U32 (tableLog, maxSymbolValue) * sizeof(U32);
    return malloc(size);
}

void  FSE_freeCTable (void* CTable)
{
    free(CTable);
}


unsigned FSE_optimalTableLog(unsigned maxTableLog, size_t srcSize, unsigned maxSymbolValue)
{
    U32 tableLog = maxTableLog;
    if (tableLog==0) tableLog = FSE_DEFAULT_TABLELOG;
    if ((FSE_highbit32((U32)(srcSize - 1)) - 2) < tableLog) tableLog = FSE_highbit32((U32)(srcSize - 1)) - 2;   /* Accuracy can be reduced */
    if ((FSE_highbit32(maxSymbolValue+1)+1) > tableLog) tableLog = FSE_highbit32(maxSymbolValue+1)+1;   /* Need a minimum to safely represent all symbol values */
    if (tableLog < FSE_MIN_TABLELOG) tableLog = FSE_MIN_TABLELOG;
    if (tableLog > FSE_MAX_TABLELOG) tableLog = FSE_MAX_TABLELOG;
    return tableLog;
}


typedef struct
{
    U32 id;
    U32 count;
} rank_t;

int FSE_compareRankT(const void* r1, const void* r2)
{
    const rank_t* R1 = (const rank_t*)r1;
    const rank_t* R2 = (const rank_t*)r2;

    return 2 * (R1->count < R2->count) - 1;
}


#if 0
static size_t FSE_adjustNormSlow(short* norm, int pointsToRemove, const unsigned* count, U32 maxSymbolValue)
{
    rank_t rank[FSE_MAX_SYMBOL_VALUE+2];
    U32 s;

    /* Init */
    for (s=0; s<=maxSymbolValue; s++)
    {
        rank[s].id = s;
        rank[s].count = count[s];
        if (norm[s] <= 1) rank[s].count = 0;
    }
    rank[maxSymbolValue+1].id = 0;
    rank[maxSymbolValue+1].count = 0;   /* ensures comparison ends here in worst case */

    /* Sort according to count */
    qsort(rank, maxSymbolValue+1, sizeof(rank_t), FSE_compareRankT);

    while(pointsToRemove)
    {
        int newRank = 1;
        rank_t savedR;
        if (norm[rank[0].id] == 1)
            return (size_t)-FSE_ERROR_GENERIC;
        norm[rank[0].id]--;
        pointsToRemove--;
        rank[0].count -= (rank[0].count + 6) >> 3;
        if (norm[rank[0].id] == 1)
            rank[0].count=0;
        savedR = rank[0];
        while (rank[newRank].count > savedR.count)
        {
            rank[newRank-1] = rank[newRank];
            newRank++;
        }
        rank[newRank-1] = savedR;
    }

    return 0;
}

#else

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
        norm[maxV] += ToDistribute;
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
                norm[s] = weight;
                tmpTotal = end;
            }
        }
    }

    return 0;
}
#endif


size_t FSE_normalizeCount (short* normalizedCounter, unsigned tableLog,
                           const unsigned* count, size_t total,
                           unsigned maxSymbolValue)
{
    /* Sanity checks */
    if (tableLog==0) tableLog = FSE_DEFAULT_TABLELOG;
    if (tableLog < FSE_MIN_TABLELOG) return (size_t)-FSE_ERROR_GENERIC;   /* Unsupported size */
    if (tableLog > FSE_MAX_TABLELOG) return (size_t)-FSE_ERROR_GENERIC;   /* Unsupported size */
    if ((1U<<tableLog) <= maxSymbolValue) return (size_t)-FSE_ERROR_GENERIC;   /* Too small tableLog, compression potentially impossible */

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


/* fake CTable, for raw (uncompressed) input */
size_t FSE_buildCTable_raw (void* CTable, unsigned nbBits)
{
    const unsigned tableSize = 1 << nbBits;
    const unsigned tableMask = tableSize - 1;
    const unsigned maxSymbolValue = tableMask;
    U16* tableU16 = ( (U16*) CTable) + 2;
    FSE_symbolCompressionTransform* symbolTT = (FSE_symbolCompressionTransform*) ((((U32*)CTable)+1) + (tableSize>>1));
    unsigned s;

    /* Sanity checks */
    if (nbBits < 1) return (size_t)-FSE_ERROR_GENERIC;             /* min size */
    if (((size_t)CTable) & 3) return (size_t)-FSE_ERROR_GENERIC;   /* Must be allocated of 4 bytes boundaries */

    /* header */
    tableU16[-2] = (U16) nbBits;
    tableU16[-1] = (U16) maxSymbolValue;

    /* Build table */
    for (s=0; s<tableSize; s++)
        tableU16[s] = (U16)(tableSize + s);

    /* Build Symbol Transformation Table */
    for (s=0; s<=maxSymbolValue; s++)
    {
        symbolTT[s].minBitsOut = (BYTE)nbBits;
        symbolTT[s].deltaFindState = s-1;
        symbolTT[s].maxState = (U16)( (tableSize*2) - 1);   /* ensures state <= maxState */
    }

    return 0;
}


/* fake CTable, for rle (100% always same symbol) input */
size_t FSE_buildCTable_rle (void* CTable, BYTE symbolValue)
{
    const unsigned tableSize = 1;
    U16* tableU16 = ( (U16*) CTable) + 2;
    FSE_symbolCompressionTransform* symbolTT = (FSE_symbolCompressionTransform*) ((U32*)CTable + 2);

    /* safety checks */
    if (((size_t)CTable) & 3) return (size_t)-FSE_ERROR_GENERIC;   /* Must be 4 bytes aligned */

    /* header */
    tableU16[-2] = (U16) 0;
    tableU16[-1] = (U16) symbolValue;

    /* Build table */
    tableU16[0] = 0;
    tableU16[1] = 0;   /* just in case */

    /* Build Symbol Transformation Table */
    {
        symbolTT[symbolValue].minBitsOut = 0;
        symbolTT[symbolValue].deltaFindState = 0;
        symbolTT[symbolValue].maxState = (U16)(2*tableSize-1);   /* ensures state <= maxState */
    }

    return 0;
}


void FSE_initCStream(FSE_CStream_t* bitC, void* start)
{
    bitC->bitContainer = 0;
    bitC->bitPos = 0;   /* reserved for unusedBits */
    bitC->startPtr = (char*)start;
    bitC->ptr = bitC->startPtr;
}

void FSE_initCState(FSE_CState_t* statePtr, const void* CTable)
{
    const U32 tableLog = ( (U16*) CTable) [0];
    statePtr->value = (ptrdiff_t)1<<tableLog;
    statePtr->stateTable = ((const U16*) CTable) + 2;
    statePtr->symbolTT = (const U32*)CTable + 1 + (tableLog ? (1<<(tableLog-1)) : 1);
    statePtr->stateLog = tableLog;
}

void FSE_addBits(FSE_CStream_t* bitC, size_t value, unsigned nbBits)
{
    static const unsigned mask[] = { 0, 1, 3, 7, 0xF, 0x1F, 0x3F, 0x7F, 0xFF, 0x1FF, 0x3FF, 0x7FF, 0xFFF, 0x1FFF, 0x3FFF, 0x7FFF, 0xFFFF, 0x1FFFF, 0x3FFFF, 0x7FFFF, 0xFFFFF, 0x1FFFFF, 0x3FFFFF, 0x7FFFFF,  0xFFFFFF, 0x1FFFFFF };   /* up to 25 bits */
    bitC->bitContainer |= (value & mask[nbBits]) << bitC->bitPos;
    bitC->bitPos += nbBits;
}

void FSE_encodeByte(FSE_CStream_t* bitC, FSE_CState_t* statePtr, BYTE symbol)
{
    const FSE_symbolCompressionTransform* const symbolTT = (const FSE_symbolCompressionTransform*) statePtr->symbolTT;
    const U16* const stateTable = (const U16*) statePtr->stateTable;
    int nbBitsOut  = symbolTT[symbol].minBitsOut;
    nbBitsOut -= (int)((symbolTT[symbol].maxState - statePtr->value) >> 31);
    FSE_addBits(bitC, statePtr->value, nbBitsOut);
    statePtr->value = stateTable[ (statePtr->value >> nbBitsOut) + symbolTT[symbol].deltaFindState];
}

void FSE_flushBits(FSE_CStream_t* bitC)
{
    size_t nbBytes = bitC->bitPos >> 3;
    FSE_writeLEST(bitC->ptr, bitC->bitContainer);
    bitC->bitPos &= 7;
    bitC->ptr += nbBytes;
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

    FSE_addBits(bitC, 1, 1);
    FSE_flushBits(bitC);

    endPtr = bitC->ptr;
    endPtr += bitC->bitPos > 0;

    return (endPtr - bitC->startPtr);
}


size_t FSE_compress_usingCTable (void* dst, size_t dstSize,
                           const void* src, size_t srcSize,
                           const void* CTable)
{
    const BYTE* const istart = (const BYTE*) src;
    const BYTE* ip;
    const BYTE* const iend = istart + srcSize;

    FSE_CStream_t bitC;
    FSE_CState_t CState1, CState2;


    /* init */
    (void)dstSize;   /* objective : ensure it fits into dstBuffer (Todo) */
    FSE_initCStream(&bitC, dst);
    FSE_initCState(&CState1, CTable);
    CState2 = CState1;

    ip=iend;

    /* join to even */
    if (srcSize & 1)
    {
        FSE_encodeByte(&bitC, &CState1, *--ip);
        FSE_flushBits(&bitC);
    }

    /* join to mod 4 */
    if ((sizeof(size_t)*8 > FSE_MAX_TABLELOG*4+7 ) && (srcSize & 2))   /* test bit 2 */
    {
        FSE_encodeByte(&bitC, &CState2, *--ip);
        FSE_encodeByte(&bitC, &CState1, *--ip);
        FSE_flushBits(&bitC);
    }

    /* 2 or 4 encoding per loop */
    while (ip>istart)
    {
        FSE_encodeByte(&bitC, &CState2, *--ip);

        if (sizeof(size_t)*8 < FSE_MAX_TABLELOG*2+7 )   /* this test must be static */
            FSE_flushBits(&bitC);

        FSE_encodeByte(&bitC, &CState1, *--ip);

        if (sizeof(size_t)*8 > FSE_MAX_TABLELOG*4+7 )   /* this test must be static */
        {
            FSE_encodeByte(&bitC, &CState2, *--ip);
            FSE_encodeByte(&bitC, &CState1, *--ip);
        }

        FSE_flushBits(&bitC);
    }

    FSE_flushCState(&bitC, &CState2);
    FSE_flushCState(&bitC, &CState1);
    return FSE_closeCStream(&bitC);
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
    CTable_max_t CTable;
    size_t errorCode;

    /* early out */
    if (dstSize < FSE_compressBound(srcSize)) return (size_t)-FSE_ERROR_dstSize_tooSmall;
    if (srcSize <= 1) return srcSize;  /* Uncompressed or RLE */
    if (!maxSymbolValue) maxSymbolValue = FSE_MAX_SYMBOL_VALUE;
    if (!tableLog) tableLog = FSE_DEFAULT_TABLELOG;

    /* Scan input and build symbol stats */
    errorCode = FSE_count (count, ip, srcSize, &maxSymbolValue);
    if (FSE_isError(errorCode)) return errorCode;
    if (errorCode == srcSize) return 1;
    if (errorCode < (srcSize >> 7)) return 0;   /* Heuristic : not compressible enough */

    tableLog = FSE_optimalTableLog(tableLog, srcSize, maxSymbolValue);
    errorCode = FSE_normalizeCount (norm, tableLog, count, srcSize, maxSymbolValue);
    if (FSE_isError(errorCode)) return errorCode;

    /* Write table description header */
    errorCode = FSE_writeHeader (op, FSE_MAX_HEADERSIZE, norm, maxSymbolValue, tableLog);
    if (FSE_isError(errorCode)) return errorCode;
    op += errorCode;

    /* Compress */
    errorCode = FSE_buildCTable (&CTable, norm, maxSymbolValue, tableLog);
    if (FSE_isError(errorCode)) return errorCode;
    op += FSE_compress_usingCTable(op, oend - op, ip, srcSize, &CTable);

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
typedef struct
{
    U16  newState;
    BYTE symbol;
    BYTE nbBits;
} FSE_decode_t;   /* size == U32 */

/* Specific corner case : RLE compression */
size_t FSE_decompressRLE(void* dst, size_t originalSize,
                   const void* cSrc, size_t cSrcSize)
{
    if (cSrcSize != 1) return (size_t)-FSE_ERROR_srcSize_wrong;
    memset(dst, *(BYTE*)cSrc, originalSize);
    return originalSize;
}


size_t FSE_buildDTable_rle (void* DTable, BYTE symbolValue)
{
    U32* const base32 = (U32*)DTable;
    FSE_decode_t* const cell = (FSE_decode_t*)(base32 + 1);

    /* Sanity check */
    if (((size_t)DTable) & 3) return (size_t)-FSE_ERROR_GENERIC;   /* Must be allocated of 4 bytes boundaries */

    base32[0] = 0;

    cell->newState = 0;
    cell->symbol = symbolValue;
    cell->nbBits = 0;

    return 0;
}


size_t FSE_buildDTable_raw (void* DTable, unsigned nbBits)
{
    U32* const base32 = (U32*)DTable;
    FSE_decode_t* dinfo = (FSE_decode_t*)(base32 + 1);
    const unsigned tableSize = 1 << nbBits;
    const unsigned tableMask = tableSize - 1;
    const unsigned maxSymbolValue = tableMask;
    unsigned s;

    /* Sanity checks */
    if (nbBits < 1) return (size_t)-FSE_ERROR_GENERIC;             /* min size */
    if (((size_t)DTable) & 3) return (size_t)-FSE_ERROR_GENERIC;   /* Must be allocated of 4 bytes boundaries */

    /* Build Decoding Table */
    base32[0] = nbBits;
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

    if (srcSize >=  sizeof(bitD_t))
    {
        U32 contain32;
        bitD->start = (char*)srcBuffer;
        bitD->ptr   = (char*)srcBuffer + srcSize - sizeof(bitD_t);
        bitD->bitContainer = FSE_readLEST(bitD->ptr);
        contain32 = ((BYTE*)srcBuffer)[srcSize-1];
        if (contain32 == 0) return (size_t)-FSE_ERROR_GENERIC;   /* stop bit not present */
        bitD->bitsConsumed = 8 - FSE_highbit32(contain32);
    }
    else
    {
        U32 contain32;
        bitD->start = (char*)srcBuffer;
        bitD->ptr   = bitD->start;
        bitD->bitContainer = *(BYTE*)(bitD->start);
        switch(srcSize)
        {
            case 7: bitD->bitContainer += (bitD_t)(((BYTE*)(bitD->start))[6]) << (sizeof(bitD_t)*8 - 16);
            case 6: bitD->bitContainer += (bitD_t)(((BYTE*)(bitD->start))[5]) << (sizeof(bitD_t)*8 - 24);
            case 5: bitD->bitContainer += (bitD_t)(((BYTE*)(bitD->start))[4]) << (sizeof(bitD_t)*8 - 32);
            case 4: bitD->bitContainer += (bitD_t)(((BYTE*)(bitD->start))[3]) << 24;
            case 3: bitD->bitContainer += (bitD_t)(((BYTE*)(bitD->start))[2]) << 16;
            case 2: bitD->bitContainer += (bitD_t)(((BYTE*)(bitD->start))[1]) <<  8;
            default:;
        }
        contain32 = ((BYTE*)srcBuffer)[srcSize-1];
        if (contain32 == 0) return (size_t)-FSE_ERROR_GENERIC;   /* stop bit not present */
        bitD->bitsConsumed = 8 - FSE_highbit32(contain32);
        bitD->bitsConsumed += (U32)(sizeof(bitD_t) - srcSize)*8;
    }

    return srcSize;
}


/* FSE_readBits
 * Read next n bits from the bitContainer.
 * Use the fast variant *only* if n > 0.
 * Note : for this function to work properly on 32-bits, don't read more than maxNbBits==25
 * return : value extracted.
 */
bitD_t FSE_readBits(FSE_DStream_t* bitD, U32 nbBits)
{
    bitD_t value = ((bitD->bitContainer << bitD->bitsConsumed) >> 1) >> (((sizeof(bitD_t)*8)-1)-nbBits);
    bitD->bitsConsumed += nbBits;
    return value;
}

bitD_t FSE_readBitsFast(FSE_DStream_t* bitD, U32 nbBits)   /* only if nbBits >= 1 */
{
    bitD_t value = (bitD->bitContainer << bitD->bitsConsumed) >> ((sizeof(bitD_t)*8)-nbBits);
    bitD->bitsConsumed += nbBits;
    return value;
}

unsigned FSE_reloadDStream(FSE_DStream_t* bitD)
{
    if (bitD->ptr >= bitD->start + sizeof(bitD_t))
    {
        bitD->ptr -= bitD->bitsConsumed >> 3;
        bitD->bitsConsumed &= 7;
        bitD->bitContainer = FSE_readLEST(bitD->ptr);
        return 0;
    }
    if (bitD->ptr == bitD->start)
    {
        if (bitD->bitsConsumed < sizeof(bitD_t)*8) return 1;
        if (bitD->bitsConsumed == sizeof(bitD_t)*8) return 2;
        return 3;
    }
    {
        U32 nbBytes = bitD->bitsConsumed >> 3;
        if (bitD->ptr - nbBytes < bitD->start)
            nbBytes = (U32)(bitD->ptr - bitD->start);  /* note : necessarily ptr > start */
        bitD->ptr -= nbBytes;
        bitD->bitsConsumed -= nbBytes*8;
        bitD->bitContainer = FSE_readLEST(bitD->ptr);   /* note : necessarily srcSize > sizeof(bitD) */
        return (bitD->ptr == bitD->start);
    }
}


void FSE_initDState(FSE_DState_t* DStatePtr, FSE_DStream_t* bitD, const void* DTable)
{
    const U32* const base32 = (const U32*)DTable;
    DStatePtr->state = FSE_readBits(bitD, base32[0]);
    FSE_reloadDStream(bitD);
    DStatePtr->table = base32 + 1;
}

BYTE FSE_decodeSymbol(FSE_DState_t* DStatePtr, FSE_DStream_t* bitD)
{
    const FSE_decode_t DInfo = ((const FSE_decode_t*)(DStatePtr->table))[DStatePtr->state];
    const U32  nbBits = DInfo.nbBits;
    BYTE symbol = DInfo.symbol;
    bitD_t lowBits = FSE_readBits(bitD, nbBits);

    DStatePtr->state = DInfo.newState + lowBits;
    return symbol;
}

BYTE FSE_decodeSymbolFast(FSE_DState_t* DStatePtr, FSE_DStream_t* bitD)
{
    const FSE_decode_t DInfo = ((const FSE_decode_t*)(DStatePtr->table))[DStatePtr->state];
    const U32 nbBits = DInfo.nbBits;
    BYTE symbol = DInfo.symbol;
    bitD_t lowBits = FSE_readBitsFast(bitD, nbBits);

    DStatePtr->state = DInfo.newState + lowBits;
    return symbol;
}

/* FSE_endOfDStream
   Tells if bitD has reached end of bitStream or not */

unsigned FSE_endOfDStream(const FSE_DStream_t* bitD)
{
    return FSE_reloadDStream((FSE_DStream_t*)bitD)==2;
}

unsigned FSE_endOfDState(const FSE_DState_t* statePtr)
{
    return statePtr->state == 0;
}


FORCE_INLINE size_t FSE_decompress_usingDTable_generic(
          void* dst, size_t maxDstSize,
    const void* cSrc, size_t cSrcSize,
    const void* DTable, unsigned fast)
{
    BYTE* const ostart = (BYTE*) dst;
    BYTE* op = ostart;
    BYTE* const omax = op + maxDstSize;
    BYTE* const olimit = omax-3;

    FSE_DStream_t bitD;
    FSE_DState_t state1, state2;
    size_t errorCode;

    /* Init */
    errorCode = FSE_initDStream(&bitD, cSrc, cSrcSize);   /* replaced last arg by maxCompressed Size */
    if (FSE_isError(errorCode)) return errorCode;

    FSE_initDState(&state1, &bitD, DTable);
    FSE_initDState(&state2, &bitD, DTable);


    /* 2 symbols per loop */
    while (!FSE_reloadDStream(&bitD) && (op<olimit))
    {
        *op++ = fast ? FSE_decodeSymbolFast(&state1, &bitD) : FSE_decodeSymbol(&state1, &bitD);

        if (FSE_MAX_TABLELOG*2+7 > sizeof(bitD_t)*8)    /* This test must be static */
            FSE_reloadDStream(&bitD);

        *op++ = fast ? FSE_decodeSymbolFast(&state2, &bitD) : FSE_decodeSymbol(&state2, &bitD);

        if (FSE_MAX_TABLELOG*4+7 < sizeof(bitD_t)*8)    /* This test must be static */
        {
            *op++ = fast ? FSE_decodeSymbolFast(&state1, &bitD) : FSE_decodeSymbol(&state1, &bitD);
            *op++ = fast ? FSE_decodeSymbolFast(&state2, &bitD) : FSE_decodeSymbol(&state2, &bitD);
        }
    }

    /* tail */
    while (1)
    {
        if ( (FSE_reloadDStream(&bitD)>2) || (op==omax) || (FSE_endOfDState(&state1) && FSE_endOfDStream(&bitD)) )
            break;

        *op++ = fast ? FSE_decodeSymbolFast(&state1, &bitD) : FSE_decodeSymbol(&state1, &bitD);

        if ( (FSE_reloadDStream(&bitD)>2) || (op==omax) || (FSE_endOfDState(&state2) && FSE_endOfDStream(&bitD)) )
            break;

        *op++ = fast ? FSE_decodeSymbolFast(&state2, &bitD) : FSE_decodeSymbol(&state2, &bitD);
    }

    /* end ? */
    if (FSE_endOfDStream(&bitD) && FSE_endOfDState(&state1) && FSE_endOfDState(&state2) )
        return op-ostart;

    if (op==omax) return (size_t)-FSE_ERROR_dstSize_tooSmall;   /* dst buffer is full, but cSrc unfinished */

    return (size_t)-FSE_ERROR_corruptionDetected;
}


size_t FSE_decompress_usingDTable(void* dst, size_t originalSize,
                            const void* cSrc, size_t cSrcSize,
                            const void* DTable, size_t fastMode)
{
    /* select fast mode (static) */
    if (fastMode) return FSE_decompress_usingDTable_generic(dst, originalSize, cSrc, cSrcSize, DTable, 1);
    return FSE_decompress_usingDTable_generic(dst, originalSize, cSrc, cSrcSize, DTable, 0);
}


size_t FSE_decompress(void* dst, size_t maxDstSize, const void* cSrc, size_t cSrcSize)
{
    const BYTE* const istart = (const BYTE*)cSrc;
    const BYTE* ip = istart;
    short counting[FSE_MAX_SYMBOL_VALUE+1];
    FSE_decode_t DTable[FSE_DTABLE_SIZE_U32(FSE_MAX_TABLELOG)];
    unsigned maxSymbolValue = FSE_MAX_SYMBOL_VALUE;
    unsigned tableLog;
    size_t errorCode, fastMode;

    if (cSrcSize<2) return (size_t)-FSE_ERROR_srcSize_wrong;   /* too small input size */

    /* normal FSE decoding mode */
    errorCode = FSE_readHeader (counting, &maxSymbolValue, &tableLog, istart, cSrcSize);
    if (FSE_isError(errorCode)) return errorCode;
    if (errorCode >= cSrcSize) return (size_t)-FSE_ERROR_srcSize_wrong;   /* too small input size */
    ip += errorCode;
    cSrcSize -= errorCode;

    fastMode = FSE_buildDTable (DTable, counting, maxSymbolValue, tableLog);
    if (FSE_isError(fastMode)) return fastMode;

    /* always return, even if it is an error code */
    return FSE_decompress_usingDTable (dst, maxDstSize, ip, cSrcSize, DTable, fastMode);
}


#endif   /* FSE_COMMONDEFS_ONLY */

/*
  2nd part of the file
  designed to be included
  for type-specific functions (template equivalent in C)
  Objective is to write such functions only once, for better maintenance
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
size_t FSE_FUNCTION_NAME(FSE_count_generic, FSE_FUNCTION_EXTENSION) (unsigned* count, const FSE_FUNCTION_TYPE* source, size_t sourceSize, unsigned* maxSymbolValuePtr, unsigned safe)
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
    return (int)max;
}

/* hidden fast variant (unsafe) */
size_t FSE_FUNCTION_NAME(FSE_countFast, FSE_FUNCTION_EXTENSION) (unsigned* count, const FSE_FUNCTION_TYPE* source, size_t sourceSize, unsigned* maxSymbolValuePtr)
{
    return FSE_FUNCTION_NAME(FSE_count_generic, FSE_FUNCTION_EXTENSION) (count, source, sourceSize, maxSymbolValuePtr, 0);
}

size_t FSE_FUNCTION_NAME(FSE_count, FSE_FUNCTION_EXTENSION) (unsigned* count, const FSE_FUNCTION_TYPE* source, size_t sourceSize, unsigned* maxSymbolValuePtr)
{
    if ((sizeof(FSE_FUNCTION_TYPE)==1) && (*maxSymbolValuePtr >= 255))
    {
        *maxSymbolValuePtr = 255;
        return FSE_FUNCTION_NAME(FSE_count_generic, FSE_FUNCTION_EXTENSION) (count, source, sourceSize, maxSymbolValuePtr, 0);
    }
    return FSE_FUNCTION_NAME(FSE_count_generic, FSE_FUNCTION_EXTENSION) (count, source, sourceSize, maxSymbolValuePtr, 1);
}


static U32 FSE_tableStep(U32 tableSize) { return (tableSize>>1) + (tableSize>>3) + 3; }

size_t FSE_FUNCTION_NAME(FSE_buildCTable, FSE_FUNCTION_EXTENSION)
(void* CTable, const short* normalizedCounter, unsigned maxSymbolValue, unsigned tableLog)
{
    const unsigned tableSize = 1 << tableLog;
    const unsigned tableMask = tableSize - 1;
    U16* tableU16 = ( (U16*) CTable) + 2;
    FSE_symbolCompressionTransform* symbolTT = (FSE_symbolCompressionTransform*) (((U32*)CTable) + 1 + (tableLog ? tableSize>>1 : 1) );
    const unsigned step = FSE_tableStep(tableSize);
    unsigned cumul[FSE_MAX_SYMBOL_VALUE+2];
    U32 position = 0;
    FSE_FUNCTION_TYPE tableSymbol[FSE_MAX_TABLESIZE];
    U32 highThreshold = tableSize-1;
    unsigned symbol;
    unsigned i;

    /* safety checks */
    if (((size_t)CTable) & 3) return (size_t)-FSE_ERROR_GENERIC;   /* Must be allocated of 4 bytes boundaries */

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
        FSE_FUNCTION_TYPE s = tableSymbol[i];
        tableU16[cumul[s]++] = (U16) (tableSize+i);   // Table U16 : sorted by symbol order; gives next state value
    }

    // Build Symbol Transformation Table
    {
        unsigned s;
        unsigned total = 0;
        for (s=0; s<=maxSymbolValue; s++)
        {
            switch (normalizedCounter[s])
            {
            case 0:
                break;
            case -1:
            case 1:
                symbolTT[s].minBitsOut = (BYTE)tableLog;
                symbolTT[s].deltaFindState = total - 1;
                total ++;
                symbolTT[s].maxState = (U16)( (tableSize*2) - 1);   /* ensures state <= maxState */
                break;
            default :
                symbolTT[s].minBitsOut = (BYTE)( (tableLog-1) - FSE_highbit32 (normalizedCounter[s]-1) );
                symbolTT[s].deltaFindState = total - normalizedCounter[s];
                total +=  normalizedCounter[s];
                symbolTT[s].maxState = (U16)( (normalizedCounter[s] << (symbolTT[s].minBitsOut+1)) - 1);
            }
        }
    }

    return 0;
}


#define FSE_DECODE_TYPE FSE_TYPE_NAME(FSE_decode_t, FSE_FUNCTION_EXTENSION)

void* FSE_FUNCTION_NAME(FSE_createDTable, FSE_FUNCTION_EXTENSION) (unsigned tableLog)
{
    if (tableLog > FSE_TABLELOG_ABSOLUTE_MAX) tableLog = FSE_TABLELOG_ABSOLUTE_MAX;
    return malloc( ((size_t)1<<tableLog) * sizeof (FSE_DECODE_TYPE) );
}

void FSE_FUNCTION_NAME(FSE_freeDTable, FSE_FUNCTION_EXTENSION) (void* DTable)
{
    free(DTable);
}


size_t FSE_FUNCTION_NAME(FSE_buildDTable, FSE_FUNCTION_EXTENSION)
(void* DTable, const short* const normalizedCounter, unsigned maxSymbolValue, unsigned tableLog)
{
    U32* const base32 = (U32*)DTable;
    FSE_DECODE_TYPE* const tableDecode = (FSE_DECODE_TYPE*) (base32+1);
    const U32 tableSize = 1 << tableLog;
    const U32 tableMask = tableSize-1;
    const U32 step = FSE_tableStep(tableSize);
    U16 symbolNext[FSE_MAX_SYMBOL_VALUE+1];
    U32 position = 0;
    U32 highThreshold = tableSize-1;
    const S16 largeLimit= 1 << (tableLog-1);
    U32 noLarge = 1;
    U32 s;

    /* Sanity Checks */
    if (maxSymbolValue > FSE_MAX_SYMBOL_VALUE) return (size_t)-FSE_ERROR_maxSymbolValue_tooLarge;
    if (tableLog > FSE_MAX_TABLELOG) return (size_t)-FSE_ERROR_tableLog_tooLarge;

    /* Init, lay down lowprob symbols */
    base32[0] = tableLog;
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
            FSE_FUNCTION_TYPE symbol = tableDecode[i].symbol;
            U16 nextState = symbolNext[symbol]++;
            tableDecode[i].nbBits = (BYTE) (tableLog - FSE_highbit32 ((U32)nextState) );
            tableDecode[i].newState = (U16) ( (nextState << tableDecode[i].nbBits) - tableSize);
        }
    }

    return noLarge;
}
