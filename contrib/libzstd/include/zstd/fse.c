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

/* **************************************************************
*  Tuning parameters
****************************************************************/
/*!MEMORY_USAGE :
*  Memory usage formula : N->2^N Bytes (examples : 10 -> 1KB; 12 -> 4KB ; 16 -> 64KB; 20 -> 1MB; etc.)
*  Increasing memory usage improves compression ratio
*  Reduced memory usage can improve speed, due to cache effect
*  Recommended max value is 14, for 16KB, which nicely fits into Intel x86 L1 cache */
#define FSE_MAX_MEMORY_USAGE 14
#define FSE_DEFAULT_MEMORY_USAGE 13

/*!FSE_MAX_SYMBOL_VALUE :
*  Maximum symbol value authorized.
*  Required for proper stack allocation */
#define FSE_MAX_SYMBOL_VALUE 255


/* **************************************************************
*  template functions type & suffix
****************************************************************/
#define FSE_FUNCTION_TYPE BYTE
#define FSE_FUNCTION_EXTENSION
#define FSE_DECODE_TYPE FSE_decode_t


#endif   /* !FSE_COMMONDEFS_ONLY */

/* **************************************************************
*  Compiler specifics
****************************************************************/
#ifdef _MSC_VER    /* Visual Studio */
#  define FORCE_INLINE static __forceinline
#  include <intrin.h>                    /* For Visual 2005 */
#  pragma warning(disable : 4127)        /* disable: C4127: conditional expression is constant */
#  pragma warning(disable : 4214)        /* disable: C4214: non-int bitfields */
#else
#  ifdef __GNUC__
#    define GCC_VERSION (__GNUC__ * 100 + __GNUC_MINOR__)
#    define FORCE_INLINE static inline __attribute__((always_inline))
#  else
#    define FORCE_INLINE static inline
#  endif
#endif


/* **************************************************************
*  Includes
****************************************************************/
#include <stdlib.h>     /* malloc, free, qsort */
#include <string.h>     /* memcpy, memset */
#include <stdio.h>      /* printf (debug) */
#include "bitstream.h"
#include "fse_static.h"


/* ***************************************************************
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


/* **************************************************************
*  Error Management
****************************************************************/
#define FSE_STATIC_ASSERT(c) { enum { FSE_static_assert = 1/(int)(!!(c)) }; }   /* use only *after* variable declarations */


/* **************************************************************
*  Complex types
****************************************************************/
typedef U32 CTable_max_t[FSE_CTABLE_SIZE_U32(FSE_MAX_TABLELOG, FSE_MAX_SYMBOL_VALUE)];
typedef U32 DTable_max_t[FSE_DTABLE_SIZE_U32(FSE_MAX_TABLELOG)];


/* **************************************************************
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
static U32 FSE_tableStep(U32 tableSize) { return (tableSize>>1) + (tableSize>>3) + 3; }

size_t FSE_buildCTable(FSE_CTable* ct, const short* normalizedCounter, unsigned maxSymbolValue, unsigned tableLog)
{
    const unsigned tableSize = 1 << tableLog;
    const unsigned tableMask = tableSize - 1;
    void* const ptr = ct;
    U16* const tableU16 = ( (U16*) ptr) + 2;
    void* const FSCT = ((U32*)ptr) + 1 /* header */ + (tableLog ? tableSize>>1 : 1) ;
    FSE_symbolCompressionTransform* const symbolTT = (FSE_symbolCompressionTransform*) (FSCT);
    const unsigned step = FSE_tableStep(tableSize);
    unsigned cumul[FSE_MAX_SYMBOL_VALUE+2];
    U32 position = 0;
    FSE_FUNCTION_TYPE tableSymbol[FSE_MAX_TABLESIZE]; /* memset() is not necessary, even if static analyzer complain about it */
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
    for (i=1; i<=maxSymbolValue+1; i++) {
        if (normalizedCounter[i-1]==-1) {  /* Low proba symbol */
            cumul[i] = cumul[i-1] + 1;
            tableSymbol[highThreshold--] = (FSE_FUNCTION_TYPE)(i-1);
        } else {
            cumul[i] = cumul[i-1] + normalizedCounter[i-1];
    }   }
    cumul[maxSymbolValue+1] = tableSize+1;

    /* Spread symbols */
    for (symbol=0; symbol<=maxSymbolValue; symbol++) {
        int nbOccurences;
        for (nbOccurences=0; nbOccurences<normalizedCounter[symbol]; nbOccurences++) {
            tableSymbol[position] = (FSE_FUNCTION_TYPE)symbol;
            position = (position + step) & tableMask;
            while (position > highThreshold) position = (position + step) & tableMask;   /* Low proba area */
    }   }

    if (position!=0) return ERROR(GENERIC);   /* Must have gone through all positions */

    /* Build table */
    for (i=0; i<tableSize; i++) {
        FSE_FUNCTION_TYPE s = tableSymbol[i];   /* note : static analyzer may not understand tableSymbol is properly initialized */
        tableU16[cumul[s]++] = (U16) (tableSize+i);   /* TableU16 : sorted by symbol order; gives next state value */
    }

    /* Build Symbol Transformation Table */
    {
        unsigned s;
        unsigned total = 0;
        for (s=0; s<=maxSymbolValue; s++) {
            switch (normalizedCounter[s])
            {
            case  0:
                break;
            case -1:
            case  1:
                symbolTT[s].deltaNbBits = (tableLog << 16) - (1<<tableLog);
                symbolTT[s].deltaFindState = total - 1;
                total ++;
                break;
            default :
                {
                    U32 maxBitsOut = tableLog - BIT_highbit32 (normalizedCounter[s]-1);
                    U32 minStatePlus = normalizedCounter[s] << maxBitsOut;
                    symbolTT[s].deltaNbBits = (maxBitsOut << 16) - minStatePlus;
                    symbolTT[s].deltaFindState = total - normalizedCounter[s];
                    total +=  normalizedCounter[s];
    }   }   }   }

    return 0;
}


FSE_DTable* FSE_createDTable (unsigned tableLog)
{
    if (tableLog > FSE_TABLELOG_ABSOLUTE_MAX) tableLog = FSE_TABLELOG_ABSOLUTE_MAX;
    return (FSE_DTable*)malloc( FSE_DTABLE_SIZE_U32(tableLog) * sizeof (U32) );
}

void FSE_freeDTable (FSE_DTable* dt)
{
    free(dt);
}

size_t FSE_buildDTable(FSE_DTable* dt, const short* normalizedCounter, unsigned maxSymbolValue, unsigned tableLog)
{
    FSE_DTableHeader DTableH;
    void* const tdPtr = dt+1;   /* because dt is unsigned, 32-bits aligned on 32-bits */
    FSE_DECODE_TYPE* const tableDecode = (FSE_DECODE_TYPE*) (tdPtr);
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
    if (maxSymbolValue > FSE_MAX_SYMBOL_VALUE) return ERROR(maxSymbolValue_tooLarge);
    if (tableLog > FSE_MAX_TABLELOG) return ERROR(tableLog_tooLarge);

    /* Init, lay down lowprob symbols */
    DTableH.tableLog = (U16)tableLog;
    for (s=0; s<=maxSymbolValue; s++) {
        if (normalizedCounter[s]==-1) {
            tableDecode[highThreshold--].symbol = (FSE_FUNCTION_TYPE)s;
            symbolNext[s] = 1;
        } else {
            if (normalizedCounter[s] >= largeLimit) noLarge=0;
            symbolNext[s] = normalizedCounter[s];
    }   }

    /* Spread symbols */
    for (s=0; s<=maxSymbolValue; s++) {
        int i;
        for (i=0; i<normalizedCounter[s]; i++) {
            tableDecode[position].symbol = (FSE_FUNCTION_TYPE)s;
            position = (position + step) & tableMask;
            while (position > highThreshold) position = (position + step) & tableMask;   /* lowprob area */
    }   }

    if (position!=0) return ERROR(GENERIC);   /* position must reach all cells once, otherwise normalizedCounter is incorrect */

    /* Build Decoding table */
    {
        U32 i;
        for (i=0; i<tableSize; i++) {
            FSE_FUNCTION_TYPE symbol = (FSE_FUNCTION_TYPE)(tableDecode[i].symbol);
            U16 nextState = symbolNext[symbol]++;
            tableDecode[i].nbBits = (BYTE) (tableLog - BIT_highbit32 ((U32)nextState) );
            tableDecode[i].newState = (U16) ( (nextState << tableDecode[i].nbBits) - tableSize);
    }   }

    DTableH.fastMode = (U16)noLarge;
    memcpy(dt, &DTableH, sizeof(DTableH));
    return 0;
}


#ifndef FSE_COMMONDEFS_ONLY
/*-****************************************
*  FSE helper functions
******************************************/
unsigned FSE_isError(size_t code) { return ERR_isError(code); }

const char* FSE_getErrorName(size_t code) { return ERR_getErrorName(code); }


/*-**************************************************************
*  FSE NCount encoding-decoding
****************************************************************/
size_t FSE_NCountWriteBound(unsigned maxSymbolValue, unsigned tableLog)
{
    size_t maxHeaderSize = (((maxSymbolValue+1) * tableLog) >> 3) + 3;
    return maxSymbolValue ? maxHeaderSize : FSE_NCOUNTBOUND;  /* maxSymbolValue==0 ? use default */
}

static short FSE_abs(short a) { return a<0 ? -a : a; }

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

    while (remaining>1) {  /* stops at 1 */
        if (previous0) {
            unsigned start = charnum;
            while (!normalizedCounter[charnum]) charnum++;
            while (charnum >= start+24) {
                start+=24;
                bitStream += 0xFFFFU << bitCount;
                if ((!writeIsSafe) && (out > oend-2)) return ERROR(dstSize_tooSmall);   /* Buffer overflow */
                out[0] = (BYTE) bitStream;
                out[1] = (BYTE)(bitStream>>8);
                out+=2;
                bitStream>>=16;
            }
            while (charnum >= start+3) {
                start+=3;
                bitStream += 3 << bitCount;
                bitCount += 2;
            }
            bitStream += (charnum-start) << bitCount;
            bitCount += 2;
            if (bitCount>16) {
                if ((!writeIsSafe) && (out > oend - 2)) return ERROR(dstSize_tooSmall);   /* Buffer overflow */
                out[0] = (BYTE)bitStream;
                out[1] = (BYTE)(bitStream>>8);
                out += 2;
                bitStream >>= 16;
                bitCount -= 16;
        }   }
        {
            short count = normalizedCounter[charnum++];
            const short max = (short)((2*threshold-1)-remaining);
            remaining -= FSE_abs(count);
            if (remaining<1) return ERROR(GENERIC);
            count++;   /* +1 for extra accuracy */
            if (count>=threshold) count += max;   /* [0..max[ [max..threshold[ (...) [threshold+max 2*threshold[ */
            bitStream += count << bitCount;
            bitCount  += nbBits;
            bitCount  -= (count<max);
            previous0 = (count==1);
            while (remaining<threshold) nbBits--, threshold>>=1;
        }
        if (bitCount>16) {
            if ((!writeIsSafe) && (out > oend - 2)) return ERROR(dstSize_tooSmall);   /* Buffer overflow */
            out[0] = (BYTE)bitStream;
            out[1] = (BYTE)(bitStream>>8);
            out += 2;
            bitStream >>= 16;
            bitCount -= 16;
    }   }

    /* flush remaining bitStream */
    if ((!writeIsSafe) && (out > oend - 2)) return ERROR(dstSize_tooSmall);   /* Buffer overflow */
    out[0] = (BYTE)bitStream;
    out[1] = (BYTE)(bitStream>>8);
    out+= (bitCount+7) /8;

    if (charnum > maxSymbolValue + 1) return ERROR(GENERIC);

    return (out-ostart);
}


size_t FSE_writeNCount (void* buffer, size_t bufferSize, const short* normalizedCounter, unsigned maxSymbolValue, unsigned tableLog)
{
    if (tableLog > FSE_MAX_TABLELOG) return ERROR(GENERIC);   /* Unsupported */
    if (tableLog < FSE_MIN_TABLELOG) return ERROR(GENERIC);   /* Unsupported */

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

    if (hbSize < 4) return ERROR(srcSize_wrong);
    bitStream = MEM_readLE32(ip);
    nbBits = (bitStream & 0xF) + FSE_MIN_TABLELOG;   /* extract tableLog */
    if (nbBits > FSE_TABLELOG_ABSOLUTE_MAX) return ERROR(tableLog_tooLarge);
    bitStream >>= 4;
    bitCount = 4;
    *tableLogPtr = nbBits;
    remaining = (1<<nbBits)+1;
    threshold = 1<<nbBits;
    nbBits++;

    while ((remaining>1) && (charnum<=*maxSVPtr)) {
        if (previous0) {
            unsigned n0 = charnum;
            while ((bitStream & 0xFFFF) == 0xFFFF) {
                n0+=24;
                if (ip < iend-5) {
                    ip+=2;
                    bitStream = MEM_readLE32(ip) >> bitCount;
                } else {
                    bitStream >>= 16;
                    bitCount+=16;
            }   }
            while ((bitStream & 3) == 3) {
                n0+=3;
                bitStream>>=2;
                bitCount+=2;
            }
            n0 += bitStream & 3;
            bitCount += 2;
            if (n0 > *maxSVPtr) return ERROR(maxSymbolValue_tooSmall);
            while (charnum < n0) normalizedCounter[charnum++] = 0;
            if ((ip <= iend-7) || (ip + (bitCount>>3) <= iend-4)) {
                ip += bitCount>>3;
                bitCount &= 7;
                bitStream = MEM_readLE32(ip) >> bitCount;
            }
            else
                bitStream >>= 2;
        }
        {
            const short max = (short)((2*threshold-1)-remaining);
            short count;

            if ((bitStream & (threshold-1)) < (U32)max) {
                count = (short)(bitStream & (threshold-1));
                bitCount   += nbBits-1;
            } else {
                count = (short)(bitStream & (2*threshold-1));
                if (count >= threshold) count -= max;
                bitCount   += nbBits;
            }

            count--;   /* extra accuracy */
            remaining -= FSE_abs(count);
            normalizedCounter[charnum++] = count;
            previous0 = !count;
            while (remaining < threshold) {
                nbBits--;
                threshold >>= 1;
            }

            if ((ip <= iend-7) || (ip + (bitCount>>3) <= iend-4)) {
                ip += bitCount>>3;
                bitCount &= 7;
            } else {
                bitCount -= (int)(8 * (iend - 4 - ip));
                ip = iend - 4;
            }
            bitStream = MEM_readLE32(ip) >> (bitCount & 31);
    }   }
    if (remaining != 1) return ERROR(GENERIC);
    *maxSVPtr = charnum-1;

    ip += (bitCount+7)>>3;
    if ((size_t)(ip-istart) > hbSize) return ERROR(srcSize_wrong);
    return ip-istart;
}


/*-**************************************************************
*  Counting histogram
****************************************************************/
/*! FSE_count_simple
    This function just counts byte values within @src,
    and store the histogram into @count.
    This function is unsafe : it doesn't check that all values within @src can fit into @count.
    For this reason, prefer using a table @count with 256 elements.
    @return : highest count for a single element
*/
static size_t FSE_count_simple(unsigned* count, unsigned* maxSymbolValuePtr,
                               const void* src, size_t srcSize)
{
    const BYTE* ip = (const BYTE*)src;
    const BYTE* const end = ip + srcSize;
    unsigned maxSymbolValue = *maxSymbolValuePtr;
    unsigned max=0;
    U32 s;

    memset(count, 0, (maxSymbolValue+1)*sizeof(*count));
    if (srcSize==0) { *maxSymbolValuePtr = 0; return 0; }

    while (ip<end) count[*ip++]++;

    while (!count[maxSymbolValue]) maxSymbolValue--;
    *maxSymbolValuePtr = maxSymbolValue;

    for (s=0; s<=maxSymbolValue; s++) if (count[s] > max) max = count[s];

    return (size_t)max;
}


static size_t FSE_count_parallel(unsigned* count, unsigned* maxSymbolValuePtr,
                                const void* source, size_t sourceSize,
                                unsigned checkMax)
{
    const BYTE* ip = (const BYTE*)source;
    const BYTE* const iend = ip+sourceSize;
    unsigned maxSymbolValue = *maxSymbolValuePtr;
    unsigned max=0;
    U32 s;

    U32 Counting1[256] = { 0 };
    U32 Counting2[256] = { 0 };
    U32 Counting3[256] = { 0 };
    U32 Counting4[256] = { 0 };

    /* safety checks */
    if (!sourceSize) {
        memset(count, 0, maxSymbolValue + 1);
        *maxSymbolValuePtr = 0;
        return 0;
    }
    if (!maxSymbolValue) maxSymbolValue = 255;            /* 0 == default */

    {   /* by stripes of 16 bytes */
        U32 cached = MEM_read32(ip); ip += 4;
        while (ip < iend-15) {
            U32 c = cached; cached = MEM_read32(ip); ip += 4;
            Counting1[(BYTE) c     ]++;
            Counting2[(BYTE)(c>>8) ]++;
            Counting3[(BYTE)(c>>16)]++;
            Counting4[       c>>24 ]++;
            c = cached; cached = MEM_read32(ip); ip += 4;
            Counting1[(BYTE) c     ]++;
            Counting2[(BYTE)(c>>8) ]++;
            Counting3[(BYTE)(c>>16)]++;
            Counting4[       c>>24 ]++;
            c = cached; cached = MEM_read32(ip); ip += 4;
            Counting1[(BYTE) c     ]++;
            Counting2[(BYTE)(c>>8) ]++;
            Counting3[(BYTE)(c>>16)]++;
            Counting4[       c>>24 ]++;
            c = cached; cached = MEM_read32(ip); ip += 4;
            Counting1[(BYTE) c     ]++;
            Counting2[(BYTE)(c>>8) ]++;
            Counting3[(BYTE)(c>>16)]++;
            Counting4[       c>>24 ]++;
        }
        ip-=4;
    }

    /* finish last symbols */
    while (ip<iend) Counting1[*ip++]++;

    if (checkMax) {   /* verify stats will fit into destination table */
        for (s=255; s>maxSymbolValue; s--) {
            Counting1[s] += Counting2[s] + Counting3[s] + Counting4[s];
            if (Counting1[s]) return ERROR(maxSymbolValue_tooSmall);
    }   }

    for (s=0; s<=maxSymbolValue; s++) {
        count[s] = Counting1[s] + Counting2[s] + Counting3[s] + Counting4[s];
        if (count[s] > max) max = count[s];
    }

    while (!count[maxSymbolValue]) maxSymbolValue--;
    *maxSymbolValuePtr = maxSymbolValue;
    return (size_t)max;
}

/* fast variant (unsafe : won't check if src contains values beyond count[] limit) */
size_t FSE_countFast(unsigned* count, unsigned* maxSymbolValuePtr,
                     const void* source, size_t sourceSize)
{
    if (sourceSize < 1500) return FSE_count_simple(count, maxSymbolValuePtr, source, sourceSize);
    return FSE_count_parallel(count, maxSymbolValuePtr, source, sourceSize, 0);
}

size_t FSE_count(unsigned* count, unsigned* maxSymbolValuePtr,
                 const void* source, size_t sourceSize)
{
    if (*maxSymbolValuePtr <255)
        return FSE_count_parallel(count, maxSymbolValuePtr, source, sourceSize, 1);
    *maxSymbolValuePtr = 255;
    return FSE_countFast(count, maxSymbolValuePtr, source, sourceSize);
}


/*-**************************************************************
*  FSE Compression Code
****************************************************************/
/*!
FSE_CTable is a variable size structure which contains :
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
    if (tableLog > FSE_MAX_TABLELOG) return ERROR(GENERIC);
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
	U32 minBitsSrc = BIT_highbit32((U32)(srcSize - 1)) + 1;
	U32 minBitsSymbols = BIT_highbit32(maxSymbolValue) + 2;
	U32 minBits = minBitsSrc < minBitsSymbols ? minBitsSrc : minBitsSymbols;
	return minBits;
}

unsigned FSE_optimalTableLog(unsigned maxTableLog, size_t srcSize, unsigned maxSymbolValue)
{
	U32 maxBitsSrc = BIT_highbit32((U32)(srcSize - 1)) - 2;
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

    for (s=0; s<=maxSymbolValue; s++) {
        if (count[s] == 0) {
            norm[s]=0;
            continue;
        }
        if (count[s] <= lowThreshold) {
            norm[s] = -1;
            distributed++;
            total -= count[s];
            continue;
        }
        if (count[s] <= lowOne) {
            norm[s] = 1;
            distributed++;
            total -= count[s];
            continue;
        }
        norm[s]=-2;
    }
    ToDistribute = (1 << tableLog) - distributed;

    if ((total / ToDistribute) > lowOne) {
        /* risk of rounding to zero */
        lowOne = (U32)((total * 3) / (ToDistribute * 2));
        for (s=0; s<=maxSymbolValue; s++) {
            if ((norm[s] == -2) && (count[s] <= lowOne)) {
                norm[s] = 1;
                distributed++;
                total -= count[s];
                continue;
        }   }
        ToDistribute = (1 << tableLog) - distributed;
    }

    if (distributed == maxSymbolValue+1) {
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
        for (s=0; s<=maxSymbolValue; s++) {
            if (norm[s]==-2) {
                U64 end = tmpTotal + (count[s] * rStep);
                U32 sStart = (U32)(tmpTotal >> vStepLog);
                U32 sEnd = (U32)(end >> vStepLog);
                U32 weight = sEnd - sStart;
                if (weight < 1)
                    return ERROR(GENERIC);
                norm[s] = (short)weight;
                tmpTotal = end;
    }   }   }

    return 0;
}


size_t FSE_normalizeCount (short* normalizedCounter, unsigned tableLog,
                           const unsigned* count, size_t total,
                           unsigned maxSymbolValue)
{
    /* Sanity checks */
    if (tableLog==0) tableLog = FSE_DEFAULT_TABLELOG;
    if (tableLog < FSE_MIN_TABLELOG) return ERROR(GENERIC);   /* Unsupported size */
    if (tableLog > FSE_MAX_TABLELOG) return ERROR(tableLog_tooLarge);   /* Unsupported size */
    if (tableLog < FSE_minTableLog(total, maxSymbolValue)) return ERROR(GENERIC);   /* Too small tableLog, compression potentially impossible */

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

        for (s=0; s<=maxSymbolValue; s++) {
            if (count[s] == total) return 0;   /* rle special case */
            if (count[s] == 0) { normalizedCounter[s]=0; continue; }
            if (count[s] <= lowThreshold) {
                normalizedCounter[s] = -1;
                stillToDistribute--;
            } else {
                short proba = (short)((count[s]*step) >> scale);
                if (proba<8) {
                    U64 restToBeat = vStep * rtbTable[proba];
                    proba += (count[s]*step) - ((U64)proba<<scale) > restToBeat;
                }
                if (proba > largestP) largestP=proba, largest=s;
                normalizedCounter[s] = proba;
                stillToDistribute -= proba;
        }   }
        if (-stillToDistribute >= (normalizedCounter[largest] >> 1)) {
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
    void* const ptr = ct;
    U16* const tableU16 = ( (U16*) ptr) + 2;
    void* const FSCT = ((U32*)ptr) + 1 /* header */ + (tableSize>>1);   /* assumption : tableLog >= 1 */
    FSE_symbolCompressionTransform* const symbolTT = (FSE_symbolCompressionTransform*) (FSCT);
    unsigned s;

    /* Sanity checks */
    if (nbBits < 1) return ERROR(GENERIC);             /* min size */

    /* header */
    tableU16[-2] = (U16) nbBits;
    tableU16[-1] = (U16) maxSymbolValue;

    /* Build table */
    for (s=0; s<tableSize; s++)
        tableU16[s] = (U16)(tableSize + s);

    /* Build Symbol Transformation Table */
    {
        const U32 deltaNbBits = (nbBits << 16) - (1 << nbBits);
        for (s=0; s<=maxSymbolValue; s++) {
            symbolTT[s].deltaNbBits = deltaNbBits;
            symbolTT[s].deltaFindState = s-1;
        }
    }

    return 0;
}

/* fake FSE_CTable, for rle (100% always same symbol) input */
size_t FSE_buildCTable_rle (FSE_CTable* ct, BYTE symbolValue)
{
    void* ptr = ct;
    U16* tableU16 = ( (U16*) ptr) + 2;
    void* FSCTptr = (U32*)ptr + 2;
    FSE_symbolCompressionTransform* symbolTT = (FSE_symbolCompressionTransform*) FSCTptr;

    /* header */
    tableU16[-2] = (U16) 0;
    tableU16[-1] = (U16) symbolValue;

    /* Build table */
    tableU16[0] = 0;
    tableU16[1] = 0;   /* just in case */

    /* Build Symbol Transformation Table */
    symbolTT[symbolValue].deltaNbBits = 0;
    symbolTT[symbolValue].deltaFindState = 0;

    return 0;
}


static size_t FSE_compress_usingCTable_generic (void* dst, size_t dstSize,
                           const void* src, size_t srcSize,
                           const FSE_CTable* ct, const unsigned fast)
{
    const BYTE* const istart = (const BYTE*) src;
    const BYTE* ip;
    const BYTE* const iend = istart + srcSize;

    size_t errorCode;
    BIT_CStream_t bitC;
    FSE_CState_t CState1, CState2;


    /* init */
    errorCode = BIT_initCStream(&bitC, dst, dstSize);
    if (FSE_isError(errorCode)) return 0;
    FSE_initCState(&CState1, ct);
    CState2 = CState1;

    ip=iend;

#define FSE_FLUSHBITS(s)  (fast ? BIT_flushBitsFast(s) : BIT_flushBits(s))

    /* join to even */
    if (srcSize & 1) {
        FSE_encodeSymbol(&bitC, &CState1, *--ip);
        FSE_FLUSHBITS(&bitC);
    }

    /* join to mod 4 */
    if ((sizeof(bitC.bitContainer)*8 > FSE_MAX_TABLELOG*4+7 ) && (srcSize & 2)) {  /* test bit 2 */
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

        if (sizeof(bitC.bitContainer)*8 > FSE_MAX_TABLELOG*4+7 ) {  /* this test must be static */
            FSE_encodeSymbol(&bitC, &CState2, *--ip);
            FSE_encodeSymbol(&bitC, &CState1, *--ip);
        }

        FSE_FLUSHBITS(&bitC);
    }

    FSE_flushCState(&bitC, &CState2);
    FSE_flushCState(&bitC, &CState1);
    return BIT_closeCStream(&bitC);
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


/*-*******************************************************
*  Decompression (Byte symbols)
*********************************************************/
size_t FSE_buildDTable_rle (FSE_DTable* dt, BYTE symbolValue)
{
    void* ptr = dt;
    FSE_DTableHeader* const DTableH = (FSE_DTableHeader*)ptr;
    void* dPtr = dt + 1;
    FSE_decode_t* const cell = (FSE_decode_t*)dPtr;

    DTableH->tableLog = 0;
    DTableH->fastMode = 0;

    cell->newState = 0;
    cell->symbol = symbolValue;
    cell->nbBits = 0;

    return 0;
}


size_t FSE_buildDTable_raw (FSE_DTable* dt, unsigned nbBits)
{
    void* ptr = dt;
    FSE_DTableHeader* const DTableH = (FSE_DTableHeader*)ptr;
    void* dPtr = dt + 1;
    FSE_decode_t* const dinfo = (FSE_decode_t*)dPtr;
    const unsigned tableSize = 1 << nbBits;
    const unsigned tableMask = tableSize - 1;
    const unsigned maxSymbolValue = tableMask;
    unsigned s;

    /* Sanity checks */
    if (nbBits < 1) return ERROR(GENERIC);         /* min size */

    /* Build Decoding Table */
    DTableH->tableLog = (U16)nbBits;
    DTableH->fastMode = 1;
    for (s=0; s<=maxSymbolValue; s++) {
        dinfo[s].newState = 0;
        dinfo[s].symbol = (BYTE)s;
        dinfo[s].nbBits = (BYTE)nbBits;
    }

    return 0;
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

    BIT_DStream_t bitD;
    FSE_DState_t state1;
    FSE_DState_t state2;
    size_t errorCode;

    /* Init */
    errorCode = BIT_initDStream(&bitD, cSrc, cSrcSize);   /* replaced last arg by maxCompressed Size */
    if (FSE_isError(errorCode)) return errorCode;

    FSE_initDState(&state1, &bitD, dt);
    FSE_initDState(&state2, &bitD, dt);

#define FSE_GETSYMBOL(statePtr) fast ? FSE_decodeSymbolFast(statePtr, &bitD) : FSE_decodeSymbol(statePtr, &bitD)

    /* 4 symbols per loop */
    for ( ; (BIT_reloadDStream(&bitD)==BIT_DStream_unfinished) && (op<olimit) ; op+=4) {
        op[0] = FSE_GETSYMBOL(&state1);

        if (FSE_MAX_TABLELOG*2+7 > sizeof(bitD.bitContainer)*8)    /* This test must be static */
            BIT_reloadDStream(&bitD);

        op[1] = FSE_GETSYMBOL(&state2);

        if (FSE_MAX_TABLELOG*4+7 > sizeof(bitD.bitContainer)*8)    /* This test must be static */
            { if (BIT_reloadDStream(&bitD) > BIT_DStream_unfinished) { op+=2; break; } }

        op[2] = FSE_GETSYMBOL(&state1);

        if (FSE_MAX_TABLELOG*2+7 > sizeof(bitD.bitContainer)*8)    /* This test must be static */
            BIT_reloadDStream(&bitD);

        op[3] = FSE_GETSYMBOL(&state2);
    }

    /* tail */
    /* note : BIT_reloadDStream(&bitD) >= FSE_DStream_partiallyFilled; Ends at exactly BIT_DStream_completed */
    while (1) {
        if ( (BIT_reloadDStream(&bitD)>BIT_DStream_completed) || (op==omax) || (BIT_endOfDStream(&bitD) && (fast || FSE_endOfDState(&state1))) )
            break;

        *op++ = FSE_GETSYMBOL(&state1);

        if ( (BIT_reloadDStream(&bitD)>BIT_DStream_completed) || (op==omax) || (BIT_endOfDStream(&bitD) && (fast || FSE_endOfDState(&state2))) )
            break;

        *op++ = FSE_GETSYMBOL(&state2);
    }

    /* end ? */
    if (BIT_endOfDStream(&bitD) && FSE_endOfDState(&state1) && FSE_endOfDState(&state2))
        return op-ostart;

    if (op==omax) return ERROR(dstSize_tooSmall);   /* dst buffer is full, but cSrc unfinished */

    return ERROR(corruption_detected);
}


size_t FSE_decompress_usingDTable(void* dst, size_t originalSize,
                            const void* cSrc, size_t cSrcSize,
                            const FSE_DTable* dt)
{
    const void* ptr = dt;
    const FSE_DTableHeader* DTableH = (const FSE_DTableHeader*)ptr;
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

    if (cSrcSize<2) return ERROR(srcSize_wrong);   /* too small input size */

    /* normal FSE decoding mode */
    errorCode = FSE_readNCount (counting, &maxSymbolValue, &tableLog, istart, cSrcSize);
    if (FSE_isError(errorCode)) return errorCode;
    if (errorCode >= cSrcSize) return ERROR(srcSize_wrong);   /* too small input size */
    ip += errorCode;
    cSrcSize -= errorCode;

    errorCode = FSE_buildDTable (dt, counting, maxSymbolValue, tableLog);
    if (FSE_isError(errorCode)) return errorCode;

    /* always return, even if it is an error code */
    return FSE_decompress_usingDTable (dst, maxDstSize, ip, cSrcSize, dt);
}



#endif   /* FSE_COMMONDEFS_ONLY */
