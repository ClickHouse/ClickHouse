/* ******************************************************************
   Huff0 : Huffman coder, part of New Generation Entropy library
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
    - FSE+Huff0 source repository : https://github.com/Cyan4973/FiniteStateEntropy
    - Public forum : https://groups.google.com/forum/#!forum/lz4c
****************************************************************** */

/* **************************************************************
*  Compiler specifics
****************************************************************/
#if defined (__cplusplus) || (defined (__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L) /* C99 */)
/* inline is defined */
#elif defined(_MSC_VER)
#  define inline __inline
#else
#  define inline /* disable inline */
#endif


#ifdef _MSC_VER    /* Visual Studio */
#  define FORCE_INLINE static __forceinline
#  pragma warning(disable : 4127)        /* disable: C4127: conditional expression is constant */
#else
#  ifdef __GNUC__
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
#include "huff0_static.h"
#include "bitstream.h"
#include "fse.h"        /* header compression */


/* **************************************************************
*  Constants
****************************************************************/
#define HUF_ABSOLUTEMAX_TABLELOG  16   /* absolute limit of HUF_MAX_TABLELOG. Beyond that value, code does not work */
#define HUF_MAX_TABLELOG  12           /* max configured tableLog (for static allocation); can be modified up to HUF_ABSOLUTEMAX_TABLELOG */
#define HUF_DEFAULT_TABLELOG  HUF_MAX_TABLELOG   /* tableLog by default, when not specified */
#define HUF_MAX_SYMBOL_VALUE 255
#if (HUF_MAX_TABLELOG > HUF_ABSOLUTEMAX_TABLELOG)
#  error "HUF_MAX_TABLELOG is too large !"
#endif


/* **************************************************************
*  Error Management
****************************************************************/
unsigned HUF_isError(size_t code) { return ERR_isError(code); }
const char* HUF_getErrorName(size_t code) { return ERR_getErrorName(code); }
#define HUF_STATIC_ASSERT(c) { enum { HUF_static_assert = 1/(int)(!!(c)) }; }   /* use only *after* variable declarations */


/* *******************************************************
*  Huff0 : Huffman block compression
*********************************************************/
struct HUF_CElt_s {
  U16  val;
  BYTE nbBits;
};   /* typedef'd to HUF_CElt within huff0_static.h */

typedef struct nodeElt_s {
    U32 count;
    U16 parent;
    BYTE byte;
    BYTE nbBits;
} nodeElt;

/*! HUF_writeCTable() :
    @dst : destination buffer
    @CTable : huffman tree to save, using huff0 representation
    @return : size of saved CTable */
size_t HUF_writeCTable (void* dst, size_t maxDstSize,
                        const HUF_CElt* CTable, U32 maxSymbolValue, U32 huffLog)
{
    BYTE bitsToWeight[HUF_MAX_TABLELOG + 1];
    BYTE huffWeight[HUF_MAX_SYMBOL_VALUE + 1];
    U32 n;
    BYTE* op = (BYTE*)dst;
    size_t size;

     /* check conditions */
    if (maxSymbolValue > HUF_MAX_SYMBOL_VALUE + 1)
        return ERROR(GENERIC);

    /* convert to weight */
    bitsToWeight[0] = 0;
    for (n=1; n<=huffLog; n++)
        bitsToWeight[n] = (BYTE)(huffLog + 1 - n);
    for (n=0; n<maxSymbolValue; n++)
        huffWeight[n] = bitsToWeight[CTable[n].nbBits];

    size = FSE_compress(op+1, maxDstSize-1, huffWeight, maxSymbolValue);   /* don't need last symbol stat : implied */
    if (HUF_isError(size)) return size;
    if (size >= 128) return ERROR(GENERIC);   /* should never happen, since maxSymbolValue <= 255 */
    if ((size <= 1) || (size >= maxSymbolValue/2)) {
        if (size==1) {  /* RLE */
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
            default : return ERROR(corruption_detected);
            }
            op[0] = (BYTE)(255-13 + code);
            return 1;
        }
         /* Not compressible */
        if (maxSymbolValue > (241-128)) return ERROR(GENERIC);   /* not implemented (not possible with current format) */
        if (((maxSymbolValue+1)/2) + 1 > maxDstSize) return ERROR(dstSize_tooSmall);   /* not enough space within dst buffer */
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


static size_t HUF_readStats(BYTE* huffWeight, size_t hwSize, U32* rankStats,
                            U32* nbSymbolsPtr, U32* tableLogPtr,
                            const void* src, size_t srcSize);


size_t HUF_readCTable (HUF_CElt* CTable, U32 maxSymbolValue, const void* src, size_t srcSize)
{
    BYTE huffWeight[HUF_MAX_SYMBOL_VALUE + 1];
    U32 rankVal[HUF_ABSOLUTEMAX_TABLELOG + 1];   /* large enough for values from 0 to 16 */
    U32 tableLog = 0;
    size_t iSize;
    U32 nbSymbols = 0;
    U32 n;
    U32 nextRankStart;
    //memset(huffWeight, 0, sizeof(huffWeight));   /* is not necessary, even though some analyzer complain ... */

    /* get symbol weights */
    iSize = HUF_readStats(huffWeight, HUF_MAX_SYMBOL_VALUE+1, rankVal, &nbSymbols, &tableLog, src, srcSize);
    if (HUF_isError(iSize)) return iSize;

    /* check result */
    if (tableLog > HUF_MAX_TABLELOG) return ERROR(tableLog_tooLarge);
    if (nbSymbols > maxSymbolValue+1) return ERROR(maxSymbolValue_tooSmall);

    /* Prepare base value per rank */
    nextRankStart = 0;
    for (n=1; n<=tableLog; n++) {
        U32 current = nextRankStart;
        nextRankStart += (rankVal[n] << (n-1));
        rankVal[n] = current;
    }

    /* fill nbBits */
    for (n=0; n<nbSymbols; n++) {
        const U32 w = huffWeight[n];
        CTable[n].nbBits = (BYTE)(tableLog + 1 - w);
    }

    /* fill val */
    {
        U16 nbPerRank[HUF_MAX_TABLELOG+1] = {0};
        U16 valPerRank[HUF_MAX_TABLELOG+1] = {0};
        for (n=0; n<nbSymbols; n++)
            nbPerRank[CTable[n].nbBits]++;
        {
            /* determine stating value per rank */
            U16 min = 0;
            for (n=HUF_MAX_TABLELOG; n>0; n--) {
                valPerRank[n] = min;      /* get starting value within each rank */
                min += nbPerRank[n];
                min >>= 1;
        }   }
        for (n=0; n<=maxSymbolValue; n++)
            CTable[n].val = valPerRank[CTable[n].nbBits]++;   /* assign value within rank, symbol order */
    }

    return iSize;
}


static U32 HUF_setMaxHeight(nodeElt* huffNode, U32 lastNonNull, U32 maxNbBits)
{
    int totalCost = 0;
    const U32 largestBits = huffNode[lastNonNull].nbBits;

    /* early exit : all is fine */
    if (largestBits <= maxNbBits) return largestBits;

    /* there are several too large elements (at least >= 2) */
    {
        const U32 baseCost = 1 << (largestBits - maxNbBits);
        U32 n = lastNonNull;

        while (huffNode[n].nbBits > maxNbBits) {
            totalCost += baseCost - (1 << (largestBits - huffNode[n].nbBits));
            huffNode[n].nbBits = (BYTE)maxNbBits;
            n --;
        } /* n stops at huffNode[n].nbBits <= maxNbBits */
        while (huffNode[n].nbBits == maxNbBits) n--;   /* n end at index of smallest symbol using (maxNbBits-1) */

        /* renorm totalCost */
        totalCost >>= (largestBits - maxNbBits);  /* note : totalCost is necessarily a multiple of baseCost */

        /* repay normalized cost */
        {
            const U32 noSymbol = 0xF0F0F0F0;
            U32 rankLast[HUF_MAX_TABLELOG+1];
            U32 currentNbBits = maxNbBits;
            int pos;

            /* Get pos of last (smallest) symbol per rank */
            memset(rankLast, 0xF0, sizeof(rankLast));
            for (pos=n ; pos >= 0; pos--) {
                if (huffNode[pos].nbBits >= currentNbBits) continue;
                currentNbBits = huffNode[pos].nbBits;   /* < maxNbBits */
                rankLast[maxNbBits-currentNbBits] = pos;
            }

            while (totalCost > 0) {
                U32 nBitsToDecrease = BIT_highbit32(totalCost) + 1;
                for ( ; nBitsToDecrease > 1; nBitsToDecrease--) {
                    U32 highPos = rankLast[nBitsToDecrease];
                    U32 lowPos = rankLast[nBitsToDecrease-1];
                    if (highPos == noSymbol) continue;
                    if (lowPos == noSymbol) break;
                    {
                        U32 highTotal = huffNode[highPos].count;
                        U32 lowTotal = 2 * huffNode[lowPos].count;
                        if (highTotal <= lowTotal) break;
                }   }
                /* only triggered when no more rank 1 symbol left => find closest one (note : there is necessarily at least one !) */
                while ((nBitsToDecrease<=HUF_MAX_TABLELOG) && (rankLast[nBitsToDecrease] == noSymbol))  /* HUF_MAX_TABLELOG test just to please gcc 5+; but it should not be necessary */
                    nBitsToDecrease ++;
                totalCost -= 1 << (nBitsToDecrease-1);
                if (rankLast[nBitsToDecrease-1] == noSymbol)
                    rankLast[nBitsToDecrease-1] = rankLast[nBitsToDecrease];   /* this rank is no longer empty */
                huffNode[rankLast[nBitsToDecrease]].nbBits ++;
                if (rankLast[nBitsToDecrease] == 0)    /* special case, reached largest symbol */
                    rankLast[nBitsToDecrease] = noSymbol;
                else {
                    rankLast[nBitsToDecrease]--;
                    if (huffNode[rankLast[nBitsToDecrease]].nbBits != maxNbBits-nBitsToDecrease)
                        rankLast[nBitsToDecrease] = noSymbol;   /* this rank is now empty */
            }   }

            while (totalCost < 0) {  /* Sometimes, cost correction overshoot */
                if (rankLast[1] == noSymbol) {  /* special case : no rank 1 symbol (using maxNbBits-1); let's create one from largest rank 0 (using maxNbBits) */
                    while (huffNode[n].nbBits == maxNbBits) n--;
                    huffNode[n+1].nbBits--;
                    rankLast[1] = n+1;
                    totalCost++;
                    continue;
                }
                huffNode[ rankLast[1] + 1 ].nbBits--;
                rankLast[1]++;
                totalCost ++;
    }   }   }

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
    for (n=0; n<=maxSymbolValue; n++) {
        U32 r = BIT_highbit32(count[n] + 1);
        rank[r].base ++;
    }
    for (n=30; n>0; n--) rank[n-1].base += rank[n].base;
    for (n=0; n<32; n++) rank[n].current = rank[n].base;
    for (n=0; n<=maxSymbolValue; n++) {
        U32 c = count[n];
        U32 r = BIT_highbit32(c+1) + 1;
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
    if (maxSymbolValue > HUF_MAX_SYMBOL_VALUE) return ERROR(GENERIC);
    memset(huffNode0, 0, sizeof(huffNode0));

    /* sort, decreasing order */
    HUF_sort(huffNode, count, maxSymbolValue);

    /* init for parents */
    nonNullRank = maxSymbolValue;
    while(huffNode[nonNullRank].count == 0) nonNullRank--;
    lowS = nonNullRank; nodeRoot = nodeNb + lowS - 1; lowN = nodeNb;
    huffNode[nodeNb].count = huffNode[lowS].count + huffNode[lowS-1].count;
    huffNode[lowS].parent = huffNode[lowS-1].parent = nodeNb;
    nodeNb++; lowS-=2;
    for (n=nodeNb; n<=nodeRoot; n++) huffNode[n].count = (U32)(1U<<30);
    huffNode0[0].count = (U32)(1U<<31);

    /* create parents */
    while (nodeNb <= nodeRoot) {
        U32 n1 = (huffNode[lowS].count < huffNode[lowN].count) ? lowS-- : lowN++;
        U32 n2 = (huffNode[lowS].count < huffNode[lowN].count) ? lowS-- : lowN++;
        huffNode[nodeNb].count = huffNode[n1].count + huffNode[n2].count;
        huffNode[n1].parent = huffNode[n2].parent = nodeNb;
        nodeNb++;
    }

    /* distribute weights (unlimited tree height) */
    huffNode[nodeRoot].nbBits = 0;
    for (n=nodeRoot-1; n>=STARTNODE; n--)
        huffNode[n].nbBits = huffNode[ huffNode[n].parent ].nbBits + 1;
    for (n=0; n<=nonNullRank; n++)
        huffNode[n].nbBits = huffNode[ huffNode[n].parent ].nbBits + 1;

    /* enforce maxTableLog */
    maxNbBits = HUF_setMaxHeight(huffNode, nonNullRank, maxNbBits);

    /* fill result into tree (val, nbBits) */
    {
        U16 nbPerRank[HUF_MAX_TABLELOG+1] = {0};
        U16 valPerRank[HUF_MAX_TABLELOG+1] = {0};
        if (maxNbBits > HUF_MAX_TABLELOG) return ERROR(GENERIC);   /* check fit into table */
        for (n=0; n<=nonNullRank; n++)
            nbPerRank[huffNode[n].nbBits]++;
        {
            /* determine stating value per rank */
            U16 min = 0;
            for (n=maxNbBits; n>0; n--) {
                valPerRank[n] = min;      /* get starting value within each rank */
                min += nbPerRank[n];
                min >>= 1;
            }
        }
        for (n=0; n<=maxSymbolValue; n++)
            tree[huffNode[n].byte].nbBits = huffNode[n].nbBits;   /* push nbBits per symbol, symbol order */
        for (n=0; n<=maxSymbolValue; n++)
            tree[n].val = valPerRank[tree[n].nbBits]++;   /* assign value within rank, symbol order */
    }

    return maxNbBits;
}

static void HUF_encodeSymbol(BIT_CStream_t* bitCPtr, U32 symbol, const HUF_CElt* CTable)
{
    BIT_addBitsFast(bitCPtr, CTable[symbol].val, CTable[symbol].nbBits);
}

size_t HUF_compressBound(size_t size) { return HUF_COMPRESSBOUND(size); }

#define HUF_FLUSHBITS(s)  (fast ? BIT_flushBitsFast(s) : BIT_flushBits(s))

#define HUF_FLUSHBITS_1(stream) \
    if (sizeof((stream)->bitContainer)*8 < HUF_MAX_TABLELOG*2+7) HUF_FLUSHBITS(stream)

#define HUF_FLUSHBITS_2(stream) \
    if (sizeof((stream)->bitContainer)*8 < HUF_MAX_TABLELOG*4+7) HUF_FLUSHBITS(stream)

size_t HUF_compress1X_usingCTable(void* dst, size_t dstSize, const void* src, size_t srcSize, const HUF_CElt* CTable)
{
    const BYTE* ip = (const BYTE*) src;
    BYTE* const ostart = (BYTE*)dst;
    BYTE* op = ostart;
    BYTE* const oend = ostart + dstSize;
    size_t n;
    const unsigned fast = (dstSize >= HUF_BLOCKBOUND(srcSize));
    size_t errorCode;
    BIT_CStream_t bitC;

    /* init */
    if (dstSize < 8) return 0;   /* not enough space to compress */
    errorCode = BIT_initCStream(&bitC, op, oend-op);
    if (HUF_isError(errorCode)) return 0;

    n = srcSize & ~3;  /* join to mod 4 */
    switch (srcSize & 3)
    {
        case 3 : HUF_encodeSymbol(&bitC, ip[n+ 2], CTable);
                 HUF_FLUSHBITS_2(&bitC);
        case 2 : HUF_encodeSymbol(&bitC, ip[n+ 1], CTable);
                 HUF_FLUSHBITS_1(&bitC);
        case 1 : HUF_encodeSymbol(&bitC, ip[n+ 0], CTable);
                 HUF_FLUSHBITS(&bitC);
        case 0 :
        default: ;
    }

    for (; n>0; n-=4) {  /* note : n&3==0 at this stage */
        HUF_encodeSymbol(&bitC, ip[n- 1], CTable);
        HUF_FLUSHBITS_1(&bitC);
        HUF_encodeSymbol(&bitC, ip[n- 2], CTable);
        HUF_FLUSHBITS_2(&bitC);
        HUF_encodeSymbol(&bitC, ip[n- 3], CTable);
        HUF_FLUSHBITS_1(&bitC);
        HUF_encodeSymbol(&bitC, ip[n- 4], CTable);
        HUF_FLUSHBITS(&bitC);
    }

    return BIT_closeCStream(&bitC);
}


size_t HUF_compress4X_usingCTable(void* dst, size_t dstSize, const void* src, size_t srcSize, const HUF_CElt* CTable)
{
    size_t segmentSize = (srcSize+3)/4;   /* first 3 segments */
    size_t errorCode;
    const BYTE* ip = (const BYTE*) src;
    const BYTE* const iend = ip + srcSize;
    BYTE* const ostart = (BYTE*) dst;
    BYTE* op = ostart;
    BYTE* const oend = ostart + dstSize;

    if (dstSize < 6 + 1 + 1 + 1 + 8) return 0;   /* minimum space to compress successfully */
    if (srcSize < 12) return 0;   /* no saving possible : too small input */
    op += 6;   /* jumpTable */

    errorCode = HUF_compress1X_usingCTable(op, oend-op, ip, segmentSize, CTable);
    if (HUF_isError(errorCode)) return errorCode;
    if (errorCode==0) return 0;
    MEM_writeLE16(ostart, (U16)errorCode);

    ip += segmentSize;
    op += errorCode;
    errorCode = HUF_compress1X_usingCTable(op, oend-op, ip, segmentSize, CTable);
    if (HUF_isError(errorCode)) return errorCode;
    if (errorCode==0) return 0;
    MEM_writeLE16(ostart+2, (U16)errorCode);

    ip += segmentSize;
    op += errorCode;
    errorCode = HUF_compress1X_usingCTable(op, oend-op, ip, segmentSize, CTable);
    if (HUF_isError(errorCode)) return errorCode;
    if (errorCode==0) return 0;
    MEM_writeLE16(ostart+4, (U16)errorCode);

    ip += segmentSize;
    op += errorCode;
    errorCode = HUF_compress1X_usingCTable(op, oend-op, ip, iend-ip, CTable);
    if (HUF_isError(errorCode)) return errorCode;
    if (errorCode==0) return 0;

    op += errorCode;
    return op-ostart;
}


static size_t HUF_compress_internal (
                void* dst, size_t dstSize,
                const void* src, size_t srcSize,
                unsigned maxSymbolValue, unsigned huffLog,
                unsigned singleStream)
{
    BYTE* const ostart = (BYTE*)dst;
    BYTE* op = ostart;
    BYTE* const oend = ostart + dstSize;

    U32 count[HUF_MAX_SYMBOL_VALUE+1];
    HUF_CElt CTable[HUF_MAX_SYMBOL_VALUE+1];
    size_t errorCode;

    /* checks & inits */
    if (srcSize < 1) return 0;  /* Uncompressed - note : 1 means rle, so first byte must be correct */
    if (dstSize < 1) return 0;  /* not compressible within dst budget */
    if (srcSize > 128 * 1024) return ERROR(srcSize_wrong);   /* current block size limit */
    if (huffLog > HUF_MAX_TABLELOG) return ERROR(tableLog_tooLarge);
    if (!maxSymbolValue) maxSymbolValue = HUF_MAX_SYMBOL_VALUE;
    if (!huffLog) huffLog = HUF_DEFAULT_TABLELOG;

    /* Scan input and build symbol stats */
    errorCode = FSE_count (count, &maxSymbolValue, (const BYTE*)src, srcSize);
    if (HUF_isError(errorCode)) return errorCode;
    if (errorCode == srcSize) { *ostart = ((const BYTE*)src)[0]; return 1; }
    if (errorCode <= (srcSize >> 7)+1) return 0;   /* Heuristic : not compressible enough */

    /* Build Huffman Tree */
    errorCode = HUF_buildCTable (CTable, count, maxSymbolValue, huffLog);
    if (HUF_isError(errorCode)) return errorCode;
    huffLog = (U32)errorCode;

    /* Write table description header */
    errorCode = HUF_writeCTable (op, dstSize, CTable, maxSymbolValue, huffLog);
    if (HUF_isError(errorCode)) return errorCode;
    if (errorCode + 12 >= srcSize) return 0;   /* not useful to try compression */
    op += errorCode;

    /* Compress */
    if (singleStream)
        errorCode = HUF_compress1X_usingCTable(op, oend - op, src, srcSize, CTable);   /* single segment */
    else
        errorCode = HUF_compress4X_usingCTable(op, oend - op, src, srcSize, CTable);
    if (HUF_isError(errorCode)) return errorCode;
    if (errorCode==0) return 0;
    op += errorCode;

    /* check compressibility */
    if ((size_t)(op-ostart) >= srcSize-1)
        return 0;

    return op-ostart;
}


size_t HUF_compress1X (void* dst, size_t dstSize,
                const void* src, size_t srcSize,
                unsigned maxSymbolValue, unsigned huffLog)
{
    return HUF_compress_internal(dst, dstSize, src, srcSize, maxSymbolValue, huffLog, 1);
}

size_t HUF_compress2 (void* dst, size_t dstSize,
                const void* src, size_t srcSize,
                unsigned maxSymbolValue, unsigned huffLog)
{
    return HUF_compress_internal(dst, dstSize, src, srcSize, maxSymbolValue, huffLog, 0);
}


size_t HUF_compress (void* dst, size_t maxDstSize, const void* src, size_t srcSize)
{
    return HUF_compress2(dst, maxDstSize, src, (U32)srcSize, 255, HUF_DEFAULT_TABLELOG);
}


/* *******************************************************
*  Huff0 : Huffman block decompression
*********************************************************/
typedef struct { BYTE byte; BYTE nbBits; } HUF_DEltX2;   /* single-symbol decoding */

typedef struct { U16 sequence; BYTE nbBits; BYTE length; } HUF_DEltX4;  /* double-symbols decoding */

typedef struct { BYTE symbol; BYTE weight; } sortedSymbol_t;

/*! HUF_readStats
    Read compact Huffman tree, saved by HUF_writeCTable
    @huffWeight : destination buffer
    @return : size read from `src`
*/
static size_t HUF_readStats(BYTE* huffWeight, size_t hwSize, U32* rankStats,
                            U32* nbSymbolsPtr, U32* tableLogPtr,
                            const void* src, size_t srcSize)
{
    U32 weightTotal;
    U32 tableLog;
    const BYTE* ip = (const BYTE*) src;
    size_t iSize = ip[0];
    size_t oSize;
    U32 n;

    //memset(huffWeight, 0, hwSize);   /* is not necessary, even though some analyzer complain ... */

    if (iSize >= 128)  { /* special header */
        if (iSize >= (242)) {  /* RLE */
            static int l[14] = { 1, 2, 3, 4, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128 };
            oSize = l[iSize-242];
            memset(huffWeight, 1, hwSize);
            iSize = 0;
        }
        else {   /* Incompressible */
            oSize = iSize - 127;
            iSize = ((oSize+1)/2);
            if (iSize+1 > srcSize) return ERROR(srcSize_wrong);
            if (oSize >= hwSize) return ERROR(corruption_detected);
            ip += 1;
            for (n=0; n<oSize; n+=2) {
                huffWeight[n]   = ip[n/2] >> 4;
                huffWeight[n+1] = ip[n/2] & 15;
    }   }   }
    else  {   /* header compressed with FSE (normal case) */
        if (iSize+1 > srcSize) return ERROR(srcSize_wrong);
        oSize = FSE_decompress(huffWeight, hwSize-1, ip+1, iSize);   /* max (hwSize-1) values decoded, as last one is implied */
        if (FSE_isError(oSize)) return oSize;
    }

    /* collect weight stats */
    memset(rankStats, 0, (HUF_ABSOLUTEMAX_TABLELOG + 1) * sizeof(U32));
    weightTotal = 0;
    for (n=0; n<oSize; n++) {
        if (huffWeight[n] >= HUF_ABSOLUTEMAX_TABLELOG) return ERROR(corruption_detected);
        rankStats[huffWeight[n]]++;
        weightTotal += (1 << huffWeight[n]) >> 1;
    }

    /* get last non-null symbol weight (implied, total must be 2^n) */
    tableLog = BIT_highbit32(weightTotal) + 1;
    if (tableLog > HUF_ABSOLUTEMAX_TABLELOG) return ERROR(corruption_detected);
    {   /* determine last weight */
        U32 total = 1 << tableLog;
        U32 rest = total - weightTotal;
        U32 verif = 1 << BIT_highbit32(rest);
        U32 lastWeight = BIT_highbit32(rest) + 1;
        if (verif != rest) return ERROR(corruption_detected);    /* last value must be a clean power of 2 */
        huffWeight[oSize] = (BYTE)lastWeight;
        rankStats[lastWeight]++;
    }

    /* check tree construction validity */
    if ((rankStats[1] < 2) || (rankStats[1] & 1)) return ERROR(corruption_detected);   /* by construction : at least 2 elts of rank 1, must be even */

    /* results */
    *nbSymbolsPtr = (U32)(oSize+1);
    *tableLogPtr = tableLog;
    return iSize+1;
}


/*-***************************/
/*  single-symbol decoding   */
/*-***************************/

size_t HUF_readDTableX2 (U16* DTable, const void* src, size_t srcSize)
{
    BYTE huffWeight[HUF_MAX_SYMBOL_VALUE + 1];
    U32 rankVal[HUF_ABSOLUTEMAX_TABLELOG + 1];   /* large enough for values from 0 to 16 */
    U32 tableLog = 0;
    size_t iSize;
    U32 nbSymbols = 0;
    U32 n;
    U32 nextRankStart;
    void* const dtPtr = DTable + 1;
    HUF_DEltX2* const dt = (HUF_DEltX2*)dtPtr;

    HUF_STATIC_ASSERT(sizeof(HUF_DEltX2) == sizeof(U16));   /* if compilation fails here, assertion is false */
    //memset(huffWeight, 0, sizeof(huffWeight));   /* is not necessary, even though some analyzer complain ... */

    iSize = HUF_readStats(huffWeight, HUF_MAX_SYMBOL_VALUE + 1, rankVal, &nbSymbols, &tableLog, src, srcSize);
    if (HUF_isError(iSize)) return iSize;

    /* check result */
    if (tableLog > DTable[0]) return ERROR(tableLog_tooLarge);   /* DTable is too small */
    DTable[0] = (U16)tableLog;   /* maybe should separate sizeof allocated DTable, from used size of DTable, in case of re-use */

    /* Prepare ranks */
    nextRankStart = 0;
    for (n=1; n<=tableLog; n++) {
        U32 current = nextRankStart;
        nextRankStart += (rankVal[n] << (n-1));
        rankVal[n] = current;
    }

    /* fill DTable */
    for (n=0; n<nbSymbols; n++) {
        const U32 w = huffWeight[n];
        const U32 length = (1 << w) >> 1;
        U32 i;
        HUF_DEltX2 D;
        D.byte = (BYTE)n; D.nbBits = (BYTE)(tableLog + 1 - w);
        for (i = rankVal[w]; i < rankVal[w] + length; i++)
            dt[i] = D;
        rankVal[w] += length;
    }

    return iSize;
}

static BYTE HUF_decodeSymbolX2(BIT_DStream_t* Dstream, const HUF_DEltX2* dt, const U32 dtLog)
{
        const size_t val = BIT_lookBitsFast(Dstream, dtLog); /* note : dtLog >= 1 */
        const BYTE c = dt[val].byte;
        BIT_skipBits(Dstream, dt[val].nbBits);
        return c;
}

#define HUF_DECODE_SYMBOLX2_0(ptr, DStreamPtr) \
    *ptr++ = HUF_decodeSymbolX2(DStreamPtr, dt, dtLog)

#define HUF_DECODE_SYMBOLX2_1(ptr, DStreamPtr) \
    if (MEM_64bits() || (HUF_MAX_TABLELOG<=12)) \
        HUF_DECODE_SYMBOLX2_0(ptr, DStreamPtr)

#define HUF_DECODE_SYMBOLX2_2(ptr, DStreamPtr) \
    if (MEM_64bits()) \
        HUF_DECODE_SYMBOLX2_0(ptr, DStreamPtr)

static inline size_t HUF_decodeStreamX2(BYTE* p, BIT_DStream_t* const bitDPtr, BYTE* const pEnd, const HUF_DEltX2* const dt, const U32 dtLog)
{
    BYTE* const pStart = p;

    /* up to 4 symbols at a time */
    while ((BIT_reloadDStream(bitDPtr) == BIT_DStream_unfinished) && (p <= pEnd-4)) {
        HUF_DECODE_SYMBOLX2_2(p, bitDPtr);
        HUF_DECODE_SYMBOLX2_1(p, bitDPtr);
        HUF_DECODE_SYMBOLX2_2(p, bitDPtr);
        HUF_DECODE_SYMBOLX2_0(p, bitDPtr);
    }

    /* closer to the end */
    while ((BIT_reloadDStream(bitDPtr) == BIT_DStream_unfinished) && (p < pEnd))
        HUF_DECODE_SYMBOLX2_0(p, bitDPtr);

    /* no more data to retrieve from bitstream, hence no need to reload */
    while (p < pEnd)
        HUF_DECODE_SYMBOLX2_0(p, bitDPtr);

    return pEnd-pStart;
}

size_t HUF_decompress1X2_usingDTable(
          void* dst,  size_t dstSize,
    const void* cSrc, size_t cSrcSize,
    const U16* DTable)
{
    BYTE* op = (BYTE*)dst;
    BYTE* const oend = op + dstSize;
    size_t errorCode;
    const U32 dtLog = DTable[0];
    const void* dtPtr = DTable;
    const HUF_DEltX2* const dt = ((const HUF_DEltX2*)dtPtr)+1;
    BIT_DStream_t bitD;
    errorCode = BIT_initDStream(&bitD, cSrc, cSrcSize);
    if (HUF_isError(errorCode)) return errorCode;

    HUF_decodeStreamX2(op, &bitD, oend, dt, dtLog);

    /* check */
    if (!BIT_endOfDStream(&bitD)) return ERROR(corruption_detected);

    return dstSize;
}

size_t HUF_decompress1X2 (void* dst, size_t dstSize, const void* cSrc, size_t cSrcSize)
{
    HUF_CREATE_STATIC_DTABLEX2(DTable, HUF_MAX_TABLELOG);
    const BYTE* ip = (const BYTE*) cSrc;
    size_t errorCode;

    errorCode = HUF_readDTableX2 (DTable, cSrc, cSrcSize);
    if (HUF_isError(errorCode)) return errorCode;
    if (errorCode >= cSrcSize) return ERROR(srcSize_wrong);
    ip += errorCode;
    cSrcSize -= errorCode;

    return HUF_decompress1X2_usingDTable (dst, dstSize, ip, cSrcSize, DTable);
}


size_t HUF_decompress4X2_usingDTable(
          void* dst,  size_t dstSize,
    const void* cSrc, size_t cSrcSize,
    const U16* DTable)
{
    const BYTE* const istart = (const BYTE*) cSrc;
    BYTE* const ostart = (BYTE*) dst;
    BYTE* const oend = ostart + dstSize;
    const void* const dtPtr = DTable;
    const HUF_DEltX2* const dt = ((const HUF_DEltX2*)dtPtr) +1;
    const U32 dtLog = DTable[0];
    size_t errorCode;

    /* Check */
    if (cSrcSize < 10) return ERROR(corruption_detected);   /* strict minimum : jump table + 1 byte per stream */

    /* Init */
    BIT_DStream_t bitD1;
    BIT_DStream_t bitD2;
    BIT_DStream_t bitD3;
    BIT_DStream_t bitD4;
    const size_t length1 = MEM_readLE16(istart);
    const size_t length2 = MEM_readLE16(istart+2);
    const size_t length3 = MEM_readLE16(istart+4);
    size_t length4;
    const BYTE* const istart1 = istart + 6;  /* jumpTable */
    const BYTE* const istart2 = istart1 + length1;
    const BYTE* const istart3 = istart2 + length2;
    const BYTE* const istart4 = istart3 + length3;
    const size_t segmentSize = (dstSize+3) / 4;
    BYTE* const opStart2 = ostart + segmentSize;
    BYTE* const opStart3 = opStart2 + segmentSize;
    BYTE* const opStart4 = opStart3 + segmentSize;
    BYTE* op1 = ostart;
    BYTE* op2 = opStart2;
    BYTE* op3 = opStart3;
    BYTE* op4 = opStart4;
    U32 endSignal;

    length4 = cSrcSize - (length1 + length2 + length3 + 6);
    if (length4 > cSrcSize) return ERROR(corruption_detected);   /* overflow */
    errorCode = BIT_initDStream(&bitD1, istart1, length1);
    if (HUF_isError(errorCode)) return errorCode;
    errorCode = BIT_initDStream(&bitD2, istart2, length2);
    if (HUF_isError(errorCode)) return errorCode;
    errorCode = BIT_initDStream(&bitD3, istart3, length3);
    if (HUF_isError(errorCode)) return errorCode;
    errorCode = BIT_initDStream(&bitD4, istart4, length4);
    if (HUF_isError(errorCode)) return errorCode;

    /* 16-32 symbols per loop (4-8 symbols per stream) */
    endSignal = BIT_reloadDStream(&bitD1) | BIT_reloadDStream(&bitD2) | BIT_reloadDStream(&bitD3) | BIT_reloadDStream(&bitD4);
    for ( ; (endSignal==BIT_DStream_unfinished) && (op4<(oend-7)) ; ) {
        HUF_DECODE_SYMBOLX2_2(op1, &bitD1);
        HUF_DECODE_SYMBOLX2_2(op2, &bitD2);
        HUF_DECODE_SYMBOLX2_2(op3, &bitD3);
        HUF_DECODE_SYMBOLX2_2(op4, &bitD4);
        HUF_DECODE_SYMBOLX2_1(op1, &bitD1);
        HUF_DECODE_SYMBOLX2_1(op2, &bitD2);
        HUF_DECODE_SYMBOLX2_1(op3, &bitD3);
        HUF_DECODE_SYMBOLX2_1(op4, &bitD4);
        HUF_DECODE_SYMBOLX2_2(op1, &bitD1);
        HUF_DECODE_SYMBOLX2_2(op2, &bitD2);
        HUF_DECODE_SYMBOLX2_2(op3, &bitD3);
        HUF_DECODE_SYMBOLX2_2(op4, &bitD4);
        HUF_DECODE_SYMBOLX2_0(op1, &bitD1);
        HUF_DECODE_SYMBOLX2_0(op2, &bitD2);
        HUF_DECODE_SYMBOLX2_0(op3, &bitD3);
        HUF_DECODE_SYMBOLX2_0(op4, &bitD4);
        endSignal = BIT_reloadDStream(&bitD1) | BIT_reloadDStream(&bitD2) | BIT_reloadDStream(&bitD3) | BIT_reloadDStream(&bitD4);
    }

    /* check corruption */
    if (op1 > opStart2) return ERROR(corruption_detected);
    if (op2 > opStart3) return ERROR(corruption_detected);
    if (op3 > opStart4) return ERROR(corruption_detected);
    /* note : op4 supposed already verified within main loop */

    /* finish bitStreams one by one */
    HUF_decodeStreamX2(op1, &bitD1, opStart2, dt, dtLog);
    HUF_decodeStreamX2(op2, &bitD2, opStart3, dt, dtLog);
    HUF_decodeStreamX2(op3, &bitD3, opStart4, dt, dtLog);
    HUF_decodeStreamX2(op4, &bitD4, oend,     dt, dtLog);

    /* check */
    endSignal = BIT_endOfDStream(&bitD1) & BIT_endOfDStream(&bitD2) & BIT_endOfDStream(&bitD3) & BIT_endOfDStream(&bitD4);
    if (!endSignal) return ERROR(corruption_detected);

    /* decoded size */
    return dstSize;
}


size_t HUF_decompress4X2 (void* dst, size_t dstSize, const void* cSrc, size_t cSrcSize)
{
    HUF_CREATE_STATIC_DTABLEX2(DTable, HUF_MAX_TABLELOG);
    const BYTE* ip = (const BYTE*) cSrc;
    size_t errorCode;

    errorCode = HUF_readDTableX2 (DTable, cSrc, cSrcSize);
    if (HUF_isError(errorCode)) return errorCode;
    if (errorCode >= cSrcSize) return ERROR(srcSize_wrong);
    ip += errorCode;
    cSrcSize -= errorCode;

    return HUF_decompress4X2_usingDTable (dst, dstSize, ip, cSrcSize, DTable);
}


/* *************************/
/* double-symbols decoding */
/* *************************/

static void HUF_fillDTableX4Level2(HUF_DEltX4* DTable, U32 sizeLog, const U32 consumed,
                           const U32* rankValOrigin, const int minWeight,
                           const sortedSymbol_t* sortedSymbols, const U32 sortedListSize,
                           U32 nbBitsBaseline, U16 baseSeq)
{
    HUF_DEltX4 DElt;
    U32 rankVal[HUF_ABSOLUTEMAX_TABLELOG + 1];
    U32 s;

    /* get pre-calculated rankVal */
    memcpy(rankVal, rankValOrigin, sizeof(rankVal));

    /* fill skipped values */
    if (minWeight>1) {
        U32 i, skipSize = rankVal[minWeight];
        MEM_writeLE16(&(DElt.sequence), baseSeq);
        DElt.nbBits   = (BYTE)(consumed);
        DElt.length   = 1;
        for (i = 0; i < skipSize; i++)
            DTable[i] = DElt;
    }

    /* fill DTable */
    for (s=0; s<sortedListSize; s++) {   /* note : sortedSymbols already skipped */
        const U32 symbol = sortedSymbols[s].symbol;
        const U32 weight = sortedSymbols[s].weight;
        const U32 nbBits = nbBitsBaseline - weight;
        const U32 length = 1 << (sizeLog-nbBits);
        const U32 start = rankVal[weight];
        U32 i = start;
        const U32 end = start + length;

        MEM_writeLE16(&(DElt.sequence), (U16)(baseSeq + (symbol << 8)));
        DElt.nbBits = (BYTE)(nbBits + consumed);
        DElt.length = 2;
        do { DTable[i++] = DElt; } while (i<end);   /* since length >= 1 */

        rankVal[weight] += length;
    }
}

typedef U32 rankVal_t[HUF_ABSOLUTEMAX_TABLELOG][HUF_ABSOLUTEMAX_TABLELOG + 1];

static void HUF_fillDTableX4(HUF_DEltX4* DTable, const U32 targetLog,
                           const sortedSymbol_t* sortedList, const U32 sortedListSize,
                           const U32* rankStart, rankVal_t rankValOrigin, const U32 maxWeight,
                           const U32 nbBitsBaseline)
{
    U32 rankVal[HUF_ABSOLUTEMAX_TABLELOG + 1];
    const int scaleLog = nbBitsBaseline - targetLog;   /* note : targetLog >= srcLog, hence scaleLog <= 1 */
    const U32 minBits  = nbBitsBaseline - maxWeight;
    U32 s;

    memcpy(rankVal, rankValOrigin, sizeof(rankVal));

    /* fill DTable */
    for (s=0; s<sortedListSize; s++) {
        const U16 symbol = sortedList[s].symbol;
        const U32 weight = sortedList[s].weight;
        const U32 nbBits = nbBitsBaseline - weight;
        const U32 start = rankVal[weight];
        const U32 length = 1 << (targetLog-nbBits);

        if (targetLog-nbBits >= minBits) {   /* enough room for a second symbol */
            U32 sortedRank;
            int minWeight = nbBits + scaleLog;
            if (minWeight < 1) minWeight = 1;
            sortedRank = rankStart[minWeight];
            HUF_fillDTableX4Level2(DTable+start, targetLog-nbBits, nbBits,
                           rankValOrigin[nbBits], minWeight,
                           sortedList+sortedRank, sortedListSize-sortedRank,
                           nbBitsBaseline, symbol);
        } else {
            U32 i;
            const U32 end = start + length;
            HUF_DEltX4 DElt;

            MEM_writeLE16(&(DElt.sequence), symbol);
            DElt.nbBits   = (BYTE)(nbBits);
            DElt.length   = 1;
            for (i = start; i < end; i++)
                DTable[i] = DElt;
        }
        rankVal[weight] += length;
    }
}

size_t HUF_readDTableX4 (U32* DTable, const void* src, size_t srcSize)
{
    BYTE weightList[HUF_MAX_SYMBOL_VALUE + 1];
    sortedSymbol_t sortedSymbol[HUF_MAX_SYMBOL_VALUE + 1];
    U32 rankStats[HUF_ABSOLUTEMAX_TABLELOG + 1] = { 0 };
    U32 rankStart0[HUF_ABSOLUTEMAX_TABLELOG + 2] = { 0 };
    U32* const rankStart = rankStart0+1;
    rankVal_t rankVal;
    U32 tableLog, maxW, sizeOfSort, nbSymbols;
    const U32 memLog = DTable[0];
    size_t iSize;
    void* dtPtr = DTable;
    HUF_DEltX4* const dt = ((HUF_DEltX4*)dtPtr) + 1;

    HUF_STATIC_ASSERT(sizeof(HUF_DEltX4) == sizeof(U32));   /* if compilation fails here, assertion is false */
    if (memLog > HUF_ABSOLUTEMAX_TABLELOG) return ERROR(tableLog_tooLarge);
    //memset(weightList, 0, sizeof(weightList));   /* is not necessary, even though some analyzer complain ... */

    iSize = HUF_readStats(weightList, HUF_MAX_SYMBOL_VALUE + 1, rankStats, &nbSymbols, &tableLog, src, srcSize);
    if (HUF_isError(iSize)) return iSize;

    /* check result */
    if (tableLog > memLog) return ERROR(tableLog_tooLarge);   /* DTable can't fit code depth */

    /* find maxWeight */
    for (maxW = tableLog; rankStats[maxW]==0; maxW--) {}  /* necessarily finds a solution before 0 */

    /* Get start index of each weight */
    {
        U32 w, nextRankStart = 0;
        for (w=1; w<=maxW; w++) {
            U32 current = nextRankStart;
            nextRankStart += rankStats[w];
            rankStart[w] = current;
        }
        rankStart[0] = nextRankStart;   /* put all 0w symbols at the end of sorted list*/
        sizeOfSort = nextRankStart;
    }

    /* sort symbols by weight */
    {
        U32 s;
        for (s=0; s<nbSymbols; s++) {
            U32 w = weightList[s];
            U32 r = rankStart[w]++;
            sortedSymbol[r].symbol = (BYTE)s;
            sortedSymbol[r].weight = (BYTE)w;
        }
        rankStart[0] = 0;   /* forget 0w symbols; this is beginning of weight(1) */
    }

    /* Build rankVal */
    {
        const U32 minBits = tableLog+1 - maxW;
        U32 nextRankVal = 0;
        U32 w, consumed;
        const int rescale = (memLog-tableLog) - 1;   /* tableLog <= memLog */
        U32* rankVal0 = rankVal[0];
        for (w=1; w<=maxW; w++) {
            U32 current = nextRankVal;
            nextRankVal += rankStats[w] << (w+rescale);
            rankVal0[w] = current;
        }
        for (consumed = minBits; consumed <= memLog - minBits; consumed++) {
            U32* rankValPtr = rankVal[consumed];
            for (w = 1; w <= maxW; w++) {
                rankValPtr[w] = rankVal0[w] >> consumed;
    }   }   }

    HUF_fillDTableX4(dt, memLog,
                   sortedSymbol, sizeOfSort,
                   rankStart0, rankVal, maxW,
                   tableLog+1);

    return iSize;
}


static U32 HUF_decodeSymbolX4(void* op, BIT_DStream_t* DStream, const HUF_DEltX4* dt, const U32 dtLog)
{
    const size_t val = BIT_lookBitsFast(DStream, dtLog);   /* note : dtLog >= 1 */
    memcpy(op, dt+val, 2);
    BIT_skipBits(DStream, dt[val].nbBits);
    return dt[val].length;
}

static U32 HUF_decodeLastSymbolX4(void* op, BIT_DStream_t* DStream, const HUF_DEltX4* dt, const U32 dtLog)
{
    const size_t val = BIT_lookBitsFast(DStream, dtLog);   /* note : dtLog >= 1 */
    memcpy(op, dt+val, 1);
    if (dt[val].length==1) BIT_skipBits(DStream, dt[val].nbBits);
    else {
        if (DStream->bitsConsumed < (sizeof(DStream->bitContainer)*8)) {
            BIT_skipBits(DStream, dt[val].nbBits);
            if (DStream->bitsConsumed > (sizeof(DStream->bitContainer)*8))
                DStream->bitsConsumed = (sizeof(DStream->bitContainer)*8);   /* ugly hack; works only because it's the last symbol. Note : can't easily extract nbBits from just this symbol */
    }   }
    return 1;
}


#define HUF_DECODE_SYMBOLX4_0(ptr, DStreamPtr) \
    ptr += HUF_decodeSymbolX4(ptr, DStreamPtr, dt, dtLog)

#define HUF_DECODE_SYMBOLX4_1(ptr, DStreamPtr) \
    if (MEM_64bits() || (HUF_MAX_TABLELOG<=12)) \
        ptr += HUF_decodeSymbolX4(ptr, DStreamPtr, dt, dtLog)

#define HUF_DECODE_SYMBOLX4_2(ptr, DStreamPtr) \
    if (MEM_64bits()) \
        ptr += HUF_decodeSymbolX4(ptr, DStreamPtr, dt, dtLog)

static inline size_t HUF_decodeStreamX4(BYTE* p, BIT_DStream_t* bitDPtr, BYTE* const pEnd, const HUF_DEltX4* const dt, const U32 dtLog)
{
    BYTE* const pStart = p;

    /* up to 8 symbols at a time */
    while ((BIT_reloadDStream(bitDPtr) == BIT_DStream_unfinished) && (p < pEnd-7)) {
        HUF_DECODE_SYMBOLX4_2(p, bitDPtr);
        HUF_DECODE_SYMBOLX4_1(p, bitDPtr);
        HUF_DECODE_SYMBOLX4_2(p, bitDPtr);
        HUF_DECODE_SYMBOLX4_0(p, bitDPtr);
    }

    /* closer to the end */
    while ((BIT_reloadDStream(bitDPtr) == BIT_DStream_unfinished) && (p <= pEnd-2))
        HUF_DECODE_SYMBOLX4_0(p, bitDPtr);

    while (p <= pEnd-2)
        HUF_DECODE_SYMBOLX4_0(p, bitDPtr);   /* no need to reload : reached the end of DStream */

    if (p < pEnd)
        p += HUF_decodeLastSymbolX4(p, bitDPtr, dt, dtLog);

    return p-pStart;
}


size_t HUF_decompress1X4_usingDTable(
          void* dst,  size_t dstSize,
    const void* cSrc, size_t cSrcSize,
    const U32* DTable)
{
    const BYTE* const istart = (const BYTE*) cSrc;
    BYTE* const ostart = (BYTE*) dst;
    BYTE* const oend = ostart + dstSize;

    const U32 dtLog = DTable[0];
    const void* const dtPtr = DTable;
    const HUF_DEltX4* const dt = ((const HUF_DEltX4*)dtPtr) +1;
    size_t errorCode;

    /* Init */
    BIT_DStream_t bitD;
    errorCode = BIT_initDStream(&bitD, istart, cSrcSize);
    if (HUF_isError(errorCode)) return errorCode;

    /* finish bitStreams one by one */
    HUF_decodeStreamX4(ostart, &bitD, oend,     dt, dtLog);

    /* check */
    if (!BIT_endOfDStream(&bitD)) return ERROR(corruption_detected);

    /* decoded size */
    return dstSize;
}

size_t HUF_decompress1X4 (void* dst, size_t dstSize, const void* cSrc, size_t cSrcSize)
{
    HUF_CREATE_STATIC_DTABLEX4(DTable, HUF_MAX_TABLELOG);
    const BYTE* ip = (const BYTE*) cSrc;

    size_t hSize = HUF_readDTableX4 (DTable, cSrc, cSrcSize);
    if (HUF_isError(hSize)) return hSize;
    if (hSize >= cSrcSize) return ERROR(srcSize_wrong);
    ip += hSize;
    cSrcSize -= hSize;

    return HUF_decompress1X4_usingDTable (dst, dstSize, ip, cSrcSize, DTable);
}

size_t HUF_decompress4X4_usingDTable(
          void* dst,  size_t dstSize,
    const void* cSrc, size_t cSrcSize,
    const U32* DTable)
{
    if (cSrcSize < 10) return ERROR(corruption_detected);   /* strict minimum : jump table + 1 byte per stream */

    {
        const BYTE* const istart = (const BYTE*) cSrc;
        BYTE* const ostart = (BYTE*) dst;
        BYTE* const oend = ostart + dstSize;
        const void* const dtPtr = DTable;
        const HUF_DEltX4* const dt = ((const HUF_DEltX4*)dtPtr) +1;
        const U32 dtLog = DTable[0];
        size_t errorCode;

        /* Init */
        BIT_DStream_t bitD1;
        BIT_DStream_t bitD2;
        BIT_DStream_t bitD3;
        BIT_DStream_t bitD4;
        const size_t length1 = MEM_readLE16(istart);
        const size_t length2 = MEM_readLE16(istart+2);
        const size_t length3 = MEM_readLE16(istart+4);
        size_t length4;
        const BYTE* const istart1 = istart + 6;  /* jumpTable */
        const BYTE* const istart2 = istart1 + length1;
        const BYTE* const istart3 = istart2 + length2;
        const BYTE* const istart4 = istart3 + length3;
        const size_t segmentSize = (dstSize+3) / 4;
        BYTE* const opStart2 = ostart + segmentSize;
        BYTE* const opStart3 = opStart2 + segmentSize;
        BYTE* const opStart4 = opStart3 + segmentSize;
        BYTE* op1 = ostart;
        BYTE* op2 = opStart2;
        BYTE* op3 = opStart3;
        BYTE* op4 = opStart4;
        U32 endSignal;

        length4 = cSrcSize - (length1 + length2 + length3 + 6);
        if (length4 > cSrcSize) return ERROR(corruption_detected);   /* overflow */
        errorCode = BIT_initDStream(&bitD1, istart1, length1);
        if (HUF_isError(errorCode)) return errorCode;
        errorCode = BIT_initDStream(&bitD2, istart2, length2);
        if (HUF_isError(errorCode)) return errorCode;
        errorCode = BIT_initDStream(&bitD3, istart3, length3);
        if (HUF_isError(errorCode)) return errorCode;
        errorCode = BIT_initDStream(&bitD4, istart4, length4);
        if (HUF_isError(errorCode)) return errorCode;

        /* 16-32 symbols per loop (4-8 symbols per stream) */
        endSignal = BIT_reloadDStream(&bitD1) | BIT_reloadDStream(&bitD2) | BIT_reloadDStream(&bitD3) | BIT_reloadDStream(&bitD4);
        for ( ; (endSignal==BIT_DStream_unfinished) && (op4<(oend-7)) ; ) {
            HUF_DECODE_SYMBOLX4_2(op1, &bitD1);
            HUF_DECODE_SYMBOLX4_2(op2, &bitD2);
            HUF_DECODE_SYMBOLX4_2(op3, &bitD3);
            HUF_DECODE_SYMBOLX4_2(op4, &bitD4);
            HUF_DECODE_SYMBOLX4_1(op1, &bitD1);
            HUF_DECODE_SYMBOLX4_1(op2, &bitD2);
            HUF_DECODE_SYMBOLX4_1(op3, &bitD3);
            HUF_DECODE_SYMBOLX4_1(op4, &bitD4);
            HUF_DECODE_SYMBOLX4_2(op1, &bitD1);
            HUF_DECODE_SYMBOLX4_2(op2, &bitD2);
            HUF_DECODE_SYMBOLX4_2(op3, &bitD3);
            HUF_DECODE_SYMBOLX4_2(op4, &bitD4);
            HUF_DECODE_SYMBOLX4_0(op1, &bitD1);
            HUF_DECODE_SYMBOLX4_0(op2, &bitD2);
            HUF_DECODE_SYMBOLX4_0(op3, &bitD3);
            HUF_DECODE_SYMBOLX4_0(op4, &bitD4);

            endSignal = BIT_reloadDStream(&bitD1) | BIT_reloadDStream(&bitD2) | BIT_reloadDStream(&bitD3) | BIT_reloadDStream(&bitD4);
        }

        /* check corruption */
        if (op1 > opStart2) return ERROR(corruption_detected);
        if (op2 > opStart3) return ERROR(corruption_detected);
        if (op3 > opStart4) return ERROR(corruption_detected);
        /* note : op4 supposed already verified within main loop */

        /* finish bitStreams one by one */
        HUF_decodeStreamX4(op1, &bitD1, opStart2, dt, dtLog);
        HUF_decodeStreamX4(op2, &bitD2, opStart3, dt, dtLog);
        HUF_decodeStreamX4(op3, &bitD3, opStart4, dt, dtLog);
        HUF_decodeStreamX4(op4, &bitD4, oend,     dt, dtLog);

        /* check */
        endSignal = BIT_endOfDStream(&bitD1) & BIT_endOfDStream(&bitD2) & BIT_endOfDStream(&bitD3) & BIT_endOfDStream(&bitD4);
        if (!endSignal) return ERROR(corruption_detected);

        /* decoded size */
        return dstSize;
    }
}


size_t HUF_decompress4X4 (void* dst, size_t dstSize, const void* cSrc, size_t cSrcSize)
{
    HUF_CREATE_STATIC_DTABLEX4(DTable, HUF_MAX_TABLELOG);
    const BYTE* ip = (const BYTE*) cSrc;

    size_t hSize = HUF_readDTableX4 (DTable, cSrc, cSrcSize);
    if (HUF_isError(hSize)) return hSize;
    if (hSize >= cSrcSize) return ERROR(srcSize_wrong);
    ip += hSize;
    cSrcSize -= hSize;

    return HUF_decompress4X4_usingDTable (dst, dstSize, ip, cSrcSize, DTable);
}


/* ********************************/
/* quad-symbol decoding           */
/* ********************************/
typedef struct { BYTE nbBits; BYTE nbBytes; } HUF_DDescX6;
typedef union { BYTE byte[4]; U32 sequence; } HUF_DSeqX6;

/* recursive, up to level 3; may benefit from <template>-like strategy to nest each level inline */
static void HUF_fillDTableX6LevelN(HUF_DDescX6* DDescription, HUF_DSeqX6* DSequence, int sizeLog,
                           const rankVal_t rankValOrigin, const U32 consumed, const int minWeight, const U32 maxWeight,
                           const sortedSymbol_t* sortedSymbols, const U32 sortedListSize, const U32* rankStart,
                           const U32 nbBitsBaseline, HUF_DSeqX6 baseSeq, HUF_DDescX6 DDesc)
{
    const int scaleLog = nbBitsBaseline - sizeLog;   /* note : targetLog >= (nbBitsBaseline-1), hence scaleLog <= 1 */
    const int minBits  = nbBitsBaseline - maxWeight;
    const U32 level = DDesc.nbBytes;
    U32 rankVal[HUF_ABSOLUTEMAX_TABLELOG + 1];
    U32 symbolStartPos, s;

    /* local rankVal, will be modified */
    memcpy(rankVal, rankValOrigin[consumed], sizeof(rankVal));

    /* fill skipped values */
    if (minWeight>1) {
        U32 i;
        const U32 skipSize = rankVal[minWeight];
        for (i = 0; i < skipSize; i++) {
            DSequence[i] = baseSeq;
            DDescription[i] = DDesc;
    }   }

    /* fill DTable */
    DDesc.nbBytes++;
    symbolStartPos = rankStart[minWeight];
    for (s=symbolStartPos; s<sortedListSize; s++) {
        const BYTE symbol = sortedSymbols[s].symbol;
        const U32  weight = sortedSymbols[s].weight;   /* >= 1 (sorted) */
        const int  nbBits = nbBitsBaseline - weight;   /* >= 1 (by construction) */
        const int  totalBits = consumed+nbBits;
        const U32  start  = rankVal[weight];
        const U32  length = 1 << (sizeLog-nbBits);
        baseSeq.byte[level] = symbol;
        DDesc.nbBits = (BYTE)totalBits;

        if ((level<3) && (sizeLog-totalBits >= minBits)) {  /* enough room for another symbol */
            int nextMinWeight = totalBits + scaleLog;
            if (nextMinWeight < 1) nextMinWeight = 1;
            HUF_fillDTableX6LevelN(DDescription+start, DSequence+start, sizeLog-nbBits,
                           rankValOrigin, totalBits, nextMinWeight, maxWeight,
                           sortedSymbols, sortedListSize, rankStart,
                           nbBitsBaseline, baseSeq, DDesc);   /* recursive (max : level 3) */
        } else {
            U32 i;
            const U32 end = start + length;
            for (i = start; i < end; i++) {
                DDescription[i] = DDesc;
                DSequence[i] = baseSeq;
        }   }
        rankVal[weight] += length;
    }
}


/* note : same preparation as X4 */
size_t HUF_readDTableX6 (U32* DTable, const void* src, size_t srcSize)
{
    BYTE weightList[HUF_MAX_SYMBOL_VALUE + 1];
    sortedSymbol_t sortedSymbol[HUF_MAX_SYMBOL_VALUE + 1];
    U32 rankStats[HUF_ABSOLUTEMAX_TABLELOG + 1] = { 0 };
    U32 rankStart0[HUF_ABSOLUTEMAX_TABLELOG + 2] = { 0 };
    U32* const rankStart = rankStart0+1;
    U32 tableLog, maxW, sizeOfSort, nbSymbols;
    rankVal_t rankVal;
    const U32 memLog = DTable[0];
    size_t iSize;

    if (memLog > HUF_ABSOLUTEMAX_TABLELOG) return ERROR(tableLog_tooLarge);
    //memset(weightList, 0, sizeof(weightList));   /* is not necessary, even though some analyzer complain ... */

    iSize = HUF_readStats(weightList, HUF_MAX_SYMBOL_VALUE + 1, rankStats, &nbSymbols, &tableLog, src, srcSize);
    if (HUF_isError(iSize)) return iSize;

    /* check result */
    if (tableLog > memLog) return ERROR(tableLog_tooLarge);   /* DTable is too small */

    /* find maxWeight */
    for (maxW = tableLog; rankStats[maxW]==0; maxW--) {}  /* necessarily finds a solution before 0 */

    /* Get start index of each weight */
    {
        U32 w, nextRankStart = 0;
        for (w=1; w<=maxW; w++) {
            U32 current = nextRankStart;
            nextRankStart += rankStats[w];
            rankStart[w] = current;
        }
        rankStart[0] = nextRankStart;   /* put all 0w symbols at the end of sorted list*/
        sizeOfSort = nextRankStart;
    }

    /* sort symbols by weight */
    {
        U32 s;
        for (s=0; s<nbSymbols; s++) {
            U32 w = weightList[s];
            U32 r = rankStart[w]++;
            sortedSymbol[r].symbol = (BYTE)s;
            sortedSymbol[r].weight = (BYTE)w;
        }
        rankStart[0] = 0;   /* forget 0w symbols; this is beginning of weight(1) */
    }

    /* Build rankVal */
    {
        const U32 minBits = tableLog+1 - maxW;
        U32 nextRankVal = 0;
        U32 w, consumed;
        const int rescale = (memLog-tableLog) - 1;   /* tableLog <= memLog */
        U32* rankVal0 = rankVal[0];
        for (w=1; w<=maxW; w++) {
            U32 current = nextRankVal;
            nextRankVal += rankStats[w] << (w+rescale);
            rankVal0[w] = current;
        }
        for (consumed = minBits; consumed <= memLog - minBits; consumed++) {
            U32* rankValPtr = rankVal[consumed];
            for (w = 1; w <= maxW; w++) {
                rankValPtr[w] = rankVal0[w] >> consumed;
    }   }   }

    /* fill tables */
    {
        void* ddPtr = DTable+1;
        HUF_DDescX6* DDescription = (HUF_DDescX6*)ddPtr;
        void* dsPtr = DTable + 1 + ((size_t)1<<(memLog-1));
        HUF_DSeqX6* DSequence = (HUF_DSeqX6*)dsPtr;
        HUF_DSeqX6 DSeq;
        HUF_DDescX6 DDesc;
        DSeq.sequence = 0;
        DDesc.nbBits = 0;
        DDesc.nbBytes = 0;
        HUF_fillDTableX6LevelN(DDescription, DSequence, memLog,
                       (const U32 (*)[HUF_ABSOLUTEMAX_TABLELOG + 1])rankVal, 0, 1, maxW,
                       sortedSymbol, sizeOfSort, rankStart0,
                       tableLog+1, DSeq, DDesc);
    }

    return iSize;
}


static U32 HUF_decodeSymbolX6(void* op, BIT_DStream_t* DStream, const HUF_DDescX6* dd, const HUF_DSeqX6* ds, const U32 dtLog)
{
    const size_t val = BIT_lookBitsFast(DStream, dtLog);   /* note : dtLog >= 1 */
    memcpy(op, ds+val, sizeof(HUF_DSeqX6));
    BIT_skipBits(DStream, dd[val].nbBits);
    return dd[val].nbBytes;
}

static U32 HUF_decodeLastSymbolsX6(void* op, const U32 maxL, BIT_DStream_t* DStream,
                                  const HUF_DDescX6* dd, const HUF_DSeqX6* ds, const U32 dtLog)
{
    const size_t val = BIT_lookBitsFast(DStream, dtLog);   /* note : dtLog >= 1 */
    U32 length = dd[val].nbBytes;
    if (length <= maxL) {
        memcpy(op, ds+val, length);
        BIT_skipBits(DStream, dd[val].nbBits);
        return length;
    }
    memcpy(op, ds+val, maxL);
    if (DStream->bitsConsumed < (sizeof(DStream->bitContainer)*8)) {
        BIT_skipBits(DStream, dd[val].nbBits);
        if (DStream->bitsConsumed > (sizeof(DStream->bitContainer)*8))
            DStream->bitsConsumed = (sizeof(DStream->bitContainer)*8);   /* ugly hack; works only because it's the last symbol. Note : can't easily extract nbBits from just this symbol */
    }
    return maxL;
}


#define HUF_DECODE_SYMBOLX6_0(ptr, DStreamPtr) \
    ptr += HUF_decodeSymbolX6(ptr, DStreamPtr, dd, ds, dtLog)

#define HUF_DECODE_SYMBOLX6_1(ptr, DStreamPtr) \
    if (MEM_64bits() || (HUF_MAX_TABLELOG<=12)) \
        HUF_DECODE_SYMBOLX6_0(ptr, DStreamPtr)

#define HUF_DECODE_SYMBOLX6_2(ptr, DStreamPtr) \
    if (MEM_64bits()) \
        HUF_DECODE_SYMBOLX6_0(ptr, DStreamPtr)

static inline size_t HUF_decodeStreamX6(BYTE* p, BIT_DStream_t* bitDPtr, BYTE* const pEnd, const U32* DTable, const U32 dtLog)
{
    const void* const ddPtr = DTable+1;
    const HUF_DDescX6* dd = (const HUF_DDescX6*)ddPtr;
    const void* const dsPtr = DTable + 1 + ((size_t)1<<(dtLog-1));
    const HUF_DSeqX6* ds = (const HUF_DSeqX6*)dsPtr;
    BYTE* const pStart = p;

    /* up to 16 symbols at a time */
    while ((BIT_reloadDStream(bitDPtr) == BIT_DStream_unfinished) && (p <= pEnd-16)) {
        HUF_DECODE_SYMBOLX6_2(p, bitDPtr);
        HUF_DECODE_SYMBOLX6_1(p, bitDPtr);
        HUF_DECODE_SYMBOLX6_2(p, bitDPtr);
        HUF_DECODE_SYMBOLX6_0(p, bitDPtr);
    }

    /* closer to the end, up to 4 symbols at a time */
    while ((BIT_reloadDStream(bitDPtr) == BIT_DStream_unfinished) && (p <= pEnd-4))
        HUF_DECODE_SYMBOLX6_0(p, bitDPtr);

    while ((BIT_reloadDStream(bitDPtr) <= BIT_DStream_endOfBuffer) && (p < pEnd))
        p += HUF_decodeLastSymbolsX6(p, (U32)(pEnd-p), bitDPtr, dd, ds, dtLog);

    return p-pStart;
}


size_t HUF_decompress1X6_usingDTable(
          void* dst,  size_t dstSize,
    const void* cSrc, size_t cSrcSize,
    const U32* DTable)
{
    const BYTE* const istart = (const BYTE*) cSrc;
    BYTE* const ostart = (BYTE*) dst;
    BYTE* const oend = ostart + dstSize;

    const U32 dtLog = DTable[0];
    size_t errorCode;

    /* Init */
    BIT_DStream_t bitD;
    errorCode = BIT_initDStream(&bitD, istart, cSrcSize);
    if (HUF_isError(errorCode)) return errorCode;

    /* finish bitStreams one by one */
    HUF_decodeStreamX6(ostart, &bitD, oend, DTable, dtLog);

    /* check */
    if (!BIT_endOfDStream(&bitD)) return ERROR(corruption_detected);

    /* decoded size */
    return dstSize;
}

size_t HUF_decompress1X6 (void* dst, size_t dstSize, const void* cSrc, size_t cSrcSize)
{
    HUF_CREATE_STATIC_DTABLEX6(DTable, HUF_MAX_TABLELOG);
    const BYTE* ip = (const BYTE*) cSrc;

    size_t hSize = HUF_readDTableX6 (DTable, cSrc, cSrcSize);
    if (HUF_isError(hSize)) return hSize;
    if (hSize >= cSrcSize) return ERROR(srcSize_wrong);
    ip += hSize;
    cSrcSize -= hSize;

    return HUF_decompress1X6_usingDTable (dst, dstSize, ip, cSrcSize, DTable);
}


size_t HUF_decompress4X6_usingDTable(
          void* dst,  size_t dstSize,
    const void* cSrc, size_t cSrcSize,
    const U32* DTable)
{
    const BYTE* const istart = (const BYTE*) cSrc;
    BYTE* const ostart = (BYTE*) dst;
    BYTE* const oend = ostart + dstSize;

    const U32 dtLog = DTable[0];
    const void* const ddPtr = DTable+1;
    const HUF_DDescX6* dd = (const HUF_DDescX6*)ddPtr;
    const void* const dsPtr = DTable + 1 + ((size_t)1<<(dtLog-1));
    const HUF_DSeqX6* ds = (const HUF_DSeqX6*)dsPtr;
    size_t errorCode;

    /* Check */
    if (cSrcSize < 10) return ERROR(corruption_detected);   /* strict minimum : jump table + 1 byte per stream */

    /* Init */
    BIT_DStream_t bitD1;
    BIT_DStream_t bitD2;
    BIT_DStream_t bitD3;
    BIT_DStream_t bitD4;
    const size_t length1 = MEM_readLE16(istart);
    const size_t length2 = MEM_readLE16(istart+2);
    const size_t length3 = MEM_readLE16(istart+4);
    size_t length4;
    const BYTE* const istart1 = istart + 6;  /* jumpTable */
    const BYTE* const istart2 = istart1 + length1;
    const BYTE* const istart3 = istart2 + length2;
    const BYTE* const istart4 = istart3 + length3;
    const size_t segmentSize = (dstSize+3) / 4;
    BYTE* const opStart2 = ostart + segmentSize;
    BYTE* const opStart3 = opStart2 + segmentSize;
    BYTE* const opStart4 = opStart3 + segmentSize;
    BYTE* op1 = ostart;
    BYTE* op2 = opStart2;
    BYTE* op3 = opStart3;
    BYTE* op4 = opStart4;
    U32 endSignal;

    length4 = cSrcSize - (length1 + length2 + length3 + 6);
    if (length4 > cSrcSize) return ERROR(corruption_detected);   /* overflow */
    errorCode = BIT_initDStream(&bitD1, istart1, length1);
    if (HUF_isError(errorCode)) return errorCode;
    errorCode = BIT_initDStream(&bitD2, istart2, length2);
    if (HUF_isError(errorCode)) return errorCode;
    errorCode = BIT_initDStream(&bitD3, istart3, length3);
    if (HUF_isError(errorCode)) return errorCode;
    errorCode = BIT_initDStream(&bitD4, istart4, length4);
    if (HUF_isError(errorCode)) return errorCode;

    /* 16-64 symbols per loop (4-16 symbols per stream) */
    endSignal = BIT_reloadDStream(&bitD1) | BIT_reloadDStream(&bitD2) | BIT_reloadDStream(&bitD3) | BIT_reloadDStream(&bitD4);
    for ( ; (op3 <= opStart4) && (endSignal==BIT_DStream_unfinished) && (op4<=(oend-16)) ; ) {
        HUF_DECODE_SYMBOLX6_2(op1, &bitD1);
        HUF_DECODE_SYMBOLX6_2(op2, &bitD2);
        HUF_DECODE_SYMBOLX6_2(op3, &bitD3);
        HUF_DECODE_SYMBOLX6_2(op4, &bitD4);
        HUF_DECODE_SYMBOLX6_1(op1, &bitD1);
        HUF_DECODE_SYMBOLX6_1(op2, &bitD2);
        HUF_DECODE_SYMBOLX6_1(op3, &bitD3);
        HUF_DECODE_SYMBOLX6_1(op4, &bitD4);
        HUF_DECODE_SYMBOLX6_2(op1, &bitD1);
        HUF_DECODE_SYMBOLX6_2(op2, &bitD2);
        HUF_DECODE_SYMBOLX6_2(op3, &bitD3);
        HUF_DECODE_SYMBOLX6_2(op4, &bitD4);
        HUF_DECODE_SYMBOLX6_0(op1, &bitD1);
        HUF_DECODE_SYMBOLX6_0(op2, &bitD2);
        HUF_DECODE_SYMBOLX6_0(op3, &bitD3);
        HUF_DECODE_SYMBOLX6_0(op4, &bitD4);

        endSignal = BIT_reloadDStream(&bitD1) | BIT_reloadDStream(&bitD2) | BIT_reloadDStream(&bitD3) | BIT_reloadDStream(&bitD4);
    }

    /* check corruption */
    if (op1 > opStart2) return ERROR(corruption_detected);
    if (op2 > opStart3) return ERROR(corruption_detected);
    if (op3 > opStart4) return ERROR(corruption_detected);
    /* note : op4 supposed already verified within main loop */

    /* finish bitStreams one by one */
    HUF_decodeStreamX6(op1, &bitD1, opStart2, DTable, dtLog);
    HUF_decodeStreamX6(op2, &bitD2, opStart3, DTable, dtLog);
    HUF_decodeStreamX6(op3, &bitD3, opStart4, DTable, dtLog);
    HUF_decodeStreamX6(op4, &bitD4, oend,     DTable, dtLog);

    /* check */
    endSignal = BIT_endOfDStream(&bitD1) & BIT_endOfDStream(&bitD2) & BIT_endOfDStream(&bitD3) & BIT_endOfDStream(&bitD4);
    if (!endSignal) return ERROR(corruption_detected);

    /* decoded size */
    return dstSize;
}


size_t HUF_decompress4X6 (void* dst, size_t dstSize, const void* cSrc, size_t cSrcSize)
{
    HUF_CREATE_STATIC_DTABLEX6(DTable, HUF_MAX_TABLELOG);
    const BYTE* ip = (const BYTE*) cSrc;

    size_t hSize = HUF_readDTableX6 (DTable, cSrc, cSrcSize);
    if (HUF_isError(hSize)) return hSize;
    if (hSize >= cSrcSize) return ERROR(srcSize_wrong);
    ip += hSize;
    cSrcSize -= hSize;

    return HUF_decompress4X6_usingDTable (dst, dstSize, ip, cSrcSize, DTable);
}


/* ********************************/
/* Generic decompression selector */
/* ********************************/

typedef struct { U32 tableTime; U32 decode256Time; } algo_time_t;
static const algo_time_t algoTime[16 /* Quantization */][3 /* single, double, quad */] =
{
    /* single, double, quad */
    {{0,0}, {1,1}, {2,2}},  /* Q==0 : impossible */
    {{0,0}, {1,1}, {2,2}},  /* Q==1 : impossible */
    {{  38,130}, {1313, 74}, {2151, 38}},   /* Q == 2 : 12-18% */
    {{ 448,128}, {1353, 74}, {2238, 41}},   /* Q == 3 : 18-25% */
    {{ 556,128}, {1353, 74}, {2238, 47}},   /* Q == 4 : 25-32% */
    {{ 714,128}, {1418, 74}, {2436, 53}},   /* Q == 5 : 32-38% */
    {{ 883,128}, {1437, 74}, {2464, 61}},   /* Q == 6 : 38-44% */
    {{ 897,128}, {1515, 75}, {2622, 68}},   /* Q == 7 : 44-50% */
    {{ 926,128}, {1613, 75}, {2730, 75}},   /* Q == 8 : 50-56% */
    {{ 947,128}, {1729, 77}, {3359, 77}},   /* Q == 9 : 56-62% */
    {{1107,128}, {2083, 81}, {4006, 84}},   /* Q ==10 : 62-69% */
    {{1177,128}, {2379, 87}, {4785, 88}},   /* Q ==11 : 69-75% */
    {{1242,128}, {2415, 93}, {5155, 84}},   /* Q ==12 : 75-81% */
    {{1349,128}, {2644,106}, {5260,106}},   /* Q ==13 : 81-87% */
    {{1455,128}, {2422,124}, {4174,124}},   /* Q ==14 : 87-93% */
    {{ 722,128}, {1891,145}, {1936,146}},   /* Q ==15 : 93-99% */
};

typedef size_t (*decompressionAlgo)(void* dst, size_t dstSize, const void* cSrc, size_t cSrcSize);

size_t HUF_decompress (void* dst, size_t dstSize, const void* cSrc, size_t cSrcSize)
{
    static const decompressionAlgo decompress[3] = { HUF_decompress4X2, HUF_decompress4X4, HUF_decompress4X6 };
    /* estimate decompression time */
    U32 Q;
    const U32 D256 = (U32)(dstSize >> 8);
    U32 Dtime[3];
    U32 algoNb = 0;
    int n;

    /* validation checks */
    if (dstSize == 0) return ERROR(dstSize_tooSmall);
    if (cSrcSize > dstSize) return ERROR(corruption_detected);   /* invalid */
    if (cSrcSize == dstSize) { memcpy(dst, cSrc, dstSize); return dstSize; }   /* not compressed */
    if (cSrcSize == 1) { memset(dst, *(const BYTE*)cSrc, dstSize); return dstSize; }   /* RLE */

    /* decoder timing evaluation */
    Q = (U32)(cSrcSize * 16 / dstSize);   /* Q < 16 since dstSize > cSrcSize */
    for (n=0; n<3; n++)
        Dtime[n] = algoTime[Q][n].tableTime + (algoTime[Q][n].decode256Time * D256);

    Dtime[1] += Dtime[1] >> 4; Dtime[2] += Dtime[2] >> 3; /* advantage to algorithms using less memory, for cache eviction */

    if (Dtime[1] < Dtime[0]) algoNb = 1;
    if (Dtime[2] < Dtime[algoNb]) algoNb = 2;

    return decompress[algoNb](dst, dstSize, cSrc, cSrcSize);

    //return HUF_decompress4X2(dst, dstSize, cSrc, cSrcSize);   /* multi-streams single-symbol decoding */
    //return HUF_decompress4X4(dst, dstSize, cSrc, cSrcSize);   /* multi-streams double-symbols decoding */
    //return HUF_decompress4X6(dst, dstSize, cSrc, cSrcSize);   /* multi-streams quad-symbols decoding */
}
