/* ******************************************************************
   Huffman encoder, part of New Generation Entropy library
   Copyright (C) 2013-2016, Yann Collet.

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
    - FSE+HUF source repository : https://github.com/Cyan4973/FiniteStateEntropy
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
#include "huf_static.h"
#include "bitstream.h"
#include "fse.h"        /* header compression */


/* **************************************************************
*  Error Management
****************************************************************/
#define HUF_STATIC_ASSERT(c) { enum { HUF_static_assert = 1/(int)(!!(c)) }; }   /* use only *after* variable declarations */


/* *******************************************************
*  HUF : Huffman block compression
*********************************************************/
struct HUF_CElt_s {
  U16  val;
  BYTE nbBits;
};   /* typedef'd to HUF_CElt within huf_static.h */

typedef struct nodeElt_s {
    U32 count;
    U16 parent;
    BYTE byte;
    BYTE nbBits;
} nodeElt;

/*! HUF_writeCTable() :
    `CTable` : huffman tree to save, using huf representation.
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



size_t HUF_readCTable (HUF_CElt* CTable, U32 maxSymbolValue, const void* src, size_t srcSize)
{
    BYTE huffWeight[HUF_MAX_SYMBOL_VALUE + 1];
    U32 rankVal[HUF_ABSOLUTEMAX_TABLELOG + 1];   /* large enough for values from 0 to 16 */
    U32 tableLog = 0;
    size_t readSize;
    U32 nbSymbols = 0;
    //memset(huffWeight, 0, sizeof(huffWeight));   /* is not necessary, even though some analyzer complain ... */

    /* get symbol weights */
    readSize = HUF_readStats(huffWeight, HUF_MAX_SYMBOL_VALUE+1, rankVal, &nbSymbols, &tableLog, src, srcSize);
    if (HUF_isError(readSize)) return readSize;

    /* check result */
    if (tableLog > HUF_MAX_TABLELOG) return ERROR(tableLog_tooLarge);
    if (nbSymbols > maxSymbolValue+1) return ERROR(maxSymbolValue_tooSmall);

    /* Prepare base value per rank */
    {   U32 n, nextRankStart = 0;
        for (n=1; n<=tableLog; n++) {
            U32 current = nextRankStart;
            nextRankStart += (rankVal[n] << (n-1));
            rankVal[n] = current;
    }   }

    /* fill nbBits */
    { U32 n; for (n=0; n<nbSymbols; n++) {
        const U32 w = huffWeight[n];
        CTable[n].nbBits = (BYTE)(tableLog + 1 - w);
    }}

    /* fill val */
    {   U16 nbPerRank[HUF_MAX_TABLELOG+1] = {0};
        U16 valPerRank[HUF_MAX_TABLELOG+1] = {0};
        { U32 n; for (n=0; n<nbSymbols; n++) nbPerRank[CTable[n].nbBits]++; }
        /* determine stating value per rank */
        {   U16 min = 0;
            U32 n; for (n=HUF_MAX_TABLELOG; n>0; n--) {
                valPerRank[n] = min;      /* get starting value within each rank */
                min += nbPerRank[n];
                min >>= 1;
        }   }
        /* assign value within rank, symbol order */
        { U32 n; for (n=0; n<=maxSymbolValue; n++) CTable[n].val = valPerRank[CTable[n].nbBits]++; }
    }

    return readSize;
}


static U32 HUF_setMaxHeight(nodeElt* huffNode, U32 lastNonNull, U32 maxNbBits)
{
    const U32 largestBits = huffNode[lastNonNull].nbBits;
    if (largestBits <= maxNbBits) return largestBits;   /* early exit : no elt > maxNbBits */

    /* there are several too large elements (at least >= 2) */
    {   int totalCost = 0;
        const U32 baseCost = 1 << (largestBits - maxNbBits);
        U32 n = lastNonNull;

        while (huffNode[n].nbBits > maxNbBits) {
            totalCost += baseCost - (1 << (largestBits - huffNode[n].nbBits));
            huffNode[n].nbBits = (BYTE)maxNbBits;
            n --;
        }  /* n stops at huffNode[n].nbBits <= maxNbBits */
        while (huffNode[n].nbBits == maxNbBits) n--;   /* n end at index of smallest symbol using < maxNbBits */

        /* renorm totalCost */
        totalCost >>= (largestBits - maxNbBits);  /* note : totalCost is necessarily a multiple of baseCost */

        /* repay normalized cost */
        {   U32 const noSymbol = 0xF0F0F0F0;
            U32 rankLast[HUF_MAX_TABLELOG+1];
            int pos;

            /* Get pos of last (smallest) symbol per rank */
            memset(rankLast, 0xF0, sizeof(rankLast));
            {   U32 currentNbBits = maxNbBits;
                for (pos=n ; pos >= 0; pos--) {
                    if (huffNode[pos].nbBits >= currentNbBits) continue;
                    currentNbBits = huffNode[pos].nbBits;   /* < maxNbBits */
                    rankLast[maxNbBits-currentNbBits] = pos;
            }   }

            while (totalCost > 0) {
                U32 nBitsToDecrease = BIT_highbit32(totalCost) + 1;
                for ( ; nBitsToDecrease > 1; nBitsToDecrease--) {
                    U32 highPos = rankLast[nBitsToDecrease];
                    U32 lowPos = rankLast[nBitsToDecrease-1];
                    if (highPos == noSymbol) continue;
                    if (lowPos == noSymbol) break;
                    {   U32 const highTotal = huffNode[highPos].count;
                        U32 const lowTotal = 2 * huffNode[lowPos].count;
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
            }   }   /* while (totalCost > 0) */

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
    }   }   }   /* there are several too large elements (at least >= 2) */

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
        U32 const c = count[n];
        U32 const r = BIT_highbit32(c+1) + 1;
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
    {   U16 nbPerRank[HUF_MAX_TABLELOG+1] = {0};
        U16 valPerRank[HUF_MAX_TABLELOG+1] = {0};
        if (maxNbBits > HUF_MAX_TABLELOG) return ERROR(GENERIC);   /* check fit into table */
        for (n=0; n<=nonNullRank; n++)
            nbPerRank[huffNode[n].nbBits]++;
        /* determine stating value per rank */
        {   U16 min = 0;
            for (n=maxNbBits; n>0; n--) {
                valPerRank[n] = min;      /* get starting value within each rank */
                min += nbPerRank[n];
                min >>= 1;
        }   }
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
    BYTE* const oend = ostart + dstSize;
    BYTE* op = ostart;
    size_t n;
    const unsigned fast = (dstSize >= HUF_BLOCKBOUND(srcSize));
    BIT_CStream_t bitC;

    /* init */
    if (dstSize < 8) return 0;   /* not enough space to compress */
    { size_t const errorCode = BIT_initCStream(&bitC, op, oend-op);
      if (HUF_isError(errorCode)) return 0; }

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
    const BYTE* ip = (const BYTE*) src;
    const BYTE* const iend = ip + srcSize;
    BYTE* const ostart = (BYTE*) dst;
    BYTE* const oend = ostart + dstSize;
    BYTE* op = ostart;
    size_t errorCode;

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
    BYTE* const oend = ostart + dstSize;
    BYTE* op = ostart;

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
