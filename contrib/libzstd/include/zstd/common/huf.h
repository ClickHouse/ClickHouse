/* ******************************************************************
   Huffman coder, part of New Generation Entropy library
   header file
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
   - Source repository : https://github.com/Cyan4973/FiniteStateEntropy
****************************************************************** */
#ifndef HUF_H_298734234
#define HUF_H_298734234

#if defined (__cplusplus)
extern "C" {
#endif


/* *** Dependencies *** */
#include <stddef.h>    /* size_t */


/* *** simple functions *** */
/**
HUF_compress() :
    Compress content from buffer 'src', of size 'srcSize', into buffer 'dst'.
    'dst' buffer must be already allocated.
    Compression runs faster if `dstCapacity` >= HUF_compressBound(srcSize).
    `srcSize` must be <= `HUF_BLOCKSIZE_MAX` == 128 KB.
    @return : size of compressed data (<= `dstCapacity`).
    Special values : if return == 0, srcData is not compressible => Nothing is stored within dst !!!
                     if return == 1, srcData is a single repeated byte symbol (RLE compression).
                     if HUF_isError(return), compression failed (more details using HUF_getErrorName())
*/
size_t HUF_compress(void* dst, size_t dstCapacity,
              const void* src, size_t srcSize);

/**
HUF_decompress() :
    Decompress HUF data from buffer 'cSrc', of size 'cSrcSize',
    into already allocated buffer 'dst', of minimum size 'dstSize'.
    `dstSize` : **must** be the ***exact*** size of original (uncompressed) data.
    Note : in contrast with FSE, HUF_decompress can regenerate
           RLE (cSrcSize==1) and uncompressed (cSrcSize==dstSize) data,
           because it knows size to regenerate.
    @return : size of regenerated data (== dstSize),
              or an error code, which can be tested using HUF_isError()
*/
size_t HUF_decompress(void* dst,  size_t dstSize,
                const void* cSrc, size_t cSrcSize);


/* ****************************************
*  Tool functions
******************************************/
#define HUF_BLOCKSIZE_MAX (128 * 1024)
size_t HUF_compressBound(size_t size);       /**< maximum compressed size (worst case) */

/* Error Management */
unsigned    HUF_isError(size_t code);        /**< tells if a return value is an error code */
const char* HUF_getErrorName(size_t code);   /**< provides error code string (useful for debugging) */


/* *** Advanced function *** */

/** HUF_compress2() :
*   Same as HUF_compress(), but offers direct control over `maxSymbolValue` and `tableLog` */
size_t HUF_compress2 (void* dst, size_t dstSize, const void* src, size_t srcSize, unsigned maxSymbolValue, unsigned tableLog);


#ifdef HUF_STATIC_LINKING_ONLY

/* *** Dependencies *** */
#include "mem.h"   /* U32 */


/* *** Constants *** */
#define HUF_TABLELOG_ABSOLUTEMAX  16   /* absolute limit of HUF_MAX_TABLELOG. Beyond that value, code does not work */
#define HUF_TABLELOG_MAX  12           /* max configured tableLog (for static allocation); can be modified up to HUF_ABSOLUTEMAX_TABLELOG */
#define HUF_TABLELOG_DEFAULT  11       /* tableLog by default, when not specified */
#define HUF_SYMBOLVALUE_MAX 255
#if (HUF_TABLELOG_MAX > HUF_TABLELOG_ABSOLUTEMAX)
#  error "HUF_TABLELOG_MAX is too large !"
#endif


/* ****************************************
*  Static allocation
******************************************/
/* HUF buffer bounds */
#define HUF_CTABLEBOUND 129
#define HUF_BLOCKBOUND(size) (size + (size>>8) + 8)   /* only true if incompressible pre-filtered with fast heuristic */
#define HUF_COMPRESSBOUND(size) (HUF_CTABLEBOUND + HUF_BLOCKBOUND(size))   /* Macro version, useful for static allocation */

/* static allocation of HUF's Compression Table */
#define HUF_CREATE_STATIC_CTABLE(name, maxSymbolValue) \
    U32 name##hb[maxSymbolValue+1]; \
    void* name##hv = &(name##hb); \
    HUF_CElt* name = (HUF_CElt*)(name##hv)   /* no final ; */

/* static allocation of HUF's DTable */
typedef U32 HUF_DTable;
#define HUF_DTABLE_SIZE(maxTableLog)   (1 + (1<<(maxTableLog)))
#define HUF_CREATE_STATIC_DTABLEX2(DTable, maxTableLog) \
        HUF_DTable DTable[HUF_DTABLE_SIZE((maxTableLog)-1)] = { ((U32)((maxTableLog)-1)*0x1000001) }
#define HUF_CREATE_STATIC_DTABLEX4(DTable, maxTableLog) \
        HUF_DTable DTable[HUF_DTABLE_SIZE(maxTableLog)] = { ((U32)(maxTableLog)*0x1000001) }


/* ****************************************
*  Advanced decompression functions
******************************************/
size_t HUF_decompress4X2 (void* dst, size_t dstSize, const void* cSrc, size_t cSrcSize);   /**< single-symbol decoder */
size_t HUF_decompress4X4 (void* dst, size_t dstSize, const void* cSrc, size_t cSrcSize);   /**< double-symbols decoder */

size_t HUF_decompress4X_DCtx (HUF_DTable* dctx, void* dst, size_t dstSize, const void* cSrc, size_t cSrcSize);   /**< decodes RLE and uncompressed */
size_t HUF_decompress4X_hufOnly(HUF_DTable* dctx, void* dst, size_t dstSize, const void* cSrc, size_t cSrcSize); /**< considers RLE and uncompressed as errors */
size_t HUF_decompress4X2_DCtx(HUF_DTable* dctx, void* dst, size_t dstSize, const void* cSrc, size_t cSrcSize);   /**< single-symbol decoder */
size_t HUF_decompress4X4_DCtx(HUF_DTable* dctx, void* dst, size_t dstSize, const void* cSrc, size_t cSrcSize);   /**< double-symbols decoder */

size_t HUF_decompress1X_DCtx (HUF_DTable* dctx, void* dst, size_t dstSize, const void* cSrc, size_t cSrcSize);
size_t HUF_decompress1X2_DCtx(HUF_DTable* dctx, void* dst, size_t dstSize, const void* cSrc, size_t cSrcSize);   /**< single-symbol decoder */
size_t HUF_decompress1X4_DCtx(HUF_DTable* dctx, void* dst, size_t dstSize, const void* cSrc, size_t cSrcSize);   /**< double-symbols decoder */


/* ****************************************
*  HUF detailed API
******************************************/
/*!
HUF_compress() does the following:
1. count symbol occurrence from source[] into table count[] using FSE_count()
2. (optional) refine tableLog using HUF_optimalTableLog()
3. build Huffman table from count using HUF_buildCTable()
4. save Huffman table to memory buffer using HUF_writeCTable()
5. encode the data stream using HUF_compress4X_usingCTable()

The following API allows targeting specific sub-functions for advanced tasks.
For example, it's possible to compress several blocks using the same 'CTable',
or to save and regenerate 'CTable' using external methods.
*/
/* FSE_count() : find it within "fse.h" */
unsigned HUF_optimalTableLog(unsigned maxTableLog, size_t srcSize, unsigned maxSymbolValue);
typedef struct HUF_CElt_s HUF_CElt;   /* incomplete type */
size_t HUF_buildCTable (HUF_CElt* CTable, const unsigned* count, unsigned maxSymbolValue, unsigned maxNbBits);
size_t HUF_writeCTable (void* dst, size_t maxDstSize, const HUF_CElt* CTable, unsigned maxSymbolValue, unsigned huffLog);
size_t HUF_compress4X_usingCTable(void* dst, size_t dstSize, const void* src, size_t srcSize, const HUF_CElt* CTable);


/*! HUF_readStats() :
    Read compact Huffman tree, saved by HUF_writeCTable().
    `huffWeight` is destination buffer.
    @return : size read from `src` , or an error Code .
    Note : Needed by HUF_readCTable() and HUF_readDTableXn() . */
size_t HUF_readStats(BYTE* huffWeight, size_t hwSize, U32* rankStats,
                     U32* nbSymbolsPtr, U32* tableLogPtr,
                     const void* src, size_t srcSize);

/** HUF_readCTable() :
*   Loading a CTable saved with HUF_writeCTable() */
size_t HUF_readCTable (HUF_CElt* CTable, unsigned maxSymbolValue, const void* src, size_t srcSize);


/*
HUF_decompress() does the following:
1. select the decompression algorithm (X2, X4) based on pre-computed heuristics
2. build Huffman table from save, using HUF_readDTableXn()
3. decode 1 or 4 segments in parallel using HUF_decompressSXn_usingDTable
*/

/** HUF_selectDecoder() :
*   Tells which decoder is likely to decode faster,
*   based on a set of pre-determined metrics.
*   @return : 0==HUF_decompress4X2, 1==HUF_decompress4X4 .
*   Assumption : 0 < cSrcSize < dstSize <= 128 KB */
U32 HUF_selectDecoder (size_t dstSize, size_t cSrcSize);

size_t HUF_readDTableX2 (HUF_DTable* DTable, const void* src, size_t srcSize);
size_t HUF_readDTableX4 (HUF_DTable* DTable, const void* src, size_t srcSize);

size_t HUF_decompress4X_usingDTable(void* dst, size_t maxDstSize, const void* cSrc, size_t cSrcSize, const HUF_DTable* DTable);
size_t HUF_decompress4X2_usingDTable(void* dst, size_t maxDstSize, const void* cSrc, size_t cSrcSize, const HUF_DTable* DTable);
size_t HUF_decompress4X4_usingDTable(void* dst, size_t maxDstSize, const void* cSrc, size_t cSrcSize, const HUF_DTable* DTable);


/* single stream variants */

size_t HUF_compress1X (void* dst, size_t dstSize, const void* src, size_t srcSize, unsigned maxSymbolValue, unsigned tableLog);
size_t HUF_compress1X_usingCTable(void* dst, size_t dstSize, const void* src, size_t srcSize, const HUF_CElt* CTable);

size_t HUF_decompress1X2 (void* dst, size_t dstSize, const void* cSrc, size_t cSrcSize);   /* single-symbol decoder */
size_t HUF_decompress1X4 (void* dst, size_t dstSize, const void* cSrc, size_t cSrcSize);   /* double-symbol decoder */

size_t HUF_decompress1X_usingDTable(void* dst, size_t maxDstSize, const void* cSrc, size_t cSrcSize, const HUF_DTable* DTable);
size_t HUF_decompress1X2_usingDTable(void* dst, size_t maxDstSize, const void* cSrc, size_t cSrcSize, const HUF_DTable* DTable);
size_t HUF_decompress1X4_usingDTable(void* dst, size_t maxDstSize, const void* cSrc, size_t cSrcSize, const HUF_DTable* DTable);


#endif /* HUF_STATIC_LINKING_ONLY */


#if defined (__cplusplus)
}
#endif

#endif   /* HUF_H_298734234 */
