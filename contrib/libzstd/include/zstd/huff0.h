/* ******************************************************************
   Huff0 : Huffman coder, part of New Generation Entropy library
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
#ifndef HUFF0_H
#define HUFF0_H

#if defined (__cplusplus)
extern "C" {
#endif


/* ****************************************
*  Dependency
******************************************/
#include <stddef.h>    /* size_t */


/* ****************************************
*  Huff0 simple functions
******************************************/
size_t HUF_compress(void* dst, size_t maxDstSize,
              const void* src, size_t srcSize);
size_t HUF_decompress(void* dst,  size_t dstSize,
                const void* cSrc, size_t cSrcSize);
/*!
HUF_compress():
    Compress content of buffer 'src', of size 'srcSize', into destination buffer 'dst'.
    'dst' buffer must be already allocated. Compression runs faster if maxDstSize >= HUF_compressBound(srcSize).
    Note : srcSize must be <= 128 KB
    @return : size of compressed data (<= maxDstSize)
    Special values : if return == 0, srcData is not compressible => Nothing is stored within dst !!!
                     if return == 1, srcData is a single repeated byte symbol (RLE compression)
                     if HUF_isError(return), compression failed (more details using HUF_getErrorName())

HUF_decompress():
    Decompress Huff0 data from buffer 'cSrc', of size 'cSrcSize',
    into already allocated destination buffer 'dst', of size 'dstSize'.
    @dstSize : must be the **exact** size of original (uncompressed) data.
    Note : in contrast with FSE, HUF_decompress can regenerate
           RLE (cSrcSize==1) and uncompressed (cSrcSize==dstSize) data,
           because it knows size to regenerate.
    @return : size of regenerated data (== dstSize)
              or an error code, which can be tested using HUF_isError()
*/


/* ****************************************
*  Tool functions
******************************************/
size_t HUF_compressBound(size_t size);       /* maximum compressed size */

/* Error Management */
unsigned    HUF_isError(size_t code);        /* tells if a return value is an error code */
const char* HUF_getErrorName(size_t code);   /* provides error code string (useful for debugging) */


/* ****************************************
*  Advanced functions
******************************************/
size_t HUF_compress2 (void* dst, size_t dstSize, const void* src, size_t srcSize, unsigned maxSymbolValue, unsigned tableLog);


#if defined (__cplusplus)
}
#endif

#endif   /* HUFF0_H */
