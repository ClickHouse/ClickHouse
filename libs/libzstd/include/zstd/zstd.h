/*
    zstd - standard compression library
    Header File
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
#pragma once

#if defined (__cplusplus)
extern "C" {
#endif

/**************************************
*  Includes
**************************************/
#include <stddef.h>   /* size_t */


/**************************************
*  Version
**************************************/
#define ZSTD_VERSION_MAJOR    0    /* for breaking interface changes  */
#define ZSTD_VERSION_MINOR    0    /* for new (non-breaking) interface capabilities */
#define ZSTD_VERSION_RELEASE  2    /* for tweaks, bug-fixes, or development */
#define ZSTD_VERSION_NUMBER (ZSTD_VERSION_MAJOR *100*100 + ZSTD_VERSION_MINOR *100 + ZSTD_VERSION_RELEASE)
unsigned ZSTD_versionNumber (void);


/**************************************
*  Simple one-step functions
**************************************/
size_t ZSTD_compress(   void* dst, size_t maxDstSize,
                  const void* src, size_t srcSize);

size_t ZSTD_decompress( void* dst, size_t maxOriginalSize,
                  const void* src, size_t compressedSize);

/*
ZSTD_compress() :
    Compresses 'srcSize' bytes from buffer 'src' into buffer 'dst', of maximum size 'dstSize'.
    Destination buffer should be sized to handle worst cases situations (input data not compressible).
    Worst case size evaluation is provided by function ZSTD_compressBound().
    return : the number of bytes written into buffer 'dst'
             or an error code if it fails (which can be tested using ZSTD_isError())

ZSTD_decompress() :
    compressedSize : is obviously the source size
    maxOriginalSize : is the size of the 'dst' buffer, which must be already allocated.
                      It must be equal or larger than originalSize, otherwise decompression will fail.
    return : the number of bytes decompressed into destination buffer (originalSize)
             or an errorCode if it fails (which can be tested using ZSTD_isError())
*/


/**************************************
*  Tool functions
**************************************/
size_t      ZSTD_compressBound(size_t srcSize);   /* maximum compressed size */

/* Error Management */
unsigned    ZSTD_isError(size_t code);         /* tells if a return value is an error code */
const char* ZSTD_getErrorName(size_t code);    /* provides error code string (useful for debugging) */


#if defined (__cplusplus)
}
#endif
