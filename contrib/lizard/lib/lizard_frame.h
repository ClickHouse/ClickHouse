/*
   Lizard auto-framing library
   Header File
   Copyright (C) 2011-2015, Yann Collet
   Copyright (C) 2016-2017, Przemyslaw Skibinski

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

/* LizardF is a stand-alone API to create Lizard-compressed frames
 * conformant with specification v1.5.1.
 * All related operations, including memory management, are handled internally by the library.
 * You don't need lizard_compress.h when using lizard_frame.h.
 * */

#pragma once

#if defined (__cplusplus)
extern "C" {
#endif

/*-************************************
*  Includes
**************************************/
#include <stddef.h>   /* size_t */


/*-************************************
*  Error management
**************************************/
typedef size_t LizardF_errorCode_t;

unsigned    LizardF_isError(LizardF_errorCode_t code);
const char* LizardF_getErrorName(LizardF_errorCode_t code);   /* return error code string; useful for debugging */


/*-************************************
*  Frame compression types
**************************************/
//#define LIZARDF_DISABLE_OBSOLETE_ENUMS
#ifndef LIZARDF_DISABLE_OBSOLETE_ENUMS
#  define LIZARDF_OBSOLETE_ENUM(x) ,x
#else
#  define LIZARDF_OBSOLETE_ENUM(x)
#endif

typedef enum {
    LizardF_default=0,
    LizardF_max128KB=1,
    LizardF_max256KB=2,
    LizardF_max1MB=3,
    LizardF_max4MB=4,
    LizardF_max16MB=5,
    LizardF_max64MB=6,
    LizardF_max256MB=7
} LizardF_blockSizeID_t;

typedef enum {
    LizardF_blockLinked=0,
    LizardF_blockIndependent
    LIZARDF_OBSOLETE_ENUM(blockLinked = LizardF_blockLinked)
    LIZARDF_OBSOLETE_ENUM(blockIndependent = LizardF_blockIndependent)
} LizardF_blockMode_t;

typedef enum {
    LizardF_noContentChecksum=0,
    LizardF_contentChecksumEnabled
    LIZARDF_OBSOLETE_ENUM(noContentChecksum = LizardF_noContentChecksum)
    LIZARDF_OBSOLETE_ENUM(contentChecksumEnabled = LizardF_contentChecksumEnabled)
} LizardF_contentChecksum_t;

typedef enum {
    LizardF_frame=0,
    LizardF_skippableFrame
    LIZARDF_OBSOLETE_ENUM(skippableFrame = LizardF_skippableFrame)
} LizardF_frameType_t;

#ifndef LIZARDF_DISABLE_OBSOLETE_ENUMS
typedef LizardF_blockSizeID_t blockSizeID_t;
typedef LizardF_blockMode_t blockMode_t;
typedef LizardF_frameType_t frameType_t;
typedef LizardF_contentChecksum_t contentChecksum_t;
#endif

typedef struct {
  LizardF_blockSizeID_t     blockSizeID;           /* max64KB, max256KB, max1MB, max4MB ; 0 == default */
  LizardF_blockMode_t       blockMode;             /* blockLinked, blockIndependent ; 0 == default */
  LizardF_contentChecksum_t contentChecksumFlag;   /* noContentChecksum, contentChecksumEnabled ; 0 == default  */
  LizardF_frameType_t       frameType;             /* LizardF_frame, skippableFrame ; 0 == default */
  unsigned long long     contentSize;           /* Size of uncompressed (original) content ; 0 == unknown */
  unsigned               reserved[2];           /* must be zero for forward compatibility */
} LizardF_frameInfo_t;

typedef struct {
  LizardF_frameInfo_t frameInfo;
  int      compressionLevel;       /* 0 == default (fast mode); values above 16 count as 16; values below 0 count as 0 */
  unsigned autoFlush;              /* 1 == always flush (reduce need for tmp buffer) */
  unsigned reserved[4];            /* must be zero for forward compatibility */
} LizardF_preferences_t;


/*-*********************************
*  Simple compression function
***********************************/
size_t LizardF_compressFrameBound(size_t srcSize, const LizardF_preferences_t* preferencesPtr);

/*!LizardF_compressFrame() :
 * Compress an entire srcBuffer into a valid Lizard frame, as defined by specification v1.5.1
 * The most important rule is that dstBuffer MUST be large enough (dstMaxSize) to ensure compression completion even in worst case.
 * You can get the minimum value of dstMaxSize by using LizardF_compressFrameBound()
 * If this condition is not respected, LizardF_compressFrame() will fail (result is an errorCode)
 * The LizardF_preferences_t structure is optional : you can provide NULL as argument. All preferences will be set to default.
 * The result of the function is the number of bytes written into dstBuffer.
 * The function outputs an error code if it fails (can be tested using LizardF_isError())
 */
size_t LizardF_compressFrame(void* dstBuffer, size_t dstMaxSize, const void* srcBuffer, size_t srcSize, const LizardF_preferences_t* preferencesPtr);



/*-***********************************
*  Advanced compression functions
*************************************/
typedef struct LizardF_cctx_s* LizardF_compressionContext_t;   /* must be aligned on 8-bytes */

typedef struct {
  unsigned stableSrc;    /* 1 == src content will remain available on future calls to LizardF_compress(); avoid saving src content within tmp buffer as future dictionary */
  unsigned reserved[3];
} LizardF_compressOptions_t;

/* Resource Management */

#define LIZARDF_VERSION 100
LizardF_errorCode_t LizardF_createCompressionContext(LizardF_compressionContext_t* cctxPtr, unsigned version);
LizardF_errorCode_t LizardF_freeCompressionContext(LizardF_compressionContext_t cctx);
/* LizardF_createCompressionContext() :
 * The first thing to do is to create a compressionContext object, which will be used in all compression operations.
 * This is achieved using LizardF_createCompressionContext(), which takes as argument a version and an LizardF_preferences_t structure.
 * The version provided MUST be LIZARDF_VERSION. It is intended to track potential version differences between different binaries.
 * The function will provide a pointer to a fully allocated LizardF_compressionContext_t object.
 * If the result LizardF_errorCode_t is not zero, there was an error during context creation.
 * Object can release its memory using LizardF_freeCompressionContext();
 */


/* Compression */

size_t LizardF_compressBegin(LizardF_compressionContext_t cctx, void* dstBuffer, size_t dstMaxSize, const LizardF_preferences_t* prefsPtr);
/* LizardF_compressBegin() :
 * will write the frame header into dstBuffer.
 * dstBuffer must be large enough to accommodate a header (dstMaxSize). Maximum header size is 15 bytes.
 * The LizardF_preferences_t structure is optional : you can provide NULL as argument, all preferences will then be set to default.
 * The result of the function is the number of bytes written into dstBuffer for the header
 * or an error code (can be tested using LizardF_isError())
 */

size_t LizardF_compressBound(size_t srcSize, const LizardF_preferences_t* prefsPtr);
/* LizardF_compressBound() :
 * Provides the minimum size of Dst buffer given srcSize to handle worst case situations.
 * Different preferences can produce different results.
 * prefsPtr is optional : you can provide NULL as argument, all preferences will then be set to cover worst case.
 * This function includes frame termination cost (4 bytes, or 8 if frame checksum is enabled)
 */

size_t LizardF_compressUpdate(LizardF_compressionContext_t cctx, void* dstBuffer, size_t dstMaxSize, const void* srcBuffer, size_t srcSize, const LizardF_compressOptions_t* cOptPtr);
/* LizardF_compressUpdate()
 * LizardF_compressUpdate() can be called repetitively to compress as much data as necessary.
 * The most important rule is that dstBuffer MUST be large enough (dstMaxSize) to ensure compression completion even in worst case.
 * You can get the minimum value of dstMaxSize by using LizardF_compressBound().
 * If this condition is not respected, LizardF_compress() will fail (result is an errorCode).
 * LizardF_compressUpdate() doesn't guarantee error recovery, so you have to reset compression context when an error occurs.
 * The LizardF_compressOptions_t structure is optional : you can provide NULL as argument.
 * The result of the function is the number of bytes written into dstBuffer : it can be zero, meaning input data was just buffered.
 * The function outputs an error code if it fails (can be tested using LizardF_isError())
 */

size_t LizardF_flush(LizardF_compressionContext_t cctx, void* dstBuffer, size_t dstMaxSize, const LizardF_compressOptions_t* cOptPtr);
/* LizardF_flush()
 * Should you need to generate compressed data immediately, without waiting for the current block to be filled,
 * you can call Lizard_flush(), which will immediately compress any remaining data buffered within cctx.
 * Note that dstMaxSize must be large enough to ensure the operation will be successful.
 * LizardF_compressOptions_t structure is optional : you can provide NULL as argument.
 * The result of the function is the number of bytes written into dstBuffer
 * (it can be zero, this means there was no data left within cctx)
 * The function outputs an error code if it fails (can be tested using LizardF_isError())
 */

size_t LizardF_compressEnd(LizardF_compressionContext_t cctx, void* dstBuffer, size_t dstMaxSize, const LizardF_compressOptions_t* cOptPtr);
/* LizardF_compressEnd()
 * When you want to properly finish the compressed frame, just call LizardF_compressEnd().
 * It will flush whatever data remained within compressionContext (like Lizard_flush())
 * but also properly finalize the frame, with an endMark and a checksum.
 * The result of the function is the number of bytes written into dstBuffer (necessarily >= 4 (endMark), or 8 if optional frame checksum is enabled)
 * The function outputs an error code if it fails (can be tested using LizardF_isError())
 * The LizardF_compressOptions_t structure is optional : you can provide NULL as argument.
 * A successful call to LizardF_compressEnd() makes cctx available again for next compression task.
 */


/*-*********************************
*  Decompression functions
***********************************/

typedef struct LizardF_dctx_s* LizardF_decompressionContext_t;   /* must be aligned on 8-bytes */

typedef struct {
  unsigned stableDst;       /* guarantee that decompressed data will still be there on next function calls (avoid storage into tmp buffers) */
  unsigned reserved[3];
} LizardF_decompressOptions_t;


/* Resource management */

/*!LizardF_createDecompressionContext() :
 * Create an LizardF_decompressionContext_t object, which will be used to track all decompression operations.
 * The version provided MUST be LIZARDF_VERSION. It is intended to track potential breaking differences between different versions.
 * The function will provide a pointer to a fully allocated and initialized LizardF_decompressionContext_t object.
 * The result is an errorCode, which can be tested using LizardF_isError().
 * dctx memory can be released using LizardF_freeDecompressionContext();
 * The result of LizardF_freeDecompressionContext() is indicative of the current state of decompressionContext when being released.
 * That is, it should be == 0 if decompression has been completed fully and correctly.
 */
LizardF_errorCode_t LizardF_createDecompressionContext(LizardF_decompressionContext_t* dctxPtr, unsigned version);
LizardF_errorCode_t LizardF_freeDecompressionContext(LizardF_decompressionContext_t dctx);


/*======   Decompression   ======*/

/*!LizardF_getFrameInfo() :
 * This function decodes frame header information (such as max blockSize, frame checksum, etc.).
 * Its usage is optional. The objective is to extract frame header information, typically for allocation purposes.
 * A header size is variable and can be from 7 to 15 bytes. It's also possible to input more bytes than that.
 * The number of bytes read from srcBuffer will be updated within *srcSizePtr (necessarily <= original value).
 * (note that LizardF_getFrameInfo() can also be used anytime *after* starting decompression, in this case 0 input byte is enough)
 * Frame header info is *copied into* an already allocated LizardF_frameInfo_t structure.
 * The function result is an hint about how many srcSize bytes LizardF_decompress() expects for next call,
 *                        or an error code which can be tested using LizardF_isError()
 *                        (typically, when there is not enough src bytes to fully decode the frame header)
 * Decompression is expected to resume from where it stopped (srcBuffer + *srcSizePtr)
 */
size_t LizardF_getFrameInfo(LizardF_decompressionContext_t dctx,
                         LizardF_frameInfo_t* frameInfoPtr,
                         const void* srcBuffer, size_t* srcSizePtr);

/*!LizardF_decompress() :
 * Call this function repetitively to regenerate data compressed within srcBuffer.
 * The function will attempt to decode *srcSizePtr bytes from srcBuffer, into dstBuffer of maximum size *dstSizePtr.
 *
 * The number of bytes regenerated into dstBuffer will be provided within *dstSizePtr (necessarily <= original value).
 *
 * The number of bytes read from srcBuffer will be provided within *srcSizePtr (necessarily <= original value).
 * If number of bytes read is < number of bytes provided, then decompression operation is not completed.
 * It typically happens when dstBuffer is not large enough to contain all decoded data.
 * LizardF_decompress() must be called again, starting from where it stopped (srcBuffer + *srcSizePtr)
 * The function will check this condition, and refuse to continue if it is not respected.
 *
 * `dstBuffer` is expected to be flushed between each call to the function, its content will be overwritten.
 * `dst` arguments can be changed at will at each consecutive call to the function.
 *
 * The function result is an hint of how many `srcSize` bytes LizardF_decompress() expects for next call.
 * Schematically, it's the size of the current (or remaining) compressed block + header of next block.
 * Respecting the hint provides some boost to performance, since it does skip intermediate buffers.
 * This is just a hint though, it's always possible to provide any srcSize.
 * When a frame is fully decoded, the function result will be 0 (no more data expected).
 * If decompression failed, function result is an error code, which can be tested using LizardF_isError().
 *
 * After a frame is fully decoded, dctx can be used again to decompress another frame.
 */
size_t LizardF_decompress(LizardF_decompressionContext_t dctx,
                       void* dstBuffer, size_t* dstSizePtr,
                       const void* srcBuffer, size_t* srcSizePtr,
                       const LizardF_decompressOptions_t* dOptPtr);



#if defined (__cplusplus)
}
#endif
