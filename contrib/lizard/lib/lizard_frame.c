/*
Lizard auto-framing library
Copyright (C) 2011-2016, Yann Collet
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

/* LizardF is a stand-alone API to create Lizard-compressed Frames
*  in full conformance with specification v1.5.0
*  All related operations, including memory management, are handled by the library.
* */


/*-************************************
*  Compiler Options
**************************************/
#ifdef _MSC_VER    /* Visual Studio */
#  pragma warning(disable : 4127)        /* disable: C4127: conditional expression is constant */
#endif



/*-************************************
*  Includes
**************************************/
#include "lizard_frame_static.h"
#include "lizard_compress.h"
#include "lizard_decompress.h"
#include "lizard_common.h"  /* LIZARD_DICT_SIZE */
#define XXH_STATIC_LINKING_ONLY
#include "xxhash/xxhash.h"
#include <stdio.h>



/* unoptimized version; solves endianess & alignment issues */
static U32 LizardF_readLE32 (const void* src)
{
    const BYTE* const srcPtr = (const BYTE*)src;
    U32 value32 = srcPtr[0];
    value32 += (srcPtr[1]<<8);
    value32 += (srcPtr[2]<<16);
    value32 += ((U32)srcPtr[3])<<24;
    return value32;
}

static void LizardF_writeLE32 (BYTE* dstPtr, U32 value32)
{
    dstPtr[0] = (BYTE)value32;
    dstPtr[1] = (BYTE)(value32 >> 8);
    dstPtr[2] = (BYTE)(value32 >> 16);
    dstPtr[3] = (BYTE)(value32 >> 24);
}

static U64 LizardF_readLE64 (const BYTE* srcPtr)
{
    U64 value64 = srcPtr[0];
    value64 += ((U64)srcPtr[1]<<8);
    value64 += ((U64)srcPtr[2]<<16);
    value64 += ((U64)srcPtr[3]<<24);
    value64 += ((U64)srcPtr[4]<<32);
    value64 += ((U64)srcPtr[5]<<40);
    value64 += ((U64)srcPtr[6]<<48);
    value64 += ((U64)srcPtr[7]<<56);
    return value64;
}

static void LizardF_writeLE64 (BYTE* dstPtr, U64 value64)
{
    dstPtr[0] = (BYTE)value64;
    dstPtr[1] = (BYTE)(value64 >> 8);
    dstPtr[2] = (BYTE)(value64 >> 16);
    dstPtr[3] = (BYTE)(value64 >> 24);
    dstPtr[4] = (BYTE)(value64 >> 32);
    dstPtr[5] = (BYTE)(value64 >> 40);
    dstPtr[6] = (BYTE)(value64 >> 48);
    dstPtr[7] = (BYTE)(value64 >> 56);
}


/*-************************************
*  Constants
**************************************/
#define _1BIT  0x01
#define _2BITS 0x03
#define _3BITS 0x07
#define _4BITS 0x0F
#define _8BITS 0xFF

#define LIZARDF_MAGIC_SKIPPABLE_START  0x184D2A50U
#define LIZARDF_MAGICNUMBER            0x184D2206U
#define LIZARDF_BLOCKUNCOMPRESSED_FLAG 0x80000000U
#define LIZARDF_BLOCKSIZEID_DEFAULT LizardF_max128KB

static const size_t minFHSize = 7;
static const size_t maxFHSize = 15;
static const size_t BHSize = 4;


/*-************************************
*  Structures and local types
**************************************/
typedef struct LizardF_cctx_s
{
    LizardF_preferences_t prefs;
    U32    version;
    U32    cStage;
    size_t maxBlockSize;
    size_t maxBufferSize;
    BYTE*  tmpBuff;
    BYTE*  tmpIn;
    size_t tmpInSize;
    U64    totalInSize;
    XXH32_state_t xxh;
    Lizard_stream_t* lizardCtxPtr;
    U32    lizardCtxLevel;     /* 0: unallocated;  1: Lizard_stream_t;  */
} LizardF_cctx_t;

typedef struct LizardF_dctx_s
{
    LizardF_frameInfo_t frameInfo;
    U32    version;
    U32    dStage;
    U64    frameRemainingSize;
    size_t maxBlockSize;
    size_t maxBufferSize;
    const BYTE* srcExpect;
    BYTE*  tmpIn;
    size_t tmpInSize;
    size_t tmpInTarget;
    BYTE*  tmpOutBuffer;
    const BYTE*  dict;
    size_t dictSize;
    BYTE*  tmpOut;
    size_t tmpOutSize;
    size_t tmpOutStart;
    XXH32_state_t xxh;
    BYTE   header[16];
} LizardF_dctx_t;


/*-************************************
*  Error management
**************************************/
#define LIZARDF_GENERATE_STRING(STRING) #STRING,
static const char* LizardF_errorStrings[] = { LIZARDF_LIST_ERRORS(LIZARDF_GENERATE_STRING) };


unsigned LizardF_isError(LizardF_errorCode_t code)
{
    return (code > (LizardF_errorCode_t)(-LizardF_ERROR_maxCode));
}

const char* LizardF_getErrorName(LizardF_errorCode_t code)
{
    static const char* codeError = "Unspecified error code";
    if (LizardF_isError(code)) return LizardF_errorStrings[-(int)(code)];
    return codeError;
}


/*-************************************
*  Private functions
**************************************/
static size_t LizardF_getBlockSize(unsigned blockSizeID)
{
    static const size_t blockSizes[7] = { 128 KB, 256 KB, 1 MB, 4 MB, 16 MB, 64 MB, 256 MB };

    if (blockSizeID == 0) blockSizeID = LIZARDF_BLOCKSIZEID_DEFAULT;
    blockSizeID -= 1;
    if (blockSizeID >= 7) return (size_t)-LizardF_ERROR_maxBlockSize_invalid;

    return blockSizes[blockSizeID];
}

static LizardF_blockSizeID_t LizardF_optimalBSID(const LizardF_blockSizeID_t requestedBSID, const size_t srcSize)
{
    LizardF_blockSizeID_t proposedBSID = LizardF_max128KB;
    size_t maxBlockSize;

    while (requestedBSID > proposedBSID)
    {
        maxBlockSize = LizardF_getBlockSize(proposedBSID);
        if (srcSize <= maxBlockSize) {
            return proposedBSID;
        }
        proposedBSID = (LizardF_blockSizeID_t)((int)proposedBSID + 1);
    }
    return requestedBSID;
}


static BYTE LizardF_headerChecksum (const void* header, size_t length)
{
    U32 xxh = XXH32(header, length, 0);
    return (BYTE)(xxh >> 8);
}


/*-************************************
*  Simple compression functions
**************************************/

size_t LizardF_compressFrameBound(size_t srcSize, const LizardF_preferences_t* preferencesPtr)
{
    LizardF_preferences_t prefs;
    size_t headerSize;
    size_t streamSize;

    if (preferencesPtr!=NULL) prefs = *preferencesPtr;
    else memset(&prefs, 0, sizeof(prefs));

    prefs.frameInfo.blockSizeID = LizardF_optimalBSID(prefs.frameInfo.blockSizeID, srcSize);
    prefs.autoFlush = 1;

    headerSize = maxFHSize;      /* header size, including magic number and frame content size*/
    streamSize = LizardF_compressBound(srcSize, &prefs);

    return headerSize + streamSize;
}



/*! LizardF_compressFrame() :
* Compress an entire srcBuffer into a valid Lizard frame, as defined by specification v1.5.0, in a single step.
* The most important rule is that dstBuffer MUST be large enough (dstMaxSize) to ensure compression completion even in worst case.
* You can get the minimum value of dstMaxSize by using LizardF_compressFrameBound()
* If this condition is not respected, LizardF_compressFrame() will fail (result is an errorCode)
* The LizardF_preferences_t structure is optional : you can provide NULL as argument. All preferences will then be set to default.
* The result of the function is the number of bytes written into dstBuffer.
* The function outputs an error code if it fails (can be tested using LizardF_isError())
*/
size_t LizardF_compressFrame(void* dstBuffer, size_t dstMaxSize, const void* srcBuffer, size_t srcSize, const LizardF_preferences_t* preferencesPtr)
{
    LizardF_cctx_t cctxI;
    LizardF_preferences_t prefs;
    LizardF_compressOptions_t options;
    LizardF_errorCode_t errorCode;
    BYTE* const dstStart = (BYTE*) dstBuffer;
    BYTE* dstPtr = dstStart;
    BYTE* const dstEnd = dstStart + dstMaxSize;

    memset(&cctxI, 0, sizeof(cctxI));   /* works because no allocation */
    memset(&options, 0, sizeof(options));

    cctxI.version = LIZARDF_VERSION;
    cctxI.maxBufferSize = 5 MB;   /* mess with real buffer size to prevent allocation; works because autoflush==1 & stableSrc==1 */

    if (preferencesPtr!=NULL)
        prefs = *preferencesPtr;
    else
        memset(&prefs, 0, sizeof(prefs));
    if (prefs.frameInfo.contentSize != 0)
        prefs.frameInfo.contentSize = (U64)srcSize;   /* auto-correct content size if selected (!=0) */

    prefs.frameInfo.blockSizeID = LizardF_optimalBSID(prefs.frameInfo.blockSizeID, srcSize);
    prefs.autoFlush = 1;
    if (srcSize <= LizardF_getBlockSize(prefs.frameInfo.blockSizeID))
        prefs.frameInfo.blockMode = LizardF_blockIndependent;   /* no need for linked blocks */

    options.stableSrc = 1;

    if (dstMaxSize < LizardF_compressFrameBound(srcSize, &prefs))
        return (size_t)-LizardF_ERROR_dstMaxSize_tooSmall;

    errorCode = LizardF_compressBegin(&cctxI, dstBuffer, dstMaxSize, &prefs);  /* write header */
    if (LizardF_isError(errorCode)) goto error;
    dstPtr += errorCode;   /* header size */

    errorCode = LizardF_compressUpdate(&cctxI, dstPtr, dstEnd-dstPtr, srcBuffer, srcSize, &options);
    if (LizardF_isError(errorCode)) goto error;
    dstPtr += errorCode;

    errorCode = LizardF_compressEnd(&cctxI, dstPtr, dstEnd-dstPtr, &options);   /* flush last block, and generate suffix */
    if (LizardF_isError(errorCode)) goto error;
    dstPtr += errorCode;

    Lizard_freeStream(cctxI.lizardCtxPtr);
    FREEMEM(cctxI.tmpBuff);
    return (dstPtr - dstStart);
error:
    Lizard_freeStream(cctxI.lizardCtxPtr);
    FREEMEM(cctxI.tmpBuff);
    return errorCode;
}


/*-*********************************
*  Advanced compression functions
***********************************/

/* LizardF_createCompressionContext() :
* The first thing to do is to create a compressionContext object, which will be used in all compression operations.
* This is achieved using LizardF_createCompressionContext(), which takes as argument a version and an LizardF_preferences_t structure.
* The version provided MUST be LIZARDF_VERSION. It is intended to track potential version differences between different binaries.
* The function will provide a pointer to an allocated LizardF_compressionContext_t object.
* If the result LizardF_errorCode_t is not OK_NoError, there was an error during context creation.
* Object can release its memory using LizardF_freeCompressionContext();
*/
LizardF_errorCode_t LizardF_createCompressionContext(LizardF_compressionContext_t* LizardF_compressionContextPtr, unsigned version)
{
    LizardF_cctx_t* cctxPtr;

    cctxPtr = (LizardF_cctx_t*)ALLOCATOR(1, sizeof(LizardF_cctx_t));
    if (cctxPtr==NULL) return (LizardF_errorCode_t)(-LizardF_ERROR_allocation_failed);

    cctxPtr->version = version;
    cctxPtr->cStage = 0;   /* Next stage : write header */

    *LizardF_compressionContextPtr = (LizardF_compressionContext_t)cctxPtr;

    return LizardF_OK_NoError;
}


LizardF_errorCode_t LizardF_freeCompressionContext(LizardF_compressionContext_t LizardF_compressionContext)
{
    LizardF_cctx_t* cctxPtr = (LizardF_cctx_t*)LizardF_compressionContext;

    if (cctxPtr != NULL) {  /* null pointers can be safely provided to this function, like free() */
       Lizard_freeStream(cctxPtr->lizardCtxPtr);
       FREEMEM(cctxPtr->tmpBuff);
       FREEMEM(LizardF_compressionContext);
    }

    return LizardF_OK_NoError;
}


/*! LizardF_compressBegin() :
* will write the frame header into dstBuffer.
* dstBuffer must be large enough to accommodate a header (dstMaxSize). Maximum header size is LizardF_MAXHEADERFRAME_SIZE bytes.
* The result of the function is the number of bytes written into dstBuffer for the header
* or an error code (can be tested using LizardF_isError())
*/
size_t LizardF_compressBegin(LizardF_compressionContext_t compressionContext, void* dstBuffer, size_t dstMaxSize, const LizardF_preferences_t* preferencesPtr)
{
    LizardF_preferences_t prefNull;
    LizardF_cctx_t* cctxPtr = (LizardF_cctx_t*)compressionContext;
    BYTE* const dstStart = (BYTE*)dstBuffer;
    BYTE* dstPtr = dstStart;
    BYTE* headerStart;
    size_t requiredBuffSize;

    if (dstMaxSize < maxFHSize) return (size_t)-LizardF_ERROR_dstMaxSize_tooSmall;
    if (cctxPtr->cStage != 0) return (size_t)-LizardF_ERROR_GENERIC;
    memset(&prefNull, 0, sizeof(prefNull));
    if (preferencesPtr == NULL) preferencesPtr = &prefNull;
    cctxPtr->prefs = *preferencesPtr;

    /* ctx Management */
    if (cctxPtr->lizardCtxLevel == 0) {
        cctxPtr->lizardCtxPtr = Lizard_createStream(cctxPtr->prefs.compressionLevel);
        cctxPtr->lizardCtxLevel = 1;
    }

    /* Buffer Management */
    if (cctxPtr->prefs.frameInfo.blockSizeID == 0) cctxPtr->prefs.frameInfo.blockSizeID = LIZARDF_BLOCKSIZEID_DEFAULT;
    cctxPtr->maxBlockSize = LizardF_getBlockSize(cctxPtr->prefs.frameInfo.blockSizeID);
    requiredBuffSize = cctxPtr->maxBlockSize + ((cctxPtr->prefs.frameInfo.blockMode == LizardF_blockLinked) * 2 * LIZARD_DICT_SIZE);

    if (preferencesPtr->autoFlush)
        requiredBuffSize = (cctxPtr->prefs.frameInfo.blockMode == LizardF_blockLinked) * LIZARD_DICT_SIZE;   /* just needs dict */

    if (cctxPtr->maxBufferSize < requiredBuffSize) {
        cctxPtr->maxBufferSize = requiredBuffSize;
        FREEMEM(cctxPtr->tmpBuff);
        cctxPtr->tmpBuff = (BYTE*)ALLOCATOR(1, requiredBuffSize);
        if (cctxPtr->tmpBuff == NULL) { printf("ERROR in LizardF_compressBegin: Cannot allocate %d MB\n", (int)(requiredBuffSize>>20)); return (size_t)-LizardF_ERROR_allocation_failed; }
    }
    cctxPtr->tmpIn = cctxPtr->tmpBuff;
    cctxPtr->tmpInSize = 0;
    XXH32_reset(&(cctxPtr->xxh), 0);
    cctxPtr->lizardCtxPtr = Lizard_resetStream((Lizard_stream_t*)(cctxPtr->lizardCtxPtr), cctxPtr->prefs.compressionLevel);
    if (!cctxPtr->lizardCtxPtr) return (size_t)-LizardF_ERROR_allocation_failed;

    /* Magic Number */
    LizardF_writeLE32(dstPtr, LIZARDF_MAGICNUMBER);
    dstPtr += 4;
    headerStart = dstPtr;

    /* FLG Byte */
    *dstPtr++ = (BYTE)(((1 & _2BITS) << 6)    /* Version('01') */
        + ((cctxPtr->prefs.frameInfo.blockMode & _1BIT ) << 5)    /* Block mode */
        + ((cctxPtr->prefs.frameInfo.contentChecksumFlag & _1BIT ) << 2)   /* Frame checksum */
        + ((cctxPtr->prefs.frameInfo.contentSize > 0) << 3));   /* Frame content size */
    /* BD Byte */
    *dstPtr++ = (BYTE)((cctxPtr->prefs.frameInfo.blockSizeID & _3BITS) << 4);
    /* Optional Frame content size field */
    if (cctxPtr->prefs.frameInfo.contentSize) {
        LizardF_writeLE64(dstPtr, cctxPtr->prefs.frameInfo.contentSize);
        dstPtr += 8;
        cctxPtr->totalInSize = 0;
    }
    /* CRC Byte */
    *dstPtr = LizardF_headerChecksum(headerStart, dstPtr - headerStart);
    dstPtr++;

    cctxPtr->cStage = 1;   /* header written, now request input data block */

    return (dstPtr - dstStart);
}


/* LizardF_compressBound() : gives the size of Dst buffer given a srcSize to handle worst case situations.
*                        The LizardF_frameInfo_t structure is optional :
*                        you can provide NULL as argument, preferences will then be set to cover worst case situations.
* */
size_t LizardF_compressBound(size_t srcSize, const LizardF_preferences_t* preferencesPtr)
{
    LizardF_preferences_t prefsNull;
    memset(&prefsNull, 0, sizeof(prefsNull));
    prefsNull.frameInfo.contentChecksumFlag = LizardF_contentChecksumEnabled;   /* worst case */
    {   const LizardF_preferences_t* prefsPtr = (preferencesPtr==NULL) ? &prefsNull : preferencesPtr;
        LizardF_blockSizeID_t bid = prefsPtr->frameInfo.blockSizeID;
        size_t blockSize = LizardF_getBlockSize(bid);
        unsigned nbBlocks = (unsigned)(srcSize / blockSize) + 1;
        size_t lastBlockSize = prefsPtr->autoFlush ? srcSize % blockSize : blockSize;
        size_t blockInfo = 4;   /* default, without block CRC option */
        size_t frameEnd = 4 + (prefsPtr->frameInfo.contentChecksumFlag*4);

        return (blockInfo * nbBlocks) + (blockSize * (nbBlocks-1)) + lastBlockSize + frameEnd;;
    }
}


typedef int (*compressFunc_t)(void* ctx, const char* src, char* dst, int srcSize, int dstSize, int level);

static size_t LizardF_compressBlock(void* dst, const void* src, size_t srcSize, compressFunc_t compress, void* lizardctx, int level)
{
    /* compress one block */
    BYTE* cSizePtr = (BYTE*)dst;
    U32 cSize;
    cSize = (U32)compress(lizardctx, (const char*)src, (char*)(cSizePtr+4), (int)(srcSize), (int)(srcSize-1), level);
    LizardF_writeLE32(cSizePtr, cSize);
    if (cSize == 0) {  /* compression failed */
        cSize = (U32)srcSize;
        LizardF_writeLE32(cSizePtr, cSize + LIZARDF_BLOCKUNCOMPRESSED_FLAG);
        memcpy(cSizePtr+4, src, srcSize);
    }
    return cSize + 4;
}



static int LizardF_localLizard_compress_continue(void* ctx, const char* src, char* dst, int srcSize, int dstSize, int level)
{
    (void)level;
    return Lizard_compress_continue((Lizard_stream_t*)ctx, src, dst, srcSize, dstSize);
}

static compressFunc_t LizardF_selectCompression(LizardF_blockMode_t blockMode)
{
    if (blockMode == LizardF_blockIndependent) return Lizard_compress_extState;
    return LizardF_localLizard_compress_continue;
}

static int LizardF_localSaveDict(LizardF_cctx_t* cctxPtr)
{
    return Lizard_saveDict ((Lizard_stream_t*)(cctxPtr->lizardCtxPtr), (char*)(cctxPtr->tmpBuff), LIZARD_DICT_SIZE);
}

typedef enum { notDone, fromTmpBuffer, fromSrcBuffer } LizardF_lastBlockStatus;

/*! LizardF_compressUpdate() :
* LizardF_compressUpdate() can be called repetitively to compress as much data as necessary.
* The most important rule is that dstBuffer MUST be large enough (dstMaxSize) to ensure compression completion even in worst case.
* If this condition is not respected, LizardF_compress() will fail (result is an errorCode)
* You can get the minimum value of dstMaxSize by using LizardF_compressBound()
* The LizardF_compressOptions_t structure is optional : you can provide NULL as argument.
* The result of the function is the number of bytes written into dstBuffer : it can be zero, meaning input data was just buffered.
* The function outputs an error code if it fails (can be tested using LizardF_isError())
*/
size_t LizardF_compressUpdate(LizardF_compressionContext_t compressionContext, void* dstBuffer, size_t dstMaxSize, const void* srcBuffer, size_t srcSize, const LizardF_compressOptions_t* compressOptionsPtr)
{
    LizardF_compressOptions_t cOptionsNull;
    LizardF_cctx_t* cctxPtr = (LizardF_cctx_t*)compressionContext;
    size_t blockSize = cctxPtr->maxBlockSize;
    const BYTE* srcPtr = (const BYTE*)srcBuffer;
    const BYTE* const srcEnd = srcPtr + srcSize;
    BYTE* const dstStart = (BYTE*)dstBuffer;
    BYTE* dstPtr = dstStart;
    LizardF_lastBlockStatus lastBlockCompressed = notDone;
    compressFunc_t compress;


    if (cctxPtr->cStage != 1) return (size_t)-LizardF_ERROR_GENERIC;
    if (dstMaxSize < LizardF_compressBound(srcSize, &(cctxPtr->prefs))) return (size_t)-LizardF_ERROR_dstMaxSize_tooSmall;
    memset(&cOptionsNull, 0, sizeof(cOptionsNull));
    if (compressOptionsPtr == NULL) compressOptionsPtr = &cOptionsNull;

    /* select compression function */
    compress = LizardF_selectCompression(cctxPtr->prefs.frameInfo.blockMode);

    /* complete tmp buffer */
    if (cctxPtr->tmpInSize > 0) {   /* some data already within tmp buffer */
        size_t sizeToCopy = blockSize - cctxPtr->tmpInSize;
        if (sizeToCopy > srcSize) {
            /* add src to tmpIn buffer */
            memcpy(cctxPtr->tmpIn + cctxPtr->tmpInSize, srcBuffer, srcSize);
            srcPtr = srcEnd;
            cctxPtr->tmpInSize += srcSize;
            /* still needs some CRC */
        } else {
            /* complete tmpIn block and then compress it */
            lastBlockCompressed = fromTmpBuffer;
            memcpy(cctxPtr->tmpIn + cctxPtr->tmpInSize, srcBuffer, sizeToCopy);
            srcPtr += sizeToCopy;

            dstPtr += LizardF_compressBlock(dstPtr, cctxPtr->tmpIn, blockSize, compress, cctxPtr->lizardCtxPtr, cctxPtr->prefs.compressionLevel);

            if (cctxPtr->prefs.frameInfo.blockMode==LizardF_blockLinked) cctxPtr->tmpIn += blockSize;
            cctxPtr->tmpInSize = 0;
        }
    }

    while ((size_t)(srcEnd - srcPtr) >= blockSize) {
        /* compress full block */
        lastBlockCompressed = fromSrcBuffer;
        dstPtr += LizardF_compressBlock(dstPtr, srcPtr, blockSize, compress, cctxPtr->lizardCtxPtr, cctxPtr->prefs.compressionLevel);
        srcPtr += blockSize;
    }

    if ((cctxPtr->prefs.autoFlush) && (srcPtr < srcEnd)) {
        /* compress remaining input < blockSize */
        lastBlockCompressed = fromSrcBuffer;
        dstPtr += LizardF_compressBlock(dstPtr, srcPtr, srcEnd - srcPtr, compress, cctxPtr->lizardCtxPtr, cctxPtr->prefs.compressionLevel);
        srcPtr  = srcEnd;
    }

    /* preserve dictionary if necessary */
    if ((cctxPtr->prefs.frameInfo.blockMode==LizardF_blockLinked) && (lastBlockCompressed==fromSrcBuffer)) {
        if (compressOptionsPtr->stableSrc) {
            cctxPtr->tmpIn = cctxPtr->tmpBuff;
        } else {
            int realDictSize = LizardF_localSaveDict(cctxPtr);
            if (realDictSize==0) return (size_t)-LizardF_ERROR_GENERIC;
            cctxPtr->tmpIn = cctxPtr->tmpBuff + realDictSize;
        }
    }

    /* keep tmpIn within limits */
    if ((cctxPtr->tmpIn + blockSize) > (cctxPtr->tmpBuff + cctxPtr->maxBufferSize)   /* necessarily LizardF_blockLinked && lastBlockCompressed==fromTmpBuffer */
        && !(cctxPtr->prefs.autoFlush))
    {
        int realDictSize = LizardF_localSaveDict(cctxPtr);
        cctxPtr->tmpIn = cctxPtr->tmpBuff + realDictSize;
    }

    /* some input data left, necessarily < blockSize */
    if (srcPtr < srcEnd) {
        /* fill tmp buffer */
        size_t sizeToCopy = srcEnd - srcPtr;
        memcpy(cctxPtr->tmpIn, srcPtr, sizeToCopy);
        cctxPtr->tmpInSize = sizeToCopy;
    }

    if (cctxPtr->prefs.frameInfo.contentChecksumFlag == LizardF_contentChecksumEnabled)
        XXH32_update(&(cctxPtr->xxh), srcBuffer, srcSize);

    cctxPtr->totalInSize += srcSize;
    return dstPtr - dstStart;
}


/*! LizardF_flush() :
* Should you need to create compressed data immediately, without waiting for a block to be filled,
* you can call Lizard_flush(), which will immediately compress any remaining data stored within compressionContext.
* The result of the function is the number of bytes written into dstBuffer
* (it can be zero, this means there was no data left within compressionContext)
* The function outputs an error code if it fails (can be tested using LizardF_isError())
* The LizardF_compressOptions_t structure is optional : you can provide NULL as argument.
*/
size_t LizardF_flush(LizardF_compressionContext_t compressionContext, void* dstBuffer, size_t dstMaxSize, const LizardF_compressOptions_t* compressOptionsPtr)
{
    LizardF_cctx_t* cctxPtr = (LizardF_cctx_t*)compressionContext;
    BYTE* const dstStart = (BYTE*)dstBuffer;
    BYTE* dstPtr = dstStart;
    compressFunc_t compress;


    if (cctxPtr->tmpInSize == 0) return 0;   /* nothing to flush */
    if (cctxPtr->cStage != 1) return (size_t)-LizardF_ERROR_GENERIC;
    if (dstMaxSize < (cctxPtr->tmpInSize + 8)) return (size_t)-LizardF_ERROR_dstMaxSize_tooSmall;   /* +8 : block header(4) + block checksum(4) */
    (void)compressOptionsPtr;   /* not yet useful */

    /* select compression function */
    compress = LizardF_selectCompression(cctxPtr->prefs.frameInfo.blockMode);

    /* compress tmp buffer */
    dstPtr += LizardF_compressBlock(dstPtr, cctxPtr->tmpIn, cctxPtr->tmpInSize, compress, cctxPtr->lizardCtxPtr, cctxPtr->prefs.compressionLevel);
    if (cctxPtr->prefs.frameInfo.blockMode==LizardF_blockLinked) cctxPtr->tmpIn += cctxPtr->tmpInSize;
    cctxPtr->tmpInSize = 0;

    /* keep tmpIn within limits */
    if ((cctxPtr->tmpIn + cctxPtr->maxBlockSize) > (cctxPtr->tmpBuff + cctxPtr->maxBufferSize)) {  /* necessarily LizardF_blockLinked */
        int realDictSize = LizardF_localSaveDict(cctxPtr);
        cctxPtr->tmpIn = cctxPtr->tmpBuff + realDictSize;
    }

    return dstPtr - dstStart;
}


/*! LizardF_compressEnd() :
* When you want to properly finish the compressed frame, just call LizardF_compressEnd().
* It will flush whatever data remained within compressionContext (like Lizard_flush())
* but also properly finalize the frame, with an endMark and a checksum.
* The result of the function is the number of bytes written into dstBuffer (necessarily >= 4 (endMark size))
* The function outputs an error code if it fails (can be tested using LizardF_isError())
* The LizardF_compressOptions_t structure is optional : you can provide NULL as argument.
* compressionContext can then be used again, starting with LizardF_compressBegin(). The preferences will remain the same.
*/
size_t LizardF_compressEnd(LizardF_compressionContext_t compressionContext, void* dstBuffer, size_t dstMaxSize, const LizardF_compressOptions_t* compressOptionsPtr)
{
    LizardF_cctx_t* cctxPtr = (LizardF_cctx_t*)compressionContext;
    BYTE* const dstStart = (BYTE*)dstBuffer;
    BYTE* dstPtr = dstStart;
    size_t errorCode;

    errorCode = LizardF_flush(compressionContext, dstBuffer, dstMaxSize, compressOptionsPtr);
    if (LizardF_isError(errorCode)) return errorCode;
    dstPtr += errorCode;

    LizardF_writeLE32(dstPtr, 0);
    dstPtr+=4;   /* endMark */

    if (cctxPtr->prefs.frameInfo.contentChecksumFlag == LizardF_contentChecksumEnabled) {
        U32 xxh = XXH32_digest(&(cctxPtr->xxh));
        LizardF_writeLE32(dstPtr, xxh);
        dstPtr+=4;   /* content Checksum */
    }

    cctxPtr->cStage = 0;   /* state is now re-usable (with identical preferences) */
    cctxPtr->maxBufferSize = 0;  /* reuse HC context */

    if (cctxPtr->prefs.frameInfo.contentSize) {
        if (cctxPtr->prefs.frameInfo.contentSize != cctxPtr->totalInSize)
            return (size_t)-LizardF_ERROR_frameSize_wrong;
    }

    return dstPtr - dstStart;
}


/*-***************************************************
*   Frame Decompression
*****************************************************/

/* Resource management */

/*! LizardF_createDecompressionContext() :
*   Create a decompressionContext object, which will track all decompression operations.
*   Provides a pointer to a fully allocated and initialized LizardF_decompressionContext object.
*   Object can later be released using LizardF_freeDecompressionContext().
*   @return : if != 0, there was an error during context creation.
*/
LizardF_errorCode_t LizardF_createDecompressionContext(LizardF_decompressionContext_t* LizardF_decompressionContextPtr, unsigned versionNumber)
{
    LizardF_dctx_t* const dctxPtr = (LizardF_dctx_t*)ALLOCATOR(1, sizeof(LizardF_dctx_t));
    if (dctxPtr==NULL) return (LizardF_errorCode_t)-LizardF_ERROR_GENERIC;

    dctxPtr->version = versionNumber;
    *LizardF_decompressionContextPtr = (LizardF_decompressionContext_t)dctxPtr;
    return LizardF_OK_NoError;
}

LizardF_errorCode_t LizardF_freeDecompressionContext(LizardF_decompressionContext_t LizardF_decompressionContext)
{
    LizardF_errorCode_t result = LizardF_OK_NoError;
    LizardF_dctx_t* const dctxPtr = (LizardF_dctx_t*)LizardF_decompressionContext;
    if (dctxPtr != NULL) {   /* can accept NULL input, like free() */
      result = (LizardF_errorCode_t)dctxPtr->dStage;
      FREEMEM(dctxPtr->tmpIn);
      FREEMEM(dctxPtr->tmpOutBuffer);
      FREEMEM(dctxPtr);
    }
    return result;
}


/* ******************************************************************** */
/* ********************* Decompression ******************************** */
/* ******************************************************************** */

typedef enum { dstage_getHeader=0, dstage_storeHeader,
    dstage_getCBlockSize, dstage_storeCBlockSize,
    dstage_copyDirect,
    dstage_getCBlock, dstage_storeCBlock,
    dstage_decodeCBlock, dstage_decodeCBlock_intoDst,
    dstage_decodeCBlock_intoTmp, dstage_flushOut,
    dstage_getSuffix, dstage_storeSuffix,
    dstage_getSFrameSize, dstage_storeSFrameSize,
    dstage_skipSkippable
} dStage_t;


/*! LizardF_headerSize() :
*   @return : size of frame header
*             or an error code, which can be tested using LizardF_isError()
*/
static size_t LizardF_headerSize(const void* src, size_t srcSize)
{
    /* minimal srcSize to determine header size */
    if (srcSize < 5) return (size_t)-LizardF_ERROR_frameHeader_incomplete;

    /* special case : skippable frames */
    if ((LizardF_readLE32(src) & 0xFFFFFFF0U) == LIZARDF_MAGIC_SKIPPABLE_START) return 8;

    /* control magic number */
    if (LizardF_readLE32(src) != LIZARDF_MAGICNUMBER) return (size_t)-LizardF_ERROR_frameType_unknown;

    /* Frame Header Size */
    {   BYTE const FLG = ((const BYTE*)src)[4];
        U32 const contentSizeFlag = (FLG>>3) & _1BIT;
        return contentSizeFlag ? maxFHSize : minFHSize;
    }
}


/*! LizardF_decodeHeader() :
   input   : `srcVoidPtr` points at the **beginning of the frame**
   output  : set internal values of dctx, such as
             dctxPtr->frameInfo and dctxPtr->dStage.
             Also allocates internal buffers.
   @return : nb Bytes read from srcVoidPtr (necessarily <= srcSize)
             or an error code (testable with LizardF_isError())
*/
static size_t LizardF_decodeHeader(LizardF_dctx_t* dctxPtr, const void* srcVoidPtr, size_t srcSize)
{
    BYTE FLG, BD, HC;
    unsigned version, blockMode, blockChecksumFlag, contentSizeFlag, contentChecksumFlag, blockSizeID;
    size_t bufferNeeded, currentBlockSize;
    size_t frameHeaderSize;
    const BYTE* srcPtr = (const BYTE*)srcVoidPtr;

    /* need to decode header to get frameInfo */
    if (srcSize < minFHSize) return (size_t)-LizardF_ERROR_frameHeader_incomplete;   /* minimal frame header size */
    memset(&(dctxPtr->frameInfo), 0, sizeof(dctxPtr->frameInfo));

    /* special case : skippable frames */
    if ((LizardF_readLE32(srcPtr) & 0xFFFFFFF0U) == LIZARDF_MAGIC_SKIPPABLE_START) {
        dctxPtr->frameInfo.frameType = LizardF_skippableFrame;
        if (srcVoidPtr == (void*)(dctxPtr->header)) {
            dctxPtr->tmpInSize = srcSize;
            dctxPtr->tmpInTarget = 8;
            dctxPtr->dStage = dstage_storeSFrameSize;
            return srcSize;
        } else {
            dctxPtr->dStage = dstage_getSFrameSize;
            return 4;
        }
    }

    /* control magic number */
    if (LizardF_readLE32(srcPtr) != LIZARDF_MAGICNUMBER) return (size_t)-LizardF_ERROR_frameType_unknown;
    dctxPtr->frameInfo.frameType = LizardF_frame;

    /* Flags */
    FLG = srcPtr[4];
    version = (FLG>>6) & _2BITS;
    blockMode = (FLG>>5) & _1BIT;
    blockChecksumFlag = (FLG>>4) & _1BIT;
    contentSizeFlag = (FLG>>3) & _1BIT;
    contentChecksumFlag = (FLG>>2) & _1BIT;

    /* Frame Header Size */
    frameHeaderSize = contentSizeFlag ? maxFHSize : minFHSize;

    if (srcSize < frameHeaderSize) {
        /* not enough input to fully decode frame header */
        if (srcPtr != dctxPtr->header)
            memcpy(dctxPtr->header, srcPtr, srcSize);
        dctxPtr->tmpInSize = srcSize;
        dctxPtr->tmpInTarget = frameHeaderSize;
        dctxPtr->dStage = dstage_storeHeader;
        return srcSize;
    }

    BD = srcPtr[5];
    blockSizeID = (BD>>4) & _3BITS;

    /* validate */
    if (version != 1) return (size_t)-LizardF_ERROR_headerVersion_wrong;        /* Version Number, only supported value */
    if (blockChecksumFlag != 0) return (size_t)-LizardF_ERROR_blockChecksum_unsupported; /* Not supported for the time being */
    if (((FLG>>0)&_2BITS) != 0) return (size_t)-LizardF_ERROR_reservedFlag_set; /* Reserved bits */
    if (((BD>>7)&_1BIT) != 0) return (size_t)-LizardF_ERROR_reservedFlag_set;   /* Reserved bit */
    if (blockSizeID < 1) return (size_t)-LizardF_ERROR_maxBlockSize_invalid;    /* 1-7 only supported values for the time being */
    if (((BD>>0)&_4BITS) != 0) return (size_t)-LizardF_ERROR_reservedFlag_set;  /* Reserved bits */

    /* check */
    HC = LizardF_headerChecksum(srcPtr+4, frameHeaderSize-5);
    if (HC != srcPtr[frameHeaderSize-1]) return (size_t)-LizardF_ERROR_headerChecksum_invalid;   /* Bad header checksum error */

    /* save */
    dctxPtr->frameInfo.blockMode = (LizardF_blockMode_t)blockMode;
    dctxPtr->frameInfo.contentChecksumFlag = (LizardF_contentChecksum_t)contentChecksumFlag;
    dctxPtr->frameInfo.blockSizeID = (LizardF_blockSizeID_t)blockSizeID;
    currentBlockSize = dctxPtr->maxBlockSize;
    dctxPtr->maxBlockSize = LizardF_getBlockSize(blockSizeID);
    if (contentSizeFlag)
        dctxPtr->frameRemainingSize = dctxPtr->frameInfo.contentSize = LizardF_readLE64(srcPtr+6);

    /* init */
    if (contentChecksumFlag) XXH32_reset(&(dctxPtr->xxh), 0);

    /* alloc */
    bufferNeeded = dctxPtr->maxBlockSize + ((dctxPtr->frameInfo.blockMode==LizardF_blockLinked) * 2 * LIZARD_DICT_SIZE);
    if (bufferNeeded > dctxPtr->maxBufferSize || dctxPtr->maxBlockSize > currentBlockSize) {   /* tmp buffers too small */
        FREEMEM(dctxPtr->tmpIn);
        FREEMEM(dctxPtr->tmpOutBuffer);
        dctxPtr->maxBufferSize = 0;
        dctxPtr->tmpIn = (BYTE*)ALLOCATOR(1, dctxPtr->maxBlockSize);
        if (dctxPtr->tmpIn == NULL) return (size_t)-LizardF_ERROR_GENERIC;
        dctxPtr->tmpOutBuffer= (BYTE*)ALLOCATOR(1, bufferNeeded);
        if (dctxPtr->tmpOutBuffer== NULL) return (size_t)-LizardF_ERROR_GENERIC;
        dctxPtr->maxBufferSize = bufferNeeded;
    }
    dctxPtr->tmpInSize = 0;
    dctxPtr->tmpInTarget = 0;
    dctxPtr->dict = dctxPtr->tmpOutBuffer;
    dctxPtr->dictSize = 0;
    dctxPtr->tmpOut = dctxPtr->tmpOutBuffer;
    dctxPtr->tmpOutStart = 0;
    dctxPtr->tmpOutSize = 0;

    dctxPtr->dStage = dstage_getCBlockSize;

    return frameHeaderSize;
}


/*! LizardF_getFrameInfo() :
*   Decodes frame header information, such as blockSize.
*   It is optional : you could start by calling directly LizardF_decompress() instead.
*   The objective is to extract header information without starting decompression, typically for allocation purposes.
*   LizardF_getFrameInfo() can also be used *after* starting decompression, on a valid LizardF_decompressionContext_t.
*   The number of bytes read from srcBuffer will be provided within *srcSizePtr (necessarily <= original value).
*   You are expected to resume decompression from where it stopped (srcBuffer + *srcSizePtr)
*   @return : hint of the better `srcSize` to use for next call to LizardF_decompress,
*             or an error code which can be tested using LizardF_isError().
*/
LizardF_errorCode_t LizardF_getFrameInfo(LizardF_decompressionContext_t dCtx, LizardF_frameInfo_t* frameInfoPtr,
                                   const void* srcBuffer, size_t* srcSizePtr)
{
    LizardF_dctx_t* dctxPtr = (LizardF_dctx_t*)dCtx;

    if (dctxPtr->dStage > dstage_storeHeader) {  /* note : requires dstage_* header related to be at beginning of enum */
        /* frameInfo already decoded */
        size_t o=0, i=0;
        *srcSizePtr = 0;
        *frameInfoPtr = dctxPtr->frameInfo;
        return LizardF_decompress(dCtx, NULL, &o, NULL, &i, NULL);  /* returns : recommended nb of bytes for LizardF_decompress() */
    } else {
        size_t nextSrcSize, o=0;
        size_t const hSize = LizardF_headerSize(srcBuffer, *srcSizePtr);
        if (LizardF_isError(hSize)) { *srcSizePtr=0; return hSize; }
        if (*srcSizePtr < hSize) { *srcSizePtr=0; return (size_t)-LizardF_ERROR_frameHeader_incomplete; }

        *srcSizePtr = hSize;
        nextSrcSize = LizardF_decompress(dCtx, NULL, &o, srcBuffer, srcSizePtr, NULL);
        if (dctxPtr->dStage <= dstage_storeHeader) return (size_t)-LizardF_ERROR_frameHeader_incomplete; /* should not happen, already checked */
        *frameInfoPtr = dctxPtr->frameInfo;
        return nextSrcSize;
    }
}


/* trivial redirector, for common prototype */
static int LizardF_decompress_safe (const char* source, char* dest, int compressedSize, int maxDecompressedSize, const char* dictStart, int dictSize)
{
    (void)dictStart; (void)dictSize;
    return Lizard_decompress_safe (source, dest, compressedSize, maxDecompressedSize);
}


static void LizardF_updateDict(LizardF_dctx_t* dctxPtr, const BYTE* dstPtr, size_t dstSize, const BYTE* dstPtr0, unsigned withinTmp)
{
    if (dctxPtr->dictSize==0)
        dctxPtr->dict = (const BYTE*)dstPtr;   /* priority to dictionary continuity */

    if (dctxPtr->dict + dctxPtr->dictSize == dstPtr) {  /* dictionary continuity */
        dctxPtr->dictSize += dstSize;
        return;
    }

    if (dstPtr - dstPtr0 + dstSize >= LIZARD_DICT_SIZE) {  /* dstBuffer large enough to become dictionary */
        dctxPtr->dict = (const BYTE*)dstPtr0;
        dctxPtr->dictSize = dstPtr - dstPtr0 + dstSize;
        return;
    }

    if ((withinTmp) && (dctxPtr->dict == dctxPtr->tmpOutBuffer)) {
        /* assumption : dctxPtr->dict + dctxPtr->dictSize == dctxPtr->tmpOut + dctxPtr->tmpOutStart */
        dctxPtr->dictSize += dstSize;
        return;
    }

    if (withinTmp) { /* copy relevant dict portion in front of tmpOut within tmpOutBuffer */
        size_t preserveSize = dctxPtr->tmpOut - dctxPtr->tmpOutBuffer;
        size_t copySize = LIZARD_DICT_SIZE - dctxPtr->tmpOutSize;
        const BYTE* oldDictEnd = dctxPtr->dict + dctxPtr->dictSize - dctxPtr->tmpOutStart;
        if (dctxPtr->tmpOutSize > LIZARD_DICT_SIZE) copySize = 0;
        if (copySize > preserveSize) copySize = preserveSize;

        memcpy(dctxPtr->tmpOutBuffer + preserveSize - copySize, oldDictEnd - copySize, copySize);

        dctxPtr->dict = dctxPtr->tmpOutBuffer;
        dctxPtr->dictSize = preserveSize + dctxPtr->tmpOutStart + dstSize;
        return;
    }

    if (dctxPtr->dict == dctxPtr->tmpOutBuffer) {    /* copy dst into tmp to complete dict */
        if (dctxPtr->dictSize + dstSize > dctxPtr->maxBufferSize) {  /* tmp buffer not large enough */
            size_t preserveSize = LIZARD_DICT_SIZE - dstSize;   /* note : dstSize < LIZARD_DICT_SIZE */
            memcpy(dctxPtr->tmpOutBuffer, dctxPtr->dict + dctxPtr->dictSize - preserveSize, preserveSize);
            dctxPtr->dictSize = preserveSize;
        }
        memcpy(dctxPtr->tmpOutBuffer + dctxPtr->dictSize, dstPtr, dstSize);
        dctxPtr->dictSize += dstSize;
        return;
    }

    /* join dict & dest into tmp */
    {   size_t preserveSize = LIZARD_DICT_SIZE - dstSize;   /* note : dstSize < LIZARD_DICT_SIZE */
        if (preserveSize > dctxPtr->dictSize) preserveSize = dctxPtr->dictSize;
        memcpy(dctxPtr->tmpOutBuffer, dctxPtr->dict + dctxPtr->dictSize - preserveSize, preserveSize);
        memcpy(dctxPtr->tmpOutBuffer + preserveSize, dstPtr, dstSize);
        dctxPtr->dict = dctxPtr->tmpOutBuffer;
        dctxPtr->dictSize = preserveSize + dstSize;
    }
}



/*! LizardF_decompress() :
* Call this function repetitively to regenerate data compressed within srcBuffer.
* The function will attempt to decode *srcSizePtr from srcBuffer, into dstBuffer of maximum size *dstSizePtr.
*
* The number of bytes regenerated into dstBuffer will be provided within *dstSizePtr (necessarily <= original value).
*
* The number of bytes effectively read from srcBuffer will be provided within *srcSizePtr (necessarily <= original value).
* If the number of bytes read is < number of bytes provided, then the decompression operation is not complete.
* You will have to call it again, continuing from where it stopped.
*
* The function result is an hint of the better srcSize to use for next call to LizardF_decompress.
* Basically, it's the size of the current (or remaining) compressed block + header of next block.
* Respecting the hint provides some boost to performance, since it allows less buffer shuffling.
* Note that this is just a hint, you can always provide any srcSize you want.
* When a frame is fully decoded, the function result will be 0.
* If decompression failed, function result is an error code which can be tested using LizardF_isError().
*/
size_t LizardF_decompress(LizardF_decompressionContext_t decompressionContext,
                       void* dstBuffer, size_t* dstSizePtr,
                       const void* srcBuffer, size_t* srcSizePtr,
                       const LizardF_decompressOptions_t* decompressOptionsPtr)
{
    LizardF_dctx_t* dctxPtr = (LizardF_dctx_t*)decompressionContext;
    LizardF_decompressOptions_t optionsNull;
    const BYTE* const srcStart = (const BYTE*)srcBuffer;
    const BYTE* const srcEnd = srcStart + *srcSizePtr;
    const BYTE* srcPtr = srcStart;
    BYTE* const dstStart = (BYTE*)dstBuffer;
    BYTE* const dstEnd = dstStart + *dstSizePtr;
    BYTE* dstPtr = dstStart;
    const BYTE* selectedIn = NULL;
    unsigned doAnotherStage = 1;
    size_t nextSrcSizeHint = 1;


    memset(&optionsNull, 0, sizeof(optionsNull));
    if (decompressOptionsPtr==NULL) decompressOptionsPtr = &optionsNull;
    *srcSizePtr = 0;
    *dstSizePtr = 0;

    /* expect to continue decoding src buffer where it left previously */
    if (dctxPtr->srcExpect != NULL) {
        if (srcStart != dctxPtr->srcExpect) return (size_t)-LizardF_ERROR_srcPtr_wrong;
    }

    /* programmed as a state machine */

    while (doAnotherStage) {

        switch(dctxPtr->dStage)
        {

        case dstage_getHeader:
            if ((size_t)(srcEnd-srcPtr) >= maxFHSize) {  /* enough to decode - shortcut */
                LizardF_errorCode_t const hSize = LizardF_decodeHeader(dctxPtr, srcPtr, srcEnd-srcPtr);
                if (LizardF_isError(hSize)) return hSize;
                srcPtr += hSize;
                break;
            }
            dctxPtr->tmpInSize = 0;
            dctxPtr->tmpInTarget = minFHSize;   /* minimum to attempt decode */
            dctxPtr->dStage = dstage_storeHeader;
            /* fallthrough */

        case dstage_storeHeader:
            {   size_t sizeToCopy = dctxPtr->tmpInTarget - dctxPtr->tmpInSize;
                if (sizeToCopy > (size_t)(srcEnd - srcPtr)) sizeToCopy =  srcEnd - srcPtr;
                memcpy(dctxPtr->header + dctxPtr->tmpInSize, srcPtr, sizeToCopy);
                dctxPtr->tmpInSize += sizeToCopy;
                srcPtr += sizeToCopy;
                if (dctxPtr->tmpInSize < dctxPtr->tmpInTarget) {
                    nextSrcSizeHint = (dctxPtr->tmpInTarget - dctxPtr->tmpInSize) + BHSize;   /* rest of header + nextBlockHeader */
                    doAnotherStage = 0;   /* not enough src data, ask for some more */
                    break;
                }
                {   LizardF_errorCode_t const hSize = LizardF_decodeHeader(dctxPtr, dctxPtr->header, dctxPtr->tmpInTarget);
                    if (LizardF_isError(hSize)) return hSize;
                }
                break;
            }

        case dstage_getCBlockSize:
            if ((size_t)(srcEnd - srcPtr) >= BHSize) {
                selectedIn = srcPtr;
                srcPtr += BHSize;
            } else {
                /* not enough input to read cBlockSize field */
                dctxPtr->tmpInSize = 0;
                dctxPtr->dStage = dstage_storeCBlockSize;
            }

            if (dctxPtr->dStage == dstage_storeCBlockSize)   /* can be skipped */
        case dstage_storeCBlockSize:
            {
                size_t sizeToCopy = BHSize - dctxPtr->tmpInSize;
                if (sizeToCopy > (size_t)(srcEnd - srcPtr)) sizeToCopy = srcEnd - srcPtr;
                memcpy(dctxPtr->tmpIn + dctxPtr->tmpInSize, srcPtr, sizeToCopy);
                srcPtr += sizeToCopy;
                dctxPtr->tmpInSize += sizeToCopy;
                if (dctxPtr->tmpInSize < BHSize) {   /* not enough input to get full cBlockSize; wait for more */
                    nextSrcSizeHint = BHSize - dctxPtr->tmpInSize;
                    doAnotherStage  = 0;
                    break;
                }
                selectedIn = dctxPtr->tmpIn;
            }

        /* case dstage_decodeCBlockSize: */   /* no more direct access, to prevent scan-build warning */
            {   size_t const nextCBlockSize = LizardF_readLE32(selectedIn) & 0x7FFFFFFFU;
                if (nextCBlockSize==0) {  /* frameEnd signal, no more CBlock */
                    dctxPtr->dStage = dstage_getSuffix;
                    break;
                }
                if (nextCBlockSize > dctxPtr->maxBlockSize) return (size_t)-LizardF_ERROR_GENERIC;   /* invalid cBlockSize */
                dctxPtr->tmpInTarget = nextCBlockSize;
                if (LizardF_readLE32(selectedIn) & LIZARDF_BLOCKUNCOMPRESSED_FLAG) {
                    dctxPtr->dStage = dstage_copyDirect;
                    break;
                }
                dctxPtr->dStage = dstage_getCBlock;
                if (dstPtr==dstEnd) {
                    nextSrcSizeHint = nextCBlockSize + BHSize;
                    doAnotherStage = 0;
                }
                break;
            }

        case dstage_copyDirect:   /* uncompressed block */
            {   size_t sizeToCopy = dctxPtr->tmpInTarget;
                if ((size_t)(srcEnd-srcPtr) < sizeToCopy) sizeToCopy = srcEnd - srcPtr;  /* not enough input to read full block */
                if ((size_t)(dstEnd-dstPtr) < sizeToCopy) sizeToCopy = dstEnd - dstPtr;
                memcpy(dstPtr, srcPtr, sizeToCopy);
                if (dctxPtr->frameInfo.contentChecksumFlag) XXH32_update(&(dctxPtr->xxh), srcPtr, sizeToCopy);
                if (dctxPtr->frameInfo.contentSize) dctxPtr->frameRemainingSize -= sizeToCopy;

                /* dictionary management */
                if (dctxPtr->frameInfo.blockMode==LizardF_blockLinked)
                    LizardF_updateDict(dctxPtr, dstPtr, sizeToCopy, dstStart, 0);

                srcPtr += sizeToCopy;
                dstPtr += sizeToCopy;
                if (sizeToCopy == dctxPtr->tmpInTarget) {  /* all copied */
                    dctxPtr->dStage = dstage_getCBlockSize;
                    break;
                }
                dctxPtr->tmpInTarget -= sizeToCopy;   /* still need to copy more */
                nextSrcSizeHint = dctxPtr->tmpInTarget + BHSize;
                doAnotherStage = 0;
                break;
            }

        case dstage_getCBlock:   /* entry from dstage_decodeCBlockSize */
            if ((size_t)(srcEnd-srcPtr) < dctxPtr->tmpInTarget) {
                dctxPtr->tmpInSize = 0;
                dctxPtr->dStage = dstage_storeCBlock;
                break;
            }
            selectedIn = srcPtr;
            srcPtr += dctxPtr->tmpInTarget;
            dctxPtr->dStage = dstage_decodeCBlock;
            break;

        case dstage_storeCBlock:
            {   size_t sizeToCopy = dctxPtr->tmpInTarget - dctxPtr->tmpInSize;
                if (sizeToCopy > (size_t)(srcEnd-srcPtr)) sizeToCopy = srcEnd-srcPtr;
                memcpy(dctxPtr->tmpIn + dctxPtr->tmpInSize, srcPtr, sizeToCopy);
                dctxPtr->tmpInSize += sizeToCopy;
                srcPtr += sizeToCopy;
                if (dctxPtr->tmpInSize < dctxPtr->tmpInTarget) { /* need more input */
                    nextSrcSizeHint = (dctxPtr->tmpInTarget - dctxPtr->tmpInSize) + BHSize;
                    doAnotherStage=0;
                    break;
                }
                selectedIn = dctxPtr->tmpIn;
                dctxPtr->dStage = dstage_decodeCBlock;
            }
            /* fallthrough */

        case dstage_decodeCBlock:
            if ((size_t)(dstEnd-dstPtr) < dctxPtr->maxBlockSize)   /* not enough place into dst : decode into tmpOut */
                dctxPtr->dStage = dstage_decodeCBlock_intoTmp;
            else
                dctxPtr->dStage = dstage_decodeCBlock_intoDst;
            break;

        case dstage_decodeCBlock_intoDst:
            {   int (*decoder)(const char*, char*, int, int, const char*, int);
                int decodedSize;

                if (dctxPtr->frameInfo.blockMode == LizardF_blockLinked)
                    decoder = Lizard_decompress_safe_usingDict;
                else
                    decoder = LizardF_decompress_safe;

                decodedSize = decoder((const char*)selectedIn, (char*)dstPtr, (int)dctxPtr->tmpInTarget, (int)dctxPtr->maxBlockSize, (const char*)dctxPtr->dict, (int)dctxPtr->dictSize);
                if (decodedSize < 0) return (size_t)-LizardF_ERROR_GENERIC;   /* decompression failed */
                if (dctxPtr->frameInfo.contentChecksumFlag) XXH32_update(&(dctxPtr->xxh), dstPtr, decodedSize);
                if (dctxPtr->frameInfo.contentSize) dctxPtr->frameRemainingSize -= decodedSize;

                /* dictionary management */
                if (dctxPtr->frameInfo.blockMode==LizardF_blockLinked)
                    LizardF_updateDict(dctxPtr, dstPtr, decodedSize, dstStart, 0);

                dstPtr += decodedSize;
                dctxPtr->dStage = dstage_getCBlockSize;
                break;
            }

        case dstage_decodeCBlock_intoTmp:
            /* not enough place into dst : decode into tmpOut */
            {   int (*decoder)(const char*, char*, int, int, const char*, int);
                int decodedSize;

                if (dctxPtr->frameInfo.blockMode == LizardF_blockLinked)
                    decoder = Lizard_decompress_safe_usingDict;
                else
                    decoder = LizardF_decompress_safe;

                /* ensure enough place for tmpOut */
                if (dctxPtr->frameInfo.blockMode == LizardF_blockLinked) {
                    if (dctxPtr->dict == dctxPtr->tmpOutBuffer) {
                        if (dctxPtr->dictSize > 2 * LIZARD_DICT_SIZE) {
                            memcpy(dctxPtr->tmpOutBuffer, dctxPtr->dict + dctxPtr->dictSize - LIZARD_DICT_SIZE, LIZARD_DICT_SIZE);
                            dctxPtr->dictSize = LIZARD_DICT_SIZE;
                        }
                        dctxPtr->tmpOut = dctxPtr->tmpOutBuffer + dctxPtr->dictSize;
                    } else {  /* dict not within tmp */
                        size_t reservedDictSpace = dctxPtr->dictSize;
                        if (reservedDictSpace > LIZARD_DICT_SIZE) reservedDictSpace = LIZARD_DICT_SIZE;
                        dctxPtr->tmpOut = dctxPtr->tmpOutBuffer + reservedDictSpace;
                    }
                }

                /* Decode */
                decodedSize = decoder((const char*)selectedIn, (char*)dctxPtr->tmpOut, (int)dctxPtr->tmpInTarget, (int)dctxPtr->maxBlockSize, (const char*)dctxPtr->dict, (int)dctxPtr->dictSize);
                if (decodedSize < 0) return (size_t)-LizardF_ERROR_decompressionFailed;   /* decompression failed */
                if (dctxPtr->frameInfo.contentChecksumFlag) XXH32_update(&(dctxPtr->xxh), dctxPtr->tmpOut, decodedSize);
                if (dctxPtr->frameInfo.contentSize) dctxPtr->frameRemainingSize -= decodedSize;
                dctxPtr->tmpOutSize = decodedSize;
                dctxPtr->tmpOutStart = 0;
                dctxPtr->dStage = dstage_flushOut;
                break;
            }

        case dstage_flushOut:  /* flush decoded data from tmpOut to dstBuffer */
            {   size_t sizeToCopy = dctxPtr->tmpOutSize - dctxPtr->tmpOutStart;
                if (sizeToCopy > (size_t)(dstEnd-dstPtr)) sizeToCopy = dstEnd-dstPtr;
                memcpy(dstPtr, dctxPtr->tmpOut + dctxPtr->tmpOutStart, sizeToCopy);

                /* dictionary management */
                if (dctxPtr->frameInfo.blockMode==LizardF_blockLinked)
                    LizardF_updateDict(dctxPtr, dstPtr, sizeToCopy, dstStart, 1);

                dctxPtr->tmpOutStart += sizeToCopy;
                dstPtr += sizeToCopy;

                /* end of flush ? */
                if (dctxPtr->tmpOutStart == dctxPtr->tmpOutSize) {
                    dctxPtr->dStage = dstage_getCBlockSize;
                    break;
                }
                nextSrcSizeHint = BHSize;
                doAnotherStage = 0;   /* still some data to flush */
                break;
            }

        case dstage_getSuffix:
            {   size_t const suffixSize = dctxPtr->frameInfo.contentChecksumFlag * 4;
                if (dctxPtr->frameRemainingSize) return (size_t)-LizardF_ERROR_frameSize_wrong;   /* incorrect frame size decoded */
                if (suffixSize == 0) {  /* frame completed */
                    nextSrcSizeHint = 0;
                    dctxPtr->dStage = dstage_getHeader;
                    doAnotherStage = 0;
                    break;
                }
                if ((srcEnd - srcPtr) < 4) {  /* not enough size for entire CRC */
                    dctxPtr->tmpInSize = 0;
                    dctxPtr->dStage = dstage_storeSuffix;
                } else {
                    selectedIn = srcPtr;
                    srcPtr += 4;
                }
            }

            if (dctxPtr->dStage == dstage_storeSuffix)   /* can be skipped */
        case dstage_storeSuffix:
            {
                size_t sizeToCopy = 4 - dctxPtr->tmpInSize;
                if (sizeToCopy > (size_t)(srcEnd - srcPtr)) sizeToCopy = srcEnd - srcPtr;
                memcpy(dctxPtr->tmpIn + dctxPtr->tmpInSize, srcPtr, sizeToCopy);
                srcPtr += sizeToCopy;
                dctxPtr->tmpInSize += sizeToCopy;
                if (dctxPtr->tmpInSize < 4) { /* not enough input to read complete suffix */
                    nextSrcSizeHint = 4 - dctxPtr->tmpInSize;
                    doAnotherStage=0;
                    break;
                }
                selectedIn = dctxPtr->tmpIn;
            }

        /* case dstage_checkSuffix: */   /* no direct call, to avoid scan-build warning */
            {   U32 const readCRC = LizardF_readLE32(selectedIn);
                U32 const resultCRC = XXH32_digest(&(dctxPtr->xxh));
                if (readCRC != resultCRC) return (size_t)-LizardF_ERROR_contentChecksum_invalid;
                nextSrcSizeHint = 0;
                dctxPtr->dStage = dstage_getHeader;
                doAnotherStage = 0;
                break;
            }

        case dstage_getSFrameSize:
            if ((srcEnd - srcPtr) >= 4) {
                selectedIn = srcPtr;
                srcPtr += 4;
            } else {
                /* not enough input to read cBlockSize field */
                dctxPtr->tmpInSize = 4;
                dctxPtr->tmpInTarget = 8;
                dctxPtr->dStage = dstage_storeSFrameSize;
            }

            if (dctxPtr->dStage == dstage_storeSFrameSize)
        case dstage_storeSFrameSize:
            {
                size_t sizeToCopy = dctxPtr->tmpInTarget - dctxPtr->tmpInSize;
                if (sizeToCopy > (size_t)(srcEnd - srcPtr)) sizeToCopy = srcEnd - srcPtr;
                memcpy(dctxPtr->header + dctxPtr->tmpInSize, srcPtr, sizeToCopy);
                srcPtr += sizeToCopy;
                dctxPtr->tmpInSize += sizeToCopy;
                if (dctxPtr->tmpInSize < dctxPtr->tmpInTarget) { /* not enough input to get full sBlockSize; wait for more */
                    nextSrcSizeHint = dctxPtr->tmpInTarget - dctxPtr->tmpInSize;
                    doAnotherStage = 0;
                    break;
                }
                selectedIn = dctxPtr->header + 4;
            }

        /* case dstage_decodeSFrameSize: */   /* no direct access */
            {   size_t const SFrameSize = LizardF_readLE32(selectedIn);
                dctxPtr->frameInfo.contentSize = SFrameSize;
                dctxPtr->tmpInTarget = SFrameSize;
                dctxPtr->dStage = dstage_skipSkippable;
                break;
            }

        case dstage_skipSkippable:
            {   size_t skipSize = dctxPtr->tmpInTarget;
                if (skipSize > (size_t)(srcEnd-srcPtr)) skipSize = srcEnd-srcPtr;
                srcPtr += skipSize;
                dctxPtr->tmpInTarget -= skipSize;
                doAnotherStage = 0;
                nextSrcSizeHint = dctxPtr->tmpInTarget;
                if (nextSrcSizeHint) break;
                dctxPtr->dStage = dstage_getHeader;
                break;
            }
        }
    }

    /* preserve dictionary within tmp if necessary */
    if ( (dctxPtr->frameInfo.blockMode==LizardF_blockLinked)
        &&(dctxPtr->dict != dctxPtr->tmpOutBuffer)
        &&(!decompressOptionsPtr->stableDst)
        &&((unsigned)(dctxPtr->dStage-1) < (unsigned)(dstage_getSuffix-1))
        )
    {
        if (dctxPtr->dStage == dstage_flushOut) {
            size_t preserveSize = dctxPtr->tmpOut - dctxPtr->tmpOutBuffer;
            size_t copySize = LIZARD_DICT_SIZE - dctxPtr->tmpOutSize;
            const BYTE* oldDictEnd = dctxPtr->dict + dctxPtr->dictSize - dctxPtr->tmpOutStart;
            if (dctxPtr->tmpOutSize > LIZARD_DICT_SIZE) copySize = 0;
            if (copySize > preserveSize) copySize = preserveSize;

            memcpy(dctxPtr->tmpOutBuffer + preserveSize - copySize, oldDictEnd - copySize, copySize);

            dctxPtr->dict = dctxPtr->tmpOutBuffer;
            dctxPtr->dictSize = preserveSize + dctxPtr->tmpOutStart;
        } else {
            size_t newDictSize = dctxPtr->dictSize;
            const BYTE* oldDictEnd = dctxPtr->dict + dctxPtr->dictSize;
            if ((newDictSize) > LIZARD_DICT_SIZE) newDictSize = LIZARD_DICT_SIZE;

            memcpy(dctxPtr->tmpOutBuffer, oldDictEnd - newDictSize, newDictSize);

            dctxPtr->dict = dctxPtr->tmpOutBuffer;
            dctxPtr->dictSize = newDictSize;
            dctxPtr->tmpOut = dctxPtr->tmpOutBuffer + newDictSize;
        }
    }

    /* require function to be called again from position where it stopped */
    if (srcPtr<srcEnd)
        dctxPtr->srcExpect = srcPtr;
    else
        dctxPtr->srcExpect = NULL;

    *srcSizePtr = (srcPtr - srcStart);
    *dstSizePtr = (dstPtr - dstStart);
    return nextSrcSizeHint;
}
