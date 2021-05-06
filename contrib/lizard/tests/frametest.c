/*
    frameTest - test tool for lizard_frame
    Copyright (C) Yann Collet 2014-2016
    Copyright (C) Przemyslaw Skibinski 2016-2017

    GPL v2 License

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License along
    with this program; if not, write to the Free Software Foundation, Inc.,
    51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

    You can contact the author at :
    - Lizard source repository : https://github.com/inikep/lizard
*/

/*-************************************
*  Compiler specific
**************************************/
#ifdef _MSC_VER    /* Visual Studio */
#  pragma warning(disable : 4127)        /* disable: C4127: conditional expression is constant */
#  pragma warning(disable : 4146)        /* disable: C4146: minus unsigned expression */
#endif


/*-************************************
*  Includes
**************************************/
#include <stdlib.h>     /* malloc, free */
#include <stdio.h>      /* fprintf */
#include <string.h>     /* strcmp */
#include <time.h>       /* clock_t, clock(), CLOCKS_PER_SEC */
#include "lizard_frame_static.h"
#include "lizard_compress.h"        /* LIZARD_VERSION_STRING */
#define XXH_STATIC_LINKING_ONLY
#include "xxhash/xxhash.h"     /* XXH64 */


/*-************************************
*  Basic Types
**************************************/
#if defined (__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L)   /* C99 */
# include <stdint.h>
typedef  uint8_t BYTE;
typedef uint16_t U16;
typedef uint32_t U32;
typedef  int32_t S32;
typedef uint64_t U64;
#else
typedef unsigned char       BYTE;
typedef unsigned short      U16;
typedef unsigned int        U32;
typedef   signed int        S32;
typedef unsigned long long  U64;
#endif


/* unoptimized version; solves endianess & alignment issues */
static void FUZ_writeLE32 (void* dstVoidPtr, U32 value32)
{
    BYTE* dstPtr = (BYTE*)dstVoidPtr;
    dstPtr[0] = (BYTE)value32;
    dstPtr[1] = (BYTE)(value32 >> 8);
    dstPtr[2] = (BYTE)(value32 >> 16);
    dstPtr[3] = (BYTE)(value32 >> 24);
}


/*-************************************
*  Constants
**************************************/
#define LIZARDF_MAGIC_SKIPPABLE_START 0x184D2A50U

#define KB *(1U<<10)
#define MB *(1U<<20)
#define GB *(1U<<30)

static const U32 nbTestsDefault = 256 KB;
#define COMPRESSIBLE_NOISE_LENGTH (2 MB)
#define FUZ_COMPRESSIBILITY_DEFAULT 50
static const U32 prime1 = 2654435761U;
static const U32 prime2 = 2246822519U;



/*-************************************
*  Macros
**************************************/
#define DISPLAY(...)          fprintf(stderr, __VA_ARGS__)
#define DISPLAYLEVEL(l, ...)  if (displayLevel>=l) { DISPLAY(__VA_ARGS__); }
#define DISPLAYUPDATE(l, ...) if (displayLevel>=l) { \
            if ((FUZ_GetClockSpan(g_clockTime) > refreshRate) || (displayLevel>=4)) \
            { g_clockTime = clock(); DISPLAY(__VA_ARGS__); \
            if (displayLevel>=4) fflush(stdout); } }
static const clock_t refreshRate = CLOCKS_PER_SEC / 6;
static clock_t g_clockTime = 0;


/*-***************************************
*  Local Parameters
*****************************************/
static U32 no_prompt = 0;
static char* programName;
static U32 displayLevel = 2;
static U32 pause = 0;


/*-*******************************************************
*  Fuzzer functions
*********************************************************/
static clock_t FUZ_GetClockSpan(clock_t clockStart)
{
    return clock() - clockStart;   /* works even if overflow; max span ~ 30 mn */
}


#define FUZ_rotl32(x,r) ((x << r) | (x >> (32 - r)))
unsigned int FUZ_rand(unsigned int* src)
{
    U32 rand32 = *src;
    rand32 *= prime1;
    rand32 += prime2;
    rand32  = FUZ_rotl32(rand32, 13);
    *src = rand32;
    return rand32 >> 5;
}


#define FUZ_RAND15BITS  (FUZ_rand(seed) & 0x7FFF)
#define FUZ_RANDLENGTH  ( (FUZ_rand(seed) & 3) ? (FUZ_rand(seed) % 15) : (FUZ_rand(seed) % 510) + 15)
static void FUZ_fillCompressibleNoiseBuffer(void* buffer, unsigned bufferSize, double proba, U32* seed)
{
    BYTE* BBuffer = (BYTE*)buffer;
    unsigned pos = 0;
    U32 P32 = (U32)(32768 * proba);

    /* First Byte */
    BBuffer[pos++] = (BYTE)(FUZ_rand(seed));

    while (pos < bufferSize) {
        /* Select : Literal (noise) or copy (within 64K) */
        if (FUZ_RAND15BITS < P32) {
            /* Copy (within 64K) */
            unsigned match, end;
            unsigned length = FUZ_RANDLENGTH + 4;
            unsigned offset = FUZ_RAND15BITS + 1;
            if (offset > pos) offset = pos;
            if (pos + length > bufferSize) length = bufferSize - pos;
            match = pos - offset;
            end = pos + length;
            while (pos < end) BBuffer[pos++] = BBuffer[match++];
        } else {
            /* Literal (noise) */
            unsigned end;
            unsigned length = FUZ_RANDLENGTH;
            if (pos + length > bufferSize) length = bufferSize - pos;
            end = pos + length;
            while (pos < end) BBuffer[pos++] = (BYTE)(FUZ_rand(seed) >> 5);
        }
    }
}


static unsigned FUZ_highbit(U32 v32)
{
    unsigned nbBits = 0;
    if (v32==0) return 0;
    while (v32) v32 >>= 1, nbBits ++;
    return nbBits;
}


int basicTests(U32 seed, double compressibility)
{
    int testResult = 0;
    void* CNBuffer;
    void* compressedBuffer;
    void* decodedBuffer;
    U32 randState = seed;
    size_t cSize, testSize;
    LizardF_preferences_t prefs;
    LizardF_decompressionContext_t dCtx = NULL;
    LizardF_compressionContext_t cctx = NULL;
    U64 crcOrig;

    /* Create compressible test buffer */
    memset(&prefs, 0, sizeof(prefs));
    CNBuffer = malloc(COMPRESSIBLE_NOISE_LENGTH);
    compressedBuffer = malloc(LizardF_compressFrameBound(COMPRESSIBLE_NOISE_LENGTH, NULL));
    decodedBuffer = malloc(COMPRESSIBLE_NOISE_LENGTH);
    FUZ_fillCompressibleNoiseBuffer(CNBuffer, COMPRESSIBLE_NOISE_LENGTH, compressibility, &randState);
    crcOrig = XXH64(CNBuffer, COMPRESSIBLE_NOISE_LENGTH, 1);

    /* Special case : null-content frame */
    testSize = 0;
    DISPLAYLEVEL(3, "LizardF_compressFrame, compress null content : \n");
    cSize = LizardF_compressFrame(compressedBuffer, LizardF_compressFrameBound(testSize, NULL), CNBuffer, testSize, NULL);
    if (LizardF_isError(cSize)) goto _output_error;
    DISPLAYLEVEL(3, "Compressed null content into a %i bytes frame \n", (int)cSize);

    DISPLAYLEVEL(3, "LizardF_createDecompressionContext \n");
    { LizardF_errorCode_t const errorCode = LizardF_createDecompressionContext(&dCtx, LIZARDF_VERSION);
      if (LizardF_isError(errorCode)) goto _output_error; }

    DISPLAYLEVEL(3, "LizardF_getFrameInfo on null-content frame (#157) \n");
    {   size_t avail_in = cSize;
        LizardF_frameInfo_t frame_info;
        LizardF_errorCode_t const errorCode = LizardF_getFrameInfo(dCtx, &frame_info, compressedBuffer, &avail_in);
        if (LizardF_isError(errorCode)) goto _output_error;
    }

    DISPLAYLEVEL(3, "LizardF_freeDecompressionContext \n");
    { LizardF_errorCode_t const errorCode = LizardF_freeDecompressionContext(dCtx);
      if (LizardF_isError(errorCode)) goto _output_error; }

    /* Trivial tests : one-step frame */
    testSize = COMPRESSIBLE_NOISE_LENGTH;
    DISPLAYLEVEL(3, "LizardF_compressFrame, using default preferences : \n");
    cSize = LizardF_compressFrame(compressedBuffer, LizardF_compressFrameBound(testSize, NULL), CNBuffer, testSize, NULL);
    if (LizardF_isError(cSize)) goto _output_error;
    DISPLAYLEVEL(3, "Compressed %i bytes into a %i bytes frame \n", (int)testSize, (int)cSize);

    DISPLAYLEVEL(3, "Decompression test : \n");
    {   size_t decodedBufferSize = COMPRESSIBLE_NOISE_LENGTH;
        size_t compressedBufferSize = cSize;
        BYTE* op = (BYTE*)decodedBuffer;
        BYTE* const oend = (BYTE*)decodedBuffer + COMPRESSIBLE_NOISE_LENGTH;
        BYTE* ip = (BYTE*)compressedBuffer;
        BYTE* const iend = (BYTE*)compressedBuffer + cSize;
        U64 crcDest;

        LizardF_errorCode_t errorCode = LizardF_createDecompressionContext(&dCtx, LIZARDF_VERSION);
        if (LizardF_isError(errorCode)) goto _output_error;

        DISPLAYLEVEL(3, "Single Block : \n");
        errorCode = LizardF_decompress(dCtx, decodedBuffer, &decodedBufferSize, compressedBuffer, &compressedBufferSize, NULL);
        if (LizardF_isError(errorCode)) goto _output_error;
        crcDest = XXH64(decodedBuffer, COMPRESSIBLE_NOISE_LENGTH, 1);
        if (crcDest != crcOrig) goto _output_error;
        DISPLAYLEVEL(3, "Regenerated %i bytes \n", (int)decodedBufferSize);

        DISPLAYLEVEL(3, "Reusing decompression context \n");
        {   size_t iSize = compressedBufferSize - 4;
            const BYTE* cBuff = (const BYTE*) compressedBuffer;
            size_t decResult;
            DISPLAYLEVEL(3, "Missing last 4 bytes : ");
            decResult = LizardF_decompress(dCtx, decodedBuffer, &decodedBufferSize, cBuff, &iSize, NULL);
            if (LizardF_isError(decResult)) goto _output_error;
            if (!decResult) goto _output_error;   /* not finished */
            DISPLAYLEVEL(3, "indeed, request %u bytes \n", (unsigned)decResult);
            cBuff += iSize;
            iSize = decResult;
            decResult = LizardF_decompress(dCtx, decodedBuffer, &decodedBufferSize, cBuff, &iSize, NULL);
            if (decResult != 0) goto _output_error;   /* should finish now */
            crcDest = XXH64(decodedBuffer, COMPRESSIBLE_NOISE_LENGTH, 1);
            if (crcDest != crcOrig) goto _output_error;
        }

        {   size_t oSize = 0;
            size_t iSize = 0;
            LizardF_frameInfo_t fi;

            DISPLAYLEVEL(3, "Start by feeding 0 bytes, to get next input size : ");
            errorCode = LizardF_decompress(dCtx, NULL, &oSize, ip, &iSize, NULL);
            if (LizardF_isError(errorCode)) goto _output_error;
            DISPLAYLEVEL(3, " %u  \n", (unsigned)errorCode);

            DISPLAYLEVEL(3, "get FrameInfo on null input : ");
            errorCode = LizardF_getFrameInfo(dCtx, &fi, ip, &iSize);
            if (errorCode != (size_t)-LizardF_ERROR_frameHeader_incomplete) goto _output_error;
            DISPLAYLEVEL(3, " correctly failed : %s \n", LizardF_getErrorName(errorCode));

            DISPLAYLEVEL(3, "get FrameInfo on not enough input : ");
            iSize = 6;
            errorCode = LizardF_getFrameInfo(dCtx, &fi, ip, &iSize);
            if (errorCode != (size_t)-LizardF_ERROR_frameHeader_incomplete) goto _output_error;
            DISPLAYLEVEL(3, " correctly failed : %s \n", LizardF_getErrorName(errorCode));
            ip += iSize;

            DISPLAYLEVEL(3, "get FrameInfo on enough input : ");
            iSize = 15 - iSize;
            errorCode = LizardF_getFrameInfo(dCtx, &fi, ip, &iSize);
            if (LizardF_isError(errorCode)) goto _output_error;
            DISPLAYLEVEL(3, " correctly decoded \n");
            ip += iSize;
        }

        DISPLAYLEVEL(3, "Byte after byte : \n");
        while (ip < iend) {
            size_t oSize = oend-op;
            size_t iSize = 1;
            errorCode = LizardF_decompress(dCtx, op, &oSize, ip, &iSize, NULL);
            if (LizardF_isError(errorCode)) goto _output_error;
            op += oSize;
            ip += iSize;
        }
        crcDest = XXH64(decodedBuffer, COMPRESSIBLE_NOISE_LENGTH, 1);
        if (crcDest != crcOrig) goto _output_error;
        DISPLAYLEVEL(3, "Regenerated %u/%u bytes \n", (unsigned)(op-(BYTE*)decodedBuffer), COMPRESSIBLE_NOISE_LENGTH);

        errorCode = LizardF_freeDecompressionContext(dCtx);
        if (LizardF_isError(errorCode)) goto _output_error;
    }

    DISPLAYLEVEL(3, "Using 128 KB block : \n");
    prefs.frameInfo.blockSizeID = LizardF_max128KB;
    prefs.frameInfo.contentChecksumFlag = LizardF_contentChecksumEnabled;
    cSize = LizardF_compressFrame(compressedBuffer, LizardF_compressFrameBound(testSize, &prefs), CNBuffer, testSize, &prefs);
    if (LizardF_isError(cSize)) goto _output_error;
    DISPLAYLEVEL(3, "Compressed %i bytes into a %i bytes frame \n", (int)testSize, (int)cSize);

    DISPLAYLEVEL(3, "without checksum : \n");
    prefs.frameInfo.contentChecksumFlag = LizardF_noContentChecksum;
    cSize = LizardF_compressFrame(compressedBuffer, LizardF_compressFrameBound(testSize, &prefs), CNBuffer, testSize, &prefs);
    if (LizardF_isError(cSize)) goto _output_error;
    DISPLAYLEVEL(3, "Compressed %i bytes into a %i bytes frame \n", (int)testSize, (int)cSize);

    DISPLAYLEVEL(3, "Using 256 KB block : \n");
    prefs.frameInfo.blockSizeID = LizardF_max256KB;
    prefs.frameInfo.contentChecksumFlag = LizardF_contentChecksumEnabled;
    cSize = LizardF_compressFrame(compressedBuffer, LizardF_compressFrameBound(testSize, &prefs), CNBuffer, testSize, &prefs);
    if (LizardF_isError(cSize)) goto _output_error;
    DISPLAYLEVEL(3, "Compressed %i bytes into a %i bytes frame \n", (int)testSize, (int)cSize);

    DISPLAYLEVEL(3, "Decompression test : \n");
    {   size_t decodedBufferSize = COMPRESSIBLE_NOISE_LENGTH;
        unsigned maxBits = FUZ_highbit((U32)decodedBufferSize);
        BYTE* op = (BYTE*)decodedBuffer;
        BYTE* const oend = (BYTE*)decodedBuffer + COMPRESSIBLE_NOISE_LENGTH;
        BYTE* ip = (BYTE*)compressedBuffer;
        BYTE* const iend = (BYTE*)compressedBuffer + cSize;
        U64 crcDest;

        LizardF_errorCode_t errorCode = LizardF_createDecompressionContext(&dCtx, LIZARDF_VERSION);
        if (LizardF_isError(errorCode)) goto _output_error;

        DISPLAYLEVEL(3, "random segment sizes : \n");
        while (ip < iend) {
            unsigned nbBits = FUZ_rand(&randState) % maxBits;
            size_t iSize = (FUZ_rand(&randState) & ((1<<nbBits)-1)) + 1;
            size_t oSize = oend-op;
            if (iSize > (size_t)(iend-ip)) iSize = iend-ip;
            //DISPLAY("%7i : + %6i\n", (int)(ip-(BYTE*)compressedBuffer), (int)iSize);
            errorCode = LizardF_decompress(dCtx, op, &oSize, ip, &iSize, NULL);
            if (LizardF_isError(errorCode)) goto _output_error;
            op += oSize;
            ip += iSize;
        }
        crcDest = XXH64(decodedBuffer, COMPRESSIBLE_NOISE_LENGTH, 1);
        if (crcDest != crcOrig) goto _output_error;
        DISPLAYLEVEL(3, "Regenerated %i bytes \n", (int)decodedBufferSize);

        errorCode = LizardF_freeDecompressionContext(dCtx);
        if (LizardF_isError(errorCode)) goto _output_error;
    }

    DISPLAYLEVEL(3, "without checksum : \n");
    prefs.frameInfo.contentChecksumFlag = LizardF_noContentChecksum;
    cSize = LizardF_compressFrame(compressedBuffer, LizardF_compressFrameBound(testSize, &prefs), CNBuffer, testSize, &prefs);
    if (LizardF_isError(cSize)) goto _output_error;
    DISPLAYLEVEL(3, "Compressed %i bytes into a %i bytes frame \n", (int)testSize, (int)cSize);

    DISPLAYLEVEL(3, "Using 1 MB block : \n");
    prefs.frameInfo.blockSizeID = LizardF_max1MB;
    prefs.frameInfo.contentChecksumFlag = LizardF_contentChecksumEnabled;
    cSize = LizardF_compressFrame(compressedBuffer, LizardF_compressFrameBound(testSize, &prefs), CNBuffer, testSize, &prefs);
    if (LizardF_isError(cSize)) goto _output_error;
    DISPLAYLEVEL(3, "Compressed %i bytes into a %i bytes frame \n", (int)testSize, (int)cSize);

    DISPLAYLEVEL(3, "without checksum : \n");
    prefs.frameInfo.contentChecksumFlag = LizardF_noContentChecksum;
    cSize = LizardF_compressFrame(compressedBuffer, LizardF_compressFrameBound(testSize, &prefs), CNBuffer, testSize, &prefs);
    if (LizardF_isError(cSize)) goto _output_error;
    DISPLAYLEVEL(3, "Compressed %i bytes into a %i bytes frame \n", (int)testSize, (int)cSize);

    DISPLAYLEVEL(3, "Using 4 MB block : \n");
    prefs.frameInfo.blockSizeID = LizardF_max4MB;
    prefs.frameInfo.contentChecksumFlag = LizardF_contentChecksumEnabled;
    cSize = LizardF_compressFrame(compressedBuffer, LizardF_compressFrameBound(testSize, &prefs), CNBuffer, testSize, &prefs);
    if (LizardF_isError(cSize)) goto _output_error;
    DISPLAYLEVEL(3, "Compressed %i bytes into a %i bytes frame \n", (int)testSize, (int)cSize);

    DISPLAYLEVEL(3, "without checksum : \n");
    prefs.frameInfo.contentChecksumFlag = LizardF_noContentChecksum;
    cSize = LizardF_compressFrame(compressedBuffer, LizardF_compressFrameBound(testSize, &prefs), CNBuffer, testSize, &prefs);
    if (LizardF_isError(cSize)) goto _output_error;
    DISPLAYLEVEL(3, "Compressed %i bytes into a %i bytes frame \n", (int)testSize, (int)cSize);

    {   size_t errorCode;
        BYTE* const ostart = (BYTE*)compressedBuffer;
        BYTE* op = ostart;
        errorCode = LizardF_createCompressionContext(&cctx, LIZARDF_VERSION);
        if (LizardF_isError(errorCode)) goto _output_error;

        DISPLAYLEVEL(3, "compress without frameSize : \n");
        memset(&(prefs.frameInfo), 0, sizeof(prefs.frameInfo));
        errorCode = LizardF_compressBegin(cctx, compressedBuffer, testSize, &prefs);
        if (LizardF_isError(errorCode)) goto _output_error;
        op += errorCode;
        errorCode = LizardF_compressUpdate(cctx, op, LizardF_compressBound(testSize, &prefs), CNBuffer, testSize, NULL);
        if (LizardF_isError(errorCode)) goto _output_error;
        op += errorCode;
        errorCode = LizardF_compressEnd(cctx, compressedBuffer, testSize, NULL);
        if (LizardF_isError(errorCode)) goto _output_error;
        DISPLAYLEVEL(3, "Compressed %i bytes into a %i bytes frame \n", (int)testSize, (int)(op-ostart));

        DISPLAYLEVEL(3, "compress with frameSize : \n");
        prefs.frameInfo.contentSize = testSize;
        op = ostart;
        errorCode = LizardF_compressBegin(cctx, compressedBuffer, testSize, &prefs);
        if (LizardF_isError(errorCode)) goto _output_error;
        op += errorCode;
        errorCode = LizardF_compressUpdate(cctx, op, LizardF_compressBound(testSize, &prefs), CNBuffer, testSize, NULL);
        if (LizardF_isError(errorCode)) goto _output_error;
        op += errorCode;
        errorCode = LizardF_compressEnd(cctx, compressedBuffer, testSize, NULL);
        if (LizardF_isError(errorCode)) goto _output_error;
        DISPLAYLEVEL(3, "Compressed %i bytes into a %i bytes frame \n", (int)testSize, (int)(op-ostart));

        DISPLAYLEVEL(3, "compress with wrong frameSize : \n");
        prefs.frameInfo.contentSize = testSize+1;
        op = ostart;
        errorCode = LizardF_compressBegin(cctx, compressedBuffer, testSize, &prefs);
        if (LizardF_isError(errorCode)) goto _output_error;
        op += errorCode;
        errorCode = LizardF_compressUpdate(cctx, op, LizardF_compressBound(testSize, &prefs), CNBuffer, testSize, NULL);
        if (LizardF_isError(errorCode)) goto _output_error;
        op += errorCode;
        errorCode = LizardF_compressEnd(cctx, op, testSize, NULL);
        if (LizardF_isError(errorCode)) { DISPLAYLEVEL(3, "Error correctly detected : %s \n", LizardF_getErrorName(errorCode)); }
        else
            goto _output_error;

        errorCode = LizardF_freeCompressionContext(cctx);
        if (LizardF_isError(errorCode)) goto _output_error;
        cctx = NULL;
    }

    DISPLAYLEVEL(3, "Skippable frame test : \n");
    {   size_t decodedBufferSize = COMPRESSIBLE_NOISE_LENGTH;
        unsigned maxBits = FUZ_highbit((U32)decodedBufferSize);
        BYTE* op = (BYTE*)decodedBuffer;
        BYTE* const oend = (BYTE*)decodedBuffer + COMPRESSIBLE_NOISE_LENGTH;
        BYTE* ip = (BYTE*)compressedBuffer;
        BYTE* iend = (BYTE*)compressedBuffer + cSize + 8;

        LizardF_errorCode_t errorCode = LizardF_createDecompressionContext(&dCtx, LIZARDF_VERSION);
        if (LizardF_isError(errorCode)) goto _output_error;

        /* generate skippable frame */
        FUZ_writeLE32(ip, LIZARDF_MAGIC_SKIPPABLE_START);
        FUZ_writeLE32(ip+4, (U32)cSize);

        DISPLAYLEVEL(3, "random segment sizes : \n");
        while (ip < iend) {
            unsigned nbBits = FUZ_rand(&randState) % maxBits;
            size_t iSize = (FUZ_rand(&randState) & ((1<<nbBits)-1)) + 1;
            size_t oSize = oend-op;
            if (iSize > (size_t)(iend-ip)) iSize = iend-ip;
            errorCode = LizardF_decompress(dCtx, op, &oSize, ip, &iSize, NULL);
            if (LizardF_isError(errorCode)) goto _output_error;
            op += oSize;
            ip += iSize;
        }
        DISPLAYLEVEL(3, "Skipped %i bytes \n", (int)decodedBufferSize);

        /* generate zero-size skippable frame */
        DISPLAYLEVEL(3, "zero-size skippable frame\n");
        ip = (BYTE*)compressedBuffer;
        op = (BYTE*)decodedBuffer;
        FUZ_writeLE32(ip, LIZARDF_MAGIC_SKIPPABLE_START+1);
        FUZ_writeLE32(ip+4, 0);
        iend = ip+8;

        while (ip < iend) {
            unsigned nbBits = FUZ_rand(&randState) % maxBits;
            size_t iSize = (FUZ_rand(&randState) & ((1<<nbBits)-1)) + 1;
            size_t oSize = oend-op;
            if (iSize > (size_t)(iend-ip)) iSize = iend-ip;
            errorCode = LizardF_decompress(dCtx, op, &oSize, ip, &iSize, NULL);
            if (LizardF_isError(errorCode)) goto _output_error;
            op += oSize;
            ip += iSize;
        }
        DISPLAYLEVEL(3, "Skipped %i bytes \n", (int)(ip - (BYTE*)compressedBuffer - 8));

        DISPLAYLEVEL(3, "Skippable frame header complete in first call \n");
        ip = (BYTE*)compressedBuffer;
        op = (BYTE*)decodedBuffer;
        FUZ_writeLE32(ip, LIZARDF_MAGIC_SKIPPABLE_START+2);
        FUZ_writeLE32(ip+4, 10);
        iend = ip+18;
        while (ip < iend) {
            size_t iSize = 10;
            size_t oSize = 10;
            if (iSize > (size_t)(iend-ip)) iSize = iend-ip;
            errorCode = LizardF_decompress(dCtx, op, &oSize, ip, &iSize, NULL);
            if (LizardF_isError(errorCode)) goto _output_error;
            op += oSize;
            ip += iSize;
        }
        DISPLAYLEVEL(3, "Skipped %i bytes \n", (int)(ip - (BYTE*)compressedBuffer - 8));
    }

    DISPLAY("Basic tests completed \n");
_end:
    free(CNBuffer);
    free(compressedBuffer);
    free(decodedBuffer);
    LizardF_freeDecompressionContext(dCtx); dCtx = NULL;
    LizardF_freeCompressionContext(cctx); cctx = NULL;
    return testResult;

_output_error:
    testResult = 1;
    DISPLAY("Error detected ! \n");
    goto _end;
}


static void locateBuffDiff(const void* buff1, const void* buff2, size_t size, unsigned nonContiguous)
{
    int p=0;
    const BYTE* b1=(const BYTE*)buff1;
    const BYTE* b2=(const BYTE*)buff2;
    if (nonContiguous) {
        DISPLAY("Non-contiguous output test (%i bytes)\n", (int)size);
        return;
    }
    while (b1[p]==b2[p]) p++;
    DISPLAY("Error at pos %i/%i : %02X != %02X \n", p, (int)size, b1[p], b2[p]);
}


static const U32 srcDataLength = 9 MB;  /* needs to be > 2x4MB to test large blocks */

int fuzzerTests(U32 seed, unsigned nbTests, unsigned startTest, double compressibility, U32 duration_s)
{
    unsigned testResult = 0;
    unsigned testNb = 0;
    void* srcBuffer = NULL;
    void* compressedBuffer = NULL;
    void* decodedBuffer = NULL;
    U32 coreRand = seed;
    LizardF_decompressionContext_t dCtx = NULL;
    LizardF_compressionContext_t cCtx = NULL;
    size_t result;
    clock_t const startClock = clock();
    clock_t const clockDuration = duration_s * CLOCKS_PER_SEC;
    XXH64_state_t xxh64;
#   define CHECK(cond, ...) if (cond) { DISPLAY("Error => "); DISPLAY(__VA_ARGS__); \
                            DISPLAY(" (seed %u, test nb %u)  \n", seed, testNb); goto _output_error; }

    /* Create buffers */
    result = LizardF_createDecompressionContext(&dCtx, LIZARDF_VERSION);
    CHECK(LizardF_isError(result), "Allocation failed (error %i)", (int)result);
    result = LizardF_createCompressionContext(&cCtx, LIZARDF_VERSION);
    CHECK(LizardF_isError(result), "Allocation failed (error %i)", (int)result);
    srcBuffer = malloc(srcDataLength);
    CHECK(srcBuffer==NULL, "srcBuffer Allocation failed");
    compressedBuffer = malloc(LizardF_compressFrameBound(srcDataLength, NULL));
    CHECK(compressedBuffer==NULL, "compressedBuffer Allocation failed");
    decodedBuffer = calloc(1, srcDataLength);   /* calloc avoids decodedBuffer being considered "garbage" by scan-build */
    CHECK(decodedBuffer==NULL, "decodedBuffer Allocation failed");
    FUZ_fillCompressibleNoiseBuffer(srcBuffer, srcDataLength, compressibility, &coreRand);

    /* jump to requested testNb */
    for (testNb =0; (testNb < startTest); testNb++) (void)FUZ_rand(&coreRand);   // sync randomizer

    /* main fuzzer test loop */
    for ( ; (testNb < nbTests) || (clockDuration > FUZ_GetClockSpan(startClock)) ; testNb++) {
        U32 randState = coreRand ^ prime1;
        unsigned BSId   = 4 + (FUZ_rand(&randState) & 3);
        unsigned BMId   = FUZ_rand(&randState) & 1;
        unsigned CCflag = FUZ_rand(&randState) & 1;
        unsigned autoflush = (FUZ_rand(&randState) & 7) == 2;
        LizardF_preferences_t prefs;
        LizardF_compressOptions_t cOptions;
        LizardF_decompressOptions_t dOptions;
        unsigned nbBits = (FUZ_rand(&randState) % (FUZ_highbit(srcDataLength-1) - 1)) + 1;
        size_t srcSize = (FUZ_rand(&randState) & ((1<<nbBits)-1)) + 1;
        size_t srcStart = FUZ_rand(&randState) % (srcDataLength - srcSize);
        U64 frameContentSize = ((FUZ_rand(&randState) & 0xF) == 1) ? srcSize : 0;
        size_t cSize;
        U64 crcOrig, crcDecoded;
        LizardF_preferences_t* prefsPtr = &prefs;

        (void)FUZ_rand(&coreRand);   /* update seed */
        memset(&prefs, 0, sizeof(prefs));
        memset(&cOptions, 0, sizeof(cOptions));
        memset(&dOptions, 0, sizeof(dOptions));
        prefs.frameInfo.blockMode = (LizardF_blockMode_t)BMId;
        prefs.frameInfo.blockSizeID = (LizardF_blockSizeID_t)BSId;
        prefs.frameInfo.contentChecksumFlag = (LizardF_contentChecksum_t)CCflag;
        prefs.frameInfo.contentSize = frameContentSize;
        prefs.autoFlush = autoflush;
        prefs.compressionLevel = LIZARD_MIN_CLEVEL + (FUZ_rand(&randState) % (1+LIZARD_MAX_CLEVEL-LIZARD_MIN_CLEVEL));
        if ((FUZ_rand(&randState) & 0x1F) == 1) prefsPtr = NULL;

        DISPLAYUPDATE(2, "\r%5u   ", testNb);
        crcOrig = XXH64((BYTE*)srcBuffer+srcStart, srcSize, 1);

        if ((FUZ_rand(&randState) & 0xFFF) == 0) {
            /* create a skippable frame (rare case) */
            BYTE* op = (BYTE*)compressedBuffer;
            FUZ_writeLE32(op, LIZARDF_MAGIC_SKIPPABLE_START + (FUZ_rand(&randState) & 15));
            FUZ_writeLE32(op+4, (U32)srcSize);
            cSize = srcSize+8;
        } else if ((FUZ_rand(&randState) & 0xF) == 2) {
            cSize = LizardF_compressFrame(compressedBuffer, LizardF_compressFrameBound(srcSize, prefsPtr), (char*)srcBuffer + srcStart, srcSize, prefsPtr);
            CHECK(LizardF_isError(cSize), "LizardF_compressFrame failed : error %i (%s)", (int)cSize, LizardF_getErrorName(cSize));
        } else {
            const BYTE* ip = (const BYTE*)srcBuffer + srcStart;
            const BYTE* const iend = ip + srcSize;
            BYTE* op = (BYTE*)compressedBuffer;
            BYTE* const oend = op + LizardF_compressFrameBound(srcDataLength, NULL);
            unsigned maxBits = FUZ_highbit((U32)srcSize);
            result = LizardF_compressBegin(cCtx, op, oend-op, prefsPtr);
            CHECK(LizardF_isError(result), "Compression header failed (error %i)", (int)result);
            op += result;
            while (ip < iend) {
                unsigned nbBitsSeg = FUZ_rand(&randState) % maxBits;
                size_t iSize = (FUZ_rand(&randState) & ((1<<nbBitsSeg)-1)) + 1;
                size_t oSize = LizardF_compressBound(iSize, prefsPtr);
                unsigned forceFlush = ((FUZ_rand(&randState) & 3) == 1);
                if (iSize > (size_t)(iend-ip)) iSize = iend-ip;
                cOptions.stableSrc = ((FUZ_rand(&randState) & 3) == 1);

                result = LizardF_compressUpdate(cCtx, op, oSize, ip, iSize, &cOptions);
                CHECK(LizardF_isError(result), "Compression failed (error %i) iSize=%d oSize=%d", (int)result, (int)iSize, (int)oSize);
                op += result;
                ip += iSize;

                if (forceFlush) {
                    result = LizardF_flush(cCtx, op, oend-op, &cOptions);
                    CHECK(LizardF_isError(result), "Compression flush failed (error %i)", (int)result);
                    op += result;
                }
            }
            result = LizardF_compressEnd(cCtx, op, oend-op, &cOptions);
            CHECK(LizardF_isError(result), "Compression completion failed (error %i)", (int)result);
            op += result;
            cSize = op-(BYTE*)compressedBuffer;
        }

        {   const BYTE* ip = (const BYTE*)compressedBuffer;
            const BYTE* const iend = ip + cSize;
            BYTE* op = (BYTE*)decodedBuffer;
            BYTE* const oend = op + srcDataLength;
            size_t totalOut = 0;
            unsigned maxBits = FUZ_highbit((U32)cSize);
            unsigned nonContiguousDst = (FUZ_rand(&randState) & 3) == 1;
            nonContiguousDst += FUZ_rand(&randState) & nonContiguousDst;   /* 0=>0; 1=>1,2 */
            XXH64_reset(&xxh64, 1);
            if (maxBits < 3) maxBits = 3;
            while (ip < iend) {
                unsigned nbBitsI = (FUZ_rand(&randState) % (maxBits-1)) + 1;
                unsigned nbBitsO = (FUZ_rand(&randState) % (maxBits)) + 1;
                size_t iSize = (FUZ_rand(&randState) & ((1<<nbBitsI)-1)) + 1;
                size_t oSize = (FUZ_rand(&randState) & ((1<<nbBitsO)-1)) + 2;
                if (iSize > (size_t)(iend-ip)) iSize = iend-ip;
                if (oSize > (size_t)(oend-op)) oSize = oend-op;
                dOptions.stableDst = FUZ_rand(&randState) & 1;
                if (nonContiguousDst==2) dOptions.stableDst = 0;
                result = LizardF_decompress(dCtx, op, &oSize, ip, &iSize, &dOptions);
                if (result == (size_t)-LizardF_ERROR_contentChecksum_invalid)
                    locateBuffDiff((BYTE*)srcBuffer+srcStart, decodedBuffer, srcSize, nonContiguousDst);
                CHECK(LizardF_isError(result), "Decompression failed (error %i:%s)", (int)result, LizardF_getErrorName((LizardF_errorCode_t)result));
                XXH64_update(&xxh64, op, (U32)oSize);
                totalOut += oSize;
                op += oSize;
                ip += iSize;
                op += nonContiguousDst;
                if (nonContiguousDst==2) op = (BYTE*)decodedBuffer;   /* overwritten destination */
            }
            CHECK(result != 0, "Frame decompression failed (error %i)", (int)result);
            if (totalOut) {  /* otherwise, it's a skippable frame */
                crcDecoded = XXH64_digest(&xxh64);
                if (crcDecoded != crcOrig) locateBuffDiff((BYTE*)srcBuffer+srcStart, decodedBuffer, srcSize, nonContiguousDst);
                CHECK(crcDecoded != crcOrig, "Decompression corruption");
            }
        }
    }

    DISPLAYLEVEL(2, "\rAll tests completed   \n");

_end:
    LizardF_freeDecompressionContext(dCtx);
    LizardF_freeCompressionContext(cCtx);
    free(srcBuffer);
    free(compressedBuffer);
    free(decodedBuffer);

    if (pause) {
        DISPLAY("press enter to finish \n");
        (void)getchar();
    }
    return testResult;

_output_error:
    testResult = 1;
    goto _end;
}


int FUZ_usage(void)
{
    DISPLAY( "Usage :\n");
    DISPLAY( "      %s [args]\n", programName);
    DISPLAY( "\n");
    DISPLAY( "Arguments :\n");
    DISPLAY( " -i#    : Nb of tests (default:%u) \n", nbTestsDefault);
    DISPLAY( " -T#    : Duration of tests, in seconds (default: use Nb of tests) \n");
    DISPLAY( " -s#    : Select seed (default:prompt user)\n");
    DISPLAY( " -t#    : Select starting test number (default:0)\n");
    DISPLAY( " -p#    : Select compressibility in %% (default:%i%%)\n", FUZ_COMPRESSIBILITY_DEFAULT);
    DISPLAY( " -v     : verbose\n");
    DISPLAY( " -h     : display help and exit\n");
    return 0;
}


int main(int argc, char** argv)
{
    U32 seed=0;
    int seedset=0;
    int argNb;
    int nbTests = nbTestsDefault;
    int testNb = 0;
    int proba = FUZ_COMPRESSIBILITY_DEFAULT;
    int result=0;
    U32 duration=0;

    /* Check command line */
    programName = argv[0];
    for(argNb=1; argNb<argc; argNb++) {
        char* argument = argv[argNb];

        if(!argument) continue;   /* Protection if argument empty */

        /* Decode command (note : aggregated commands are allowed) */
        if (argument[0]=='-') {
            if (!strcmp(argument, "--no-prompt")) {
                no_prompt=1;
                seedset=1;
                displayLevel=1;
                continue;
            }
            argument++;

            while (*argument!=0) {
                switch(*argument)
                {
                case 'h':
                    return FUZ_usage();
                case 'v':
                    argument++;
                    displayLevel=4;
                    break;
                case 'q':
                    argument++;
                    displayLevel--;
                    break;
                case 'p': /* pause at the end */
                    argument++;
                    pause = 1;
                    break;

                case 'i':
                    argument++;
                    nbTests=0; duration=0;
                    while ((*argument>='0') && (*argument<='9')) {
                        nbTests *= 10;
                        nbTests += *argument - '0';
                        argument++;
                    }
                    break;

                case 'T':
                    argument++;
                    nbTests = 0; duration = 0;
                    for (;;) {
                        switch(*argument)
                        {
                            case 'm': duration *= 60; argument++; continue;
                            case 's':
                            case 'n': argument++; continue;
                            case '0':
                            case '1':
                            case '2':
                            case '3':
                            case '4':
                            case '5':
                            case '6':
                            case '7':
                            case '8':
                            case '9': duration *= 10; duration += *argument++ - '0'; continue;
                        }
                        break;
                    }
                    break;

                case 's':
                    argument++;
                    seed=0;
                    seedset=1;
                    while ((*argument>='0') && (*argument<='9')) {
                        seed *= 10;
                        seed += *argument - '0';
                        argument++;
                    }
                    break;
                case 't':
                    argument++;
                    testNb=0;
                    while ((*argument>='0') && (*argument<='9')) {
                        testNb *= 10;
                        testNb += *argument - '0';
                        argument++;
                    }
                    break;
                case 'P':   /* compressibility % */
                    argument++;
                    proba=0;
                    while ((*argument>='0') && (*argument<='9')) {
                        proba *= 10;
                        proba += *argument - '0';
                        argument++;
                    }
                    if (proba<0) proba=0;
                    if (proba>100) proba=100;
                    break;
                default:
                    ;
                    return FUZ_usage();
                }
            }
        }
    }

    /* Get Seed */
    printf("Starting lizard_frame tester (%i-bits, %s)\n", (int)(sizeof(size_t)*8), LIZARD_VERSION_STRING);

    if (!seedset) {
        time_t const t = time(NULL);
        U32 const h = XXH32(&t, sizeof(t), 1);
        seed = h % 10000;
    }
    printf("Seed = %u\n", seed);
    if (proba!=FUZ_COMPRESSIBILITY_DEFAULT) printf("Compressibility : %i%%\n", proba);

    if (nbTests<=0) nbTests=1;

    if (testNb==0) result = basicTests(seed, ((double)proba) / 100);
    if (result) return 1;
    return fuzzerTests(seed, nbTests, testNb, ((double)proba) / 100, duration);
}
