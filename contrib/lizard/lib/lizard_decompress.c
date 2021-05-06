/*
   Lizard - Fast LZ compression algorithm
   Copyright (C) 2011-2016, Yann Collet
   Copyright (C) 2016-2017, Przemyslaw Skibinski <inikep@gmail.com>

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


/**************************************
*  Includes
**************************************/
//#define LIZARD_STATS 1 // 0=simple stats, 1=more, 2=full
#ifdef LIZARD_STATS
    #include "test/lizard_stats.h"
#endif
#include "lizard_compress.h"
#include "lizard_decompress.h"
#include "lizard_common.h"
#include <stdio.h> // printf
#include <stdint.h> // intptr_t


/*-************************************
*  Local Structures and types
**************************************/
typedef enum { noDict = 0, withPrefix64k, usingExtDict } dict_directive;
typedef enum { full = 0, partial = 1 } earlyEnd_directive;

#include "lizard_decompress_lz4.h"
#ifndef USE_LZ4_ONLY
    #ifdef LIZARD_USE_TEST
        #include "test/lizard_common_test.h"
        #include "test/lizard_decompress_test.h"
    #else
        #include "lizard_decompress_liz.h"
    #endif
#endif
#include "entropy/huf.h"


/*-*****************************
*  Decompression functions
*******************************/

FORCE_INLINE size_t Lizard_readStream(int flag, const BYTE** ip, const BYTE* const iend, BYTE* op, BYTE* const oend, const BYTE** streamPtr, const BYTE** streamEnd, int streamFlag)
{
    if (!flag) {
        if (*ip > iend - 3) return 0;
        *streamPtr = *ip + 3;
        *streamEnd = *streamPtr + MEM_readLE24(*ip);
        if (*streamEnd < *streamPtr) return 0;
        *ip = *streamEnd;
#ifdef LIZARD_STATS
        uncompr_stream[streamFlag] += *streamEnd-*streamPtr;
#else
        (void)streamFlag;
#endif
        return 1;
    } else {
#ifndef LIZARD_NO_HUFFMAN
        size_t res, streamLen, comprStreamLen;

        if (*ip > iend - 6) return 0;
        streamLen = MEM_readLE24(*ip);
        comprStreamLen = MEM_readLE24(*ip + 3);

        if ((op > oend - streamLen) || (*ip + comprStreamLen > iend - 6)) return 0;
        res = HUF_decompress(op, streamLen, *ip + 6, comprStreamLen);
        if (HUF_isError(res) || (res != streamLen)) return 0;
        
        *ip += comprStreamLen + 6;
        *streamPtr = op;
        *streamEnd = *streamPtr + streamLen;
#ifdef LIZARD_STATS
        compr_stream[streamFlag] += comprStreamLen + 6;
        decompr_stream[streamFlag] += *streamEnd-*streamPtr;
#endif
        return 1;
#else
        fprintf(stderr, "compiled with LIZARD_NO_HUFFMAN\n");
        (void)op; (void)oend;
        return 0;
#endif
    }
}


FORCE_INLINE int Lizard_decompress_generic(
                 const char* source,
                 char* const dest,
                 int inputSize,
                 int outputSize,         /* this value is the max size of Output Buffer. */
                 int partialDecoding,    /* full, partial */
                 int targetOutputSize,   /* only used if partialDecoding==partial */
                 int dict,               /* noDict, withPrefix64k, usingExtDict */
                 const BYTE* const lowPrefix,  /* == dest if dict == noDict */
                 const BYTE* const dictStart,  /* only if dict==usingExtDict */
                 const size_t dictSize         /* note : = 0 if noDict */
                 )
{
    /* Local Variables */
    const BYTE* ip = (const BYTE*) source, *istart = (const BYTE*) source;
    const BYTE* const iend = ip + inputSize;
    BYTE* op = (BYTE*) dest;
    BYTE* const oend = op + outputSize;
    BYTE* oexit = op + targetOutputSize;
    Lizard_parameters params;
    Lizard_dstream_t ctx;
    BYTE* decompFlagsBase, *decompOff24Base, *decompOff16Base, *decompLiteralsBase = NULL;
    int res, compressionLevel;

    if (inputSize < 1) { LIZARD_LOG_DECOMPRESS("inputSize=%d outputSize=%d targetOutputSize=%d partialDecoding=%d\n", inputSize, outputSize, targetOutputSize, partialDecoding); return 0; }

    compressionLevel = *ip++;

    if (compressionLevel < LIZARD_MIN_CLEVEL || compressionLevel > LIZARD_MAX_CLEVEL) {
        LIZARD_LOG_DECOMPRESS("ERROR Lizard_decompress_generic inputSize=%d compressionLevel=%d\n", inputSize, compressionLevel);
        return -1;
    }

    LIZARD_LOG_DECOMPRESS("Lizard_decompress_generic ip=%p inputSize=%d targetOutputSize=%d dest=%p outputSize=%d cLevel=%d dict=%d dictSize=%d dictStart=%p partialDecoding=%d\n", ip, inputSize, targetOutputSize, dest, outputSize, compressionLevel, dict, (int)dictSize, dictStart, partialDecoding);

    decompLiteralsBase = (BYTE*)malloc(4*LIZARD_HUF_BLOCK_SIZE);
    if (!decompLiteralsBase) return -1;
    decompFlagsBase = decompLiteralsBase + LIZARD_HUF_BLOCK_SIZE;
    decompOff24Base = decompFlagsBase + LIZARD_HUF_BLOCK_SIZE;
    decompOff16Base = decompOff24Base + LIZARD_HUF_BLOCK_SIZE;

#ifdef LIZARD_STATS
    init_stats();
#endif
    (void)istart;

    while (ip < iend)
    {
        res = *ip++;
        if (res == LIZARD_FLAG_UNCOMPRESSED) /* uncompressed */
        {
            uint32_t length;
            if (ip > iend - 3) { LIZARD_LOG_DECOMPRESS("UNCOMPRESSED  ip[%p] > iend[%p] - 3\n", ip, iend); goto _output_error; }
            length = MEM_readLE24(ip);
            ip += 3;
         //   printf("%d: total=%d block=%d UNCOMPRESSED op=%p oexit=%p oend=%p\n", (int)(op-(BYTE*)dest) ,(int)(ip-istart), length, op, oexit, oend);
            if (ip + length > iend || op + length > oend) { LIZARD_LOG_DECOMPRESS("UNCOMPRESSED  ip[%p]+length[%d] > iend[%p]\n", ip, length, iend); goto _output_error; }
            memcpy(op, ip, length);
            op += length;
            ip += length;
            if ((partialDecoding) && (op >= oexit)) break;
#ifdef LIZARD_STATS
            uncompr_stream[LIZARD_STREAM_UNCOMPRESSED] += length;
#endif
            continue;
        }
        
        if (res&LIZARD_FLAG_LEN) {
            LIZARD_LOG_DECOMPRESS("res=%d\n", res); goto _output_error;
        }

        if (ip > iend - 5*3) goto _output_error;
        ctx.lenPtr = (const BYTE*)ip + 3;
        ctx.lenEnd = ctx.lenPtr + MEM_readLE24(ip);
        if (ctx.lenEnd < ctx.lenPtr || (ctx.lenEnd > iend - 3)) goto _output_error;
#ifdef LIZARD_STATS
        uncompr_stream[LIZARD_STREAM_LEN] += ctx.lenEnd-ctx.lenPtr + 3;
#endif
        ip = ctx.lenEnd;

        {   size_t streamLen;
#ifdef LIZARD_USE_LOGS
            const BYTE* ipos;
            size_t comprFlagsLen, comprLiteralsLen, total;
#endif
            streamLen = Lizard_readStream(res&LIZARD_FLAG_OFFSET16, &ip, iend, decompOff16Base, decompOff16Base + LIZARD_HUF_BLOCK_SIZE, &ctx.offset16Ptr, &ctx.offset16End, LIZARD_STREAM_OFFSET16);
            if (streamLen == 0) goto _output_error;

            streamLen = Lizard_readStream(res&LIZARD_FLAG_OFFSET24, &ip, iend, decompOff24Base, decompOff24Base + LIZARD_HUF_BLOCK_SIZE, &ctx.offset24Ptr, &ctx.offset24End, LIZARD_STREAM_OFFSET24);
            if (streamLen == 0) goto _output_error;

#ifdef LIZARD_USE_LOGS
            ipos = ip;
            streamLen = Lizard_readStream(res&LIZARD_FLAG_FLAGS, &ip, iend, decompFlagsBase, decompFlagsBase + LIZARD_HUF_BLOCK_SIZE, &ctx.flagsPtr, &ctx.flagsEnd, LIZARD_STREAM_FLAGS);
            if (streamLen == 0) goto _output_error;
            streamLen = (size_t)(ctx.flagsEnd-ctx.flagsPtr);
            comprFlagsLen = ((size_t)(ip - ipos) + 3 >= streamLen) ? 0 : (size_t)(ip - ipos);
            ipos = ip;
#else
            streamLen = Lizard_readStream(res&LIZARD_FLAG_FLAGS, &ip, iend, decompFlagsBase, decompFlagsBase + LIZARD_HUF_BLOCK_SIZE, &ctx.flagsPtr, &ctx.flagsEnd, LIZARD_STREAM_FLAGS);
            if (streamLen == 0) goto _output_error;
#endif

            streamLen = Lizard_readStream(res&LIZARD_FLAG_LITERALS, &ip, iend, decompLiteralsBase, decompLiteralsBase + LIZARD_HUF_BLOCK_SIZE, &ctx.literalsPtr, &ctx.literalsEnd, LIZARD_STREAM_LITERALS);
            if (streamLen == 0) goto _output_error;
#ifdef LIZARD_USE_LOGS
            streamLen = (size_t)(ctx.literalsEnd-ctx.literalsPtr);
            comprLiteralsLen = ((size_t)(ip - ipos) + 3 >= streamLen) ? 0 : (size_t)(ip - ipos);
            total = (size_t)(ip-(ctx.lenEnd-1));
#endif

            if (ip > iend) goto _output_error;

            LIZARD_LOG_DECOMPRESS("%d: total=%d block=%d flagsLen=%d(HUF=%d) literalsLen=%d(HUF=%d) offset16Len=%d offset24Len=%d lengthsLen=%d \n", (int)(op-(BYTE*)dest) ,(int)(ip-istart), (int)total, 
                        (int)(ctx.flagsEnd-ctx.flagsPtr), (int)comprFlagsLen, (int)(ctx.literalsEnd-ctx.literalsPtr), (int)comprLiteralsLen, 
                        (int)(ctx.offset16End-ctx.offset16Ptr), (int)(ctx.offset24End-ctx.offset24Ptr), (int)(ctx.lenEnd-ctx.lenPtr));
        }

        ctx.last_off = -LIZARD_INIT_LAST_OFFSET;
        params = Lizard_defaultParameters[compressionLevel - LIZARD_MIN_CLEVEL];
        if (params.decompressType == Lizard_coderwords_LZ4)
            res = Lizard_decompress_LZ4(&ctx, op, outputSize, partialDecoding, targetOutputSize, dict, lowPrefix, dictStart, dictSize, compressionLevel);
        else 
#ifdef USE_LZ4_ONLY
            res = Lizard_decompress_LZ4(&ctx, op, outputSize, partialDecoding, targetOutputSize, dict, lowPrefix, dictStart, dictSize, compressionLevel);
#else
            res = Lizard_decompress_LIZv1(&ctx, op, outputSize, partialDecoding, targetOutputSize, dict, lowPrefix, dictStart, dictSize, compressionLevel);
#endif        
        LIZARD_LOG_DECOMPRESS("Lizard_decompress_generic res=%d inputSize=%d\n", res, (int)(ctx.literalsEnd-ctx.lenEnd));

        if (res <= 0) { free(decompLiteralsBase); return res; }
        
        op += res;
        outputSize -= res;
        if ((partialDecoding) && (op >= oexit)) break;
    }

#ifdef LIZARD_STATS
    print_stats();
#endif

    LIZARD_LOG_DECOMPRESS("Lizard_decompress_generic total=%d\n", (int)(op-(BYTE*)dest));
    free(decompLiteralsBase);
    return (int)(op-(BYTE*)dest);

_output_error:
    LIZARD_LOG_DECOMPRESS("Lizard_decompress_generic ERROR ip=%p iend=%p\n", ip, iend);
    free(decompLiteralsBase);
    return -1;
}


int Lizard_decompress_safe(const char* source, char* dest, int compressedSize, int maxDecompressedSize)
{
    return Lizard_decompress_generic(source, dest, compressedSize, maxDecompressedSize, full, 0, noDict, (BYTE*)dest, NULL, 0);
}

int Lizard_decompress_safe_partial(const char* source, char* dest, int compressedSize, int targetOutputSize, int maxDecompressedSize)
{
    return Lizard_decompress_generic(source, dest, compressedSize, maxDecompressedSize, partial, targetOutputSize, noDict, (BYTE*)dest, NULL, 0);
}


/*===== streaming decompression functions =====*/


/*
 * If you prefer dynamic allocation methods,
 * Lizard_createStreamDecode()
 * provides a pointer (void*) towards an initialized Lizard_streamDecode_t structure.
 */
Lizard_streamDecode_t* Lizard_createStreamDecode(void)
{
    Lizard_streamDecode_t* lizards = (Lizard_streamDecode_t*) ALLOCATOR(1, sizeof(Lizard_streamDecode_t));
    return lizards;
}

int Lizard_freeStreamDecode (Lizard_streamDecode_t* Lizard_stream)
{
    FREEMEM(Lizard_stream);
    return 0;
}

/*!
 * Lizard_setStreamDecode() :
 * Use this function to instruct where to find the dictionary.
 * This function is not necessary if previous data is still available where it was decoded.
 * Loading a size of 0 is allowed (same effect as no dictionary).
 * Return : 1 if OK, 0 if error
 */
int Lizard_setStreamDecode (Lizard_streamDecode_t* Lizard_streamDecode, const char* dictionary, int dictSize)
{
    Lizard_streamDecode_t* lizardsd = (Lizard_streamDecode_t*) Lizard_streamDecode;
    lizardsd->prefixSize = (size_t) dictSize;
    lizardsd->prefixEnd = (const BYTE*) dictionary + dictSize;
    lizardsd->externalDict = NULL;
    lizardsd->extDictSize  = 0;
    return 1;
}

/*
*_continue() :
    These decoding functions allow decompression of multiple blocks in "streaming" mode.
    Previously decoded blocks must still be available at the memory position where they were decoded.
    If it's not possible, save the relevant part of decoded data into a safe buffer,
    and indicate where it stands using Lizard_setStreamDecode()
*/
int Lizard_decompress_safe_continue (Lizard_streamDecode_t* Lizard_streamDecode, const char* source, char* dest, int compressedSize, int maxOutputSize)
{
    Lizard_streamDecode_t* lizardsd = (Lizard_streamDecode_t*) Lizard_streamDecode;
    int result;

    if (lizardsd->prefixEnd == (BYTE*)dest) {
        result = Lizard_decompress_generic(source, dest, compressedSize, maxOutputSize,
                                        full, 0, usingExtDict, lizardsd->prefixEnd - lizardsd->prefixSize, lizardsd->externalDict, lizardsd->extDictSize);
        if (result <= 0) return result;
        lizardsd->prefixSize += result;
        lizardsd->prefixEnd  += result;
    } else {
        lizardsd->extDictSize = lizardsd->prefixSize;
        lizardsd->externalDict = lizardsd->prefixEnd - lizardsd->extDictSize;
        result = Lizard_decompress_generic(source, dest, compressedSize, maxOutputSize,
                                        full, 0, usingExtDict, (BYTE*)dest, lizardsd->externalDict, lizardsd->extDictSize);
        if (result <= 0) return result;
        lizardsd->prefixSize = result;
        lizardsd->prefixEnd  = (BYTE*)dest + result;
    }

    return result;
}


/*
Advanced decoding functions :
*_usingDict() :
    These decoding functions work the same as "_continue" ones,
    the dictionary must be explicitly provided within parameters
*/

int Lizard_decompress_safe_usingDict(const char* source, char* dest, int compressedSize, int maxOutputSize, const char* dictStart, int dictSize)
{
    if (dictSize==0)
        return Lizard_decompress_generic(source, dest, compressedSize, maxOutputSize, full, 0, noDict, (BYTE*)dest, NULL, 0);
    if (dictStart+dictSize == dest)
    {
        if (dictSize >= (int)(LIZARD_DICT_SIZE - 1))
            return Lizard_decompress_generic(source, dest, compressedSize, maxOutputSize, full, 0, withPrefix64k, (BYTE*)dest-LIZARD_DICT_SIZE, NULL, 0);
        return Lizard_decompress_generic(source, dest, compressedSize, maxOutputSize, full, 0, noDict, (BYTE*)dest-dictSize, NULL, 0);
    }
    return Lizard_decompress_generic(source, dest, compressedSize, maxOutputSize, full, 0, usingExtDict, (BYTE*)dest, (const BYTE*)dictStart, dictSize);
}

/* debug function */
int Lizard_decompress_safe_forceExtDict(const char* source, char* dest, int compressedSize, int maxOutputSize, const char* dictStart, int dictSize)
{
    return Lizard_decompress_generic(source, dest, compressedSize, maxOutputSize, full, 0, usingExtDict, (BYTE*)dest, (const BYTE*)dictStart, dictSize);
}

