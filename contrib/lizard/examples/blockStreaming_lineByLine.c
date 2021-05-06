// Lizard streaming API example : line-by-line logfile compression
// Copyright : Takayuki Matsuoka


#ifdef _MSC_VER    /* Visual Studio */
#  define _CRT_SECURE_NO_WARNINGS
#  define snprintf sprintf_s
#endif
#include "lizard_common.h"
#include "lizard_decompress.h"
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static size_t write_uint16(FILE* fp, uint16_t i)
{
    return fwrite(&i, sizeof(i), 1, fp);
}

static size_t write_bin(FILE* fp, const void* array, int arrayBytes)
{
    return fwrite(array, 1, arrayBytes, fp);
}

static size_t read_uint16(FILE* fp, uint16_t* i)
{
    return fread(i, sizeof(*i), 1, fp);
}

static size_t read_bin(FILE* fp, void* array, int arrayBytes)
{
    return fread(array, 1, arrayBytes, fp);
}


static void test_compress(
    FILE* outFp,
    FILE* inpFp,
    size_t messageMaxBytes,
    size_t ringBufferBytes)
{
    Lizard_stream_t* const lizardStream = Lizard_createStream_MinLevel();
    const size_t cmpBufBytes = LIZARD_COMPRESSBOUND(messageMaxBytes);
    char* const cmpBuf = (char*) malloc(cmpBufBytes);
    char* const inpBuf = (char*) malloc(ringBufferBytes);
    int inpOffset = 0;

    for ( ; ; )
    {
        char* const inpPtr = &inpBuf[inpOffset];

#if 0
        // Read random length data to the ring buffer.
        const int randomLength = (rand() % messageMaxBytes) + 1;
        const int inpBytes = (int) read_bin(inpFp, inpPtr, randomLength);
        if (0 == inpBytes) break;
#else
        // Read line to the ring buffer.
        int inpBytes = 0;
        if (!fgets(inpPtr, (int) messageMaxBytes, inpFp))
            break;
        inpBytes = (int) strlen(inpPtr);
#endif

        {
            const int cmpBytes = Lizard_compress_continue(lizardStream, inpPtr, cmpBuf, inpBytes, cmpBufBytes);
            if (cmpBytes <= 0) break;
            write_uint16(outFp, (uint16_t) cmpBytes);
            write_bin(outFp, cmpBuf, cmpBytes);

            // Add and wraparound the ringbuffer offset
            inpOffset += inpBytes;
            if ((size_t)inpOffset >= ringBufferBytes - messageMaxBytes) inpOffset = 0;
        }
    }
    write_uint16(outFp, 0);

    free(inpBuf);
    free(cmpBuf);
    Lizard_freeStream(lizardStream);
}


static void test_decompress(
    FILE* outFp,
    FILE* inpFp,
    size_t messageMaxBytes,
    size_t ringBufferBytes)
{
    Lizard_streamDecode_t* const lizardStreamDecode = Lizard_createStreamDecode();
    char* const cmpBuf = (char*) malloc(LIZARD_COMPRESSBOUND(messageMaxBytes));
    char* const decBuf = (char*) malloc(ringBufferBytes);
    int decOffset = 0;

    for ( ; ; )
    {
        uint16_t cmpBytes = 0;

        if (read_uint16(inpFp, &cmpBytes) != 1) break;
        if (cmpBytes <= 0) break;
        if (read_bin(inpFp, cmpBuf, cmpBytes) != cmpBytes) break;

        {
            char* const decPtr = &decBuf[decOffset];
            const int decBytes = Lizard_decompress_safe_continue(
                lizardStreamDecode, cmpBuf, decPtr, cmpBytes, (int) messageMaxBytes);
            if (decBytes <= 0) break;
            write_bin(outFp, decPtr, decBytes);

            // Add and wraparound the ringbuffer offset
            decOffset += decBytes;
            if ((size_t)decOffset >= ringBufferBytes - messageMaxBytes) decOffset = 0;
        }
    }

    free(decBuf);
    free(cmpBuf);
    Lizard_freeStreamDecode(lizardStreamDecode);
}


static int compare(FILE* f0, FILE* f1)
{
    int result = 0;
    const size_t tempBufferBytes = 65536;
    char* const b0 = (char*) malloc(tempBufferBytes);
    char* const b1 = (char*) malloc(tempBufferBytes);

    while(0 == result)
    {
        const size_t r0 = fread(b0, 1, tempBufferBytes, f0);
        const size_t r1 = fread(b1, 1, tempBufferBytes, f1);

        result = (int) r0 - (int) r1;

        if (0 == r0 || 0 == r1) break;
        if (0 == result) result = memcmp(b0, b1, r0);
    }

    free(b1);
    free(b0);
    return result;
}


int main(int argc, char* argv[])
{
    enum {
        MESSAGE_MAX_BYTES   = 1024,
        RING_BUFFER_BYTES   = 1024 * 256 + MESSAGE_MAX_BYTES,
    };

    char inpFilename[256] = { 0 };
    char lizardFilename[256] = { 0 };
    char decFilename[256] = { 0 };

    if (argc < 2)
    {
        printf("Please specify input filename\n");
        return 0;
    }

    snprintf(inpFilename, 256, "%s", argv[1]);
    snprintf(lizardFilename, 256, "%s.lizs", argv[1]);
    snprintf(decFilename, 256, "%s.lizs.dec", argv[1]);

    printf("inp = [%s]\n", inpFilename);
    printf("lizard = [%s]\n", lizardFilename);
    printf("dec = [%s]\n", decFilename);

    // compress
    {
        FILE* inpFp = fopen(inpFilename, "rb");
        FILE* outFp = fopen(lizardFilename, "wb");

        test_compress(outFp, inpFp, MESSAGE_MAX_BYTES, RING_BUFFER_BYTES);

        fclose(outFp);
        fclose(inpFp);
    }

    // decompress
    {
        FILE* inpFp = fopen(lizardFilename, "rb");
        FILE* outFp = fopen(decFilename, "wb");

        test_decompress(outFp, inpFp, MESSAGE_MAX_BYTES, RING_BUFFER_BYTES);

        fclose(outFp);
        fclose(inpFp);
    }

    // verify
    {
        FILE* inpFp = fopen(inpFilename, "rb");
        FILE* decFp = fopen(decFilename, "rb");

        const int cmp = compare(inpFp, decFp);
        if (0 == cmp)
            printf("Verify : OK\n");
        else
            printf("Verify : NG\n");

        fclose(decFp);
        fclose(inpFp);
    }

    return 0;
}
