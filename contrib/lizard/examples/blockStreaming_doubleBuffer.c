// Lizard streaming API example : double buffer
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

enum {
    BLOCK_BYTES = 1024 * 8,
//  BLOCK_BYTES = 1024 * 64,
};


size_t write_int(FILE* fp, int i) {
    return fwrite(&i, sizeof(i), 1, fp);
}

size_t write_bin(FILE* fp, const void* array, size_t arrayBytes) {
    return fwrite(array, 1, arrayBytes, fp);
}

size_t read_int(FILE* fp, int* i) {
    return fread(i, sizeof(*i), 1, fp);
}

size_t read_bin(FILE* fp, void* array, size_t arrayBytes) {
    return fread(array, 1, arrayBytes, fp);
}


void test_compress(FILE* outFp, FILE* inpFp)
{
    Lizard_stream_t* lizardStream = Lizard_createStream_MinLevel();
    char inpBuf[2][BLOCK_BYTES];
    int  inpBufIndex = 0;

    if (!lizardStream) return;
    lizardStream = Lizard_resetStream_MinLevel(lizardStream);
    if (!lizardStream) return;

    for(;;) {
        char* const inpPtr = inpBuf[inpBufIndex];
        const int inpBytes = (int) read_bin(inpFp, inpPtr, BLOCK_BYTES);
        if(0 == inpBytes) {
            break;
        }

        {
            char cmpBuf[LIZARD_COMPRESSBOUND(BLOCK_BYTES)];
            const int cmpBytes = Lizard_compress_continue(lizardStream, inpPtr, cmpBuf, inpBytes, sizeof(cmpBuf));
            if(cmpBytes <= 0) {
                break;
            }
            write_int(outFp, cmpBytes);
            write_bin(outFp, cmpBuf, (size_t) cmpBytes);
        }

        inpBufIndex = (inpBufIndex + 1) % 2;
    }

    write_int(outFp, 0);
    Lizard_freeStream(lizardStream);
}


void test_decompress(FILE* outFp, FILE* inpFp)
{
    Lizard_streamDecode_t lizardStreamDecode_body;
    Lizard_streamDecode_t* lizardStreamDecode = &lizardStreamDecode_body;

    char decBuf[2][BLOCK_BYTES];
    int  decBufIndex = 0;

    Lizard_setStreamDecode(lizardStreamDecode, NULL, 0);

    for(;;) {
        char cmpBuf[LIZARD_COMPRESSBOUND(BLOCK_BYTES)];
        int  cmpBytes = 0;

        {
            const size_t readCount0 = read_int(inpFp, &cmpBytes);
            if(readCount0 != 1 || cmpBytes <= 0) {
                break;
            }

            const size_t readCount1 = read_bin(inpFp, cmpBuf, (size_t) cmpBytes);
            if(readCount1 != (size_t) cmpBytes) {
                break;
            }
        }

        {
            char* const decPtr = decBuf[decBufIndex];
            const int decBytes = Lizard_decompress_safe_continue(
                lizardStreamDecode, cmpBuf, decPtr, cmpBytes, BLOCK_BYTES);
            if(decBytes <= 0) {
                break;
            }
            write_bin(outFp, decPtr, (size_t) decBytes);
        }

        decBufIndex = (decBufIndex + 1) % 2;
    }
}


int compare(FILE* fp0, FILE* fp1)
{
    int result = 0;

    while(0 == result) {
        char b0[65536];
        char b1[65536];
        const size_t r0 = read_bin(fp0, b0, sizeof(b0));
        const size_t r1 = read_bin(fp1, b1, sizeof(b1));

        result = (int) r0 - (int) r1;

        if(0 == r0 || 0 == r1) {
            break;
        }
        if(0 == result) {
            result = memcmp(b0, b1, r0);
        }
    }

    return result;
}


int main(int argc, char* argv[])
{
    char inpFilename[256] = { 0 };
    char lizardFilename[256] = { 0 };
    char decFilename[256] = { 0 };

    if(argc < 2) {
        printf("Please specify input filename\n");
        return 0;
    }

    snprintf(inpFilename, 256, "%s", argv[1]);
    snprintf(lizardFilename, 256, "%s.lizs-%d", argv[1], BLOCK_BYTES);
    snprintf(decFilename, 256, "%s.lizs-%d.dec", argv[1], BLOCK_BYTES);

    printf("inp = [%s]\n", inpFilename);
    printf("lizard = [%s]\n", lizardFilename);
    printf("dec = [%s]\n", decFilename);

    // compress
    {
        FILE* inpFp = fopen(inpFilename, "rb");
        FILE* outFp = fopen(lizardFilename, "wb");

        printf("compress : %s -> %s\n", inpFilename, lizardFilename);
        test_compress(outFp, inpFp);
        printf("compress : done\n");

        fclose(outFp);
        fclose(inpFp);
    }

    // decompress
    {
        FILE* inpFp = fopen(lizardFilename, "rb");
        FILE* outFp = fopen(decFilename, "wb");

        printf("decompress : %s -> %s\n", lizardFilename, decFilename);
        test_decompress(outFp, inpFp);
        printf("decompress : done\n");

        fclose(outFp);
        fclose(inpFp);
    }

    // verify
    {
        FILE* inpFp = fopen(inpFilename, "rb");
        FILE* decFp = fopen(decFilename, "rb");

        printf("verify : %s <-> %s\n", inpFilename, decFilename);
        const int cmp = compare(inpFp, decFp);
        if(0 == cmp) {
            printf("verify : OK\n");
        } else {
            printf("verify : NG\n");
        }

        fclose(decFp);
        fclose(inpFp);
    }

    return 0;
}
