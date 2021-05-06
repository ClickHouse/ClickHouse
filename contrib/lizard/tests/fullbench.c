/*
    bench.c - Demo program to benchmark open-source compression algorithm
    Copyright (C) Yann Collet 2012-2015
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

/**************************************
*  Compiler Options
**************************************/
/* Disable some Visual warning messages */
#define _CRT_SECURE_NO_WARNINGS
#define _CRT_SECURE_NO_DEPRECATE     /* VS2005 */

/* Unix Large Files support (>4GB) */
#if (defined(__sun__) && (!defined(__LP64__)))   // Sun Solaris 32-bits requires specific definitions
#  define _LARGEFILE_SOURCE
#  define _FILE_OFFSET_BITS 64
#elif ! defined(__LP64__)                        // No point defining Large file for 64 bit
#  define _LARGEFILE64_SOURCE
#endif

// S_ISREG & gettimeofday() are not supported by MSVC
#if defined(_MSC_VER) || defined(_WIN32)
#  define BMK_LEGACY_TIMER 1
#endif


/**************************************
*  Includes
**************************************/
#include <stdlib.h>      /* malloc, free */
#include <stdio.h>       /* fprintf, fopen, ftello64 */
#include <sys/types.h>   /* stat64 */
#include <sys/stat.h>    /* stat64 */
#include <string.h>      /* strcmp */
#include <time.h>        /* clock_t, clock(), CLOCKS_PER_SEC */

#include "lizard_compress.h"
#include "lizard_decompress.h"
#include "lizard_common.h"  /* Lizard_compress_MinLevel, Lizard_createStream_MinLevel */
#include "lizard_frame.h"

#include "xxhash/xxhash.h"


/**************************************
*  Compiler Options
**************************************/
/* S_ISREG & gettimeofday() are not supported by MSVC */
#if !defined(S_ISREG)
#  define S_ISREG(x) (((x) & S_IFMT) == S_IFREG)
#endif



/**************************************
*  Constants
**************************************/
#define PROGRAM_DESCRIPTION "Lizard speed analyzer"
#define AUTHOR "Yann Collet"
#define WELCOME_MESSAGE "*** %s v%s %i-bits, by %s ***\n", PROGRAM_DESCRIPTION, LIZARD_VERSION_STRING, (int)(sizeof(void*)*8), AUTHOR

#define NBLOOPS    6
#define TIMELOOP   (CLOCKS_PER_SEC * 25 / 10)

#define KNUTH      2654435761U
#define MAX_MEM    (1920 MB)
#define DEFAULT_CHUNKSIZE   (4 MB)

#define ALL_COMPRESSORS 0
#define ALL_DECOMPRESSORS 0


/**************************************
*  Local structures
**************************************/
struct chunkParameters
{
    U32   id;
    char* origBuffer;
    char* compressedBuffer;
    int   origSize;
    int   compressedSize;
};


/**************************************
*  Macros
**************************************/
#define DISPLAY(...) fprintf(stderr, __VA_ARGS__)
#define PROGRESS(...) g_noPrompt ? 0 : DISPLAY(__VA_ARGS__)


/**************************************
*  Benchmark Parameters
**************************************/
static int g_chunkSize = DEFAULT_CHUNKSIZE;
static int g_nbIterations = NBLOOPS;
static int g_pause = 0;
static int g_compressionTest = 1;
static int g_compressionAlgo = ALL_COMPRESSORS;
static int g_decompressionTest = 1;
static int g_decompressionAlgo = ALL_DECOMPRESSORS;
static int g_noPrompt = 0;

static void BMK_setBlocksize(int bsize)
{
    g_chunkSize = bsize;
    DISPLAY("-Using Block Size of %i KB-\n", g_chunkSize>>10);
}

static void BMK_setNbIterations(int nbLoops)
{
    g_nbIterations = nbLoops;
    DISPLAY("- %i iterations -\n", g_nbIterations);
}

static void BMK_setPause(void)
{
    g_pause = 1;
}


/*********************************************************
*  Private functions
*********************************************************/
static clock_t BMK_GetClockSpan( clock_t clockStart )
{
    return clock() - clockStart;   /* works even if overflow; max span ~30 mn */
}


static size_t BMK_findMaxMem(U64 requiredMem)
{
    size_t step = 64 MB;
    BYTE* testmem=NULL;

    requiredMem = (((requiredMem >> 26) + 1) << 26);
    requiredMem += 2*step;
    if (requiredMem > MAX_MEM) requiredMem = MAX_MEM;

    while (!testmem) {
        if (requiredMem > step) requiredMem -= step;
        else requiredMem >>= 1;
        testmem = (BYTE*) malloc ((size_t)requiredMem);
    }
    free (testmem);

    /* keep some space available */
    if (requiredMem > step) requiredMem -= step;
    else requiredMem >>= 1;

    return (size_t)requiredMem;
}


static U64 BMK_GetFileSize(const char* infilename)
{
    int r;
#if defined(_MSC_VER)
    struct _stat64 statbuf;
    r = _stat64(infilename, &statbuf);
#else
    struct stat statbuf;
    r = stat(infilename, &statbuf);
#endif
    if (r || !S_ISREG(statbuf.st_mode)) return 0;   /* No good... */
    return (U64)statbuf.st_size;
}


/*********************************************************
*  Benchmark function
*********************************************************/
Lizard_stream_t *Lizard_stream;
static void local_Lizard_createStream(void)
{
    Lizard_stream = Lizard_resetStream_MinLevel(Lizard_stream);
}

static int local_Lizard_saveDict(const char* in, char* out, int inSize)
{
    (void)in;
    return Lizard_saveDict(Lizard_stream, out, inSize);
}

static int local_Lizard_compress_default_large(const char* in, char* out, int inSize)
{
    return Lizard_compress_MinLevel(in, out, inSize, Lizard_compressBound(inSize));
}

static int local_Lizard_compress_default_small(const char* in, char* out, int inSize)
{
    return Lizard_compress_MinLevel(in, out, inSize, Lizard_compressBound(inSize)-1);
}

static int local_Lizard_compress_withState(const char* in, char* out, int inSize)
{
    return Lizard_compress_extState_MinLevel(Lizard_stream, in, out, inSize, Lizard_compressBound(inSize));
}

static int local_Lizard_compress_limitedOutput_withState(const char* in, char* out, int inSize)
{
    return Lizard_compress_extState_MinLevel(Lizard_stream, in, out, inSize, Lizard_compressBound(inSize)-1);
}

static int local_Lizard_compress_continue(const char* in, char* out, int inSize)
{
    return Lizard_compress_continue(Lizard_stream, in, out, inSize, Lizard_compressBound(inSize));
}

static int local_Lizard_compress_limitedOutput_continue(const char* in, char* out, int inSize)
{
    return Lizard_compress_continue(Lizard_stream, in, out, inSize, Lizard_compressBound(inSize)-1);
}


/* HC compression functions */
Lizard_stream_t* Lizard_streamPtr;
static void local_Lizard_resetStream(void)
{
    Lizard_streamPtr = Lizard_resetStream(Lizard_streamPtr, 0);
}

static int local_Lizard_saveDictHC(const char* in, char* out, int inSize)
{
    (void)in;
    return Lizard_saveDict(Lizard_streamPtr, out, inSize);
}

static int local_Lizard_compress_extState(const char* in, char* out, int inSize)
{
    return Lizard_compress_extState(Lizard_streamPtr, in, out, inSize, Lizard_compressBound(inSize), 0);
}

static int local_Lizard_compress_extState_limitedOutput(const char* in, char* out, int inSize)
{
    return Lizard_compress_extState(Lizard_streamPtr, in, out, inSize, Lizard_compressBound(inSize)-1, 0);
}

static int local_Lizard_compress(const char* in, char* out, int inSize)
{
    return Lizard_compress(in, out, inSize, Lizard_compressBound(inSize), 0);
}

static int local_Lizard_compress_limitedOutput(const char* in, char* out, int inSize)
{
    return Lizard_compress(in, out, inSize, Lizard_compressBound(inSize)-1, 0);
}

static int local_Lizard_compressHC_continue(const char* in, char* out, int inSize)
{
    return Lizard_compress_continue(Lizard_streamPtr, in, out, inSize, Lizard_compressBound(inSize));
}

static int local_Lizard_compress_continue_limitedOutput(const char* in, char* out, int inSize)
{
    return Lizard_compress_continue(Lizard_streamPtr, in, out, inSize, Lizard_compressBound(inSize)-1);
}


/* decompression functions */
static int local_Lizard_decompress_safe_usingDict(const char* in, char* out, int inSize, int outSize)
{
    (void)inSize;
    Lizard_decompress_safe_usingDict(in, out, inSize, outSize, out - 65536, 65536);
    return outSize;
}

extern int Lizard_decompress_safe_forceExtDict(const char* in, char* out, int inSize, int outSize, const char* dict, int dictSize);

static int local_Lizard_decompress_safe_forceExtDict(const char* in, char* out, int inSize, int outSize)
{
    (void)inSize;
    Lizard_decompress_safe_forceExtDict(in, out, inSize, outSize, out - 65536, 65536);
    return outSize;
}

static int local_Lizard_decompress_safe_partial(const char* in, char* out, int inSize, int outSize)
{
    return Lizard_decompress_safe_partial(in, out, inSize, outSize - 5, outSize);
}


/* frame functions */
static int local_LizardF_compressFrame(const char* in, char* out, int inSize)
{
    return (int)LizardF_compressFrame(out, 2*inSize + 16, in, inSize, NULL);
}

static LizardF_decompressionContext_t g_dCtx;

static int local_LizardF_decompress(const char* in, char* out, int inSize, int outSize)
{
    size_t srcSize = inSize;
    size_t dstSize = outSize;
    size_t result;
    result = LizardF_decompress(g_dCtx, out, &dstSize, in, &srcSize, NULL);
    if (result!=0) { DISPLAY("Error decompressing frame : unfinished frame (%d)\n", (int)result); exit(8); }
    if (srcSize != (size_t)inSize) { DISPLAY("Error decompressing frame : read size incorrect\n"); exit(9); }
    return (int)dstSize;
}


#define NB_COMPRESSION_ALGORITHMS 100
#define NB_DECOMPRESSION_ALGORITHMS 100
int fullSpeedBench(const char** fileNamesTable, int nbFiles)
{
    int fileIdx=0;

    /* Init */
    { size_t const errorCode = LizardF_createDecompressionContext(&g_dCtx, LIZARDF_VERSION);
      if (LizardF_isError(errorCode)) { DISPLAY("dctx allocation issue \n"); return 10; } }

  Lizard_streamPtr = Lizard_createStream(0);
  if (!Lizard_streamPtr) { DISPLAY("Lizard_streamPtr allocation issue \n"); return 10; }

  Lizard_stream = Lizard_createStream_MinLevel();
  if (!Lizard_stream) { DISPLAY("Lizard_stream allocation issue \n"); return 10; }

    /* Loop for each fileName */
    while (fileIdx<nbFiles) {
      char* orig_buff = NULL;
      struct chunkParameters* chunkP = NULL;
      char* compressed_buff=NULL;
      const char* const inFileName = fileNamesTable[fileIdx++];
      FILE* const inFile = fopen( inFileName, "rb" );
      U64   inFileSize;
      size_t benchedSize;
      int nbChunks;
      int maxCompressedChunkSize;
      size_t readSize;
      int compressedBuffSize;
      U32 crcOriginal;
      size_t errorCode;

      /* Check file existence */
      if (inFile==NULL) { DISPLAY( "Pb opening %s\n", inFileName); return 11; }

      /* Memory size adjustments */
      inFileSize = BMK_GetFileSize(inFileName);
      if (inFileSize==0) { DISPLAY( "file is empty\n"); fclose(inFile); return 11; }
      benchedSize = BMK_findMaxMem(inFileSize*2) / 2;   /* because 2 buffers */
      if (benchedSize==0) { DISPLAY( "not enough memory\n"); fclose(inFile); return 11; }
      if ((U64)benchedSize > inFileSize) benchedSize = (size_t)inFileSize;
      if (benchedSize < inFileSize)
          DISPLAY("Not enough memory for '%s' full size; testing %i MB only...\n", inFileName, (int)(benchedSize>>20));

      /* Allocation */
      chunkP = (struct chunkParameters*) malloc(((benchedSize / (size_t)g_chunkSize)+1) * sizeof(struct chunkParameters));
      orig_buff = (char*) malloc(benchedSize);
      nbChunks = (int) ((benchedSize + (g_chunkSize-1)) / g_chunkSize);
      maxCompressedChunkSize = Lizard_compressBound(g_chunkSize);
      compressedBuffSize = nbChunks * maxCompressedChunkSize;
      compressed_buff = (char*)malloc((size_t)compressedBuffSize);
      if(!chunkP || !orig_buff || !compressed_buff) {
          DISPLAY("\nError: not enough memory!\n");
          fclose(inFile);
          free(orig_buff);
          free(compressed_buff);
          free(chunkP);
          return(12);
      }

      /* Fill in src buffer */
      DISPLAY("Loading %s...       \r", inFileName);
      readSize = fread(orig_buff, 1, benchedSize, inFile);
      fclose(inFile);

      if (readSize != benchedSize) {
        DISPLAY("\nError: problem reading file '%s' !!    \n", inFileName);
        free(orig_buff);
        free(compressed_buff);
        free(chunkP);
        return 13;
      }

      /* Calculating input Checksum */
      crcOriginal = XXH32(orig_buff, benchedSize,0);


      /* Bench */
      { int loopNb, nb_loops, chunkNb, cAlgNb, dAlgNb;
        size_t cSize=0;
        double ratio=0.;

        DISPLAY("\r%79s\r", "");
        DISPLAY(" %s : \n", inFileName);

        /* Bench Compression Algorithms */
        for (cAlgNb=0; (cAlgNb <= NB_COMPRESSION_ALGORITHMS) && (g_compressionTest); cAlgNb++) {
            const char* compressorName;
            int (*compressionFunction)(const char*, char*, int);
            void (*initFunction)(void) = NULL;
            double bestTime = 100000000.;

            /* filter compressionAlgo only */
            if ((g_compressionAlgo != ALL_COMPRESSORS) && (g_compressionAlgo != cAlgNb)) continue;

            /* Init data chunks */
            {   int i;
                size_t remaining = benchedSize;
                char* in = orig_buff;
                char* out = compressed_buff;
                nbChunks = (int) (((int)benchedSize + (g_chunkSize-1))/ g_chunkSize);
                for (i=0; i<nbChunks; i++) {
                    chunkP[i].id = i;
                    chunkP[i].origBuffer = in; in += g_chunkSize;
                    if ((int)remaining > g_chunkSize) { chunkP[i].origSize = g_chunkSize; remaining -= g_chunkSize; } else { chunkP[i].origSize = (int)remaining; remaining = 0; }
                    chunkP[i].compressedBuffer = out; out += maxCompressedChunkSize;
                    chunkP[i].compressedSize = 0;
                }
            }

            switch(cAlgNb)
            {
            case 0 : DISPLAY("Compression functions : \n"); continue;
            case 1 : compressionFunction = local_Lizard_compress_default_large; compressorName = "Lizard_compress_MinLevel"; break;
            case 2 : compressionFunction = local_Lizard_compress_default_small; compressorName = "Lizard_compress_MinLevel(small dst)"; break;

            case 10: compressionFunction = local_Lizard_compress; compressorName = "Lizard_compress"; break;
            case 11: compressionFunction = local_Lizard_compress_limitedOutput; compressorName = "Lizard_compress limitedOutput"; break;
            case 12 : compressionFunction = local_Lizard_compress_extState; compressorName = "Lizard_compress_extState"; break;
            case 13: compressionFunction = local_Lizard_compress_extState_limitedOutput; compressorName = "Lizard_compress_extState limitedOutput"; break;
            case 14: compressionFunction = local_Lizard_compressHC_continue; initFunction = local_Lizard_resetStream; compressorName = "Lizard_compress_continue"; break;
            case 15: compressionFunction = local_Lizard_compress_continue_limitedOutput; initFunction = local_Lizard_resetStream; compressorName = "Lizard_compress_continue limitedOutput"; break;
            case 30: compressionFunction = local_LizardF_compressFrame; compressorName = "LizardF_compressFrame";
                        chunkP[0].origSize = (int)benchedSize; nbChunks=1;
                        break;
            case 40: compressionFunction = local_Lizard_saveDict; compressorName = "Lizard_saveDict";
                        Lizard_loadDict(Lizard_stream, chunkP[0].origBuffer, chunkP[0].origSize);
                        break;
            case 41: compressionFunction = local_Lizard_saveDictHC; compressorName = "Lizard_saveDict";
                        Lizard_loadDict(Lizard_streamPtr, chunkP[0].origBuffer, chunkP[0].origSize);
                        break;
            case 16: compressionFunction = local_Lizard_compress_withState; compressorName = "Lizard_compress_extState_MinLevel(1)"; break;
            case 17: compressionFunction = local_Lizard_compress_limitedOutput_withState; compressorName = "Lizard_compress_extState_MinLevel(1) limitedOutput"; break;
            case 18: compressionFunction = local_Lizard_compress_continue; initFunction = local_Lizard_createStream; compressorName = "Lizard_compress_continue(1)"; break;
            case 19: compressionFunction = local_Lizard_compress_limitedOutput_continue; initFunction = local_Lizard_createStream; compressorName = "Lizard_compress_continue(1) limitedOutput"; break;
            case 60: DISPLAY("Obsolete compression functions : \n"); continue;
            default :
                continue;   /* unknown ID : just skip */
            }

            for (loopNb = 1; loopNb <= g_nbIterations; loopNb++) {
                double averageTime;
                clock_t clockTime;

                PROGRESS("%1i- %-28.28s :%9i ->\r", loopNb, compressorName, (int)benchedSize);
                { size_t i; for (i=0; i<benchedSize; i++) compressed_buff[i]=(char)i; }     /* warming up memory */

                nb_loops = 0;
                clockTime = clock();
                while(clock() == clockTime);
                clockTime = clock();
                while(BMK_GetClockSpan(clockTime) < TIMELOOP) {
                    if (initFunction!=NULL) initFunction();
                    for (chunkNb=0; chunkNb<nbChunks; chunkNb++) {
                        chunkP[chunkNb].compressedSize = compressionFunction(chunkP[chunkNb].origBuffer, chunkP[chunkNb].compressedBuffer, chunkP[chunkNb].origSize);
                        if (chunkP[chunkNb].compressedSize==0) DISPLAY("ERROR ! %s() = 0 !! \n", compressorName), exit(1);
                    }
                    nb_loops++;
                }
                clockTime = BMK_GetClockSpan(clockTime);

                nb_loops += !nb_loops;   /* avoid division by zero */
                averageTime = ((double)clockTime) / nb_loops / CLOCKS_PER_SEC;
                if (averageTime < bestTime) bestTime = averageTime;
                cSize=0; for (chunkNb=0; chunkNb<nbChunks; chunkNb++) cSize += chunkP[chunkNb].compressedSize;
                ratio = (double)cSize/(double)benchedSize*100.;
                PROGRESS("%1i- %-28.28s :%9i ->%9i (%5.2f%%),%7.1f MB/s\r", loopNb, compressorName, (int)benchedSize, (int)cSize, ratio, (double)benchedSize / bestTime / 1000000);
            }

            if (ratio<100.)
                DISPLAY("%2i-%-28.28s :%9i ->%9i (%5.2f%%),%7.1f MB/s\n", cAlgNb, compressorName, (int)benchedSize, (int)cSize, ratio, (double)benchedSize / bestTime / 1000000);
            else
                DISPLAY("%2i-%-28.28s :%9i ->%9i (%5.1f%%),%7.1f MB/s\n", cAlgNb, compressorName, (int)benchedSize, (int)cSize, ratio, (double)benchedSize / bestTime / 100000);
        }

        /* Prepare layout for decompression */
        /* Init data chunks */
        { int i;
          size_t remaining = benchedSize;
          char* in = orig_buff;
          char* out = compressed_buff;

          nbChunks = (int) (((int)benchedSize + (g_chunkSize-1))/ g_chunkSize);
          for (i=0; i<nbChunks; i++) {
              chunkP[i].id = i;
              chunkP[i].origBuffer = in; in += g_chunkSize;
              if ((int)remaining > g_chunkSize) { chunkP[i].origSize = g_chunkSize; remaining -= g_chunkSize; } else { chunkP[i].origSize = (int)remaining; remaining = 0; }
              chunkP[i].compressedBuffer = out; out += maxCompressedChunkSize;
              chunkP[i].compressedSize = 0;
          }
        }
        for (chunkNb=0; chunkNb<nbChunks; chunkNb++) {
            chunkP[chunkNb].compressedSize = Lizard_compress_MinLevel(chunkP[chunkNb].origBuffer, chunkP[chunkNb].compressedBuffer, chunkP[chunkNb].origSize, Lizard_compressBound(chunkP[chunkNb].origSize));
            if (chunkP[chunkNb].compressedSize==0) DISPLAY("ERROR ! %s() = 0 !! \n", "Lizard_compress_MinLevel"), exit(1);
        }

        /* Decompression Algorithms */
        for (dAlgNb=0; (dAlgNb <= NB_DECOMPRESSION_ALGORITHMS) && (g_decompressionTest); dAlgNb++) {
            const char* dName;
            int (*decompressionFunction)(const char*, char*, int, int);
            double bestTime = 100000000.;

            if ((g_decompressionAlgo != ALL_DECOMPRESSORS) && (g_decompressionAlgo != dAlgNb)) continue;

            switch(dAlgNb)
            {
            case 0: DISPLAY("Decompression functions : \n"); continue;
            case 4: decompressionFunction = Lizard_decompress_safe; dName = "Lizard_decompress_safe"; break;
            case 6: decompressionFunction = local_Lizard_decompress_safe_usingDict; dName = "Lizard_decompress_safe_usingDict"; break;
            case 7: decompressionFunction = local_Lizard_decompress_safe_partial; dName = "Lizard_decompress_safe_partial"; break;
            case 8: decompressionFunction = local_Lizard_decompress_safe_forceExtDict; dName = "Lizard_decompress_safe_forceExtDict"; break;
            case 9: decompressionFunction = local_LizardF_decompress; dName = "LizardF_decompress";
                    errorCode = LizardF_compressFrame(compressed_buff, compressedBuffSize, orig_buff, benchedSize, NULL);
                    if (LizardF_isError(errorCode)) {
                        DISPLAY("Error while preparing compressed frame\n");
                        free(orig_buff);
                        free(compressed_buff);
                        free(chunkP);
                        return 1;
                    }
                    chunkP[0].origSize = (int)benchedSize;
                    chunkP[0].compressedSize = (int)errorCode;
                    nbChunks = 1;
                    break;
            default :
                continue;   /* skip if unknown ID */
            }

            { size_t i; for (i=0; i<benchedSize; i++) orig_buff[i]=0; }     /* zeroing source area, for CRC checking */

            for (loopNb = 1; loopNb <= g_nbIterations; loopNb++) {
                double averageTime;
                clock_t clockTime;
                U32 crcDecoded;

                PROGRESS("%1i- %-29.29s :%10i ->\r", loopNb, dName, (int)benchedSize);

                nb_loops = 0;
                clockTime = clock();
                while(clock() == clockTime);
                clockTime = clock();
                while(BMK_GetClockSpan(clockTime) < TIMELOOP) {
                    for (chunkNb=0; chunkNb<nbChunks; chunkNb++) {
                        int decodedSize = decompressionFunction(chunkP[chunkNb].compressedBuffer, chunkP[chunkNb].origBuffer, chunkP[chunkNb].compressedSize, chunkP[chunkNb].origSize);
                        if (chunkP[chunkNb].origSize != decodedSize) DISPLAY("ERROR ! %s() == %i != %i !! \n", dName, decodedSize, chunkP[chunkNb].origSize), exit(1);
                    }
                    nb_loops++;
                }
                clockTime = BMK_GetClockSpan(clockTime);

                nb_loops += !nb_loops;   /* Avoid division by zero */
                averageTime = (double)clockTime / nb_loops / CLOCKS_PER_SEC;
                if (averageTime < bestTime) bestTime = averageTime;

                PROGRESS("%1i- %-29.29s :%10i -> %7.1f MB/s\r", loopNb, dName, (int)benchedSize, (double)benchedSize / bestTime / 1000000);

                /* CRC Checking */
                crcDecoded = XXH32(orig_buff, (int)benchedSize, 0);
                if (crcOriginal!=crcDecoded) { DISPLAY("\n!!! WARNING !!! %14s : Invalid Checksum : %x != %x\n", inFileName, (unsigned)crcOriginal, (unsigned)crcDecoded); exit(1); }
            }

            DISPLAY("%2i-%-29.29s :%10i -> %7.1f MB/s\n", dAlgNb, dName, (int)benchedSize, (double)benchedSize / bestTime / 1000000);
        }
      }
      free(orig_buff);
      free(compressed_buff);
      free(chunkP);
    }

    Lizard_freeStream(Lizard_stream);
    Lizard_freeStream(Lizard_streamPtr);
    LizardF_freeDecompressionContext(g_dCtx);
    if (g_pause) { printf("press enter...\n"); (void)getchar(); }

    return 0;
}


static int usage(const char* exename)
{
    DISPLAY( "Usage :\n");
    DISPLAY( "      %s [arg] file1 file2 ... fileX\n", exename);
    DISPLAY( "Arguments :\n");
    DISPLAY( " -c     : compression tests only\n");
    DISPLAY( " -d     : decompression tests only\n");
    DISPLAY( " -H/-h  : Help (this text + advanced options)\n");
    return 0;
}

static int usage_advanced(void)
{
    DISPLAY( "\nAdvanced options :\n");
    DISPLAY( " -c#    : test only compression function # [1-%i]\n", NB_COMPRESSION_ALGORITHMS);
    DISPLAY( " -d#    : test only decompression function # [1-%i]\n", NB_DECOMPRESSION_ALGORITHMS);
    DISPLAY( " -i#    : iteration loops [1-9](default : %i)\n", NBLOOPS);
    DISPLAY( " -B#    : Block size [4-7](default : 7)\n");
    return 0;
}

static int badusage(const char* exename)
{
    DISPLAY("Wrong parameters\n");
    usage(exename);
    return 0;
}

int main(int argc, const char** argv)
{
    int i,
        filenamesStart=2;
    const char* exename = argv[0];
    const char* input_filename=0;

    // Welcome message
    DISPLAY(WELCOME_MESSAGE);

    if (argc<2) { badusage(exename); return 1; }

    for(i=1; i<argc; i++) {
        const char* argument = argv[i];

        if(!argument) continue;   // Protection if argument empty
        if (!strcmp(argument, "--no-prompt")) {
            g_noPrompt = 1;
            continue;
        }

        // Decode command (note : aggregated commands are allowed)
        if (argument[0]=='-') {
            while (argument[1]!=0) {
                argument ++;

                switch(argument[0])
                {
                    // Select compression algorithm only
                case 'c':
                    g_decompressionTest = 0;
                    while ((argument[1]>= '0') && (argument[1]<= '9')) {
                        g_compressionAlgo *= 10;
                        g_compressionAlgo += argument[1] - '0';
                        argument++;
                    }
                    break;

                    // Select decompression algorithm only
                case 'd':
                    g_compressionTest = 0;
                    while ((argument[1]>= '0') && (argument[1]<= '9')) {
                        g_decompressionAlgo *= 10;
                        g_decompressionAlgo += argument[1] - '0';
                        argument++;
                    }
                    break;

                    // Display help on usage
                case 'h' :
                case 'H': usage(exename); usage_advanced(); return 0;

                    // Modify Block Properties
                case 'B':
                    while (argument[1]!=0)
                    switch(argument[1])
                    {
                    case '4':
                    case '5':
                    case '6':
                    case '7':
                    {   int B = argument[1] - '0';
                        int S = 1 << (8 + 2*B);
                        BMK_setBlocksize(S);
                        argument++;
                        break;
                    }
                    case 'D': argument++; break;
                    default : goto _exit_blockProperties;
                    }
_exit_blockProperties:
                    break;

                    // Modify Nb Iterations
                case 'i':
                    if ((argument[1] >='0') && (argument[1] <='9')) {
                        int iters = argument[1] - '0';
                        BMK_setNbIterations(iters);
                        argument++;
                    }
                    break;

                    // Pause at the end (hidden option)
                case 'p': BMK_setPause(); break;

                    // Unknown command
                default : badusage(exename); return 1;
                }
            }
            continue;
        }

        // first provided filename is input
        if (!input_filename) { input_filename=argument; filenamesStart=i; continue; }

    }

    // No input filename ==> Error
    if(!input_filename) { badusage(exename); return 1; }

    return fullSpeedBench(argv+filenamesStart, argc-filenamesStart);

}
