/*
  Lizardio.c - Lizard File/Stream Interface
  Copyright (C) Yann Collet 2011-2015
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
/*
  Note : this is stand-alone program.
  It is not part of Lizard compression library, it is a user code of the Lizard library.
  - The license of Lizard library is BSD.
  - The license of xxHash library is BSD.
  - The license of this source file is GPLv2.
*/

/*-************************************
*  Compiler options
**************************************/
#ifdef _MSC_VER    /* Visual Studio */
#  pragma warning(disable : 4127)    /* disable: C4127: conditional expression is constant */
#endif
#if defined(__MINGW32__) && !defined(_POSIX_SOURCE)
#  define _POSIX_SOURCE 1          /* disable %llu warnings with MinGW on Windows */
#endif


/*****************************
*  Includes
*****************************/
#include "platform.h"  /* Large File Support, SET_BINARY_MODE, SET_SPARSE_FILE_MODE, PLATFORM_POSIX_VERSION, __64BIT__ */
#include "util.h"      /* UTIL_getFileStat, UTIL_setFileStat */ 
#include <stdio.h>     /* fprintf, fopen, fread, stdin, stdout, fflush, getchar */
#include <stdlib.h>    /* malloc, free */
#include <string.h>    /* strcmp, strlen */
#include <time.h>      /* clock */
#include <sys/types.h> /* stat64 */
#include <sys/stat.h>  /* stat64 */
#include "lizardio.h"
#include "lizard_frame.h"



/*****************************
*  Constants
*****************************/
#define KB *(1 <<10)
#define MB *(1 <<20)
#define GB *(1U<<30)

#define _1BIT  0x01
#define _2BITS 0x03
#define _3BITS 0x07
#define _4BITS 0x0F
#define _8BITS 0xFF

#define MAGICNUMBER_SIZE    4
#define LIZARDIO_MAGICNUMBER   0x184D2206U
#define LIZARDIO_SKIPPABLE0    0x184D2A50U
#define LIZARDIO_SKIPPABLEMASK 0xFFFFFFF0U

#define CACHELINE 64
#define MIN_STREAM_BUFSIZE (192 KB)
#define LIZARDIO_BLOCKSIZEID_DEFAULT 7

#define sizeT sizeof(size_t)
#define maskT (sizeT - 1)


/**************************************
*  Macros
**************************************/
#define DISPLAY(...)         fprintf(stderr, __VA_ARGS__)
#define DISPLAYLEVEL(l, ...) if (g_displayLevel>=l) { DISPLAY(__VA_ARGS__); }
static int g_displayLevel = 0;   /* 0 : no display  ; 1: errors  ; 2 : + result + interaction + warnings ; 3 : + progression; 4 : + information */

#define DISPLAYUPDATE(l, ...) if (g_displayLevel>=l) { \
            if (((clock_t)(g_time - clock()) > refreshRate) || (g_displayLevel>=4)) \
            { g_time = clock(); DISPLAY(__VA_ARGS__); \
            if (g_displayLevel>=4) fflush(stderr); } }
static const clock_t refreshRate = CLOCKS_PER_SEC / 6;
static clock_t g_time = 0;


/**************************************
*  Local Parameters
**************************************/
static int g_overwrite = 1;
static int g_testMode = 0;
static int g_blockSizeId = LIZARDIO_BLOCKSIZEID_DEFAULT;
static int g_blockChecksum = 0;
static int g_streamChecksum = 1;
static int g_blockIndependence = 1;
static int g_sparseFileSupport = 1;
static int g_contentSizeFlag = 0;


/**************************************
*  Exceptions
***************************************/
#ifndef DEBUG
#  define DEBUG 0
#endif
#define DEBUGOUTPUT(...) if (DEBUG) DISPLAY(__VA_ARGS__);
#define EXM_THROW(error, ...)                                             \
{                                                                         \
    DEBUGOUTPUT("Error defined at %s, line %i : \n", __FILE__, __LINE__); \
    DISPLAYLEVEL(1, "Error %i : ", error);                                \
    DISPLAYLEVEL(1, __VA_ARGS__);                                         \
    DISPLAYLEVEL(1, " \n");                                               \
    exit(error);                                                          \
}


/**************************************
*  Version modifiers
**************************************/
#define EXTENDED_ARGUMENTS
#define EXTENDED_HELP
#define EXTENDED_FORMAT
#define DEFAULT_DECOMPRESSOR LizardIO_decompressLizardF


/* ************************************************** */
/* ****************** Parameters ******************** */
/* ************************************************** */

/* Default setting : overwrite = 1; return : overwrite mode (0/1) */
int LizardIO_setOverwrite(int yes)
{
   g_overwrite = (yes!=0);
   return g_overwrite;
}

/* Default setting : testMode = 0; return : testMode (0/1) */
int LizardIO_setTestMode(int yes)
{
   g_testMode = (yes!=0);
   return g_testMode;
}

/* blockSizeID : valid values : 1-7 */
size_t LizardIO_setBlockSizeID(unsigned bsid)
{
    static const int blockSizeTable[] = { 128 KB, 256 KB, 1 MB, 4 MB, 16 MB, 64 MB, 256 MB };
    static const unsigned minBlockSizeID = 1;
    static const unsigned maxBlockSizeID = 7;
    if ((bsid < minBlockSizeID) || (bsid > maxBlockSizeID)) return 0;

    g_blockSizeId = bsid;
    return blockSizeTable[g_blockSizeId-minBlockSizeID];
} 


static size_t LizardIO_GetBlockSize_FromBlockId(unsigned blockSizeID)
{
    static const size_t blockSizes[7] = { 128 KB, 256 KB, 1 MB, 4 MB, 16 MB, 64 MB, 256 MB };

    if (blockSizeID == 0) blockSizeID = LIZARDIO_BLOCKSIZEID_DEFAULT;
    blockSizeID -= 1;
    if (blockSizeID >= 7) blockSizeID = LIZARDIO_BLOCKSIZEID_DEFAULT - 1;

    return blockSizes[blockSizeID];
}



int LizardIO_setBlockMode(LizardIO_blockMode_t blockMode)
{
    g_blockIndependence = (blockMode == LizardIO_blockIndependent);
    return g_blockIndependence;
}

/* Default setting : no checksum */
int LizardIO_setBlockChecksumMode(int xxhash)
{
    g_blockChecksum = (xxhash != 0);
    return g_blockChecksum;
}

/* Default setting : checksum enabled */
int LizardIO_setStreamChecksumMode(int xxhash)
{
    g_streamChecksum = (xxhash != 0);
    return g_streamChecksum;
}

/* Default setting : 0 (no notification) */
int LizardIO_setNotificationLevel(int level)
{
    g_displayLevel = level;
    return g_displayLevel;
}

/* Default setting : 0 (disabled) */
int LizardIO_setSparseFile(int enable)
{
    g_sparseFileSupport = (enable!=0);
    return g_sparseFileSupport;
}

/* Default setting : 0 (disabled) */
int LizardIO_setContentSize(int enable)
{
    g_contentSizeFlag = (enable!=0);
    return g_contentSizeFlag;
}

static U32 g_removeSrcFile = 0;
void LizardIO_setRemoveSrcFile(unsigned flag) { g_removeSrcFile = (flag>0); }



/* ************************************************************************ **
** ********************** Lizard File / Pipe compression ********************* **
** ************************************************************************ */

static int LizardIO_isSkippableMagicNumber(unsigned int magic) { return (magic & LIZARDIO_SKIPPABLEMASK) == LIZARDIO_SKIPPABLE0; }


/** LizardIO_openSrcFile() :
 * condition : `dstFileName` must be non-NULL.
 * @result : FILE* to `dstFileName`, or NULL if it fails */
static FILE* LizardIO_openSrcFile(const char* srcFileName)
{
    FILE* f;

    if (!strcmp (srcFileName, stdinmark)) {
        DISPLAYLEVEL(4,"Using stdin for input\n");
        f = stdin;
        SET_BINARY_MODE(stdin);
    } else {
        f = fopen(srcFileName, "rb");
        if ( f==NULL ) DISPLAYLEVEL(1, "%s: %s \n", srcFileName, strerror(errno));
    }

    return f;
}

/** FIO_openDstFile() :
 * condition : `dstFileName` must be non-NULL.
 * @result : FILE* to `dstFileName`, or NULL if it fails */
static FILE* LizardIO_openDstFile(const char* dstFileName)
{
    FILE* f;

    if (!strcmp (dstFileName, stdoutmark)) {
        DISPLAYLEVEL(4,"Using stdout for output\n");
        f = stdout;
        SET_BINARY_MODE(stdout);
        if (g_sparseFileSupport==1) {
            g_sparseFileSupport = 0;
            DISPLAYLEVEL(4, "Sparse File Support is automatically disabled on stdout ; try --sparse \n");
        }
    } else {
        if (!g_overwrite && strcmp (dstFileName, nulmark)) {  /* Check if destination file already exists */
            f = fopen( dstFileName, "rb" );
            if (f != NULL) {  /* dest exists, prompt for overwrite authorization */
                fclose(f);
                if (g_displayLevel <= 1) {  /* No interaction possible */
                    DISPLAY("%s already exists; not overwritten  \n", dstFileName);
                    return NULL;
                }
                DISPLAY("%s already exists; do you wish to overwrite (y/N) ? ", dstFileName);
                {   int ch = getchar();
                    if ((ch!='Y') && (ch!='y')) {
                        DISPLAY("    not overwritten  \n");
                        return NULL;
                    }
                    while ((ch!=EOF) && (ch!='\n')) ch = getchar();  /* flush rest of input line */
        }   }   }
        f = fopen( dstFileName, "wb" );
        if (f==NULL) DISPLAYLEVEL(1, "%s: %s\n", dstFileName, strerror(errno));
    }

    /* sparse file */
    if (f && g_sparseFileSupport) { SET_SPARSE_FILE_MODE(f); }

    return f;
}


/* unoptimized version; solves endianess & alignment issues */
static void LizardIO_writeLE32 (void* p, unsigned value32)
{
    unsigned char* dstPtr = (unsigned char*)p;
    dstPtr[0] = (unsigned char)value32;
    dstPtr[1] = (unsigned char)(value32 >> 8);
    dstPtr[2] = (unsigned char)(value32 >> 16);
    dstPtr[3] = (unsigned char)(value32 >> 24);
}



/*********************************************
*  Compression using Frame format
*********************************************/

typedef struct {
    void*  srcBuffer;
    size_t srcBufferSize;
    void*  dstBuffer;
    size_t dstBufferSize;
    LizardF_compressionContext_t ctx;
} cRess_t;

static cRess_t LizardIO_createCResources(void)
{
    const size_t blockSize = (size_t)LizardIO_GetBlockSize_FromBlockId (g_blockSizeId);
    cRess_t ress;

    LizardF_errorCode_t const errorCode = LizardF_createCompressionContext(&(ress.ctx), LIZARDF_VERSION);
    if (LizardF_isError(errorCode)) EXM_THROW(30, "Allocation error : can't create LizardF context : %s", LizardF_getErrorName(errorCode));

    /* Allocate Memory */
    ress.srcBuffer = malloc(blockSize);
    ress.srcBufferSize = blockSize;
    ress.dstBufferSize = LizardF_compressFrameBound(blockSize, NULL);   /* cover worst case */
    ress.dstBuffer = malloc(ress.dstBufferSize);
    if (!ress.srcBuffer || !ress.dstBuffer) EXM_THROW(31, "Allocation error : not enough memory");

    return ress;
}

static void LizardIO_freeCResources(cRess_t ress)
{
    free(ress.srcBuffer);
    free(ress.dstBuffer);
    { LizardF_errorCode_t const errorCode = LizardF_freeCompressionContext(ress.ctx);
      if (LizardF_isError(errorCode)) EXM_THROW(38, "Error : can't free LizardF context resource : %s", LizardF_getErrorName(errorCode)); }
}

/*
 * LizardIO_compressFilename_extRess()
 * result : 0 : compression completed correctly
 *          1 : missing or pb opening srcFileName
 */
static int LizardIO_compressFilename_extRess(cRess_t ress, const char* srcFileName, const char* dstFileName, int compressionLevel)
{
    unsigned long long filesize = 0;
    unsigned long long compressedfilesize = 0;
    FILE* srcFile;
    FILE* dstFile;
    void* const srcBuffer = ress.srcBuffer;
    void* const dstBuffer = ress.dstBuffer;
    const size_t dstBufferSize = ress.dstBufferSize;
    const size_t blockSize = (size_t)LizardIO_GetBlockSize_FromBlockId (g_blockSizeId);
    size_t readSize;
    LizardF_compressionContext_t ctx = ress.ctx;   /* just a pointer */
    LizardF_preferences_t prefs;

    /* Init */
    srcFile = LizardIO_openSrcFile(srcFileName);
    if (srcFile == NULL) return 1;
    dstFile = LizardIO_openDstFile(dstFileName);
    if (dstFile == NULL) { fclose(srcFile); return 1; }
    memset(&prefs, 0, sizeof(prefs));


    /* Set compression parameters */
    prefs.autoFlush = 1;
    prefs.compressionLevel = compressionLevel;
    prefs.frameInfo.blockMode = (LizardF_blockMode_t)g_blockIndependence;
    prefs.frameInfo.blockSizeID = (LizardF_blockSizeID_t)g_blockSizeId;
    prefs.frameInfo.contentChecksumFlag = (LizardF_contentChecksum_t)g_streamChecksum;
    if (g_contentSizeFlag) {
      U64 const fileSize = UTIL_getFileSize(srcFileName);
      prefs.frameInfo.contentSize = fileSize;   /* == 0 if input == stdin */
      if (fileSize==0)
          DISPLAYLEVEL(3, "Warning : cannot determine input content size \n");
    }

    /* read first block */
    readSize  = fread(srcBuffer, (size_t)1, blockSize, srcFile);
    if (ferror(srcFile)) EXM_THROW(30, "Error reading %s ", srcFileName);
    filesize += readSize;

    /* single-block file */
    if (readSize < blockSize) {
        /* Compress in single pass */
        size_t const cSize = LizardF_compressFrame(dstBuffer, dstBufferSize, srcBuffer, readSize, &prefs);
        if (LizardF_isError(cSize)) EXM_THROW(31, "Compression failed : %s", LizardF_getErrorName(cSize));
        compressedfilesize = cSize;
        DISPLAYUPDATE(2, "\rRead : %u MB   ==> %.2f%%   ",
                      (unsigned)(filesize>>20), (double)compressedfilesize/(filesize+!filesize)*100);   /* avoid division by zero */

        /* Write Block */
        {   size_t const sizeCheck = fwrite(dstBuffer, 1, cSize, dstFile);
            if (sizeCheck!=cSize) EXM_THROW(32, "Write error : cannot write compressed block");
    }   }

    else

    /* multiple-blocks file */
    {
        /* Write Archive Header */
        size_t headerSize = LizardF_compressBegin(ctx, dstBuffer, dstBufferSize, &prefs);
        if (LizardF_isError(headerSize)) EXM_THROW(33, "File header generation failed : %s", LizardF_getErrorName(headerSize));
        { size_t const sizeCheck = fwrite(dstBuffer, 1, headerSize, dstFile);
          if (sizeCheck!=headerSize) EXM_THROW(34, "Write error : cannot write header"); }
        compressedfilesize += headerSize;

        /* Main Loop */
        while (readSize>0) {
            size_t outSize;

            /* Compress Block */
            outSize = LizardF_compressUpdate(ctx, dstBuffer, dstBufferSize, srcBuffer, readSize, NULL);
            if (LizardF_isError(outSize)) EXM_THROW(35, "Compression failed : %s", LizardF_getErrorName(outSize));
            compressedfilesize += outSize;
            DISPLAYUPDATE(2, "\rRead : %u MB   ==> %.2f%%   ", (unsigned)(filesize>>20), (double)compressedfilesize/filesize*100);

            /* Write Block */
            { size_t const sizeCheck = fwrite(dstBuffer, 1, outSize, dstFile);
              if (sizeCheck!=outSize) EXM_THROW(36, "Write error : cannot write compressed block"); }

            /* Read next block */
            readSize  = fread(srcBuffer, (size_t)1, (size_t)blockSize, srcFile);
            filesize += readSize;
        }
        if (ferror(srcFile)) EXM_THROW(37, "Error reading %s ", srcFileName);

        /* End of Stream mark */
        headerSize = LizardF_compressEnd(ctx, dstBuffer, dstBufferSize, NULL);
        if (LizardF_isError(headerSize)) EXM_THROW(38, "End of file generation failed : %s", LizardF_getErrorName(headerSize));

        { size_t const sizeCheck = fwrite(dstBuffer, 1, headerSize, dstFile);
          if (sizeCheck!=headerSize) EXM_THROW(39, "Write error : cannot write end of stream"); }
        compressedfilesize += headerSize;
    }

    /* Release files */
    fclose (srcFile);
    fclose (dstFile);

    /* Copy owner, file permissions and modification time */
    {   stat_t statbuf;
        if (strcmp (srcFileName, stdinmark) && strcmp (dstFileName, stdoutmark) && UTIL_getFileStat(srcFileName, &statbuf))
            UTIL_setFileStat(dstFileName, &statbuf);
    }

    if (g_removeSrcFile) { if (remove(srcFileName)) EXM_THROW(40, "Remove error : %s: %s", srcFileName, strerror(errno)); } /* remove source file : --rm */

    /* Final Status */
    DISPLAYLEVEL(2, "\r%79s\r", "");
    DISPLAYLEVEL(2, "Compressed %llu bytes into %llu bytes ==> %.2f%%\n",
        filesize, compressedfilesize, (double)compressedfilesize/(filesize + !filesize)*100);   /* avoid division by zero */

    return 0;
}


int LizardIO_compressFilename(const char* srcFileName, const char* dstFileName, int compressionLevel)
{
    clock_t const start = clock();
    cRess_t const ress = LizardIO_createCResources();

    int const issueWithSrcFile = LizardIO_compressFilename_extRess(ress, srcFileName, dstFileName, compressionLevel);

    /* Free resources */
    LizardIO_freeCResources(ress);

    /* Final Status */
    {   clock_t const end = clock();
        double const seconds = (double)(end - start) / CLOCKS_PER_SEC;
        DISPLAYLEVEL(4, "Completed in %.2f sec \n", seconds);
    }

    return issueWithSrcFile;
}


#define FNSPACE 30
int LizardIO_compressMultipleFilenames(const char** inFileNamesTable, int ifntSize, const char* suffix, int compressionLevel)
{
    int i;
    int missed_files = 0;
    char* dstFileName = (char*)malloc(FNSPACE);
    size_t ofnSize = FNSPACE;
    const size_t suffixSize = strlen(suffix);
    cRess_t const ress = LizardIO_createCResources();

	if (dstFileName == NULL) return ifntSize;   /* not enough memory */

    /* loop on each file */
    for (i=0; i<ifntSize; i++) {
        size_t const ifnSize = strlen(inFileNamesTable[i]);
        if (ofnSize <= ifnSize+suffixSize+1) { free(dstFileName); ofnSize = ifnSize + 20; dstFileName = (char*)malloc(ofnSize); if (dstFileName == NULL) return ifntSize; }
        strcpy(dstFileName, inFileNamesTable[i]);
        strcat(dstFileName, suffix);

        missed_files += LizardIO_compressFilename_extRess(ress, inFileNamesTable[i], dstFileName, compressionLevel);
    }

    /* Close & Free */
    LizardIO_freeCResources(ress);
    free(dstFileName);

    return missed_files;
}


/* ********************************************************************* */
/* ********************** Lizard file-stream Decompression **************** */
/* ********************************************************************* */

static unsigned LizardIO_readLE32 (const void* s)
{
    const unsigned char* const srcPtr = (const unsigned char*)s;
    unsigned value32 = srcPtr[0];
    value32 += (srcPtr[1]<<8);
    value32 += (srcPtr[2]<<16);
    value32 += ((unsigned)srcPtr[3])<<24;
    return value32;
}

static unsigned LizardIO_fwriteSparse(FILE* file, const void* buffer, size_t bufferSize, unsigned storedSkips)
{
    const size_t* const bufferT = (const size_t*)buffer;   /* Buffer is supposed malloc'ed, hence aligned on size_t */
    const size_t* ptrT = bufferT;
    size_t  bufferSizeT = bufferSize / sizeT;
    const size_t* const bufferTEnd = bufferT + bufferSizeT;
    static const size_t segmentSizeT = (32 KB) / sizeT;

    if (!g_sparseFileSupport) {  /* normal write */
        size_t const sizeCheck = fwrite(buffer, 1, bufferSize, file);
        if (sizeCheck != bufferSize) EXM_THROW(70, "Write error : cannot write decoded block");
        return 0;
    }

    /* avoid int overflow */
    if (storedSkips > 1 GB) {
        int const seekResult = UTIL_fseek(file, 1 GB, SEEK_CUR);
        if (seekResult != 0) EXM_THROW(71, "1 GB skip error (sparse file support)");
        storedSkips -= 1 GB;
    }

    while (ptrT < bufferTEnd) {
        size_t seg0SizeT = segmentSizeT;
        size_t nb0T;

        /* count leading zeros */
        if (seg0SizeT > bufferSizeT) seg0SizeT = bufferSizeT;
        bufferSizeT -= seg0SizeT;
        for (nb0T=0; (nb0T < seg0SizeT) && (ptrT[nb0T] == 0); nb0T++) ;
        storedSkips += (unsigned)(nb0T * sizeT);

        if (nb0T != seg0SizeT) {   /* not all 0s */
            int const seekResult = UTIL_fseek(file, storedSkips, SEEK_CUR);
            if (seekResult) EXM_THROW(72, "Sparse skip error ; try --no-sparse");
            storedSkips = 0;
            seg0SizeT -= nb0T;
            ptrT += nb0T;
            {   size_t const sizeCheck = fwrite(ptrT, sizeT, seg0SizeT, file);
                if (sizeCheck != seg0SizeT) EXM_THROW(73, "Write error : cannot write decoded block");
        }   }
        ptrT += seg0SizeT;
    }

    if (bufferSize & maskT) {  /* size not multiple of sizeT : implies end of block */
        const char* const restStart = (const char*)bufferTEnd;
        const char* restPtr = restStart;
        size_t const restSize =  bufferSize & maskT;
        const char* const restEnd = restStart + restSize;
        for (; (restPtr < restEnd) && (*restPtr == 0); restPtr++) ;
        storedSkips += (unsigned) (restPtr - restStart);
        if (restPtr != restEnd) {
            int const seekResult = UTIL_fseek(file, storedSkips, SEEK_CUR);
            if (seekResult) EXM_THROW(74, "Sparse skip error ; try --no-sparse");
            storedSkips = 0;
            {   size_t const sizeCheck = fwrite(restPtr, 1, restEnd - restPtr, file);
                if (sizeCheck != (size_t)(restEnd - restPtr)) EXM_THROW(75, "Write error : cannot write decoded end of block");
        }   }
    }

    return storedSkips;
}

static void LizardIO_fwriteSparseEnd(FILE* file, unsigned storedSkips)
{
    if (storedSkips>0) {   /* implies g_sparseFileSupport>0 */
        int const seekResult = UTIL_fseek(file, storedSkips-1, SEEK_CUR);
        if (seekResult != 0) EXM_THROW(69, "Final skip error (sparse file)\n");
        {   const char lastZeroByte[1] = { 0 };
            size_t const sizeCheck = fwrite(lastZeroByte, 1, 1, file);
            if (sizeCheck != 1) EXM_THROW(69, "Write error : cannot write last zero\n");
    }   }
}



typedef struct {
    void*  srcBuffer;
    size_t srcBufferSize;
    void*  dstBuffer;
    size_t dstBufferSize;
    FILE*  dstFile;
    LizardF_decompressionContext_t dCtx;
} dRess_t;

static const size_t LizardIO_dBufferSize = 64 KB;
static unsigned g_magicRead = 0;
static dRess_t LizardIO_createDResources(void)
{
    dRess_t ress;

    /* init */
    LizardF_errorCode_t const errorCode = LizardF_createDecompressionContext(&ress.dCtx, LIZARDF_VERSION);
    if (LizardF_isError(errorCode)) EXM_THROW(60, "Can't create LizardF context : %s", LizardF_getErrorName(errorCode));

    /* Allocate Memory */
    ress.srcBufferSize = LizardIO_dBufferSize;
    ress.srcBuffer = malloc(ress.srcBufferSize);
    ress.dstBufferSize = LizardIO_dBufferSize;
    ress.dstBuffer = malloc(ress.dstBufferSize);
    if (!ress.srcBuffer || !ress.dstBuffer) EXM_THROW(61, "Allocation error : not enough memory");

    ress.dstFile = NULL;
    return ress;
}

static void LizardIO_freeDResources(dRess_t ress)
{
    LizardF_errorCode_t errorCode = LizardF_freeDecompressionContext(ress.dCtx);
    if (LizardF_isError(errorCode)) EXM_THROW(69, "Error : can't free LizardF context resource : %s", LizardF_getErrorName(errorCode));
    free(ress.srcBuffer);
    free(ress.dstBuffer);
}


static unsigned long long LizardIO_decompressLizardF(dRess_t ress, FILE* srcFile, FILE* dstFile)
{
    unsigned long long filesize = 0;
    LizardF_errorCode_t nextToLoad;
    unsigned storedSkips = 0;

    /* Init feed with magic number (already consumed from FILE* sFile) */
    {   size_t inSize = MAGICNUMBER_SIZE;
        size_t outSize= 0;
        LizardIO_writeLE32(ress.srcBuffer, LIZARDIO_MAGICNUMBER);
        nextToLoad = LizardF_decompress(ress.dCtx, ress.dstBuffer, &outSize, ress.srcBuffer, &inSize, NULL);
        if (LizardF_isError(nextToLoad)) EXM_THROW(62, "Header error : %s", LizardF_getErrorName(nextToLoad));
    }

    /* Main Loop */
    for (;nextToLoad;) {
        size_t readSize;
        size_t pos = 0;
        size_t decodedBytes = ress.dstBufferSize;

        /* Read input */
        if (nextToLoad > ress.srcBufferSize) nextToLoad = ress.srcBufferSize;
        readSize = fread(ress.srcBuffer, 1, nextToLoad, srcFile);
        if (!readSize) break;   /* reached end of file or stream */

        while ((pos < readSize) || (decodedBytes == ress.dstBufferSize)) {  /* still to read, or still to flush */
            /* Decode Input (at least partially) */
            size_t remaining = readSize - pos;
            decodedBytes = ress.dstBufferSize;
            nextToLoad = LizardF_decompress(ress.dCtx, ress.dstBuffer, &decodedBytes, (char*)(ress.srcBuffer)+pos, &remaining, NULL);
            if (LizardF_isError(nextToLoad)) EXM_THROW(66, "Decompression error : %s", LizardF_getErrorName(nextToLoad));
            pos += remaining;

            /* Write Block */
            if (decodedBytes) {
                if (!g_testMode)
                    storedSkips = LizardIO_fwriteSparse(dstFile, ress.dstBuffer, decodedBytes, storedSkips);
                filesize += decodedBytes;
                DISPLAYUPDATE(2, "\rDecompressed : %u MB  ", (unsigned)(filesize>>20));
            }

            if (!nextToLoad) break;
        }
    }
    /* can be out because readSize == 0, which could be an fread() error */
    if (ferror(srcFile)) EXM_THROW(67, "Read error");

    if (!g_testMode) LizardIO_fwriteSparseEnd(dstFile, storedSkips);
    if (nextToLoad!=0) EXM_THROW(68, "Unfinished stream");

    return filesize;
}


#define PTSIZE  (64 KB)
#define PTSIZET (PTSIZE / sizeof(size_t))
static unsigned long long LizardIO_passThrough(FILE* finput, FILE* foutput, unsigned char MNstore[MAGICNUMBER_SIZE])
{
	size_t buffer[PTSIZET];
    size_t readBytes = 1;
    unsigned long long total = MAGICNUMBER_SIZE;
    unsigned storedSkips = 0;

    size_t const sizeCheck = fwrite(MNstore, 1, MAGICNUMBER_SIZE, foutput);
    if (sizeCheck != MAGICNUMBER_SIZE) EXM_THROW(50, "Pass-through write error");

    while (readBytes) {
        readBytes = fread(buffer, 1, PTSIZE, finput);
        total += readBytes;
        storedSkips = LizardIO_fwriteSparse(foutput, buffer, readBytes, storedSkips);
    }
    if (ferror(finput)) EXM_THROW(51, "Read Error")

    LizardIO_fwriteSparseEnd(foutput, storedSkips);
    return total;
}


/** Safely handle cases when (unsigned)offset > LONG_MAX */
static int fseek_u32(FILE *fp, unsigned offset, int where)
{
    const unsigned stepMax = 1U << 30;
    int errorNb = 0;

    if (where != SEEK_CUR) return -1;  /* Only allows SEEK_CUR */
    while (offset > 0) {
        unsigned s = offset;
        if (s > stepMax) s = stepMax;
        errorNb = UTIL_fseek(fp, (long) s, SEEK_CUR);
        if (errorNb != 0) break;
        offset -= s;
    }
    return errorNb;
}

#define ENDOFSTREAM ((unsigned long long)-1)
static unsigned long long selectDecoder(dRess_t ress, FILE* finput, FILE* foutput)
{
    unsigned char MNstore[MAGICNUMBER_SIZE];
    unsigned magicNumber;
    static unsigned nbCalls = 0;

    /* init */
    nbCalls++;

    /* Check Archive Header */
    if (g_magicRead) {  /* magic number already read from finput (see legacy frame)*/
      magicNumber = g_magicRead;
      g_magicRead = 0;
    } else {
      size_t const nbReadBytes = fread(MNstore, 1, MAGICNUMBER_SIZE, finput);
      if (nbReadBytes==0) { nbCalls = 0; return ENDOFSTREAM; }   /* EOF */
      if (nbReadBytes != MAGICNUMBER_SIZE) EXM_THROW(40, "Unrecognized header : Magic Number unreadable");
      magicNumber = LizardIO_readLE32(MNstore);   /* Little Endian format */
    }
    if (LizardIO_isSkippableMagicNumber(magicNumber)) magicNumber = LIZARDIO_SKIPPABLE0;  /* fold skippable magic numbers */

    switch(magicNumber)
    {
    case LIZARDIO_MAGICNUMBER:
        return LizardIO_decompressLizardF(ress, finput, foutput);
    case LIZARDIO_SKIPPABLE0:
        DISPLAYLEVEL(4, "Skipping detected skippable area \n");
        { size_t const nbReadBytes = fread(MNstore, 1, 4, finput);
          if (nbReadBytes != 4) EXM_THROW(42, "Stream error : skippable size unreadable"); }
        { unsigned const size = LizardIO_readLE32(MNstore);     /* Little Endian format */
          int const errorNb = fseek_u32(finput, size, SEEK_CUR);
          if (errorNb != 0) EXM_THROW(43, "Stream error : cannot skip skippable area"); }
        return 0;
    EXTENDED_FORMAT;  /* macro extension for custom formats */
    default:
        if (nbCalls == 1) {  /* just started */
            if (!g_testMode && g_overwrite) {
                nbCalls = 0;
                return LizardIO_passThrough(finput, foutput, MNstore);
            }
            EXM_THROW(44,"Unrecognized header : file cannot be decoded");   /* Wrong magic number at the beginning of 1st stream */
        }
        DISPLAYLEVEL(2, "Stream followed by undecodable data\n");
        return ENDOFSTREAM;
    }
}


static int LizardIO_decompressSrcFile(dRess_t ress, const char* input_filename, const char* output_filename)
{
    FILE* const foutput = ress.dstFile;
    unsigned long long filesize = 0, decodedSize=0;
    FILE* finput;

    /* Init */
    finput = LizardIO_openSrcFile(input_filename);
    if (finput==NULL) return 1;

    /* Loop over multiple streams */
    do {
        decodedSize = selectDecoder(ress, finput, foutput);
        if (decodedSize != ENDOFSTREAM)
            filesize += decodedSize;
    } while (decodedSize != ENDOFSTREAM);

    /* Close */
    fclose(finput);

    if (g_removeSrcFile) { if (remove(input_filename)) EXM_THROW(45, "Remove error : %s: %s", input_filename, strerror(errno)); }  /* remove source file : --rm */

    /* Final Status */
    DISPLAYLEVEL(2, "\r%79s\r", "");
    DISPLAYLEVEL(2, "%-20.20s : decoded %llu bytes \n", input_filename, filesize);
    (void)output_filename;

    return 0;
}


static int LizardIO_decompressDstFile(dRess_t ress, const char* input_filename, const char* output_filename)
{
    FILE* foutput;

    /* Init */
    foutput = LizardIO_openDstFile(output_filename);
    if (foutput==NULL) return 1;   /* failure */

    ress.dstFile = foutput;
    LizardIO_decompressSrcFile(ress, input_filename, output_filename);

    fclose(foutput);

    /* Copy owner, file permissions and modification time */
    {   stat_t statbuf;
        if (strcmp (input_filename, stdinmark) && strcmp (output_filename, stdoutmark) && UTIL_getFileStat(input_filename, &statbuf))
            UTIL_setFileStat(output_filename, &statbuf);
    }

    return 0;
}


int LizardIO_decompressFilename(const char* input_filename, const char* output_filename)
{
    dRess_t const ress = LizardIO_createDResources();
    clock_t const start = clock();

    int const missingFiles = LizardIO_decompressDstFile(ress, input_filename, output_filename);

    {   clock_t const end = clock();
        double const seconds = (double)(end - start) / CLOCKS_PER_SEC;
        DISPLAYLEVEL(4, "Done in %.2f sec  \n", seconds);
    }

    LizardIO_freeDResources(ress);
    return missingFiles;
}


int LizardIO_decompressMultipleFilenames(const char** inFileNamesTable, int ifntSize, const char* suffix)
{
    int i;
    int skippedFiles = 0;
    int missingFiles = 0;
    char* outFileName = (char*)malloc(FNSPACE);
    size_t ofnSize = FNSPACE;
    size_t const suffixSize = strlen(suffix);
    dRess_t ress = LizardIO_createDResources();

    if (outFileName==NULL) return ifntSize;   /* not enough memory */
    ress.dstFile = LizardIO_openDstFile(stdoutmark);

    for (i=0; i<ifntSize; i++) {
        size_t const ifnSize = strlen(inFileNamesTable[i]);
        const char* const suffixPtr = inFileNamesTable[i] + ifnSize - suffixSize;
        if (!strcmp(suffix, stdoutmark)) {
            missingFiles += LizardIO_decompressSrcFile(ress, inFileNamesTable[i], stdoutmark);
            continue;
        }
        if (ofnSize <= ifnSize-suffixSize+1) { free(outFileName); ofnSize = ifnSize + 20; outFileName = (char*)malloc(ofnSize); if (outFileName==NULL)  return ifntSize; }
        if (ifnSize <= suffixSize  ||  strcmp(suffixPtr, suffix) != 0) {
            DISPLAYLEVEL(1, "File extension doesn't match expected LIZARD_EXTENSION (%4s); will not process file: %s\n", suffix, inFileNamesTable[i]);
            skippedFiles++;
            continue;
        }
        memcpy(outFileName, inFileNamesTable[i], ifnSize - suffixSize);
        outFileName[ifnSize-suffixSize] = '\0';
        missingFiles += LizardIO_decompressDstFile(ress, inFileNamesTable[i], outFileName);
    }

    LizardIO_freeDResources(ress);
    free(outFileName);
    return missingFiles + skippedFiles;
}
