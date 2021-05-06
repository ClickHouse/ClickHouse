/*
   Lizard - Fast LZ compression algorithm
   Header File
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
#ifndef LIZARD_H_2983
#define LIZARD_H_2983

#if defined (__cplusplus)
extern "C" {
#endif

/*
 * lizard_compress.h provides block compression functions. It gives full buffer control to user.
 * Block compression functions are not-enough to send information,
 * since it's still necessary to provide metadata (such as compressed size),
 * and each application can do it in whichever way it wants.
 * For interoperability, there is Lizard frame specification (lizard_Frame_format.md).
 * A library is provided to take care of it, see lizard_frame.h.
*/


/*^***************************************************************
*  Export parameters
*****************************************************************/
/*
*  LIZARD_DLL_EXPORT :
*  Enable exporting of functions when building a Windows DLL
*/
#if defined(LIZARD_DLL_EXPORT) && (LIZARD_DLL_EXPORT==1)
#  define LIZARDLIB_API __declspec(dllexport)
#elif defined(LIZARD_DLL_IMPORT) && (LIZARD_DLL_IMPORT==1)
#  define LIZARDLIB_API __declspec(dllimport) /* It isn't required but allows to generate better code, saving a function pointer load from the IAT and an indirect jump.*/
#else
#  define LIZARDLIB_API
#endif


/*-************************************
*  Version
**************************************/
#define LIZARD_VERSION_MAJOR    1    /* for breaking interface changes  */
#define LIZARD_VERSION_MINOR    0    /* for new (non-breaking) interface capabilities */
#define LIZARD_VERSION_RELEASE  0    /* for tweaks, bug-fixes, or development */

#define LIZARD_VERSION_NUMBER (LIZARD_VERSION_MAJOR *100*100 + LIZARD_VERSION_MINOR *100 + LIZARD_VERSION_RELEASE)
int Lizard_versionNumber (void);

#define LIZARD_LIB_VERSION LIZARD_VERSION_MAJOR.LIZARD_VERSION_MINOR.LIZARD_VERSION_RELEASE
#define LIZARD_QUOTE(str) #str
#define LIZARD_EXPAND_AND_QUOTE(str) LIZARD_QUOTE(str)
#define LIZARD_VERSION_STRING LIZARD_EXPAND_AND_QUOTE(LIZARD_LIB_VERSION)
const char* Lizard_versionString (void);

typedef struct Lizard_stream_s Lizard_stream_t;

#define LIZARD_MIN_CLEVEL      10  /* minimum compression level */
#ifndef LIZARD_NO_HUFFMAN
    #define LIZARD_MAX_CLEVEL      49  /* maximum compression level */
#else
    #define LIZARD_MAX_CLEVEL      29  /* maximum compression level */
#endif
#define LIZARD_DEFAULT_CLEVEL  17


/*-************************************
*  Simple Functions
**************************************/

LIZARDLIB_API int Lizard_compress (const char* src, char* dst, int srcSize, int maxDstSize, int compressionLevel); 

/*
Lizard_compress() :
    Compresses 'sourceSize' bytes from buffer 'source'
    into already allocated 'dest' buffer of size 'maxDestSize'.
    Compression is guaranteed to succeed if 'maxDestSize' >= Lizard_compressBound(sourceSize).
    It also runs faster, so it's a recommended setting.
    If the function cannot compress 'source' into a more limited 'dest' budget,
    compression stops *immediately*, and the function result is zero.
    As a consequence, 'dest' content is not valid.
    This function never writes outside 'dest' buffer, nor read outside 'source' buffer.
        sourceSize  : Max supported value is LIZARD_MAX_INPUT_VALUE
        maxDestSize : full or partial size of buffer 'dest' (which must be already allocated)
        return : the number of bytes written into buffer 'dest' (necessarily <= maxOutputSize)
              or 0 if compression fails
*/


/*-************************************
*  Advanced Functions
**************************************/
#define LIZARD_MAX_INPUT_SIZE  0x7E000000   /* 2 113 929 216 bytes */
#define LIZARD_BLOCK_SIZE      (1<<17)
#define LIZARD_BLOCK64K_SIZE   (1<<16)
#define LIZARD_COMPRESSBOUND(isize)  ((unsigned)(isize) > (unsigned)LIZARD_MAX_INPUT_SIZE ? 0 : (isize) + 1 + 1 + ((isize/LIZARD_BLOCK_SIZE)+1)*4)


/*!
Lizard_compressBound() :
    Provides the maximum size that Lizard compression may output in a "worst case" scenario (input data not compressible)
    This function is primarily useful for memory allocation purposes (destination buffer size).
    Macro LIZARD_COMPRESSBOUND() is also provided for compilation-time evaluation (stack memory allocation for example).
    Note that Lizard_compress() compress faster when dest buffer size is >= Lizard_compressBound(srcSize)
        inputSize  : max supported value is LIZARD_MAX_INPUT_SIZE
        return : maximum output size in a "worst case" scenario
              or 0, if input size is too large ( > LIZARD_MAX_INPUT_SIZE)
*/
LIZARDLIB_API int Lizard_compressBound(int inputSize);


/*!
Lizard_compress_extState() :
    Same compression function, just using an externally allocated memory space to store compression state.
    Use Lizard_sizeofState() to know how much memory must be allocated,
    and allocate it on 8-bytes boundaries (using malloc() typically).
    Then, provide it as 'void* state' to compression function.
*/
LIZARDLIB_API int Lizard_sizeofState(int compressionLevel); 

LIZARDLIB_API int Lizard_compress_extState(void* state, const char* src, char* dst, int srcSize, int maxDstSize, int compressionLevel);



/*-*********************************************
*  Streaming Compression Functions
***********************************************/

/*! Lizard_createStream() will allocate and initialize an `Lizard_stream_t` structure.
 *  Lizard_freeStream() releases its memory.
 *  In the context of a DLL (liblizard), please use these methods rather than the static struct.
 *  They are more future proof, in case of a change of `Lizard_stream_t` size.
 */
LIZARDLIB_API Lizard_stream_t* Lizard_createStream(int compressionLevel);
LIZARDLIB_API int           Lizard_freeStream (Lizard_stream_t* streamPtr);


/*! Lizard_resetStream() :
 *  Use this function to reset/reuse an allocated `Lizard_stream_t` structure
 */
LIZARDLIB_API Lizard_stream_t* Lizard_resetStream (Lizard_stream_t* streamPtr, int compressionLevel); 


/*! Lizard_loadDict() :
 *  Use this function to load a static dictionary into Lizard_stream.
 *  Any previous data will be forgotten, only 'dictionary' will remain in memory.
 *  Loading a size of 0 is allowed.
 *  Return : dictionary size, in bytes (necessarily <= LIZARD_DICT_SIZE)
 */
LIZARDLIB_API int Lizard_loadDict (Lizard_stream_t* streamPtr, const char* dictionary, int dictSize);


/*! Lizard_compress_continue() :
 *  Compress buffer content 'src', using data from previously compressed blocks as dictionary to improve compression ratio.
 *  Important : Previous data blocks are assumed to still be present and unmodified !
 *  'dst' buffer must be already allocated.
 *  If maxDstSize >= Lizard_compressBound(srcSize), compression is guaranteed to succeed, and runs faster.
 *  If not, and if compressed data cannot fit into 'dst' buffer size, compression stops, and function returns a zero.
 */
LIZARDLIB_API int Lizard_compress_continue (Lizard_stream_t* streamPtr, const char* src, char* dst, int srcSize, int maxDstSize);


/*! Lizard_saveDict() :
 *  If previously compressed data block is not guaranteed to remain available at its memory location,
 *  save it into a safer place (char* safeBuffer).
 *  Note : you don't need to call Lizard_loadDict() afterwards,
 *         dictionary is immediately usable, you can therefore call Lizard_compress_continue().
 *  Return : saved dictionary size in bytes (necessarily <= dictSize), or 0 if error.
 */
LIZARDLIB_API int Lizard_saveDict (Lizard_stream_t* streamPtr, char* safeBuffer, int dictSize);





#if defined (__cplusplus)
}
#endif

#endif /* LIZARD_H_2983827168210 */
