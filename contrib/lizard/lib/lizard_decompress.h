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
#ifndef LIZARD_DECOMPRESS_H_2983
#define LIZARD_DECOMPRESS_H_2983

#if defined (__cplusplus)
extern "C" {
#endif


/*^***************************************************************
*  Export parameters
*****************************************************************/
/*
*  LIZARD_DLL_EXPORT :
*  Enable exporting of functions when building a Windows DLL
*/
#if defined(LIZARD_DLL_EXPORT) && (LIZARD_DLL_EXPORT==1)
#  define LIZARDDLIB_API __declspec(dllexport)
#elif defined(LIZARD_DLL_IMPORT) && (LIZARD_DLL_IMPORT==1)
#  define LIZARDDLIB_API __declspec(dllimport) /* It isn't required but allows to generate better code, saving a function pointer load from the IAT and an indirect jump.*/
#else
#  define LIZARDDLIB_API
#endif


/*-************************************
*  Simple Functions
**************************************/

/*
Lizard_decompress_safe() :
    compressedSize : is the precise full size of the compressed block.
    maxDecompressedSize : is the size of destination buffer, which must be already allocated.
    return : the number of bytes decompressed into destination buffer (necessarily <= maxDecompressedSize)
             If destination buffer is not large enough, decoding will stop and output an error code (<0).
             If the source stream is detected malformed, the function will stop decoding and return a negative result.
             This function is protected against buffer overflow exploits, including malicious data packets.
             It never writes outside output buffer, nor reads outside input buffer.
*/
LIZARDDLIB_API int Lizard_decompress_safe (const char* source, char* dest, int compressedSize, int maxDecompressedSize);



/*!
Lizard_decompress_safe_partial() :
    This function decompress a compressed block of size 'compressedSize' at position 'source'
    into destination buffer 'dest' of size 'maxDecompressedSize'.
    The function tries to stop decompressing operation as soon as 'targetOutputSize' has been reached,
    reducing decompression time.
    return : the number of bytes decoded in the destination buffer (necessarily <= maxDecompressedSize)
       Note : this number can be < 'targetOutputSize' should the compressed block to decode be smaller.
             Always control how many bytes were decoded.
             If the source stream is detected malformed, the function will stop decoding and return a negative result.
             This function never writes outside of output buffer, and never reads outside of input buffer. It is therefore protected against malicious data packets
*/
LIZARDDLIB_API int Lizard_decompress_safe_partial (const char* source, char* dest, int compressedSize, int targetOutputSize, int maxDecompressedSize);



/*-**********************************************
*  Streaming Decompression Functions
************************************************/
typedef struct Lizard_streamDecode_s Lizard_streamDecode_t;

/*
 * Lizard_streamDecode_t
 * information structure to track an Lizard stream.
 * init this structure content using Lizard_setStreamDecode or memset() before first use !
 *
 * In the context of a DLL (liblizard) please prefer usage of construction methods below.
 * They are more future proof, in case of a change of Lizard_streamDecode_t size in the future.
 * Lizard_createStreamDecode will allocate and initialize an Lizard_streamDecode_t structure
 * Lizard_freeStreamDecode releases its memory.
 */
LIZARDDLIB_API Lizard_streamDecode_t* Lizard_createStreamDecode(void);
LIZARDDLIB_API int                 Lizard_freeStreamDecode (Lizard_streamDecode_t* Lizard_stream);

/*! Lizard_setStreamDecode() :
 *  Use this function to instruct where to find the dictionary.
 *  Setting a size of 0 is allowed (same effect as reset).
 *  @return : 1 if OK, 0 if error
 */
LIZARDDLIB_API int Lizard_setStreamDecode (Lizard_streamDecode_t* Lizard_streamDecode, const char* dictionary, int dictSize);

/*
*_continue() :
    These decoding functions allow decompression of multiple blocks in "streaming" mode.
    Previously decoded blocks *must* remain available at the memory position where they were decoded (up to LIZARD_DICT_SIZE)
    In the case of a ring buffers, decoding buffer must be either :
    - Exactly same size as encoding buffer, with same update rule (block boundaries at same positions)
      In which case, the decoding & encoding ring buffer can have any size, including small ones ( < LIZARD_DICT_SIZE).
    - Larger than encoding buffer, by a minimum of maxBlockSize more bytes.
      maxBlockSize is implementation dependent. It's the maximum size you intend to compress into a single block.
      In which case, encoding and decoding buffers do not need to be synchronized,
      and encoding ring buffer can have any size, including small ones ( < LIZARD_DICT_SIZE).
    - _At least_ LIZARD_DICT_SIZE + 8 bytes + maxBlockSize.
      In which case, encoding and decoding buffers do not need to be synchronized,
      and encoding ring buffer can have any size, including larger than decoding buffer.
    Whenever these conditions are not possible, save the last LIZARD_DICT_SIZE of decoded data into a safe buffer,
    and indicate where it is saved using Lizard_setStreamDecode()
*/
LIZARDDLIB_API int Lizard_decompress_safe_continue (Lizard_streamDecode_t* Lizard_streamDecode, const char* source, char* dest, int compressedSize, int maxDecompressedSize);


/*
Advanced decoding functions :
*_usingDict() :
    These decoding functions work the same as
    a combination of Lizard_setStreamDecode() followed by Lizard_decompress_x_continue()
    They are stand-alone. They don't need nor update an Lizard_streamDecode_t structure.
*/
LIZARDDLIB_API int Lizard_decompress_safe_usingDict (const char* source, char* dest, int compressedSize, int maxDecompressedSize, const char* dictStart, int dictSize);


#if defined (__cplusplus)
}
#endif

#endif /* LIZARD_DECOMPRESS_H_2983827168210 */
