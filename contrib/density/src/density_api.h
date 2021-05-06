/*
 * Density
 *
 * Copyright (c) 2013, Guillaume Voirin
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     1. Redistributions of source code must retain the above copyright notice, this
 *        list of conditions and the following disclaimer.
 *
 *     2. Redistributions in binary form must reproduce the above copyright notice,
 *        this list of conditions and the following disclaimer in the documentation
 *        and/or other materials provided with the distribution.
 *
 *     3. Neither the name of the copyright holder nor the names of its
 *        contributors may be used to endorse or promote products derived from
 *        this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * 18/10/13 22:41
 */

#ifndef DENSITY_API_H
#define DENSITY_API_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>

#if defined(_WIN64) || defined(_WIN32)
#define DENSITY_WINDOWS_EXPORT __declspec(dllexport)
#else
#define DENSITY_WINDOWS_EXPORT
#endif


/***********************************************************************************************************************
 *                                                                                                                     *
 * API data structures                                                                                                 *
 *                                                                                                                     *
 ***********************************************************************************************************************/

typedef uint8_t density_byte;
typedef bool density_bool;

typedef enum {
    DENSITY_ALGORITHM_CHAMELEON = 1,
    DENSITY_ALGORITHM_CHEETAH = 2,
    DENSITY_ALGORITHM_LION = 3,
} DENSITY_ALGORITHM;

typedef enum {
    DENSITY_STATE_OK = 0,                                        // Everything went alright
    DENSITY_STATE_ERROR_INPUT_BUFFER_TOO_SMALL,                  // Input buffer size is too small
    DENSITY_STATE_ERROR_OUTPUT_BUFFER_TOO_SMALL,                 // Output buffer size is too small
    DENSITY_STATE_ERROR_DURING_PROCESSING,                       // Error during processing
    DENSITY_STATE_ERROR_INVALID_CONTEXT,                         // Invalid context
    DENSITY_STATE_ERROR_INVALID_ALGORITHM,                       // Invalid algorithm
} DENSITY_STATE;

typedef struct {
    DENSITY_ALGORITHM algorithm;
    bool dictionary_type;
    size_t dictionary_size;
    void* dictionary;
} density_context;

typedef struct {
    DENSITY_STATE state;
    uint_fast64_t bytesRead;
    uint_fast64_t bytesWritten;
    density_context* context;
} density_processing_result;



/***********************************************************************************************************************
 *                                                                                                                     *
 * Density version information                                                                                         *
 *                                                                                                                     *
 ***********************************************************************************************************************/

/*
 * Returns the major density version
 */
DENSITY_WINDOWS_EXPORT uint8_t density_version_major(void);

/*
 * Returns the minor density version
 */
DENSITY_WINDOWS_EXPORT uint8_t density_version_minor(void);

/*
 * Returns the density revision
 */
DENSITY_WINDOWS_EXPORT uint8_t density_version_revision(void);



/***********************************************************************************************************************
 *                                                                                                                     *
 * Density API functions                                                                                               *
 *                                                                                                                     *
 ***********************************************************************************************************************/

/*
 * Return the required size of an algorithm's dictionary
 *
 * @param algorithm the algorithm to use this dictionary for
 */
DENSITY_WINDOWS_EXPORT size_t density_get_dictionary_size(DENSITY_ALGORITHM algorithm);

/*
 * Return an output buffer byte size which guarantees enough space for encoding input_size bytes
 *
 * @param input_size the size of the input data which is about to be compressed
 */
DENSITY_WINDOWS_EXPORT uint_fast64_t density_compress_safe_size(const uint_fast64_t input_size);

/*
 * Return an output buffer byte size which, if expected_decompressed_output_size is correct, will enable density to decompress properly
 *
 * @param expected_decompressed_output_size the expected (original) size of the decompressed data
 */
DENSITY_WINDOWS_EXPORT uint_fast64_t density_decompress_safe_size(const uint_fast64_t expected_decompressed_output_size);

/*
 * Releases a context from memory.
 *
 * @param context the context to free
 * @param mem_free the memory freeing function. If set to NULL, free() is used
 */
DENSITY_WINDOWS_EXPORT void density_free_context(density_context *const context, void (*mem_free)(void *));

/*
 * Allocate a context in memory using the provided function and optional dictionary
 *
 * @param algorithm the required algorithm
 * @param custom_dictionary use an eventual custom dictionary ? If set to true the context's dictionary will have to be allocated
 * @param mem_alloc the memory allocation function. If set to NULL, malloc() is used
 */
DENSITY_WINDOWS_EXPORT density_processing_result density_compress_prepare_context(const DENSITY_ALGORITHM algorithm, const bool custom_dictionary, void *(*mem_alloc)(size_t));

/*
 * Compress an input_buffer of input_size bytes and store the result in output_buffer, using the provided context.
 * Important note   * this function could be unsafe memory-wise if not used properly.
 *
 * @param input_buffer a buffer of bytes
 * @param input_size the size in bytes of input_buffer
 * @param output_buffer a buffer of bytes
 * @param output_size the size of output_buffer, must be at least DENSITY_MINIMUM_OUTPUT_BUFFER_SIZE
 * @param context a pointer to a context structure
 */
DENSITY_WINDOWS_EXPORT density_processing_result density_compress_with_context(const uint8_t *input_buffer, const uint_fast64_t input_size, uint8_t *output_buffer, const uint_fast64_t output_size, density_context *const context);

/*
 * Compress an input_buffer of input_size bytes and store the result in output_buffer.
 *
 * @param input_buffer a buffer of bytes
 * @param input_size the size in bytes of input_buffer
 * @param output_buffer a buffer of bytes
 * @param output_size the size of output_buffer, must be at least DENSITY_MINIMUM_OUTPUT_BUFFER_SIZE
 * @param algorithm the algorithm to use
 */
DENSITY_WINDOWS_EXPORT density_processing_result density_compress(const uint8_t *input_buffer, const uint_fast64_t input_size, uint8_t *output_buffer, const uint_fast64_t output_size, const DENSITY_ALGORITHM algorithm);

/*
 * Reads the compressed data's header and creates an adequate decompression context.
 *
 * @param input_buffer a buffer of bytes
 * @param input_size the size in bytes of input_buffer
 * @param custom_dictionary use a custom dictionary ? If set to true the context's dictionary will have to be allocated
 * @param mem_alloc the memory allocation function. If set to NULL, malloc() is used
 */
DENSITY_WINDOWS_EXPORT density_processing_result density_decompress_prepare_context(const uint8_t *input_buffer, const uint_fast64_t input_size, const bool custom_dictionary, void *(*mem_alloc)(size_t));

/*
 * Decompress an input_buffer of input_size bytes and store the result in output_buffer, using the provided dictionary.
 * Important notes  * You must know in advance the algorithm used for compression to provide the proper dictionary.
 *                  * This function could be unsafe memory-wise if not used properly.
 *
 * @param input_buffer a buffer of bytes
 * @param input_size the size in bytes of input_buffer
 * @param output_buffer a buffer of bytes
 * @param output_size the size of output_buffer, must be at least DENSITY_MINIMUM_OUTPUT_BUFFER_SIZE
 * @param dictionaries a pointer to a dictionary
 */
DENSITY_WINDOWS_EXPORT density_processing_result density_decompress_with_context(const uint8_t * input_buffer, const uint_fast64_t input_size, uint8_t *output_buffer, const uint_fast64_t output_size, density_context *const context);

/*
 * Decompress an input_buffer of input_size bytes and store the result in output_buffer.
 *
 * @param input_buffer a buffer of bytes
 * @param input_size the size in bytes of input_buffer
 * @param output_buffer a buffer of bytes
 * @param output_size the size of output_buffer, must be at least DENSITY_MINIMUM_OUTPUT_BUFFER_SIZE
 */
DENSITY_WINDOWS_EXPORT density_processing_result density_decompress(const uint8_t *input_buffer, const uint_fast64_t input_size, uint8_t *output_buffer, const uint_fast64_t output_size);

#ifdef __cplusplus
}
#endif

#endif
