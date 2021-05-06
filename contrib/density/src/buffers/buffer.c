/*
 * Density
 *
 * Copyright (c) 2015, Guillaume Voirin
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
 * 3/02/15 19:53
 */

#include "buffer.h"

DENSITY_WINDOWS_EXPORT uint_fast64_t density_compress_safe_size(const uint_fast64_t input_size) {
    const uint_fast64_t slack = DENSITY_MAX_3(DENSITY_CHAMELEON_MAXIMUM_COMPRESSED_UNIT_SIZE, DENSITY_CHEETAH_MAXIMUM_COMPRESSED_UNIT_SIZE, DENSITY_LION_MAXIMUM_COMPRESSED_UNIT_SIZE);

    // Chameleon longest output
    uint_fast64_t chameleon_longest_output_size = 0;
    chameleon_longest_output_size += sizeof(density_header);
    chameleon_longest_output_size += sizeof(density_chameleon_signature) * (1 + (input_size >> (5 + 3)));   // Signature space (1 bit <=> 4 bytes)
    chameleon_longest_output_size += sizeof(density_chameleon_signature);                                   // Eventual supplementary signature for end marker
    chameleon_longest_output_size += input_size;                                                            // Everything encoded as plain data

    // Cheetah longest output
    uint_fast64_t cheetah_longest_output_size = 0;
    cheetah_longest_output_size += sizeof(density_header);
    cheetah_longest_output_size += sizeof(density_cheetah_signature) * (1 + (input_size >> (4 + 3)));       // Signature space (2 bits <=> 4 bytes)
    cheetah_longest_output_size += sizeof(density_cheetah_signature);                                       // Eventual supplementary signature for end marker
    cheetah_longest_output_size += input_size;                                                              // Everything encoded as plain data

    // Lion longest output
    uint_fast64_t lion_longest_output_size = 0;
    lion_longest_output_size += sizeof(density_header);
    lion_longest_output_size += sizeof(density_lion_signature) * (1 + ((input_size * 7) >> (5 + 3)));       // Signature space (7 bits <=> 4 bytes), although this size is technically impossible
    lion_longest_output_size += sizeof(density_lion_signature);                                             // Eventual supplementary signature for end marker
    lion_longest_output_size += input_size;                                                                 // Everything encoded as plain data

    return DENSITY_MAX_3(chameleon_longest_output_size, cheetah_longest_output_size, lion_longest_output_size) + slack;
}

DENSITY_WINDOWS_EXPORT uint_fast64_t density_decompress_safe_size(const uint_fast64_t expected_decompressed_output_size) {
    const uint_fast64_t slack = DENSITY_MAX_3(DENSITY_CHAMELEON_DECOMPRESSED_UNIT_SIZE, DENSITY_CHEETAH_DECOMPRESSED_UNIT_SIZE, DENSITY_LION_MAXIMUM_DECOMPRESSED_UNIT_SIZE);

    return expected_decompressed_output_size + slack;
}

DENSITY_FORCE_INLINE DENSITY_STATE density_convert_algorithm_exit_status(const density_algorithm_exit_status status) {
    switch (status) {
        case DENSITY_ALGORITHMS_EXIT_STATUS_FINISHED:
            return DENSITY_STATE_OK;
        case DENSITY_ALGORITHMS_EXIT_STATUS_INPUT_STALL:
            return DENSITY_STATE_ERROR_INPUT_BUFFER_TOO_SMALL;
        case DENSITY_ALGORITHMS_EXIT_STATUS_OUTPUT_STALL:
            return DENSITY_STATE_ERROR_OUTPUT_BUFFER_TOO_SMALL;
        default:
            return DENSITY_STATE_ERROR_DURING_PROCESSING;
    }
}

DENSITY_FORCE_INLINE density_processing_result density_make_result(const DENSITY_STATE state, const uint_fast64_t read, const uint_fast64_t written, density_context *const context) {
    density_processing_result result;
    result.state = state;
    result.bytesRead = read;
    result.bytesWritten = written;
    result.context = context;
    return result;
}

DENSITY_FORCE_INLINE density_context* density_allocate_context(const DENSITY_ALGORITHM algorithm, const bool custom_dictionary, void *(*mem_alloc)(size_t)) {
    density_context* context = mem_alloc(sizeof(density_context));
    context->algorithm = algorithm;
    context->dictionary_size = density_get_dictionary_size(context->algorithm);
    context->dictionary_type = custom_dictionary;
    if(!context->dictionary_type) {
        context->dictionary = mem_alloc(context->dictionary_size);
        DENSITY_MEMSET(context->dictionary, 0, context->dictionary_size);
    }
    return context;
}

DENSITY_WINDOWS_EXPORT void density_free_context(density_context *const context, void (*mem_free)(void *)) {
    if(mem_free == NULL)
        mem_free = free;
    if(!context->dictionary_type)
        mem_free(context->dictionary);
    mem_free(context);
}

DENSITY_WINDOWS_EXPORT density_processing_result density_compress_prepare_context(const DENSITY_ALGORITHM algorithm, const bool custom_dictionary, void *(*mem_alloc)(size_t)) {
    if(mem_alloc == NULL)
        mem_alloc = malloc;

    return density_make_result(DENSITY_STATE_OK, 0, 0, density_allocate_context(algorithm, custom_dictionary, mem_alloc));
}

DENSITY_WINDOWS_EXPORT density_processing_result density_compress_with_context(const uint8_t * input_buffer, const uint_fast64_t input_size, uint8_t * output_buffer, const uint_fast64_t output_size, density_context *const context) {
    if (output_size < sizeof(density_header))
        return density_make_result(DENSITY_STATE_ERROR_OUTPUT_BUFFER_TOO_SMALL, 0, 0, context);
    if(context == NULL)
        return density_make_result(DENSITY_STATE_ERROR_INVALID_CONTEXT, 0, 0, context);

    // Variables setup
    const uint8_t *in = input_buffer;
    uint8_t *out = output_buffer;
    density_algorithm_state state;
    density_algorithm_exit_status status = DENSITY_ALGORITHMS_EXIT_STATUS_ERROR_DURING_PROCESSING;

    // Header
    density_header_write(&out, context->algorithm);

    // Compression
    density_algorithms_prepare_state(&state, context->dictionary);
    switch (context->algorithm) {
        case DENSITY_ALGORITHM_CHAMELEON:
            status = density_chameleon_encode(&state, &in, input_size, &out, output_size);
            break;
        case DENSITY_ALGORITHM_CHEETAH:
            status = density_cheetah_encode(&state, &in, input_size, &out, output_size);
            break;
        case DENSITY_ALGORITHM_LION:
            status = density_lion_encode(&state, &in, input_size, &out, output_size);
            break;
    }

    // Result
    return density_make_result(density_convert_algorithm_exit_status(status), in - input_buffer, out - output_buffer, context);
}

DENSITY_WINDOWS_EXPORT density_processing_result density_decompress_prepare_context(const uint8_t *input_buffer, const uint_fast64_t input_size, const bool custom_dictionary, void *(*mem_alloc)(size_t)) {
    if (input_size < sizeof(density_header))
        return density_make_result(DENSITY_STATE_ERROR_INPUT_BUFFER_TOO_SMALL, 0, 0, NULL);

    // Variables setup
    const uint8_t* in = input_buffer;
    if(mem_alloc == NULL)
        mem_alloc = malloc;

    // Read header
    density_header main_header;
    density_header_read(&in, &main_header);

    // Setup context
    density_context *const context = density_allocate_context(main_header.algorithm, custom_dictionary, mem_alloc);
    return density_make_result(DENSITY_STATE_OK, in - input_buffer, 0, context);
}

DENSITY_WINDOWS_EXPORT density_processing_result density_decompress_with_context(const uint8_t * input_buffer, const uint_fast64_t input_size, uint8_t * output_buffer, const uint_fast64_t output_size, density_context *const context) {
    if(context == NULL)
        return density_make_result(DENSITY_STATE_ERROR_INVALID_CONTEXT, 0, 0, context);

    // Variables setup
    const uint8_t *in = input_buffer;
    uint8_t *out = output_buffer;
    density_algorithm_state state;
    density_algorithm_exit_status status = DENSITY_ALGORITHMS_EXIT_STATUS_ERROR_DURING_PROCESSING;

    // Decompression
    density_algorithms_prepare_state(&state, context->dictionary);
    switch (context->algorithm) {
        case DENSITY_ALGORITHM_CHAMELEON:
            status = density_chameleon_decode(&state, &in, input_size, &out, output_size);
            break;
        case DENSITY_ALGORITHM_CHEETAH:
            status = density_cheetah_decode(&state, &in, input_size, &out, output_size);
            break;
        case DENSITY_ALGORITHM_LION:
            status = density_lion_decode(&state, &in, input_size, &out, output_size);
            break;
    }

    // Result
    return density_make_result(density_convert_algorithm_exit_status(status), in - input_buffer, out - output_buffer, context);
}

DENSITY_WINDOWS_EXPORT density_processing_result density_compress(const uint8_t *input_buffer, const uint_fast64_t input_size, uint8_t *output_buffer, const uint_fast64_t output_size, const DENSITY_ALGORITHM algorithm) {
    density_processing_result result = density_compress_prepare_context(algorithm, false, malloc);
    if(result.state) {
        density_free_context(result.context, free);
        return result;
    }

    result = density_compress_with_context(input_buffer, input_size, output_buffer, output_size, result.context);
    density_free_context(result.context, free);
    return result;
}

DENSITY_WINDOWS_EXPORT density_processing_result density_decompress(const uint8_t *input_buffer, const uint_fast64_t input_size, uint8_t *output_buffer, const uint_fast64_t output_size) {
    density_processing_result result = density_decompress_prepare_context(input_buffer, input_size, false, malloc);
    if(result.state) {
        density_free_context(result.context, free);
        return result;
    }

    result = density_decompress_with_context(input_buffer + result.bytesRead, input_size - result.bytesRead, output_buffer, output_size, result.context);
    density_free_context(result.context, free);
    return result;
}
