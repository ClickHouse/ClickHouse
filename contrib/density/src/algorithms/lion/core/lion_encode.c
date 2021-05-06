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
 * 24/06/15 18:57
 *
 * --------------
 * Lion algorithm
 * --------------
 *
 * Author(s)
 * Guillaume Voirin (https://github.com/gpnuma)
 *
 * Description
 * Multiform compression algorithm
 */

#include "lion_encode.h"

DENSITY_FORCE_INLINE void density_lion_encode_prepare_signature(uint8_t **DENSITY_RESTRICT out, uint_fast64_t **DENSITY_RESTRICT signature_pointer, uint_fast64_t *const DENSITY_RESTRICT signature) {
    *signature = 0;
    *signature_pointer = (density_lion_signature *) *out;
    *out += sizeof(density_lion_signature);
}

DENSITY_FORCE_INLINE void density_lion_encode_push_to_proximity_signature(uint_fast64_t *const DENSITY_RESTRICT signature, uint_fast8_t *const DENSITY_RESTRICT shift, const uint64_t content, const uint_fast8_t bits) {
    *signature |= (content << *shift);
    *shift += bits;
}

DENSITY_FORCE_INLINE void density_lion_encode_push_to_signature(uint8_t **DENSITY_RESTRICT out, density_lion_signature **DENSITY_RESTRICT signature_pointer, density_lion_signature *const DENSITY_RESTRICT signature, uint_fast8_t *const DENSITY_RESTRICT shift, const uint64_t content, const uint_fast8_t bits) {
    if (DENSITY_LIKELY(*shift)) {
        density_lion_encode_push_to_proximity_signature(signature, shift, content, bits);

        if (DENSITY_UNLIKELY(*shift >= density_bitsizeof(density_lion_signature))) {
#ifdef DENSITY_LITTLE_ENDIAN
            DENSITY_MEMCPY(*signature_pointer, signature, sizeof(density_lion_signature));
#elif defined(DENSITY_BIG_ENDIAN)
            const density_lion_signature endian_signature = DENSITY_LITTLE_ENDIAN_64(*signature);
            DENSITY_MEMCPY(*signature_pointer, &endian_signature, sizeof(density_lion_signature));
#else
#error
#endif

            const uint_fast8_t remainder = (uint_fast8_t)(*shift & 0x3f);
            *shift = 0;
            if (remainder) {
                density_lion_encode_prepare_signature(out, signature_pointer, signature);
                density_lion_encode_push_to_proximity_signature(signature, shift, content >> (bits - remainder), remainder);
            }
        }
    } else {
        density_lion_encode_prepare_signature(out, signature_pointer, signature);
        density_lion_encode_push_to_proximity_signature(signature, shift, content, bits);
    }
}

DENSITY_FORCE_INLINE void density_lion_encode_push_zero_to_signature(uint8_t **DENSITY_RESTRICT out, density_lion_signature **DENSITY_RESTRICT signature_pointer, density_lion_signature *const DENSITY_RESTRICT signature, uint_fast8_t *const DENSITY_RESTRICT shift, const uint_fast8_t bits) {
    if (DENSITY_LIKELY(*shift)) {
        *shift += bits;

        if (DENSITY_UNLIKELY(*shift >= density_bitsizeof(density_lion_signature))) {
#ifdef DENSITY_LITTLE_ENDIAN
            DENSITY_MEMCPY(*signature_pointer, signature, sizeof(density_lion_signature));
#elif defined(DENSITY_BIG_ENDIAN)
            const density_lion_signature endian_signature = DENSITY_LITTLE_ENDIAN_64(*signature);
            DENSITY_MEMCPY(*signature_pointer, &endian_signature, sizeof(density_lion_signature));
#else
#error
#endif

            const uint_fast8_t remainder = (uint_fast8_t)(*shift & 0x3f);
            if (remainder) {
                density_lion_encode_prepare_signature(out, signature_pointer, signature);
                *shift = remainder;
            } else
                *shift = 0;
        }
    } else {
        density_lion_encode_prepare_signature(out, signature_pointer, signature);
        *shift = bits;
    }
}

DENSITY_FORCE_INLINE void density_lion_encode_push_code_to_signature(uint8_t **DENSITY_RESTRICT out, density_lion_signature **DENSITY_RESTRICT signature_pointer, density_lion_signature *const DENSITY_RESTRICT signature, uint_fast8_t *const DENSITY_RESTRICT shift, const density_lion_entropy_code code) {
    density_lion_encode_push_to_signature(out, signature_pointer, signature, shift, code.value, code.bitLength);
}

DENSITY_FORCE_INLINE void density_lion_encode_kernel_4(uint8_t **DENSITY_RESTRICT out, uint_fast16_t *DENSITY_RESTRICT last_hash, density_lion_signature **DENSITY_RESTRICT signature_pointer, density_lion_signature *const DENSITY_RESTRICT signature, uint_fast8_t *const DENSITY_RESTRICT shift, density_lion_dictionary *const DENSITY_RESTRICT dictionary, const uint16_t hash, density_lion_form_data *const data, const uint32_t unit) {
    density_lion_dictionary_chunk_prediction_entry *const predictions = &dictionary->predictions[*last_hash];
	DENSITY_PREFETCH(&dictionary->predictions[hash]);

    if (*(uint32_t *) predictions ^ unit) {
        if (*((uint32_t *) predictions + 1) ^ unit) {
            if (*((uint32_t *) predictions + 2) ^ unit) {
                density_lion_dictionary_chunk_entry *const in_dictionary = &dictionary->chunks[hash];
                if (*(uint32_t *) in_dictionary ^ unit) {
                    if (*((uint32_t *) in_dictionary + 1) ^ unit) {
                        if (*((uint32_t *) in_dictionary + 2) ^ unit) {
                            if (*((uint32_t *) in_dictionary + 3) ^ unit) {
                                density_lion_encode_push_code_to_signature(out, signature_pointer, signature, shift, density_lion_form_model_get_encoding(data, DENSITY_LION_FORM_PLAIN));
                                DENSITY_MEMCPY(*out, &unit, sizeof(uint32_t));
                                *out += sizeof(uint32_t);
                            } else {
                                density_lion_encode_push_code_to_signature(out, signature_pointer, signature, shift, density_lion_form_model_get_encoding(data, DENSITY_LION_FORM_DICTIONARY_D));
#ifdef DENSITY_LITTLE_ENDIAN
                                DENSITY_MEMCPY(*out, &hash, sizeof(uint16_t));
#elif defined(DENSITY_BIG_ENDIAN)
                                const uint16_t endian_hash = DENSITY_LITTLE_ENDIAN_16(hash);
                                DENSITY_MEMCPY(*out, &endian_hash, sizeof(uint16_t));
#else
#error
#endif
                                *out += sizeof(uint16_t);
                            }
                        } else {
                            density_lion_encode_push_code_to_signature(out, signature_pointer, signature, shift, density_lion_form_model_get_encoding(data, DENSITY_LION_FORM_DICTIONARY_C));
#ifdef DENSITY_LITTLE_ENDIAN
                            DENSITY_MEMCPY(*out, &hash, sizeof(uint16_t));
#elif defined(DENSITY_BIG_ENDIAN)
                            const uint16_t endian_hash = DENSITY_LITTLE_ENDIAN_16(hash);
                            DENSITY_MEMCPY(*out, &endian_hash, sizeof(uint16_t));
#else
#error
#endif
                            *out += sizeof(uint16_t);
                        }
                    } else {
                        density_lion_encode_push_code_to_signature(out, signature_pointer, signature, shift, density_lion_form_model_get_encoding(data, DENSITY_LION_FORM_DICTIONARY_B));
#ifdef DENSITY_LITTLE_ENDIAN
                        DENSITY_MEMCPY(*out, &hash, sizeof(uint16_t));
#elif defined(DENSITY_BIG_ENDIAN)
                        const uint16_t endian_hash = DENSITY_LITTLE_ENDIAN_16(hash);
                        DENSITY_MEMCPY(*out, &endian_hash, sizeof(uint16_t));
#else
#error
#endif
                        *out += sizeof(uint16_t);
                    }
                    DENSITY_MEMMOVE((uint32_t *) in_dictionary + 1, in_dictionary, 3 * sizeof(uint32_t));
                    *(uint32_t *) in_dictionary = unit; // Does not ensure dictionary content consistency between endiannesses
                } else {
                    density_lion_encode_push_code_to_signature(out, signature_pointer, signature, shift, density_lion_form_model_get_encoding(data, DENSITY_LION_FORM_DICTIONARY_A));
#ifdef DENSITY_LITTLE_ENDIAN
                    DENSITY_MEMCPY(*out, &hash, sizeof(uint16_t));
#elif defined(DENSITY_BIG_ENDIAN)
                    const uint16_t endian_hash = DENSITY_LITTLE_ENDIAN_16(hash);
                    DENSITY_MEMCPY(*out, &endian_hash, sizeof(uint16_t));
#else
#error
#endif
                    *out += sizeof(uint16_t);
                }
            } else {
                density_lion_encode_push_code_to_signature(out, signature_pointer, signature, shift, density_lion_form_model_get_encoding(data, DENSITY_LION_FORM_PREDICTIONS_C));
            }
        } else {
            density_lion_encode_push_code_to_signature(out, signature_pointer, signature, shift, density_lion_form_model_get_encoding(data, DENSITY_LION_FORM_PREDICTIONS_B));
        }
        DENSITY_MEMMOVE((uint32_t *) predictions + 1, predictions, 2 * sizeof(uint32_t));
        *(uint32_t *) predictions = unit;   // Does not ensure dictionary content consistency between endiannesses
    } else
        density_lion_encode_push_code_to_signature(out, signature_pointer, signature, shift, density_lion_form_model_get_encoding(data, DENSITY_LION_FORM_PREDICTIONS_A));
    *last_hash = hash;
}

DENSITY_FORCE_INLINE void density_lion_encode_4(const uint8_t **DENSITY_RESTRICT in, uint8_t **DENSITY_RESTRICT out, uint_fast16_t *DENSITY_RESTRICT last_hash, density_lion_signature **DENSITY_RESTRICT signature_pointer, density_lion_signature *const DENSITY_RESTRICT signature, uint_fast8_t *const DENSITY_RESTRICT shift, density_lion_dictionary *const DENSITY_RESTRICT dictionary, density_lion_form_data *const data, uint32_t *DENSITY_RESTRICT unit) {
    DENSITY_MEMCPY(unit, *in, sizeof(uint32_t));
    density_lion_encode_kernel_4(out, last_hash, signature_pointer, signature, shift, dictionary, DENSITY_LION_HASH_ALGORITHM(DENSITY_LITTLE_ENDIAN_32(*unit)), data, *unit);
    *in += sizeof(uint32_t);
}

DENSITY_FORCE_INLINE void density_lion_encode_generic(const uint8_t **DENSITY_RESTRICT in, uint8_t **DENSITY_RESTRICT out, uint_fast16_t *DENSITY_RESTRICT last_hash, density_lion_signature **DENSITY_RESTRICT signature_pointer, density_lion_signature *const DENSITY_RESTRICT signature, uint_fast8_t *const DENSITY_RESTRICT shift, density_lion_dictionary *const DENSITY_RESTRICT dictionary, const uint_fast8_t chunks_per_process_unit, density_lion_form_data *const data, uint32_t *DENSITY_RESTRICT unit) {
#ifdef __clang__
    for (uint_fast8_t count = 0; count < (chunks_per_process_unit >> 2); count++) {
        DENSITY_UNROLL_4(density_lion_encode_4(in, out, last_hash, signature_pointer, signature, shift, dictionary, data, unit));
    }
#else
    for (uint_fast8_t count = 0; count < (chunks_per_process_unit >> 1); count++) {
        DENSITY_UNROLL_2(density_lion_encode_4(in, out, last_hash, signature_pointer, signature, shift, dictionary, data, unit));
    }
#endif
}

DENSITY_FORCE_INLINE void density_lion_encode_32(const uint8_t **DENSITY_RESTRICT in, uint8_t **DENSITY_RESTRICT out, uint_fast16_t *DENSITY_RESTRICT last_hash, density_lion_signature **DENSITY_RESTRICT signature_pointer, density_lion_signature *const DENSITY_RESTRICT signature, uint_fast8_t *const DENSITY_RESTRICT shift, density_lion_dictionary *const DENSITY_RESTRICT dictionary, density_lion_form_data *const data, uint32_t *DENSITY_RESTRICT unit) {
    density_lion_encode_generic(in, out, last_hash, signature_pointer, signature, shift, dictionary, DENSITY_LION_CHUNKS_PER_PROCESS_UNIT_SMALL, data, unit);
}

DENSITY_FORCE_INLINE void density_lion_encode_256(const uint8_t **DENSITY_RESTRICT in, uint8_t **DENSITY_RESTRICT out, uint_fast16_t *DENSITY_RESTRICT last_hash, density_lion_signature **DENSITY_RESTRICT signature_pointer, density_lion_signature *const DENSITY_RESTRICT signature, uint_fast8_t *const DENSITY_RESTRICT shift, density_lion_dictionary *const DENSITY_RESTRICT dictionary, density_lion_form_data *const data, uint32_t *DENSITY_RESTRICT unit) {
    density_lion_encode_generic(in, out, last_hash, signature_pointer, signature, shift, dictionary, DENSITY_LION_CHUNKS_PER_PROCESS_UNIT_BIG, data, unit);
}

DENSITY_WINDOWS_EXPORT DENSITY_FORCE_INLINE density_algorithm_exit_status density_lion_encode(density_algorithm_state *const DENSITY_RESTRICT state, const uint8_t **DENSITY_RESTRICT in, const uint_fast64_t in_size, uint8_t **DENSITY_RESTRICT out, const uint_fast64_t out_size) {
    if (out_size < DENSITY_LION_MAXIMUM_COMPRESSED_UNIT_SIZE)
        return DENSITY_ALGORITHMS_EXIT_STATUS_OUTPUT_STALL;

    density_lion_signature signature = 0;
    density_lion_signature *signature_pointer = NULL;
    uint_fast8_t shift = 0;
    density_lion_form_data data;
    density_lion_form_model_init(&data);
    uint_fast16_t last_hash = 0;
    uint32_t unit;

    uint8_t *out_limit = *out + out_size - DENSITY_LION_MAXIMUM_COMPRESSED_UNIT_SIZE;
    uint_fast64_t limit_256 = (in_size >> 8);

    while (DENSITY_LIKELY(limit_256-- && *out <= out_limit)) {
        if (DENSITY_UNLIKELY(!(state->counter & 0xf))) {
            DENSITY_ALGORITHM_REDUCE_COPY_PENALTY_START;
        }
        state->counter++;
        if (DENSITY_UNLIKELY(state->copy_penalty)) {
            DENSITY_ALGORITHM_COPY(DENSITY_LION_WORK_BLOCK_SIZE);
            DENSITY_ALGORITHM_INCREASE_COPY_PENALTY_START;
        } else {
            const uint8_t *out_start = *out;
            DENSITY_PREFETCH(*in + DENSITY_LION_WORK_BLOCK_SIZE);
            density_lion_encode_256(in, out, &last_hash, &signature_pointer, &signature, &shift, (density_lion_dictionary *const) state->dictionary, &data, &unit);
            DENSITY_ALGORITHM_TEST_INCOMPRESSIBILITY((*out - out_start), DENSITY_LION_WORK_BLOCK_SIZE);
        }
    }

    if (*out > out_limit)
        return DENSITY_ALGORITHMS_EXIT_STATUS_OUTPUT_STALL;

    uint_fast64_t remaining;

    switch (in_size & 0xff) {
        case 0:
        case 1:
        case 2:
        case 3:
            density_lion_encode_push_code_to_signature(out, &signature_pointer, &signature, &shift, density_lion_form_model_get_encoding(&data, DENSITY_LION_FORM_PLAIN)); // End marker
#ifdef DENSITY_LITTLE_ENDIAN
            DENSITY_MEMCPY(signature_pointer, &signature, sizeof(density_lion_signature));
#elif defined(DENSITY_BIG_ENDIAN)
            const density_lion_signature endian_signature = DENSITY_LITTLE_ENDIAN_64(signature);
            DENSITY_MEMCPY(signature_pointer, &endian_signature, sizeof(density_lion_signature));
#else
#error
#endif
            goto process_remaining_bytes;
        default:
            break;
    }

    uint_fast64_t limit_4 = (in_size & 0xff) >> 2;
    while (limit_4--)
        density_lion_encode_4(in, out, &last_hash, &signature_pointer, &signature, &shift, (density_lion_dictionary *const) state->dictionary, &data, &unit);

    density_lion_encode_push_code_to_signature(out, &signature_pointer, &signature, &shift, density_lion_form_model_get_encoding(&data, DENSITY_LION_FORM_PLAIN)); // End marker
#ifdef DENSITY_LITTLE_ENDIAN
    DENSITY_MEMCPY(signature_pointer, &signature, sizeof(density_lion_signature));
#elif defined(DENSITY_BIG_ENDIAN)
    const density_lion_signature endian_signature = DENSITY_LITTLE_ENDIAN_64(signature);
    DENSITY_MEMCPY(signature_pointer, &endian_signature, sizeof(density_lion_signature));
#else
#error
#endif

    process_remaining_bytes:
    remaining = in_size & 0x3;
    if (remaining) {
        DENSITY_MEMCPY(*out, *in, remaining);
        *in += remaining;
        *out += remaining;
    }

    return DENSITY_ALGORITHMS_EXIT_STATUS_FINISHED;
}
