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
 * 24/06/15 20:55
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

#include "lion_decode.h"

DENSITY_FORCE_INLINE void density_lion_decode_read_signature(const uint8_t **DENSITY_RESTRICT in, uint_fast64_t *const DENSITY_RESTRICT signature) {
#ifdef DENSITY_LITTLE_ENDIAN
    DENSITY_MEMCPY(signature, *in, sizeof(density_lion_signature));
#elif defined(DENSITY_BIG_ENDIAN)
    density_lion_signature endian_signature;
    DENSITY_MEMCPY(&endian_signature, *in, sizeof(density_lion_signature));
    *signature = DENSITY_LITTLE_ENDIAN_64(endian_signature);
#else
#error
#endif
    *in += sizeof(density_lion_signature);
}

DENSITY_FORCE_INLINE void density_lion_decode_update_predictions_model(density_lion_dictionary_chunk_prediction_entry *const DENSITY_RESTRICT predictions, const uint32_t chunk) {
    DENSITY_MEMMOVE((uint32_t *) predictions + 1, predictions, 2 * sizeof(uint32_t));
    *(uint32_t *) predictions = chunk;     // Move chunk to the top of the predictions list, does not ensure dictionary content consistency between endiannesses
}

DENSITY_FORCE_INLINE void density_lion_decode_update_dictionary_model(density_lion_dictionary_chunk_entry *const DENSITY_RESTRICT entry, const uint32_t chunk) {
    DENSITY_MEMMOVE((uint32_t *) entry + 1, entry, 3 * sizeof(uint32_t));
    *(uint32_t *) entry = chunk;    // Does not ensure dictionary content consistency between endiannesses
}

DENSITY_FORCE_INLINE void density_lion_decode_read_hash(const uint8_t **DENSITY_RESTRICT in, uint16_t *DENSITY_RESTRICT const hash) {
#ifdef DENSITY_LITTLE_ENDIAN
    DENSITY_MEMCPY(hash, *in, sizeof(uint16_t));
#elif defined(DENSITY_BIG_ENDIAN)
    uint16_t endian_hash;
    DENSITY_MEMCPY(&endian_hash, *in, sizeof(uint16_t));
    *hash = DENSITY_LITTLE_ENDIAN_16(endian_hash);
#else
#error
#endif
    *in += sizeof(uint16_t);
}

DENSITY_FORCE_INLINE void density_lion_decode_prediction_generic(uint8_t **DENSITY_RESTRICT out, uint16_t *DENSITY_RESTRICT const hash, uint32_t *DENSITY_RESTRICT const unit) {
    *hash = DENSITY_LION_HASH_ALGORITHM(DENSITY_LITTLE_ENDIAN_32(*unit));
    DENSITY_MEMCPY(*out, unit, sizeof(uint32_t));
    *out += sizeof(uint32_t);
}

DENSITY_FORCE_INLINE void density_lion_decode_dictionary_generic(uint8_t **DENSITY_RESTRICT out, uint_fast16_t *DENSITY_RESTRICT last_hash, density_lion_dictionary *const DENSITY_RESTRICT dictionary, uint32_t *DENSITY_RESTRICT const unit) {
    DENSITY_MEMCPY(*out, unit, sizeof(uint32_t));
    *out += sizeof(uint32_t);
    density_lion_dictionary_chunk_prediction_entry *prediction = &(dictionary->predictions[*last_hash]);
    density_lion_decode_update_predictions_model(prediction, *unit);
}

void density_lion_decode_prediction_a(const uint8_t **DENSITY_RESTRICT in, uint8_t **DENSITY_RESTRICT out, uint_fast16_t *DENSITY_RESTRICT last_hash, density_lion_dictionary *const DENSITY_RESTRICT dictionary, uint16_t *DENSITY_RESTRICT const hash, uint32_t *DENSITY_RESTRICT const unit) {
    (void)in;
    *unit = dictionary->predictions[*last_hash].next_chunk_a;
    density_lion_decode_prediction_generic(out, hash, unit);

    *last_hash = *hash;
}

void density_lion_decode_prediction_b(const uint8_t **DENSITY_RESTRICT in, uint8_t **DENSITY_RESTRICT out, uint_fast16_t *DENSITY_RESTRICT last_hash, density_lion_dictionary *const DENSITY_RESTRICT dictionary, uint16_t *DENSITY_RESTRICT const hash, uint32_t *DENSITY_RESTRICT const unit) {
    (void)in;
    density_lion_dictionary_chunk_prediction_entry *const prediction = &dictionary->predictions[*last_hash];
    *unit = prediction->next_chunk_b;
    density_lion_decode_update_predictions_model(prediction, *unit);
    density_lion_decode_prediction_generic(out, hash, unit);

    *last_hash = *hash;
}

void density_lion_decode_prediction_c(const uint8_t **DENSITY_RESTRICT in, uint8_t **DENSITY_RESTRICT out, uint_fast16_t *DENSITY_RESTRICT last_hash, density_lion_dictionary *const DENSITY_RESTRICT dictionary, uint16_t *DENSITY_RESTRICT const hash, uint32_t *DENSITY_RESTRICT const unit) {
    (void)in;
    density_lion_dictionary_chunk_prediction_entry *const prediction = &dictionary->predictions[*last_hash];
    *unit = prediction->next_chunk_c;
    density_lion_decode_update_predictions_model(prediction, *unit);
    density_lion_decode_prediction_generic(out, hash, unit);

    *last_hash = *hash;
}

void density_lion_decode_dictionary_a(const uint8_t **DENSITY_RESTRICT in, uint8_t **DENSITY_RESTRICT out, uint_fast16_t *DENSITY_RESTRICT last_hash, density_lion_dictionary *const DENSITY_RESTRICT dictionary, uint16_t *DENSITY_RESTRICT const hash, uint32_t *DENSITY_RESTRICT const unit) {
    density_lion_decode_read_hash(in, hash);
    DENSITY_PREFETCH(&dictionary->predictions[*hash]);
    *unit = dictionary->chunks[*hash].chunk_a;
    density_lion_decode_dictionary_generic(out, last_hash, dictionary, unit);

    *last_hash = *hash;
}

void density_lion_decode_dictionary_b(const uint8_t **DENSITY_RESTRICT in, uint8_t **DENSITY_RESTRICT out, uint_fast16_t *DENSITY_RESTRICT last_hash, density_lion_dictionary *const DENSITY_RESTRICT dictionary, uint16_t *DENSITY_RESTRICT const hash, uint32_t *DENSITY_RESTRICT const unit) {
    density_lion_decode_read_hash(in, hash);
  	DENSITY_PREFETCH(&dictionary->predictions[*hash]);
    density_lion_dictionary_chunk_entry *entry = &dictionary->chunks[*hash];
    *unit = entry->chunk_b;
    density_lion_decode_update_dictionary_model(entry, *unit);
    density_lion_decode_dictionary_generic(out, last_hash, dictionary, unit);

    *last_hash = *hash;
}

void density_lion_decode_dictionary_c(const uint8_t **DENSITY_RESTRICT in, uint8_t **DENSITY_RESTRICT out, uint_fast16_t *DENSITY_RESTRICT last_hash, density_lion_dictionary *const DENSITY_RESTRICT dictionary, uint16_t *DENSITY_RESTRICT const hash, uint32_t *DENSITY_RESTRICT const unit) {
    density_lion_decode_read_hash(in, hash);
  	DENSITY_PREFETCH(&dictionary->predictions[*hash]);
    density_lion_dictionary_chunk_entry *entry = &dictionary->chunks[*hash];
    *unit = entry->chunk_c;
    density_lion_decode_update_dictionary_model(entry, *unit);
    density_lion_decode_dictionary_generic(out, last_hash, dictionary, unit);

    *last_hash = *hash;
}

void density_lion_decode_dictionary_d(const uint8_t **DENSITY_RESTRICT in, uint8_t **DENSITY_RESTRICT out, uint_fast16_t *DENSITY_RESTRICT last_hash, density_lion_dictionary *const DENSITY_RESTRICT dictionary, uint16_t *DENSITY_RESTRICT const hash, uint32_t *DENSITY_RESTRICT const unit) {
    density_lion_decode_read_hash(in, hash);
  	DENSITY_PREFETCH(&dictionary->predictions[*hash]);
    density_lion_dictionary_chunk_entry *entry = &dictionary->chunks[*hash];
    *unit = entry->chunk_d;
    density_lion_decode_update_dictionary_model(entry, *unit);
    density_lion_decode_dictionary_generic(out, last_hash, dictionary, unit);

    *last_hash = *hash;
}

void density_lion_decode_plain(const uint8_t **DENSITY_RESTRICT in, uint8_t **DENSITY_RESTRICT out, uint_fast16_t *DENSITY_RESTRICT last_hash, density_lion_dictionary *const DENSITY_RESTRICT dictionary, uint16_t *DENSITY_RESTRICT const hash, uint32_t *DENSITY_RESTRICT const unit) {
    DENSITY_MEMCPY(unit, *in, sizeof(uint32_t));
    *in += sizeof(uint32_t);
    *hash = DENSITY_LION_HASH_ALGORITHM(DENSITY_LITTLE_ENDIAN_32(*unit));
    density_lion_dictionary_chunk_entry *entry = &dictionary->chunks[*hash];
    density_lion_decode_update_dictionary_model(entry, *unit);
    DENSITY_MEMCPY(*out, unit, sizeof(uint32_t));
    *out += sizeof(uint32_t);
    density_lion_dictionary_chunk_prediction_entry *prediction = &(dictionary->predictions[*last_hash]);
    density_lion_decode_update_predictions_model(prediction, *unit);

    *last_hash = *hash;
}

DENSITY_FORCE_INLINE void density_lion_decode_4(const uint8_t **DENSITY_RESTRICT in, uint8_t **DENSITY_RESTRICT out, uint_fast16_t *DENSITY_RESTRICT last_hash, density_lion_dictionary *const DENSITY_RESTRICT dictionary, density_lion_form_data *const data, const DENSITY_LION_FORM form) {
    uint16_t hash;
    uint32_t unit;

    data->attachments[form](in, out, last_hash, dictionary, &hash, &unit);
}

DENSITY_FORCE_INLINE DENSITY_LION_FORM density_lion_decode_read_form(const uint8_t **DENSITY_RESTRICT in, uint_fast64_t *const DENSITY_RESTRICT signature, uint_fast8_t *const DENSITY_RESTRICT shift, density_lion_form_data *const form_data) {
    const uint_fast8_t trailing_zeroes = DENSITY_CTZ(0x80 | (*signature >> *shift));
    if (DENSITY_LIKELY(!trailing_zeroes)) {
        *shift = (uint_fast8_t)((*shift + 1) & 0x3f);
        return density_lion_form_model_increment_usage(form_data, (density_lion_form_node *) form_data->formsPool);
    } else if (DENSITY_LIKELY(trailing_zeroes <= 6)) {
        *shift = (uint_fast8_t)((*shift + (trailing_zeroes + 1)) & 0x3f);
        return density_lion_form_model_increment_usage(form_data, (density_lion_form_node *) form_data->formsPool + trailing_zeroes);
    } else {
        if (DENSITY_LIKELY(*shift <= (density_bitsizeof(density_lion_signature) - 7))) {
            *shift = (uint_fast8_t)((*shift + 7) & 0x3f);
            return density_lion_form_model_increment_usage(form_data, (density_lion_form_node *) form_data->formsPool + 7);
        } else {
            density_lion_decode_read_signature(in, signature);
            const uint_fast8_t primary_trailing_zeroes = (uint_fast8_t)(density_bitsizeof(density_lion_signature) - *shift);
            const uint_fast8_t ctz_barrier_shift = (uint_fast8_t)(7 - primary_trailing_zeroes);
            const uint_fast8_t secondary_trailing_zeroes = DENSITY_CTZ(((uint64_t)1 << ctz_barrier_shift) | *signature);
            if (DENSITY_LIKELY(secondary_trailing_zeroes != ctz_barrier_shift))
                *shift = (uint_fast8_t)(secondary_trailing_zeroes + 1);
            else
                *shift = secondary_trailing_zeroes;
            return density_lion_form_model_increment_usage(form_data, (density_lion_form_node *) form_data->formsPool + primary_trailing_zeroes + secondary_trailing_zeroes);
        }
    }
}

DENSITY_FORCE_INLINE void density_lion_decode_process_form(const uint8_t **DENSITY_RESTRICT in, uint8_t **DENSITY_RESTRICT out, uint_fast16_t *DENSITY_RESTRICT last_hash, density_lion_dictionary *const DENSITY_RESTRICT dictionary, density_lion_form_data *const form_data, uint_fast64_t *const DENSITY_RESTRICT signature, uint_fast8_t *const DENSITY_RESTRICT shift) {
    if (DENSITY_UNLIKELY(!*shift))
        density_lion_decode_read_signature(in, signature);

    switch ((*signature >> *shift) & 0x1) {
        case 0:
            density_lion_decode_4(in, out, last_hash, dictionary, form_data, density_lion_decode_read_form(in, signature, shift, form_data));
            break;
        default:
            density_lion_decode_4(in, out, last_hash, dictionary, form_data, density_lion_form_model_increment_usage(form_data, (density_lion_form_node *) form_data->formsPool));
            *shift = (uint_fast8_t)((*shift + 1) & 0x3f);
            break;
    }
}

DENSITY_FORCE_INLINE void density_lion_decode_256(const uint8_t **DENSITY_RESTRICT in, uint8_t **DENSITY_RESTRICT out, uint_fast16_t *DENSITY_RESTRICT last_hash, density_lion_dictionary *const DENSITY_RESTRICT dictionary, density_lion_form_data *const form_data, uint_fast64_t *const DENSITY_RESTRICT signature, uint_fast8_t *const DENSITY_RESTRICT shift) {
#ifdef __clang__
    for (uint_fast8_t count = 0; count < (DENSITY_LION_CHUNKS_PER_PROCESS_UNIT_BIG >> 2); count++) {
        DENSITY_UNROLL_4(density_lion_decode_process_form(in, out, last_hash, dictionary, form_data, signature, shift));
    }
#else
    for (uint_fast8_t count = 0; count < (DENSITY_LION_CHUNKS_PER_PROCESS_UNIT_BIG >> 2); count++) {
        DENSITY_UNROLL_4(density_lion_decode_process_form(in, out, last_hash, dictionary, form_data, signature, shift));
    }
#endif
}

DENSITY_WINDOWS_EXPORT DENSITY_FORCE_INLINE density_algorithm_exit_status density_lion_decode(density_algorithm_state *const DENSITY_RESTRICT state, const uint8_t **DENSITY_RESTRICT in, const uint_fast64_t in_size, uint8_t **DENSITY_RESTRICT out, const uint_fast64_t out_size) {
    if (out_size < DENSITY_LION_MAXIMUM_DECOMPRESSED_UNIT_SIZE)
        return DENSITY_ALGORITHMS_EXIT_STATUS_OUTPUT_STALL;

    density_lion_signature signature = 0;
    density_lion_form_data data;
    density_lion_form_model_init(&data);
    void (*attachments[DENSITY_LION_NUMBER_OF_FORMS])(const uint8_t **, uint8_t **, uint_fast16_t *, void *const, uint16_t *const, uint32_t *const) = {(void (*)(const uint8_t **, uint8_t **, uint_fast16_t *, void *const, uint16_t *const, uint32_t *const)) &density_lion_decode_prediction_a, (void (*)(const uint8_t **, uint8_t **, uint_fast16_t *, void *const, uint16_t *const, uint32_t *const)) &density_lion_decode_prediction_b, (void (*)(const uint8_t **, uint8_t **, uint_fast16_t *, void *const, uint16_t *const, uint32_t *const)) &density_lion_decode_prediction_c, (void (*)(const uint8_t **, uint8_t **, uint_fast16_t *, void *const, uint16_t *const, uint32_t *const)) &density_lion_decode_dictionary_a, (void (*)(const uint8_t **, uint8_t **, uint_fast16_t *, void *const, uint16_t *const, uint32_t *const)) &density_lion_decode_dictionary_b, (void (*)(const uint8_t **, uint8_t **, uint_fast16_t *, void *const, uint16_t *const, uint32_t *const)) &density_lion_decode_dictionary_c, (void (*)(const uint8_t **, uint8_t **, uint_fast16_t *, void *const, uint16_t *const, uint32_t *const)) &density_lion_decode_dictionary_d, (void (*)(const uint8_t **, uint8_t **, uint_fast16_t *, void *const, uint16_t *const, uint32_t *const)) &density_lion_decode_plain};
    density_lion_form_model_attach(&data, attachments);
    uint_fast8_t shift = 0;
    uint_fast64_t remaining;
    uint_fast16_t last_hash = 0;
    DENSITY_LION_FORM form;

    const uint8_t *start = *in;

    if (in_size < DENSITY_LION_MAXIMUM_COMPRESSED_UNIT_SIZE) {
        goto read_and_decode_4;
    }

    const uint8_t *in_limit = *in + in_size - DENSITY_LION_MAXIMUM_COMPRESSED_UNIT_SIZE;
    uint8_t *out_limit = *out + out_size - DENSITY_LION_MAXIMUM_DECOMPRESSED_UNIT_SIZE;

    while (DENSITY_LIKELY(*in <= in_limit && *out <= out_limit)) {
        if (DENSITY_UNLIKELY(!(state->counter & 0xf))) {
            DENSITY_ALGORITHM_REDUCE_COPY_PENALTY_START;
        }
        state->counter++;
        if (DENSITY_UNLIKELY(state->copy_penalty)) {
            DENSITY_ALGORITHM_COPY(DENSITY_LION_WORK_BLOCK_SIZE);
            DENSITY_ALGORITHM_INCREASE_COPY_PENALTY_START;
        } else {
            const uint8_t *in_start = *in;
            density_lion_decode_256(in, out, &last_hash, (density_lion_dictionary *const) state->dictionary, &data, &signature, &shift);
            DENSITY_ALGORITHM_TEST_INCOMPRESSIBILITY((*in - in_start), DENSITY_LION_WORK_BLOCK_SIZE);
        }
    }

    if (*out > out_limit)
        return DENSITY_ALGORITHMS_EXIT_STATUS_OUTPUT_STALL;

    read_and_decode_4:
    if (DENSITY_UNLIKELY(!shift)) {
        if (in_size - (*in - start) < sizeof(density_lion_signature))
            return DENSITY_ALGORITHMS_EXIT_STATUS_INPUT_STALL;

        density_lion_decode_read_signature(in, &signature);
    }
    form = density_lion_decode_read_form(in, &signature, &shift, &data);
    switch (in_size - (*in - start)) {
        case 0:
        case 1:
            switch (form) {
                case DENSITY_LION_FORM_PLAIN:
                    goto process_remaining_bytes;   // End marker
                case DENSITY_LION_FORM_PREDICTIONS_A:
                case DENSITY_LION_FORM_PREDICTIONS_B:
                case DENSITY_LION_FORM_PREDICTIONS_C:
                    density_lion_decode_4(in, out, &last_hash, (density_lion_dictionary *const) state->dictionary, &data, form);
                    break;
                default:
                    return DENSITY_ALGORITHMS_EXIT_STATUS_ERROR_DURING_PROCESSING;   // Not enough bytes to read a hash
            }
            break;
        case 2:
        case 3:
            switch (form) {
                case DENSITY_LION_FORM_PLAIN:
                    goto process_remaining_bytes;   // End marker
                default:
                    density_lion_decode_4(in, out, &last_hash, (density_lion_dictionary *const) state->dictionary, &data, form);
                    break;
            }
            break;
        default:
            density_lion_decode_4(in, out, &last_hash, (density_lion_dictionary *const) state->dictionary, &data, form);
            break;
    }
    goto read_and_decode_4;

    process_remaining_bytes:
    remaining = in_size - (*in - start);
    DENSITY_MEMCPY(*out, *in, remaining);
    *in += remaining;
    *out += remaining;

    return DENSITY_ALGORITHMS_EXIT_STATUS_FINISHED;
}
