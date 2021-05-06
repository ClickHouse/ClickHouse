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
 * 5/02/15 20:57
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

#ifndef DENSITY_LION_H
#define DENSITY_LION_H

#include "../../globals.h"

#define DENSITY_LION_HASH32_MULTIPLIER                                  (uint32_t)0x9D6EF916lu
#define DENSITY_LION_CHUNK_HASH_BITS                                    16

#define DENSITY_LION_HASH_ALGORITHM(value32)                            (uint16_t)(value32 * DENSITY_LION_HASH32_MULTIPLIER >> (32 - DENSITY_LION_CHUNK_HASH_BITS))

typedef enum {
    DENSITY_LION_FORM_PREDICTIONS_A = 0,
    DENSITY_LION_FORM_PREDICTIONS_B,
    DENSITY_LION_FORM_PREDICTIONS_C,
    DENSITY_LION_FORM_DICTIONARY_A,
    DENSITY_LION_FORM_DICTIONARY_B,
    DENSITY_LION_FORM_DICTIONARY_C,
    DENSITY_LION_FORM_DICTIONARY_D,
    DENSITY_LION_FORM_PLAIN,
} DENSITY_LION_FORM;

typedef enum {
    DENSITY_LION_PREDICTIONS_SIGNATURE_FLAG_A = 0x0,
    DENSITY_LION_PREDICTIONS_SIGNATURE_FLAG_B = 0x1,
} DENSITY_LION_PREDICTIONS_SIGNATURE_FLAG;

#pragma pack(push)
#pragma pack(4)
typedef struct {
    uint_fast8_t value;
    uint_fast8_t bitLength;
} density_lion_entropy_code;
#pragma pack(pop)

typedef uint64_t                                                        density_lion_signature;

#define DENSITY_LION_MAXIMUM_COMPRESSED_BODY_SIZE_PER_SIGNATURE         (density_bitsizeof(density_lion_signature) * sizeof(uint32_t))  // Plain writes
#define DENSITY_LION_MAXIMUM_COMPRESSED_UNIT_SIZE                       (sizeof(density_lion_signature) + DENSITY_LION_MAXIMUM_COMPRESSED_BODY_SIZE_PER_SIGNATURE)

#define DENSITY_LION_MAXIMUM_DECOMPRESSED_UNIT_SIZE                     (density_bitsizeof(density_lion_signature) * sizeof(uint32_t))  // Smallest form size times work unit size

#define DENSITY_LION_CHUNKS_PER_PROCESS_UNIT_SMALL                      8
#define DENSITY_LION_CHUNKS_PER_PROCESS_UNIT_BIG                        64
#define DENSITY_LION_PROCESS_UNIT_SIZE_SMALL                            (DENSITY_LION_CHUNKS_PER_PROCESS_UNIT_SMALL * sizeof(uint32_t))
#define DENSITY_LION_PROCESS_UNIT_SIZE_BIG                              (DENSITY_LION_CHUNKS_PER_PROCESS_UNIT_BIG * sizeof(uint32_t))

#define DENSITY_LION_WORK_BLOCK_SIZE                                   256
#define DENSITY_LION_COPY_PENALTY                                      2

#endif
