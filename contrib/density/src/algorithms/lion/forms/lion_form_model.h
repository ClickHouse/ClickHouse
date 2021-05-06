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
 * 9/03/15 12:04
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

#ifndef DENSITY_LION_FORM_MODEL_H
#define DENSITY_LION_FORM_MODEL_H

#include "../../../globals.h"
#include "../lion.h"

#define DENSITY_LION_NUMBER_OF_FORMS                                    8

// Unary codes (reversed) except the last one
#define DENSITY_LION_FORM_MODEL_ENTROPY_CODES {\
    {DENSITY_BINARY_TO_UINT(1), 1},\
    {DENSITY_BINARY_TO_UINT(10), 2},\
    {DENSITY_BINARY_TO_UINT(100), 3},\
    {DENSITY_BINARY_TO_UINT(1000), 4},\
    {DENSITY_BINARY_TO_UINT(10000), 5},\
    {DENSITY_BINARY_TO_UINT(100000), 6},\
    {DENSITY_BINARY_TO_UINT(1000000), 7},\
    {DENSITY_BINARY_TO_UINT(0000000), 7},\
}

#pragma pack(push)
#pragma pack(4)
typedef struct {
    void* previousForm;
    DENSITY_LION_FORM form;
    uint8_t rank;
} density_lion_form_node;

typedef struct {
    union {
        uint8_t usages_as_uint8_t[DENSITY_LION_NUMBER_OF_FORMS];
        uint64_t usages_as_uint64_t;
    } usages;

    void (*attachments[DENSITY_LION_NUMBER_OF_FORMS])(const uint8_t **, uint8_t **, uint_fast16_t *, void *const, uint16_t *const, uint32_t *const);
    density_lion_form_node formsPool[DENSITY_LION_NUMBER_OF_FORMS];
    density_lion_form_node *formsIndex[DENSITY_LION_NUMBER_OF_FORMS];
    uint8_t nextAvailableForm;
} density_lion_form_data;
#pragma pack(pop)

DENSITY_WINDOWS_EXPORT void density_lion_form_model_init(density_lion_form_data *const);

DENSITY_WINDOWS_EXPORT void density_lion_form_model_attach(density_lion_form_data *const, void (*[DENSITY_LION_NUMBER_OF_FORMS])(const uint8_t **, uint8_t **, uint_fast16_t *, void *const, uint16_t *const, uint32_t *const));

DENSITY_WINDOWS_EXPORT void density_lion_form_model_update(density_lion_form_data *const DENSITY_RESTRICT_DECLARE, density_lion_form_node *const DENSITY_RESTRICT_DECLARE, const uint8_t, density_lion_form_node *const DENSITY_RESTRICT_DECLARE, const uint8_t);

DENSITY_WINDOWS_EXPORT DENSITY_LION_FORM density_lion_form_model_increment_usage(density_lion_form_data *const, density_lion_form_node *const DENSITY_RESTRICT_DECLARE);

DENSITY_WINDOWS_EXPORT density_lion_entropy_code density_lion_form_model_get_encoding(density_lion_form_data *const, const DENSITY_LION_FORM);

#endif
