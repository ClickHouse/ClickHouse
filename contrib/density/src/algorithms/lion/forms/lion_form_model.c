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
 * 9/03/15 11:19
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

#include "lion_form_model.h"

const density_lion_entropy_code density_lion_form_entropy_codes[DENSITY_LION_NUMBER_OF_FORMS] = DENSITY_LION_FORM_MODEL_ENTROPY_CODES;

DENSITY_WINDOWS_EXPORT DENSITY_FORCE_INLINE void density_lion_form_model_init(density_lion_form_data *const data) {
    density_lion_form_node *rank_0 = &data->formsPool[0];
    rank_0->form = DENSITY_LION_FORM_PLAIN;
    rank_0->rank = 0;
    rank_0->previousForm = NULL;
    data->formsIndex[DENSITY_LION_FORM_PLAIN] = rank_0;

    density_lion_form_node *rank_1 = &data->formsPool[1];
    rank_1->form = DENSITY_LION_FORM_DICTIONARY_A;
    rank_1->rank = 1;
    rank_1->previousForm = rank_0;
    data->formsIndex[DENSITY_LION_FORM_DICTIONARY_A] = rank_1;

    density_lion_form_node *rank_2 = &data->formsPool[2];
    rank_2->form = DENSITY_LION_FORM_DICTIONARY_B;
    rank_2->rank = 2;
    rank_2->previousForm = rank_1;
    data->formsIndex[DENSITY_LION_FORM_DICTIONARY_B] = rank_2;

    density_lion_form_node *rank_3 = &data->formsPool[3];
    rank_3->form = DENSITY_LION_FORM_PREDICTIONS_A;
    rank_3->rank = 3;
    rank_3->previousForm = rank_2;
    data->formsIndex[DENSITY_LION_FORM_PREDICTIONS_A] = rank_3;

    density_lion_form_node *rank_4 = &data->formsPool[4];
    rank_4->form = DENSITY_LION_FORM_PREDICTIONS_B;
    rank_4->rank = 4;
    rank_4->previousForm = rank_3;
    data->formsIndex[DENSITY_LION_FORM_PREDICTIONS_B] = rank_4;

    density_lion_form_node *rank_5 = &data->formsPool[5];
    rank_5->form = DENSITY_LION_FORM_DICTIONARY_C;
    rank_5->rank = 5;
    rank_5->previousForm = rank_4;
    data->formsIndex[DENSITY_LION_FORM_DICTIONARY_C] = rank_5;

    density_lion_form_node *rank_6 = &data->formsPool[6];
    rank_6->form = DENSITY_LION_FORM_PREDICTIONS_C;
    rank_6->rank = 6;
    rank_6->previousForm = rank_5;
    data->formsIndex[DENSITY_LION_FORM_PREDICTIONS_C] = rank_6;

    density_lion_form_node *rank_7 = &data->formsPool[7];
    rank_7->form = DENSITY_LION_FORM_DICTIONARY_D;
    rank_7->rank = 7;
    rank_7->previousForm = rank_6;
    data->formsIndex[DENSITY_LION_FORM_DICTIONARY_D] = rank_7;

    data->usages.usages_as_uint64_t = 0;
}

DENSITY_WINDOWS_EXPORT DENSITY_FORCE_INLINE void density_lion_form_model_attach(density_lion_form_data *const data, void (*attachments[DENSITY_LION_NUMBER_OF_FORMS])(const uint8_t **, uint8_t **, uint_fast16_t *, void *const, uint16_t *const, uint32_t *const)) {
    for(uint_fast8_t count = 0; count < DENSITY_LION_NUMBER_OF_FORMS; count ++)
        data->attachments[count] = attachments[count];
}

DENSITY_WINDOWS_EXPORT DENSITY_FORCE_INLINE void density_lion_form_model_update(density_lion_form_data *const DENSITY_RESTRICT data, density_lion_form_node *const DENSITY_RESTRICT form, const uint8_t usage, density_lion_form_node *const DENSITY_RESTRICT previous_form, const uint8_t previous_usage) {
    if (DENSITY_UNLIKELY(previous_usage < usage)) {    // Relative stability is assumed
        const DENSITY_LION_FORM form_value = form->form;
        const DENSITY_LION_FORM previous_form_value = previous_form->form;

        previous_form->form = form_value;
        form->form = previous_form_value;

        data->formsIndex[form_value] = previous_form;
        data->formsIndex[previous_form_value] = form;
    }
}

DENSITY_FORCE_INLINE void density_lion_form_model_flatten(density_lion_form_data *const data, const uint8_t usage) {
    if (DENSITY_UNLIKELY(usage & 0x80))
        data->usages.usages_as_uint64_t = (data->usages.usages_as_uint64_t >> 1) & 0x7f7f7f7f7f7f7f7fllu; // Flatten usage values
}

DENSITY_WINDOWS_EXPORT DENSITY_FORCE_INLINE DENSITY_LION_FORM density_lion_form_model_increment_usage(density_lion_form_data *const data, density_lion_form_node *const DENSITY_RESTRICT form) {
    const DENSITY_LION_FORM form_value = form->form;
    const uint8_t usage = ++data->usages.usages_as_uint8_t[form_value];

    density_lion_form_node *const previous_form = form->previousForm;

    if (previous_form)
        density_lion_form_model_update(data, form, usage, previous_form, data->usages.usages_as_uint8_t[previous_form->form]);
    else
        density_lion_form_model_flatten(data, usage);

    return form_value;
}

DENSITY_WINDOWS_EXPORT DENSITY_FORCE_INLINE density_lion_entropy_code density_lion_form_model_get_encoding(density_lion_form_data *const data, const DENSITY_LION_FORM form) {
    const uint8_t usage = ++data->usages.usages_as_uint8_t[form];

    density_lion_form_node *const form_found = data->formsIndex[form];
    density_lion_form_node *const previous_form = form_found->previousForm;

    if (previous_form) {
        density_lion_form_model_update(data, form_found, usage, previous_form, data->usages.usages_as_uint8_t[previous_form->form]);

        return density_lion_form_entropy_codes[form_found->rank];
    } else {
        density_lion_form_model_flatten(data, usage);

        return density_lion_form_entropy_codes[0];
    }
}
