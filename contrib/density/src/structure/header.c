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
 * 11/10/13 17:56
 */

#include "header.h"

DENSITY_WINDOWS_EXPORT DENSITY_FORCE_INLINE void density_header_read(const uint8_t **DENSITY_RESTRICT in, density_header *DENSITY_RESTRICT header) {
    header->version[0] = *(*in);
    header->version[1] = *(*in + 1);
    header->version[2] = *(*in + 2);
    header->algorithm = *(*in + 3);

    *in += sizeof(density_header);
}

DENSITY_WINDOWS_EXPORT DENSITY_FORCE_INLINE void density_header_write(uint8_t **DENSITY_RESTRICT out, const DENSITY_ALGORITHM algorithm) {
    *(*out) = DENSITY_MAJOR_VERSION;
    *(*out + 1) = DENSITY_MINOR_VERSION;
    *(*out + 2) = DENSITY_REVISION;
    *(*out + 3) = algorithm;
    *(*out + 4) = 0;
    *(*out + 5) = 0;
    *(*out + 6) = 0;
    *(*out + 7) = 0;

    *out += sizeof(density_header);
}
