#pragma once

/// Arcadia compatibility DEVTOOLS-3976
#if defined(MEMRCHR_INCLUDE)
#include MEMRCHR_INCLUDE
#else

/*
 * Copyright (C) 2008 The Android Open Source Project
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *  * Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 * COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
 * OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
 * AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#ifdef __APPLE__

#include <stddef.h>
#include <string.h>
#if defined (__cplusplus)
extern "C" {
#endif
inline void *memrchr(const void *s, int c, size_t n) {
    if (n > 0) {
        const char*  p = static_cast<const char *>(s);
        const char*  q = p + n;
        while (1) {
            q--; if (q < p || q[0] == c) break;
            q--; if (q < p || q[0] == c) break;
            q--; if (q < p || q[0] == c) break;
            q--; if (q < p || q[0] == c) break;
        }
        if (q >= p)
            return reinterpret_cast<void *>(const_cast<char *>(q));
    }
    return NULL;
}
#if defined (__cplusplus)
}
#endif
#endif
#endif
