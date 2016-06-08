/* -*- Mode: C++; c-basic-offset: 2; indent-tabs-mode: nil -*-
 * Copyright (c) 2014, gperftools Contributors
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 * 
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef GETENV_SAFE_H
#define GETENV_SAFE_H

#ifdef __cplusplus
extern "C" {
#endif

/* 
 * This getenv function is safe to call before the C runtime is initialized.
 * On Windows, it utilizes GetEnvironmentVariable() and on unix it uses
 * /proc/self/environ instead calling getenv().  It's intended to be used in
 * routines that run before main(), when the state required for getenv() may
 * not be set up yet.  In particular, errno isn't set up until relatively late
 * (after the pthreads library has a chance to make it threadsafe), and
 * getenv() doesn't work until then.
 * On some platforms, this call will utilize the same, static buffer for
 * repeated GetenvBeforeMain() calls. Callers should not expect pointers from
 * this routine to be long lived.
 * Note that on unix, /proc only has the environment at the time the
 * application was started, so this routine ignores setenv() calls/etc.  Also
 * note it only reads the first 16K of the environment.
 * 
 * NOTE: this is version of GetenvBeforeMain that's usable from
 * C. Implementation is in sysinfo.cc
 */
const char* TCMallocGetenvSafe(const char* name);

#ifdef __cplusplus
}
#endif

#endif
