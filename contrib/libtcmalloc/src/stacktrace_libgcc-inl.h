// -*- Mode: C++; c-basic-offset: 2; indent-tabs-mode: nil -*-
// Copyright (c) 2016, gperftools Contributors
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

// This file implements backtrace capturing via libgcc's
// _Unwind_Backtrace. This generally works almost always. It will fail
// sometimes when we're trying to capture backtrace from signal
// handler (i.e. in cpu profiler) while some C++ code is throwing
// exception.

#ifndef BASE_STACKTRACE_LIBGCC_INL_H_
#define BASE_STACKTRACE_LIBGCC_INL_H_
// Note: this file is included into stacktrace.cc more than once.
// Anything that should only be defined once should be here:

extern "C" {
#include <assert.h>
#include <string.h>   // for memset()
}

#include <unwind.h>

#include "gperftools/stacktrace.h"

struct libgcc_backtrace_data {
  void **array;
  int skip;
  int pos;
  int limit;
};

static _Unwind_Reason_Code libgcc_backtrace_helper(struct _Unwind_Context *ctx,
                                                   void *_data) {
  libgcc_backtrace_data *data =
    reinterpret_cast<libgcc_backtrace_data *>(_data);

  if (data->skip > 0) {
    data->skip--;
    return _URC_NO_REASON;
  }

  if (data->pos < data->limit) {
    void *ip = reinterpret_cast<void *>(_Unwind_GetIP(ctx));;
    data->array[data->pos++] = ip;
  }

  return _URC_NO_REASON;
}

#endif  // BASE_STACKTRACE_LIBGCC_INL_H_

// Note: this part of the file is included several times.
// Do not put globals below.

// The following 4 functions are generated from the code below:
//   GetStack{Trace,Frames}()
//   GetStack{Trace,Frames}WithContext()
//
// These functions take the following args:
//   void** result: the stack-trace, as an array
//   int* sizes: the size of each stack frame, as an array
//               (GetStackFrames* only)
//   int max_depth: the size of the result (and sizes) array(s)
//   int skip_count: how many stack pointers to skip before storing in result
//   void* ucp: a ucontext_t* (GetStack{Trace,Frames}WithContext only)
static int GET_STACK_TRACE_OR_FRAMES {
  libgcc_backtrace_data data;
  data.array = result;
  // we're also skipping current and parent's frame
  data.skip = skip_count + 2;
  data.pos = 0;
  data.limit = max_depth;

  _Unwind_Backtrace(libgcc_backtrace_helper, &data);

  if (data.pos > 1 && data.array[data.pos - 1] == NULL)
    --data.pos;

#if IS_STACK_FRAMES
  // No implementation for finding out the stack frame sizes.
  memset(sizes, 0, sizeof(*sizes) * data.pos);
#endif

  return data.pos;
}
