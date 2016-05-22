// -*- Mode: C++; c-basic-offset: 2; indent-tabs-mode: nil -*-
// Copyright (c) 2013, Google Inc.
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

// ---
// Author: Jean Lee <xiaoyur347@gmail.com>
// based on gcc Code-Gen-Options "-finstrument-functions" listed in
// http://gcc.gnu.org/onlinedocs/gcc/Code-Gen-Options.html .
// Should run configure with CXXFLAGS="-finstrument-functions".

// This file is a backtrace implementation for systems :
// * The glibc implementation of backtrace() may cause a call to malloc,
//   and cause a deadlock in HeapProfiler.
// * The libunwind implementation prints no backtrace.

// The backtrace arrays are stored in "thread_back_trace" variable.
// Maybe to use thread local storage is better and should save memorys.

#ifndef BASE_STACKTRACE_INSTRUMENT_INL_H_
#define BASE_STACKTRACE_INSTRUMENT_INL_H_
// Note: this file is included into stacktrace.cc more than once.
// Anything that should only be defined once should be here:

#include <execinfo.h>
#include <string.h>
#include <unistd.h>
#include <sys/syscall.h>
#include "gperftools/stacktrace.h"

#define gettid() syscall(__NR_gettid)
#ifndef __x86_64__
#define MAX_THREAD (32768)
#else
#define MAX_THREAD (65536)
#endif
#define MAX_DEPTH  (30)
#define ATTRIBUTE_NOINSTRUMENT __attribute__ ((no_instrument_function))

typedef struct {
  int   stack_depth;
  void* frame[MAX_DEPTH];
}BACK_TRACE;

static BACK_TRACE thread_back_trace[MAX_THREAD];
extern "C" {
void __cyg_profile_func_enter(void *func_address,
                              void *call_site) ATTRIBUTE_NOINSTRUMENT;
void __cyg_profile_func_enter(void *func_address, void *call_site) {
  (void)func_address;

  BACK_TRACE* backtrace = thread_back_trace + gettid();
  int stack_depth = backtrace->stack_depth;
  backtrace->stack_depth = stack_depth + 1;
  if ( stack_depth >= MAX_DEPTH ) {
    return;
  }
  backtrace->frame[stack_depth] = call_site;
}

void __cyg_profile_func_exit(void *func_address,
                             void *call_site) ATTRIBUTE_NOINSTRUMENT;
void __cyg_profile_func_exit(void *func_address, void *call_site) {
  (void)func_address;
  (void)call_site;

  BACK_TRACE* backtrace = thread_back_trace + gettid();
  int stack_depth = backtrace->stack_depth;
  backtrace->stack_depth = stack_depth - 1;
  if ( stack_depth >= MAX_DEPTH ) {
    return;
  }
  backtrace->frame[stack_depth] = 0;
}
}  // extern "C"

static int cyg_backtrace(void **buffer, int size) {
  BACK_TRACE* backtrace = thread_back_trace + gettid();
  int stack_depth = backtrace->stack_depth;
  if ( stack_depth >= MAX_DEPTH ) {
    stack_depth = MAX_DEPTH;
  }
  int nSize = (size > stack_depth) ? stack_depth : size;
  for (int i = 0; i < nSize; i++) {
  buffer[i] = backtrace->frame[nSize - i - 1];
  }

  return nSize;
}

#endif  // BASE_STACKTRACE_INSTRUMENT_INL_H_


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
  static const int kStackLength = 64;
  void * stack[kStackLength];
  int size;
  memset(stack, 0, sizeof(stack));

  size = cyg_backtrace(stack, kStackLength);
  skip_count += 2;  // we want to skip the current and parent frame as well
  int result_count = size - skip_count;
  if (result_count < 0)
    result_count = 0;
  if (result_count > max_depth)
    result_count = max_depth;
  for (int i = 0; i < result_count; i++)
    result[i] = stack[i + skip_count];

#if IS_STACK_FRAMES
  // No implementation for finding out the stack frame sizes yet.
  memset(sizes, 0, sizeof(*sizes) * result_count);
#endif

  return result_count;
}
