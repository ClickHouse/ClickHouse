// -*- Mode: C++; c-basic-offset: 2; indent-tabs-mode: nil -*-
// NOTE: this is NOT to be #include-d normally. It's internal
// implementation detail of stacktrace.cc
//

// Copyright (c) 2014, gperftools Contributors.
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
// Author: Aliaksey Kandratsenka <alk@tut.by>
//
//  based on stacktrace.cc and stacktrace_config.h by Sanjay Ghemawat
//  and Paul Pluzhnikov from Google Inc

#define SIS_CONCAT2(a, b) a##b
#define SIS_CONCAT(a, b) SIS_CONCAT2(a,b)

#define SIS_STRINGIFY(a) SIS_STRINGIFY2(a)
#define SIS_STRINGIFY2(a) #a

#define IS_STACK_FRAMES 0
#define IS_WITH_CONTEXT 0
#define GET_STACK_TRACE_OR_FRAMES \
  SIS_CONCAT(GetStackTrace_, GST_SUFFIX)(void **result, int max_depth, int skip_count)
#include STACKTRACE_INL_HEADER
#undef IS_STACK_FRAMES
#undef IS_WITH_CONTEXT
#undef GET_STACK_TRACE_OR_FRAMES

#define IS_STACK_FRAMES 1
#define IS_WITH_CONTEXT 0
#define GET_STACK_TRACE_OR_FRAMES \
  SIS_CONCAT(GetStackFrames_, GST_SUFFIX)(void **result, int *sizes, int max_depth, int skip_count)
#include STACKTRACE_INL_HEADER
#undef IS_STACK_FRAMES
#undef IS_WITH_CONTEXT
#undef GET_STACK_TRACE_OR_FRAMES

#define IS_STACK_FRAMES 0
#define IS_WITH_CONTEXT 1
#define GET_STACK_TRACE_OR_FRAMES \
  SIS_CONCAT(GetStackTraceWithContext_, GST_SUFFIX)(void **result, int max_depth, \
                                                   int skip_count, const void *ucp)
#include STACKTRACE_INL_HEADER
#undef IS_STACK_FRAMES
#undef IS_WITH_CONTEXT
#undef GET_STACK_TRACE_OR_FRAMES

#define IS_STACK_FRAMES 1
#define IS_WITH_CONTEXT 1
#define GET_STACK_TRACE_OR_FRAMES \
  SIS_CONCAT(GetStackFramesWithContext_, GST_SUFFIX)(void **result, int *sizes, int max_depth, \
                                                    int skip_count, const void *ucp)
#include STACKTRACE_INL_HEADER
#undef IS_STACK_FRAMES
#undef IS_WITH_CONTEXT
#undef GET_STACK_TRACE_OR_FRAMES

static GetStackImplementation SIS_CONCAT(impl__,GST_SUFFIX) = {
  SIS_CONCAT(GetStackFrames_, GST_SUFFIX),
  SIS_CONCAT(GetStackFramesWithContext_, GST_SUFFIX),
  SIS_CONCAT(GetStackTrace_, GST_SUFFIX),
  SIS_CONCAT(GetStackTraceWithContext_, GST_SUFFIX),
  SIS_STRINGIFY(GST_SUFFIX)
};

#undef SIS_CONCAT2
#undef SIS_CONCAT
