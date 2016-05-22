// -*- Mode: C++; c-basic-offset: 2; indent-tabs-mode: nil -*-
// Copyright (c) 2011, Google Inc.
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
// Author: Craig Silverstein <opensource@google.com>
//
// Used to override malloc routines on systems that define the
// memory allocation routines to be weak symbols in their libc
// (almost all unix-based systems are like this), on gcc, which
// suppports the 'alias' attribute.

#ifndef TCMALLOC_LIBC_OVERRIDE_GCC_AND_WEAK_INL_H_
#define TCMALLOC_LIBC_OVERRIDE_GCC_AND_WEAK_INL_H_

#ifdef HAVE_SYS_CDEFS_H
#include <sys/cdefs.h>    // for __THROW
#endif
#include <gperftools/tcmalloc.h>

#include "getenv_safe.h" // TCMallocGetenvSafe
#include "base/commandlineflags.h"

#ifndef __THROW    // I guess we're not on a glibc-like system
# define __THROW   // __THROW is just an optimization, so ok to make it ""
#endif

#ifndef __GNUC__
# error libc_override_gcc_and_weak.h is for gcc distributions only.
#endif

#define ALIAS(tc_fn)   __attribute__ ((alias (#tc_fn), used))

void* operator new(size_t size) throw (std::bad_alloc)
    ALIAS(tc_new);
void operator delete(void* p) throw()
    ALIAS(tc_delete);
void* operator new[](size_t size) throw (std::bad_alloc)
    ALIAS(tc_newarray);
void operator delete[](void* p) throw()
    ALIAS(tc_deletearray);
void* operator new(size_t size, const std::nothrow_t& nt) throw()
    ALIAS(tc_new_nothrow);
void* operator new[](size_t size, const std::nothrow_t& nt) throw()
    ALIAS(tc_newarray_nothrow);
void operator delete(void* p, const std::nothrow_t& nt) throw()
    ALIAS(tc_delete_nothrow);
void operator delete[](void* p, const std::nothrow_t& nt) throw()
    ALIAS(tc_deletearray_nothrow);

#if defined(ENABLE_SIZED_DELETE)

void operator delete(void *p, size_t size) throw()
    ALIAS(tc_delete_sized);
void operator delete[](void *p, size_t size) throw()
    ALIAS(tc_deletearray_sized);

#elif defined(ENABLE_DYNAMIC_SIZED_DELETE) && \
  (__GNUC__ * 100 + __GNUC_MINOR__) >= 405

static void delegate_sized_delete(void *p, size_t s) throw() {
  (operator delete)(p);
}

static void delegate_sized_deletearray(void *p, size_t s) throw() {
  (operator delete[])(p);
}

extern "C" __attribute__((weak))
int tcmalloc_sized_delete_enabled(void);

static bool sized_delete_enabled(void) {
  if (tcmalloc_sized_delete_enabled != 0) {
    return !!tcmalloc_sized_delete_enabled();
  }

  const char *flag = TCMallocGetenvSafe("TCMALLOC_ENABLE_SIZED_DELETE");
  return tcmalloc::commandlineflags::StringToBool(flag, false);
}

extern "C" {

static void *resolve_delete_sized(void) {
  if (sized_delete_enabled()) {
    return reinterpret_cast<void *>(tc_delete_sized);
  }
  return reinterpret_cast<void *>(delegate_sized_delete);
}

static void *resolve_deletearray_sized(void) {
  if (sized_delete_enabled()) {
    return reinterpret_cast<void *>(tc_deletearray_sized);
  }
  return reinterpret_cast<void *>(delegate_sized_deletearray);
}

}

void operator delete(void *p, size_t size) throw()
  __attribute__((ifunc("resolve_delete_sized")));
void operator delete[](void *p, size_t size) throw()
  __attribute__((ifunc("resolve_deletearray_sized")));

#else /* !ENABLE_SIZED_DELETE && !ENABLE_DYN_SIZED_DELETE */

void operator delete(void *p, size_t size) throw()
  ALIAS(tc_delete);
void operator delete[](void *p, size_t size) throw()
  ALIAS(tc_deletearray);

#endif /* !ENABLE_SIZED_DELETE && !ENABLE_DYN_SIZED_DELETE */

extern "C" {
  void* malloc(size_t size) __THROW               ALIAS(tc_malloc);
  void free(void* ptr) __THROW                    ALIAS(tc_free);
  void* realloc(void* ptr, size_t size) __THROW   ALIAS(tc_realloc);
  void* calloc(size_t n, size_t size) __THROW     ALIAS(tc_calloc);
  void cfree(void* ptr) __THROW                   ALIAS(tc_cfree);
  void* memalign(size_t align, size_t s) __THROW  ALIAS(tc_memalign);
  void* valloc(size_t size) __THROW               ALIAS(tc_valloc);
  void* pvalloc(size_t size) __THROW              ALIAS(tc_pvalloc);
  int posix_memalign(void** r, size_t a, size_t s) __THROW
      ALIAS(tc_posix_memalign);
#ifndef __UCLIBC__
  void malloc_stats(void) __THROW                 ALIAS(tc_malloc_stats);
#endif
  int mallopt(int cmd, int value) __THROW         ALIAS(tc_mallopt);
#ifdef HAVE_STRUCT_MALLINFO
  struct mallinfo mallinfo(void) __THROW          ALIAS(tc_mallinfo);
#endif
  size_t malloc_size(void* p) __THROW             ALIAS(tc_malloc_size);
#if defined(__ANDROID__)
  size_t malloc_usable_size(const void* p) __THROW
         ALIAS(tc_malloc_size);
#else
  size_t malloc_usable_size(void* p) __THROW      ALIAS(tc_malloc_size);
#endif
}   // extern "C"

#undef ALIAS

// No need to do anything at tcmalloc-registration time: we do it all
// via overriding weak symbols (at link time).
static void ReplaceSystemAlloc() { }

#endif  // TCMALLOC_LIBC_OVERRIDE_GCC_AND_WEAK_INL_H_
