// Copyright 2006-2008 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#ifndef RE2_UTIL_ATOMICOPS_H__
#define RE2_UTIL_ATOMICOPS_H__

// The memory ordering constraints resemble the ones in C11.
// RELAXED - no memory ordering, just an atomic operation.
// CONSUME - data-dependent ordering.
// ACQUIRE - prevents memory accesses from hoisting above the operation.
// RELEASE - prevents memory accesses from sinking below the operation.

#if (__clang_major__ * 100 + __clang_minor__ >= 303) || \
	(__GNUC__ * 1000 + __GNUC_MINOR__ * 100 + __GNUC_PATCHLEVEL__ >= 40801)

#define ATOMIC_LOAD_RELAXED(x, p) do { (x) = __atomic_load_n((p), __ATOMIC_RELAXED); } while (0)
#define ATOMIC_LOAD_CONSUME(x, p) do { (x) = __atomic_load_n((p), __ATOMIC_CONSUME); } while (0)
#define ATOMIC_LOAD_ACQUIRE(x, p) do { (x) = __atomic_load_n((p), __ATOMIC_ACQUIRE); } while (0)
#define ATOMIC_STORE_RELAXED(p, v) __atomic_store_n((p), (v), __ATOMIC_RELAXED)
#define ATOMIC_STORE_RELEASE(p, v) __atomic_store_n((p), (v), __ATOMIC_RELEASE)

#else  // old compiler

#define ATOMIC_LOAD_RELAXED(x, p) do { (x) = *(p); } while (0)
#define ATOMIC_LOAD_CONSUME(x, p) do { (x) = *(p); MaybeReadMemoryBarrier(); } while (0)
#define ATOMIC_LOAD_ACQUIRE(x, p) do { (x) = *(p); ReadMemoryBarrier(); } while (0)
#define ATOMIC_STORE_RELAXED(p, v) do { *(p) = (v); } while (0)
#define ATOMIC_STORE_RELEASE(p, v) do { WriteMemoryBarrier(); *(p) = (v); } while (0)

// WriteMemoryBarrier(), ReadMemoryBarrier() and MaybeReadMemoryBarrier()
// are an implementation detail and must not be used in the rest of the code.

#if defined(__i386__)

static inline void WriteMemoryBarrier() {
  int x;
  __asm__ __volatile__("xchgl (%0),%0"  // The lock prefix is implicit for xchg.
                       :: "r" (&x));
}

#elif defined(__x86_64__)

// 64-bit implementations of memory barrier can be simpler, because
// "sfence" is guaranteed to exist.
static inline void WriteMemoryBarrier() {
  __asm__ __volatile__("sfence" : : : "memory");
}

#elif defined(__ppc__)

static inline void WriteMemoryBarrier() {
  __asm__ __volatile__("eieio" : : : "memory");
}

#elif defined(__alpha__)

static inline void WriteMemoryBarrier() {
  __asm__ __volatile__("wmb" : : : "memory");
}

#elif defined(__aarch64__)

static inline void WriteMemoryBarrier() {
  __asm__ __volatile__("dmb st" : : : "memory");
}

#else

#include "util/mutex.h"

static inline void WriteMemoryBarrier() {
  // Slight overkill, but good enough:
  // any mutex implementation must have
  // a read barrier after the lock operation and
  // a write barrier before the unlock operation.
  //
  // It may be worthwhile to write architecture-specific
  // barriers for the common platforms, as above, but
  // this is a correct fallback.
  re2::Mutex mu;
  re2::MutexLock l(&mu);
}

/*
#error Need WriteMemoryBarrier for architecture.

// Windows
inline void WriteMemoryBarrier() {
  LONG x;
  ::InterlockedExchange(&x, 0);
}
*/

#endif

// Alpha has very weak memory ordering. If relying on WriteBarriers, one must
// use read barriers for the readers too.
#if defined(__alpha__)

static inline void MaybeReadMemoryBarrier() {
  __asm__ __volatile__("mb" : : : "memory");
}

#else

static inline void MaybeReadMemoryBarrier() {}

#endif  // __alpha__

// Read barrier for various targets.

#if defined(__aarch64__)

static inline void ReadMemoryBarrier() {
  __asm__ __volatile__("dmb ld" : : : "memory");
}

#elif defined(__alpha__)

static inline void ReadMemoryBarrier() {
  __asm__ __volatile__("mb" : : : "memory");
}

#else

static inline void ReadMemoryBarrier() {}

#endif

#endif  // old compiler

#ifndef NO_THREAD_SAFETY_ANALYSIS
#define NO_THREAD_SAFETY_ANALYSIS
#endif

#endif  // RE2_UTIL_ATOMICOPS_H__
