#pragma once

#include "defaults.h"

using TAtomicBase = intptr_t;
using TAtomic = volatile TAtomicBase;

#if defined(__GNUC__)
#include "atomic_gcc.h"
#elif defined(_MSC_VER)
#include "atomic_win.h"
#else
#error unsupported platform
#endif

#if !defined(ATOMIC_COMPILER_BARRIER)
#define ATOMIC_COMPILER_BARRIER()
#endif

static inline TAtomicBase AtomicSub(TAtomic& a, TAtomicBase v) {
    return AtomicAdd(a, -v);
}

static inline TAtomicBase AtomicGetAndSub(TAtomic& a, TAtomicBase v) {
    return AtomicGetAndAdd(a, -v);
}

#if defined(USE_GENERIC_SETGET)
static inline TAtomicBase AtomicGet(const TAtomic& a) {
    return a;
}

static inline void AtomicSet(TAtomic& a, TAtomicBase v) {
    a = v;
}
#endif

static inline bool AtomicTryLock(TAtomic* a) {
    return AtomicCas(a, 1, 0);
}

static inline bool AtomicTryAndTryLock(TAtomic* a) {
    return (AtomicGet(*a) == 0) && AtomicTryLock(a);
}

static inline void AtomicUnlock(TAtomic* a) {
    ATOMIC_COMPILER_BARRIER();
    AtomicSet(*a, 0);
}

#include "atomic_ops.h"
