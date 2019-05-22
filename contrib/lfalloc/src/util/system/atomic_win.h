#pragma once

#include <intrin.h>

#define USE_GENERIC_SETGET

#if defined(_i386_)

#pragma intrinsic(_InterlockedIncrement)
#pragma intrinsic(_InterlockedDecrement)
#pragma intrinsic(_InterlockedExchangeAdd)
#pragma intrinsic(_InterlockedExchange)
#pragma intrinsic(_InterlockedCompareExchange)

static inline intptr_t AtomicIncrement(TAtomic& a) {
    return _InterlockedIncrement((volatile long*)&a);
}

static inline intptr_t AtomicGetAndIncrement(TAtomic& a) {
    return _InterlockedIncrement((volatile long*)&a) - 1;
}

static inline intptr_t AtomicDecrement(TAtomic& a) {
    return _InterlockedDecrement((volatile long*)&a);
}

static inline intptr_t AtomicGetAndDecrement(TAtomic& a) {
    return _InterlockedDecrement((volatile long*)&a) + 1;
}

static inline intptr_t AtomicAdd(TAtomic& a, intptr_t b) {
    return _InterlockedExchangeAdd((volatile long*)&a, b) + b;
}

static inline intptr_t AtomicGetAndAdd(TAtomic& a, intptr_t b) {
    return _InterlockedExchangeAdd((volatile long*)&a, b);
}

static inline intptr_t AtomicSwap(TAtomic* a, intptr_t b) {
    return _InterlockedExchange((volatile long*)a, b);
}

static inline bool AtomicCas(TAtomic* a, intptr_t exchange, intptr_t compare) {
    return _InterlockedCompareExchange((volatile long*)a, exchange, compare) == compare;
}

static inline intptr_t AtomicGetAndCas(TAtomic* a, intptr_t exchange, intptr_t compare) {
    return _InterlockedCompareExchange((volatile long*)a, exchange, compare);
}

#else // _x86_64_

#pragma intrinsic(_InterlockedIncrement64)
#pragma intrinsic(_InterlockedDecrement64)
#pragma intrinsic(_InterlockedExchangeAdd64)
#pragma intrinsic(_InterlockedExchange64)
#pragma intrinsic(_InterlockedCompareExchange64)

static inline intptr_t AtomicIncrement(TAtomic& a) {
    return _InterlockedIncrement64((volatile __int64*)&a);
}

static inline intptr_t AtomicGetAndIncrement(TAtomic& a) {
    return _InterlockedIncrement64((volatile __int64*)&a) - 1;
}

static inline intptr_t AtomicDecrement(TAtomic& a) {
    return _InterlockedDecrement64((volatile __int64*)&a);
}

static inline intptr_t AtomicGetAndDecrement(TAtomic& a) {
    return _InterlockedDecrement64((volatile __int64*)&a) + 1;
}

static inline intptr_t AtomicAdd(TAtomic& a, intptr_t b) {
    return _InterlockedExchangeAdd64((volatile __int64*)&a, b) + b;
}

static inline intptr_t AtomicGetAndAdd(TAtomic& a, intptr_t b) {
    return _InterlockedExchangeAdd64((volatile __int64*)&a, b);
}

static inline intptr_t AtomicSwap(TAtomic* a, intptr_t b) {
    return _InterlockedExchange64((volatile __int64*)a, b);
}

static inline bool AtomicCas(TAtomic* a, intptr_t exchange, intptr_t compare) {
    return _InterlockedCompareExchange64((volatile __int64*)a, exchange, compare) == compare;
}

static inline intptr_t AtomicGetAndCas(TAtomic* a, intptr_t exchange, intptr_t compare) {
    return _InterlockedCompareExchange64((volatile __int64*)a, exchange, compare);
}

static inline intptr_t AtomicOr(TAtomic& a, intptr_t b) {
    return _InterlockedOr64(&a, b) | b;
}

static inline intptr_t AtomicAnd(TAtomic& a, intptr_t b) {
    return _InterlockedAnd64(&a, b) & b;
}

static inline intptr_t AtomicXor(TAtomic& a, intptr_t b) {
    return _InterlockedXor64(&a, b) ^ b;
}

#endif // _x86_

//TODO
static inline void AtomicBarrier() {
    TAtomic val = 0;

    AtomicSwap(&val, 0);
}
