#pragma once

#define ATOMIC_COMPILER_BARRIER() __asm__ __volatile__("" \
                                                       :  \
                                                       :  \
                                                       : "memory")

static inline TAtomicBase AtomicGet(const TAtomic& a) {
    TAtomicBase tmp;
#if defined(_arm64_)
    __asm__ __volatile__(
        "ldar %x[value], %[ptr]  \n\t"
        : [value] "=r"(tmp)
        : [ptr] "Q"(a)
        : "memory");
#else
    __atomic_load(&a, &tmp, __ATOMIC_ACQUIRE);
#endif
    return tmp;
}

static inline void AtomicSet(TAtomic& a, TAtomicBase v) {
#if defined(_arm64_)
    __asm__ __volatile__(
        "stlr %x[value], %[ptr]  \n\t"
        : [ptr] "=Q"(a)
        : [value] "r"(v)
        : "memory");
#else
    __atomic_store(&a, &v, __ATOMIC_RELEASE);
#endif
}

static inline intptr_t AtomicIncrement(TAtomic& p) {
    return __atomic_add_fetch(&p, 1, __ATOMIC_SEQ_CST);
}

static inline intptr_t AtomicGetAndIncrement(TAtomic& p) {
    return __atomic_fetch_add(&p, 1, __ATOMIC_SEQ_CST);
}

static inline intptr_t AtomicDecrement(TAtomic& p) {
    return __atomic_sub_fetch(&p, 1, __ATOMIC_SEQ_CST);
}

static inline intptr_t AtomicGetAndDecrement(TAtomic& p) {
    return __atomic_fetch_sub(&p, 1, __ATOMIC_SEQ_CST);
}

static inline intptr_t AtomicAdd(TAtomic& p, intptr_t v) {
    return __atomic_add_fetch(&p, v, __ATOMIC_SEQ_CST);
}

static inline intptr_t AtomicGetAndAdd(TAtomic& p, intptr_t v) {
    return __atomic_fetch_add(&p, v, __ATOMIC_SEQ_CST);
}

static inline intptr_t AtomicSwap(TAtomic* p, intptr_t v) {
    (void)p; // disable strange 'parameter set but not used' warning on gcc
    intptr_t ret;
    __atomic_exchange(p, &v, &ret, __ATOMIC_SEQ_CST);
    return ret;
}

static inline bool AtomicCas(TAtomic* a, intptr_t exchange, intptr_t compare) {
    (void)a; // disable strange 'parameter set but not used' warning on gcc
    return __atomic_compare_exchange(a, &compare, &exchange, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
}

static inline intptr_t AtomicGetAndCas(TAtomic* a, intptr_t exchange, intptr_t compare) {
    (void)a; // disable strange 'parameter set but not used' warning on gcc
    __atomic_compare_exchange(a, &compare, &exchange, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
    return compare;
}

static inline intptr_t AtomicOr(TAtomic& a, intptr_t b) {
    return __atomic_or_fetch(&a, b, __ATOMIC_SEQ_CST);
}

static inline intptr_t AtomicXor(TAtomic& a, intptr_t b) {
    return __atomic_xor_fetch(&a, b, __ATOMIC_SEQ_CST);
}

static inline intptr_t AtomicAnd(TAtomic& a, intptr_t b) {
    return __atomic_and_fetch(&a, b, __ATOMIC_SEQ_CST);
}

static inline void AtomicBarrier() {
    __sync_synchronize();
}
