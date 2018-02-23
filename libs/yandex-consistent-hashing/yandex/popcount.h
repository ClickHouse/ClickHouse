#pragma once

#include <stdint.h>
#include <cstddef>
#include <type_traits>
using std::size_t;

#include "bitops.h"

#if defined(_MSC_VER)
#include <intrin.h>
#endif

#ifdef __SSE2__
constexpr bool HavePOPCNTInstr = true;
#else
constexpr bool HavePOPCNTInstr = false;
#pragma GCC warning "SSE2 is not detected, PopCount function will be too slow"
#endif

static inline uint32_t PopCountImpl(uint8_t n) {
    extern uint8_t const* PopCountLUT8;
    return PopCountLUT8[n];
}

static inline uint32_t PopCountImpl(uint16_t n) {
#if defined(_MSC_VER)
    return __popcnt16(n);
#else
    extern uint8_t const* PopCountLUT16;
    return PopCountLUT16[n];
#endif
}

static inline uint32_t PopCountImpl(uint32_t n) {
#if defined(_MSC_VER)
    return __popcnt(n);
#else
#if defined(__x86_64__)

    if (HavePOPCNTInstr) {
        uint32_t r;

        __asm__("popcnt %1, %0;"
                : "=r"(r)
                : "r"(n)
                :);

        return r;
    }
#endif

    return PopCountImpl((uint16_t)LO_16(n)) + PopCountImpl((uint16_t)HI_16(n));
#endif
}

static inline uint32_t PopCountImpl(uint64_t n) {
#if defined(_MSC_VER) && !defined(_i386_)
    return __popcnt64(n);
#else
#if defined(__x86_64__)

    if (HavePOPCNTInstr) {
        uint64_t r;

        __asm__("popcnt %1, %0;"
                : "=r"(r)
                : "r"(n)
                :);

        return r;
    }
#endif

    return PopCountImpl((uint32_t)LO_32(n)) + PopCountImpl((uint32_t)HI_32(n));
#endif
}

template <class T>
static inline uint32_t PopCount(T n) {
    using TCvt = std::make_unsigned_t<std::decay_t<T>>;

    return PopCountImpl(static_cast<TCvt>(n));
}
