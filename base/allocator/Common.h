#pragma once

/// Basic macros and utilities shared by the whole allocator.
/// jemalloc: `jemalloc_internal_macros.h`, `util.h`, `bit_util.h`, `jemalloc_internal_types.h`, `assert.h`.

#include <allocator/Config.h>

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <sys/types.h>

#define ALLOCATOR_ALWAYS_INLINE inline __attribute__((always_inline))
#define ALLOCATOR_NOINLINE __attribute__((noinline))
#define ALLOCATOR_COLD __attribute__((cold))
#define ALLOCATOR_LIKELY(x) __builtin_expect(!!(x), 1)
#define ALLOCATOR_UNLIKELY(x) __builtin_expect(!!(x), 0)
#define ALLOCATOR_FORMAT_PRINTF(format_string, args) __attribute__((format(printf, format_string, args)))
#define ALLOCATOR_UNREACHABLE() __builtin_unreachable()
/// `util_assume`: lets the compiler assume the condition (e.g. the range of a bit index), the expression must not have
/// side effects.
#define ALLOCATOR_ASSUME(e) __builtin_assume(e)

namespace jemalloc
{

[[noreturn]] void assertionFailed(const char * file, int line, const char * function, const char * expression);

}

/// Internal consistency checks. jemalloc never enables `config_debug` in ClickHouse builds, so these are only for
/// development (`ALLOCATOR_DEBUG`); the expression must not have side effects.
#define ALLOCATOR_ASSERT(e) \
    do \
    { \
        if constexpr (::jemalloc::config::debug) \
        { \
            if (ALLOCATOR_UNLIKELY(!(e))) \
                ::jemalloc::assertionFailed(__FILE__, __LINE__, __func__, #e); \
        } \
    } while (false)

#define ALLOCATOR_NOT_REACHED() \
    do \
    { \
        if constexpr (::jemalloc::config::debug) \
            ::jemalloc::assertionFailed(__FILE__, __LINE__, __func__, "unreachable code"); \
        ALLOCATOR_UNREACHABLE(); \
    } while (false)

namespace jemalloc
{

/// Size class index type (`SizeClassIdx`) and page size class index type (`PageSizeClassIdx`).
using SizeClassIdx = unsigned;
using PageSizeClassIdx = unsigned;

/// The `zero` flag passed in `MALLOCX_ZERO`.
inline constexpr int MALLOCX_ZERO_FLAG = 0x40;

/// MALLOCX_* flag decoding (jemalloc_internal_types.h).
inline constexpr int MALLOCX_LOG2_ALIGN_MASK = 0x3f;
inline constexpr int MALLOCX_THREAD_CACHE_MASK = (((1 << MALLOCX_THREAD_CACHE_BITS) - 1) << 8);
inline constexpr int MALLOCX_ARENA_MASK = (((1 << MALLOCX_ARENA_BITS) - 1) << 20);
inline constexpr int MALLOCX_THREAD_CACHE_NONE_FLAG = ((-1) + 2) << 8;

constexpr size_t alignmentFromFlags(int flags)
{
    return (size_t(1) << (flags & MALLOCX_LOG2_ALIGN_MASK)) & ~size_t(1);
}

constexpr size_t alignmentFromFlagsSpecified(int flags)
{
    return size_t(1) << (flags & MALLOCX_LOG2_ALIGN_MASK);
}

constexpr bool zeroFromFlags(int flags)
{
    return (flags & MALLOCX_ZERO_FLAG) != 0;
}

constexpr unsigned threadCacheFromFlags(int flags)
{
    return ((unsigned(flags) & MALLOCX_THREAD_CACHE_MASK) >> 8) - 2;
}

constexpr unsigned arenaFromFlags(int flags)
{
    return ((unsigned(flags) & MALLOCX_ARENA_MASK) >> 20) - 1;
}

/// --- Alignment helpers ---------------------------------------------------------------------------------------------

/// ALIGNMENT_ADDR2BASE / ALIGNMENT_ADDR2OFFSET / ALIGNMENT_CEILING.
constexpr uintptr_t alignmentAddrToBase(uintptr_t a, size_t alignment)
{
    return a & ~(alignment - 1);
}

constexpr size_t alignmentAddrToOffset(uintptr_t a, size_t alignment)
{
    return a & (alignment - 1);
}

constexpr size_t alignmentCeiling(size_t s, size_t alignment)
{
    return (s + (alignment - 1)) & ~(alignment - 1);
}

inline void * alignmentAddrToBase(const void * a, size_t alignment)
{
    return reinterpret_cast<void *>(alignmentAddrToBase(reinterpret_cast<uintptr_t>(a), alignment));
}

/// PAGE_ADDR2BASE / PAGE_CEILING / PAGE_FLOOR.
constexpr size_t pageCeiling(size_t s)
{
    return (s + PAGE_MASK) & ~PAGE_MASK;
}

constexpr size_t pageFloor(size_t s)
{
    return s & ~PAGE_MASK;
}

inline void * pageAddrToBase(const void * a)
{
    return reinterpret_cast<void *>(reinterpret_cast<uintptr_t>(a) & ~PAGE_MASK);
}

/// HUGEPAGE_ADDR2BASE / HUGEPAGE_CEILING.
constexpr size_t hugePageCeiling(size_t s)
{
    return (s + HUGE_PAGE_MASK) & ~HUGE_PAGE_MASK;
}

inline void * hugePageAddrToBase(const void * a)
{
    return reinterpret_cast<void *>(reinterpret_cast<uintptr_t>(a) & ~HUGE_PAGE_MASK);
}

/// QUANTUM_CEILING / CACHELINE_CEILING.
constexpr size_t quantumCeiling(size_t s)
{
    return (s + QUANTUM_MASK) & ~QUANTUM_MASK;
}

constexpr size_t cacheLineCeiling(size_t s)
{
    return (s + CACHE_LINE_MASK) & ~CACHE_LINE_MASK;
}

/// --- Bit utilities (bit_util.h) ------------------------------------------------------------------------------------

/// Index of the lowest set bit; `x` must be non-zero. (`ffs_u`, `ffs_zu`, `ffs_u64`, ...)
template <typename T>
constexpr unsigned findFirstSet(T x)
{
    ALLOCATOR_ASSERT(x != 0);
    ALLOCATOR_ASSUME(x != 0);
    return static_cast<unsigned>(std::countr_zero(x));
}

/// Index of the highest set bit; `x` must be non-zero. (`fls_*`)
template <typename T>
constexpr unsigned findLastSet(T x)
{
    ALLOCATOR_ASSERT(x != 0);
    ALLOCATOR_ASSUME(x != 0);
    return static_cast<unsigned>(sizeof(T) * 8 - 1 - std::countl_zero(x));
}

/// Number of set bits (`popcount_*`).
template <typename T>
constexpr unsigned popcount(T x)
{
    return static_cast<unsigned>(std::popcount(x));
}

/// Return the lowest set bit index and clear it (`cfs_lu`); `*x` must be non-zero.
template <typename T>
constexpr unsigned clearFirstSet(T & x)
{
    unsigned bit = findFirstSet(x);
    x ^= T(1) << bit;
    return bit;
}

/// floor(log2(x)), x must be non-zero (`lg_floor`).
constexpr unsigned log2Floor(size_t x)
{
    return findLastSet(x);
}

/// ceil(log2(x)), x must be non-zero (`log2_ceil`).
constexpr unsigned log2Ceil(size_t x)
{
    return log2Floor(x) + ((x & (x - 1)) == 0 ? 0 : 1);
}

/// The smallest power of two >= x; 0 for 0 (`pow2_ceil_u64`, `pow2_ceil_zu`).
/// jemalloc returns x for x <= 1.
constexpr uint64_t pow2Ceil(uint64_t x)
{
    if (x <= 1)
        return x;
    return uint64_t(1) << (64 - std::countl_zero(x - 1));
}

/// Compile-time versions used for array sizes (LG_FLOOR / LG_CEIL macros).
consteval unsigned log2FloorConst(uint64_t x)
{
    unsigned r = 0;
    while (x >>= 1)
        ++r;
    return r;
}

consteval unsigned log2CeilConst(uint64_t x)
{
    return log2FloorConst(x) + ((x & (x - 1)) == 0 ? 0 : 1);
}

/// --- Misc ----------------------------------------------------------------------------------------------------------

template <typename T>
constexpr T minOf(T a, T b)
{
    return a < b ? a : b;
}

template <typename T>
constexpr T maxOf(T a, T b)
{
    return a > b ? a : b;
}

/// Prevent the compiler from optimizing away a value or reordering memory accesses around it.
ALLOCATOR_ALWAYS_INLINE void compilerBarrier()
{
    __asm__ __volatile__("" ::: "memory");
}

}
