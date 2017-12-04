/* libdivide.h
   Copyright 2010 ridiculous_fish
*/
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"

#if defined(_WIN32) || defined(WIN32)
#define LIBDIVIDE_WINDOWS 1
#endif

#if defined(_MSC_VER)
#define LIBDIVIDE_VC 1
#endif

#ifdef __cplusplus
#include <cstdlib>
#include <cstdio>
#include <cassert>
#else
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#endif

#if ! LIBDIVIDE_HAS_STDINT_TYPES && (! LIBDIVIDE_VC || _MSC_VER >= 1600)
/* Only Visual C++ 2010 and later include stdint.h */
#include <stdint.h>
#define LIBDIVIDE_HAS_STDINT_TYPES 1
#endif

#if ! LIBDIVIDE_HAS_STDINT_TYPES
typedef __int32 int32_t;
typedef unsigned __int32 uint32_t;
typedef __int64 int64_t;
typedef unsigned __int64 uint64_t;
typedef __int8 int8_t;
typedef unsigned __int8 uint8_t;
#endif

#if LIBDIVIDE_USE_SSE2
    #include <emmintrin.h>
#endif

#if LIBDIVIDE_VC
    #include <intrin.h>
#endif

#ifndef __has_builtin
#define __has_builtin(x) 0  // Compatibility with non-clang compilers.
#endif

#ifdef __ICC
#define HAS_INT128_T 0
#else
#define HAS_INT128_T __LP64__
#endif

#if defined(__x86_64__) || defined(_WIN64) || defined(_M_64)
#define LIBDIVIDE_IS_X86_64 1
#endif

#if defined(__i386__)
#define LIBDIVIDE_IS_i386 1
#endif

#if __GNUC__ || __clang__
#define LIBDIVIDE_GCC_STYLE_ASM 1
#endif


/* libdivide may use the pmuldq (vector signed 32x32->64 mult instruction) which is in SSE 4.1.  However, signed multiplication can be emulated efficiently with unsigned multiplication, and SSE 4.1 is currently rare, so it is OK to not turn this on */
#ifdef LIBDIVIDE_USE_SSE4_1
#include <smmintrin.h>
#endif

#ifdef __cplusplus
/* We place libdivide within the libdivide namespace, and that goes in an anonymous namespace so that the functions are only visible to files that #include this header and don't get external linkage.  At least that's the theory. */
namespace {
namespace libdivide {
#endif

/* Explanation of "more" field: bit 6 is whether to use shift path.  If we are using the shift path, bit 7 is whether the divisor is negative in the signed case; in the unsigned case it is 0.   Bits 0-4 is shift value (for shift path or mult path).  In 32 bit case, bit 5 is always 0.  We use bit 7 as the "negative divisor indicator" so that we can use sign extension to efficiently go to a full-width -1.


u32: [0-4] shift value
     [5] ignored
     [6] add indicator
     [7] shift path

s32: [0-4] shift value
     [5] shift path
     [6] add indicator
     [7] indicates negative divisor

u64: [0-5] shift value
     [6] add indicator
     [7] shift path

s64: [0-5] shift value
     [6] add indicator
     [7] indicates negative divisor
     magic number of 0 indicates shift path (we ran out of bits!)
*/

enum {
    LIBDIVIDE_32_SHIFT_MASK = 0x1F,
    LIBDIVIDE_64_SHIFT_MASK = 0x3F,
    LIBDIVIDE_ADD_MARKER = 0x40,
    LIBDIVIDE_U32_SHIFT_PATH = 0x80,
    LIBDIVIDE_U64_SHIFT_PATH = 0x80,
    LIBDIVIDE_S32_SHIFT_PATH = 0x20,
    LIBDIVIDE_NEGATIVE_DIVISOR = 0x80
};


struct libdivide_u32_t {
    uint32_t magic;
    uint8_t more;
};

struct libdivide_s32_t {
    int32_t magic;
    uint8_t more;
};

struct libdivide_u64_t {
    uint64_t magic;
    uint8_t more;
};

struct libdivide_s64_t {
    int64_t magic;
    uint8_t more;
};



#ifndef LIBDIVIDE_API
    #ifdef __cplusplus
        /* In C++, we don't want our public functions to be static, because they are arguments to templates and static functions can't do that.  They get internal linkage through virtue of the anonymous namespace.  In C, they should be static. */
        #define LIBDIVIDE_API
    #else
        #define LIBDIVIDE_API static
    #endif
#endif

#ifdef __APPLE__
typedef signed long Int64;
typedef unsigned long UInt64;
#endif

LIBDIVIDE_API struct libdivide_s32_t libdivide_s32_gen(int32_t y);
LIBDIVIDE_API struct libdivide_u32_t libdivide_u32_gen(uint32_t y);
LIBDIVIDE_API struct libdivide_s64_t libdivide_s64_gen(int64_t y);
LIBDIVIDE_API struct libdivide_u64_t libdivide_u64_gen(uint64_t y);
#if defined(__APPLE__) && defined(__cplusplus)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"
LIBDIVIDE_API struct libdivide_s64_t libdivide_s64_gen(Int64 y) { return libdivide_s64_gen(int64_t(y)); };
LIBDIVIDE_API struct libdivide_u64_t libdivide_u64_gen(UInt64 y) { return libdivide_u64_gen(uint64_t(y)); };
#pragma GCC diagnostic pop
#endif

LIBDIVIDE_API int32_t  libdivide_s32_do(int32_t numer, const struct libdivide_s32_t *denom);
LIBDIVIDE_API uint32_t libdivide_u32_do(uint32_t numer, const struct libdivide_u32_t *denom);
LIBDIVIDE_API int64_t  libdivide_s64_do(int64_t numer, const struct libdivide_s64_t *denom);
LIBDIVIDE_API uint64_t libdivide_u64_do(uint64_t y, const struct libdivide_u64_t *denom);
#if defined(__APPLE__) && defined(__cplusplus)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"
LIBDIVIDE_API Int64  libdivide_s64_do(Int64 numer, const struct libdivide_s64_t *denom) { return Int64(libdivide_s64_do(int64_t(numer), denom)); };
LIBDIVIDE_API UInt64 libdivide_u64_do(UInt64 y, const struct libdivide_u64_t *denom) { return UInt64(libdivide_u64_do(uint64_t(y), denom)); };
#pragma GCC diagnostic pop
#endif

LIBDIVIDE_API int libdivide_u32_get_algorithm(const struct libdivide_u32_t *denom);
LIBDIVIDE_API uint32_t libdivide_u32_do_alg0(uint32_t numer, const struct libdivide_u32_t *denom);
LIBDIVIDE_API uint32_t libdivide_u32_do_alg1(uint32_t numer, const struct libdivide_u32_t *denom);
LIBDIVIDE_API uint32_t libdivide_u32_do_alg2(uint32_t numer, const struct libdivide_u32_t *denom);

LIBDIVIDE_API int libdivide_u64_get_algorithm(const struct libdivide_u64_t *denom);
LIBDIVIDE_API uint64_t libdivide_u64_do_alg0(uint64_t numer, const struct libdivide_u64_t *denom);
LIBDIVIDE_API uint64_t libdivide_u64_do_alg1(uint64_t numer, const struct libdivide_u64_t *denom);
LIBDIVIDE_API uint64_t libdivide_u64_do_alg2(uint64_t numer, const struct libdivide_u64_t *denom);
#if defined(__APPLE__) && defined(__cplusplus)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"
LIBDIVIDE_API UInt64 libdivide_u64_do_alg0(UInt64 numer, const struct libdivide_u64_t *denom) { return UInt64(libdivide_u64_do_alg0(uint64_t(numer), denom)); }
LIBDIVIDE_API UInt64 libdivide_u64_do_alg1(UInt64 numer, const struct libdivide_u64_t *denom) { return UInt64(libdivide_u64_do_alg1(uint64_t(numer), denom)); }
LIBDIVIDE_API UInt64 libdivide_u64_do_alg2(UInt64 numer, const struct libdivide_u64_t *denom) { return UInt64(libdivide_u64_do_alg2(uint64_t(numer), denom)); }
#pragma GCC diagnostic pop
#endif

LIBDIVIDE_API int libdivide_s32_get_algorithm(const struct libdivide_s32_t *denom);
LIBDIVIDE_API int32_t libdivide_s32_do_alg0(int32_t numer, const struct libdivide_s32_t *denom);
LIBDIVIDE_API int32_t libdivide_s32_do_alg1(int32_t numer, const struct libdivide_s32_t *denom);
LIBDIVIDE_API int32_t libdivide_s32_do_alg2(int32_t numer, const struct libdivide_s32_t *denom);
LIBDIVIDE_API int32_t libdivide_s32_do_alg3(int32_t numer, const struct libdivide_s32_t *denom);
LIBDIVIDE_API int32_t libdivide_s32_do_alg4(int32_t numer, const struct libdivide_s32_t *denom);

LIBDIVIDE_API int libdivide_s64_get_algorithm(const struct libdivide_s64_t *denom);
LIBDIVIDE_API int64_t libdivide_s64_do_alg0(int64_t numer, const struct libdivide_s64_t *denom);
LIBDIVIDE_API int64_t libdivide_s64_do_alg1(int64_t numer, const struct libdivide_s64_t *denom);
LIBDIVIDE_API int64_t libdivide_s64_do_alg2(int64_t numer, const struct libdivide_s64_t *denom);
LIBDIVIDE_API int64_t libdivide_s64_do_alg3(int64_t numer, const struct libdivide_s64_t *denom);
LIBDIVIDE_API int64_t libdivide_s64_do_alg4(int64_t numer, const struct libdivide_s64_t *denom);
#if defined(__APPLE__) && defined(__cplusplus)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"
LIBDIVIDE_API Int64 libdivide_s64_do_alg0(Int64 numer, const struct libdivide_s64_t *denom) { return Int64(libdivide_s64_do_alg0(int64_t(numer), denom)); }
LIBDIVIDE_API Int64 libdivide_s64_do_alg1(Int64 numer, const struct libdivide_s64_t *denom) { return Int64(libdivide_s64_do_alg1(int64_t(numer), denom)); }
LIBDIVIDE_API Int64 libdivide_s64_do_alg2(Int64 numer, const struct libdivide_s64_t *denom) { return Int64(libdivide_s64_do_alg2(int64_t(numer), denom)); }
LIBDIVIDE_API Int64 libdivide_s64_do_alg3(Int64 numer, const struct libdivide_s64_t *denom) { return Int64(libdivide_s64_do_alg3(int64_t(numer), denom)); }
LIBDIVIDE_API Int64 libdivide_s64_do_alg4(Int64 numer, const struct libdivide_s64_t *denom) { return Int64(libdivide_s64_do_alg4(int64_t(numer), denom)); }
#pragma GCC diagnostic pop
#endif


#if LIBDIVIDE_USE_SSE2
LIBDIVIDE_API __m128i libdivide_u32_do_vector(__m128i numers, const struct libdivide_u32_t * denom);
LIBDIVIDE_API __m128i libdivide_s32_do_vector(__m128i numers, const struct libdivide_s32_t * denom);
LIBDIVIDE_API __m128i libdivide_u64_do_vector(__m128i numers, const struct libdivide_u64_t * denom);
LIBDIVIDE_API __m128i libdivide_s64_do_vector(__m128i numers, const struct libdivide_s64_t * denom);

LIBDIVIDE_API __m128i libdivide_u32_do_vector_alg0(__m128i numers, const struct libdivide_u32_t * denom);
LIBDIVIDE_API __m128i libdivide_u32_do_vector_alg1(__m128i numers, const struct libdivide_u32_t * denom);
LIBDIVIDE_API __m128i libdivide_u32_do_vector_alg2(__m128i numers, const struct libdivide_u32_t * denom);

LIBDIVIDE_API __m128i libdivide_s32_do_vector_alg0(__m128i numers, const struct libdivide_s32_t * denom);
LIBDIVIDE_API __m128i libdivide_s32_do_vector_alg1(__m128i numers, const struct libdivide_s32_t * denom);
LIBDIVIDE_API __m128i libdivide_s32_do_vector_alg2(__m128i numers, const struct libdivide_s32_t * denom);
LIBDIVIDE_API __m128i libdivide_s32_do_vector_alg3(__m128i numers, const struct libdivide_s32_t * denom);
LIBDIVIDE_API __m128i libdivide_s32_do_vector_alg4(__m128i numers, const struct libdivide_s32_t * denom);

LIBDIVIDE_API __m128i libdivide_u64_do_vector_alg0(__m128i numers, const struct libdivide_u64_t * denom);
LIBDIVIDE_API __m128i libdivide_u64_do_vector_alg1(__m128i numers, const struct libdivide_u64_t * denom);
LIBDIVIDE_API __m128i libdivide_u64_do_vector_alg2(__m128i numers, const struct libdivide_u64_t * denom);

LIBDIVIDE_API __m128i libdivide_s64_do_vector_alg0(__m128i numers, const struct libdivide_s64_t * denom);
LIBDIVIDE_API __m128i libdivide_s64_do_vector_alg1(__m128i numers, const struct libdivide_s64_t * denom);
LIBDIVIDE_API __m128i libdivide_s64_do_vector_alg2(__m128i numers, const struct libdivide_s64_t * denom);
LIBDIVIDE_API __m128i libdivide_s64_do_vector_alg3(__m128i numers, const struct libdivide_s64_t * denom);
LIBDIVIDE_API __m128i libdivide_s64_do_vector_alg4(__m128i numers, const struct libdivide_s64_t * denom);
#endif



//////// Internal Utility Functions

static inline uint32_t libdivide__mullhi_u32(uint32_t x, uint32_t y) {
    uint64_t xl = x, yl = y;
    uint64_t rl = xl * yl;
    return (uint32_t)(rl >> 32);
}

static uint64_t libdivide__mullhi_u64(uint64_t x, uint64_t y) {
#if HAS_INT128_T
    __uint128_t xl = x, yl = y;
    __uint128_t rl = xl * yl;
    return (uint64_t)(rl >> 64);
#else
    //full 128 bits are x0 * y0 + (x0 * y1 << 32) + (x1 * y0 << 32) + (x1 * y1 << 64)
    const uint32_t mask = 0xFFFFFFFF;
    const uint32_t x0 = (uint32_t)(x & mask), x1 = (uint32_t)(x >> 32);
    const uint32_t y0 = (uint32_t)(y & mask), y1 = (uint32_t)(y >> 32);
    const uint32_t x0y0_hi = libdivide__mullhi_u32(x0, y0);
    const uint64_t x0y1 = x0 * (uint64_t)y1;
    const uint64_t x1y0 = x1 * (uint64_t)y0;
    const uint64_t x1y1 = x1 * (uint64_t)y1;

    uint64_t temp = x1y0 + x0y0_hi;
    uint64_t temp_lo = temp & mask, temp_hi = temp >> 32;
    return x1y1 + temp_hi + ((temp_lo + x0y1) >> 32);
#endif
}

static inline int64_t libdivide__mullhi_s64(int64_t x, int64_t y) {
#if HAS_INT128_T
    __int128_t xl = x, yl = y;
    __int128_t rl = xl * yl;
    return (int64_t)(rl >> 64);
#else
    //full 128 bits are x0 * y0 + (x0 * y1 << 32) + (x1 * y0 << 32) + (x1 * y1 << 64)
    const uint32_t mask = 0xFFFFFFFF;
    const uint32_t x0 = (uint32_t)(x & mask), y0 = (uint32_t)(y & mask);
    const int32_t x1 = (int32_t)(x >> 32), y1 = (int32_t)(y >> 32);
    const uint32_t x0y0_hi = libdivide__mullhi_u32(x0, y0);
    const int64_t t = x1*(int64_t)y0 + x0y0_hi;
    const int64_t w1 = x0*(int64_t)y1 + (t & mask);
    return x1*(int64_t)y1 + (t >> 32) + (w1 >> 32);
#endif
}

#if LIBDIVIDE_USE_SSE2

static inline __m128i libdivide__u64_to_m128(uint64_t x) {
#if LIBDIVIDE_VC && ! _WIN64
    //64 bit windows doesn't seem to have an implementation of any of these load intrinsics, and 32 bit Visual C++ crashes
    _declspec(align(16)) uint64_t temp[2] = {x, x};
    return _mm_load_si128((const __m128i*)temp);
#elif defined(__ICC)
    uint64_t __attribute__((aligned(16))) temp[2] = {x,x};
    return _mm_load_si128((const __m128i*)temp);
#elif __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wc++11-narrowing" // narrowing from uint64_t (aka 'unsigned long') to 'long long'
    // clang does not provide this intrinsic either
    return (__m128i){x, x};
#pragma clang diagnostic pop
#else
    // everyone else gets it right
    return _mm_set1_epi64x(x);
#endif
}

static inline __m128i libdivide_get_FFFFFFFF00000000(void) {
    //returns the same as _mm_set1_epi64(0xFFFFFFFF00000000ULL) without touching memory
    __m128i result = _mm_set1_epi8(-1); //optimizes to pcmpeqd on OS X
    return _mm_slli_epi64(result, 32);
}

static inline __m128i libdivide_get_00000000FFFFFFFF(void) {
    //returns the same as _mm_set1_epi64(0x00000000FFFFFFFFULL) without touching memory
    __m128i result = _mm_set1_epi8(-1); //optimizes to pcmpeqd on OS X
    result = _mm_srli_epi64(result, 32);
    return result;
}

#if __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wuninitialized"
#endif
static inline __m128i libdivide_get_0000FFFF(void) {
    //returns the same as _mm_set1_epi32(0x0000FFFFULL) without touching memory
    __m128i result; //we don't care what its contents are
    result = _mm_cmpeq_epi8(result, result); //all 1s
    result = _mm_srli_epi32(result, 16);
    return result;
}
#if __clang__
#pragma clang diagnostic pop
#endif

static inline __m128i libdivide_s64_signbits(__m128i v) {
    //we want to compute v >> 63, that is, _mm_srai_epi64(v, 63).  But there is no 64 bit shift right arithmetic instruction in SSE2.  So we have to fake it by first duplicating the high 32 bit values, and then using a 32 bit shift.  Another option would be to use _mm_srli_epi64(v, 63) and then subtract that from 0, but that approach appears to be substantially slower for unknown reasons
    __m128i hiBitsDuped = _mm_shuffle_epi32(v, _MM_SHUFFLE(3, 3, 1, 1));
    __m128i signBits = _mm_srai_epi32(hiBitsDuped, 31);
    return signBits;
}

/* Returns an __m128i whose low 32 bits are equal to amt and has zero elsewhere. */
static inline __m128i libdivide_u32_to_m128i(uint32_t amt) {
    return _mm_set_epi32(0, 0, 0, amt);
}

static inline __m128i libdivide_s64_shift_right_vector(__m128i v, int amt) {
    //implementation of _mm_sra_epi64.  Here we have two 64 bit values which are shifted right to logically become (64 - amt) values, and are then sign extended from a (64 - amt) bit number.
    const int b = 64 - amt;
    __m128i m = libdivide__u64_to_m128(1ULL << (b - 1));
    __m128i x = _mm_srl_epi64(v, libdivide_u32_to_m128i(amt));
    __m128i result = _mm_sub_epi64(_mm_xor_si128(x, m), m); //result = x^m - m
    return result;
}

/* Here, b is assumed to contain one 32 bit value repeated four times.  If it did not, the function would not work. */
static inline __m128i libdivide__mullhi_u32_flat_vector(__m128i a, __m128i b) {
    __m128i hi_product_0Z2Z = _mm_srli_epi64(_mm_mul_epu32(a, b), 32);
    __m128i a1X3X = _mm_srli_epi64(a, 32);
    __m128i hi_product_Z1Z3 = _mm_and_si128(_mm_mul_epu32(a1X3X, b), libdivide_get_FFFFFFFF00000000());
    return _mm_or_si128(hi_product_0Z2Z, hi_product_Z1Z3); // = hi_product_0123
}


/* Here, y is assumed to contain one 64 bit value repeated twice. */
static inline __m128i libdivide_mullhi_u64_flat_vector(__m128i x, __m128i y) {
    //full 128 bits are x0 * y0 + (x0 * y1 << 32) + (x1 * y0 << 32) + (x1 * y1 << 64)
    const __m128i mask = libdivide_get_00000000FFFFFFFF();
    const __m128i x0 = _mm_and_si128(x, mask), x1 = _mm_srli_epi64(x, 32); //x0 is low half of 2 64 bit values, x1 is high half in low slots
    const __m128i y0 = _mm_and_si128(y, mask), y1 = _mm_srli_epi64(y, 32);
    const __m128i x0y0_hi = _mm_srli_epi64(_mm_mul_epu32(x0, y0), 32); //x0 happens to have the low half of the two 64 bit values in 32 bit slots 0 and 2, so _mm_mul_epu32 computes their full product, and then we shift right by 32 to get just the high values
    const __m128i x0y1 = _mm_mul_epu32(x0, y1);
    const __m128i x1y0 = _mm_mul_epu32(x1, y0);
    const __m128i x1y1 = _mm_mul_epu32(x1, y1);

    const __m128i temp = _mm_add_epi64(x1y0, x0y0_hi);
    __m128i temp_lo = _mm_and_si128(temp, mask), temp_hi = _mm_srli_epi64(temp, 32);
    temp_lo = _mm_srli_epi64(_mm_add_epi64(temp_lo, x0y1), 32);
    temp_hi = _mm_add_epi64(x1y1, temp_hi);

    return _mm_add_epi64(temp_lo, temp_hi);
}

/* y is one 64 bit value repeated twice */
static inline __m128i libdivide_mullhi_s64_flat_vector(__m128i x, __m128i y) {
    __m128i p = libdivide_mullhi_u64_flat_vector(x, y);
    __m128i t1 = _mm_and_si128(libdivide_s64_signbits(x), y);
    p = _mm_sub_epi64(p, t1);
    __m128i t2 = _mm_and_si128(libdivide_s64_signbits(y), x);
    p = _mm_sub_epi64(p, t2);
    return p;
}

#ifdef LIBDIVIDE_USE_SSE4_1

/* b is one 32 bit value repeated four times. */
static inline __m128i libdivide_mullhi_s32_flat_vector(__m128i a, __m128i b) {
    __m128i hi_product_0Z2Z = _mm_srli_epi64(_mm_mul_epi32(a, b), 32);
    __m128i a1X3X = _mm_srli_epi64(a, 32);
    __m128i hi_product_Z1Z3 = _mm_and_si128(_mm_mul_epi32(a1X3X, b), libdivide_get_FFFFFFFF00000000());
    return _mm_or_si128(hi_product_0Z2Z, hi_product_Z1Z3); // = hi_product_0123
}

#else

/* SSE2 does not have a signed multiplication instruction, but we can convert unsigned to signed pretty efficiently.  Again, b is just a 32 bit value repeated four times. */
static inline __m128i libdivide_mullhi_s32_flat_vector(__m128i a, __m128i b) {
    __m128i p = libdivide__mullhi_u32_flat_vector(a, b);
    __m128i t1 = _mm_and_si128(_mm_srai_epi32(a, 31), b); //t1 = (a >> 31) & y, arithmetic shift
    __m128i t2 = _mm_and_si128(_mm_srai_epi32(b, 31), a);
    p = _mm_sub_epi32(p, t1);
    p = _mm_sub_epi32(p, t2);
    return p;
}
#endif
#endif

static inline int32_t libdivide__count_trailing_zeros32(uint32_t val) {
#if __GNUC__ || __has_builtin(__builtin_ctz)
    /* Fast way to count trailing zeros */
    return __builtin_ctz(val);
#elif LIBDIVIDE_VC
    unsigned long result;
    if (_BitScanForward(&result, val)) {
        return result;
    }
    return 0;
#else
    /* Dorky way to count trailing zeros.   Note that this hangs for val = 0! */
    int32_t result = 0;
    val = (val ^ (val - 1)) >> 1;  // Set v's trailing 0s to 1s and zero rest
    while (val) {
        val >>= 1;
        result++;
    }
    return result;
#endif
}

static inline int32_t libdivide__count_trailing_zeros64(uint64_t val) {
#if __LP64__ && (__GNUC__ || __has_builtin(__builtin_ctzll))
    /* Fast way to count trailing zeros.  Note that we disable this in 32 bit because gcc does something horrible - it calls through to a dynamically bound function. */
    return __builtin_ctzll(val);
#elif LIBDIVIDE_VC && _WIN64
    unsigned long result;
    if (_BitScanForward64(&result, val)) {
            return result;
    }
    return 0;
#else
    /* Pretty good way to count trailing zeros.  Note that this hangs for val = 0! */
    uint32_t lo = val & 0xFFFFFFFF;
    if (lo != 0) return libdivide__count_trailing_zeros32(lo);
    return 32 + libdivide__count_trailing_zeros32(val >> 32);
#endif
}

static inline int32_t libdivide__count_leading_zeros32(uint32_t val) {
#if __GNUC__ || __has_builtin(__builtin_clzll)
    /* Fast way to count leading zeros */
    return __builtin_clz(val);
#elif LIBDIVIDE_VC
    unsigned long result;
    if (_BitScanReverse(&result, val)) {
        return 31 - result;
    }
    return 0;
#else
    /* Dorky way to count leading zeros.  Note that this hangs for val = 0! */
    int32_t result = 0;
    while (! (val & (1U << 31))) {
        val <<= 1;
        result++;
    }
    return result;
#endif
}

static inline int32_t libdivide__count_leading_zeros64(uint64_t val) {
#if __GNUC__ || __has_builtin(__builtin_clzll)
    /* Fast way to count leading zeros */
    return __builtin_clzll(val);
#elif LIBDIVIDE_VC && _WIN64
    unsigned long result;
    if (_BitScanReverse64(&result, val)) {
        return 63 - result;
    }
    return 0;
#else
    /* Dorky way to count leading zeros.  Note that this hangs for val = 0! */
    int32_t result = 0;
    while (! (val & (1ULL << 63))) {
        val <<= 1;
        result++;
    }
    return result;
#endif
}

//libdivide_64_div_32_to_32: divides a 64 bit uint {u1, u0} by a 32 bit uint {v}.  The result must fit in 32 bits.  Returns the quotient directly and the remainder in *r
#if (LIBDIVIDE_IS_i386 || LIBDIVIDE_IS_X86_64) && LIBDIVIDE_GCC_STYLE_ASM
static uint32_t libdivide_64_div_32_to_32(uint32_t u1, uint32_t u0, uint32_t v, uint32_t *r) {
    uint32_t result;
    __asm__("divl %[v]"
            : "=a"(result), "=d"(*r)
            : [v] "r"(v), "a"(u0), "d"(u1)
            );
    return result;
}
#else
static uint32_t libdivide_64_div_32_to_32(uint32_t u1, uint32_t u0, uint32_t v, uint32_t *r) {
    uint64_t n = (((uint64_t)u1) << 32) | u0;
    uint32_t result = (uint32_t)(n / v);
    *r = (uint32_t)(n - result * (uint64_t)v);
    return result;
}
#endif

#if LIBDIVIDE_IS_X86_64 && LIBDIVIDE_GCC_STYLE_ASM
static uint64_t libdivide_128_div_64_to_64(uint64_t u1, uint64_t u0, uint64_t v, uint64_t *r) {
    //u0 -> rax
    //u1 -> rdx
    //divq
    uint64_t result;
    __asm__("divq %[v]"
            : "=a"(result), "=d"(*r)
            : [v] "r"(v), "a"(u0), "d"(u1)
            );
    return result;

}
#else

/* Code taken from Hacker's Delight, http://www.hackersdelight.org/HDcode/divlu.c .  License permits inclusion here per http://www.hackersdelight.org/permissions.htm
 */
static uint64_t libdivide_128_div_64_to_64(uint64_t u1, uint64_t u0, uint64_t v, uint64_t *r) {
    const uint64_t b = (1ULL << 32); // Number base (16 bits).
    uint64_t un1, un0,        // Norm. dividend LSD's.
    vn1, vn0,        // Norm. divisor digits.
    q1, q0,          // Quotient digits.
    un64, un21, un10,// Dividend digit pairs.
    rhat;            // A remainder.
    int s;                  // Shift amount for norm.

    if (u1 >= v) {            // If overflow, set rem.
        if (r != NULL)         // to an impossible value,
            *r = (uint64_t)(-1);    // and return the largest
        return (uint64_t)(-1);}    // possible quotient.

    /* count leading zeros */
    s = libdivide__count_leading_zeros64(v); // 0 <= s <= 63.
    if (s > 0) {
        v = v << s;           // Normalize divisor.
        un64 = (u1 << s) | ((u0 >> (64 - s)) & (-s >> 31));
        un10 = u0 << s;       // Shift dividend left.
    } else {
        // Avoid undefined behavior.
        un64 = u1 | u0;
        un10 = u0;
    }

    vn1 = v >> 32;            // Break divisor up into
    vn0 = v & 0xFFFFFFFF;     // two 32-bit digits.

    un1 = un10 >> 32;         // Break right half of
    un0 = un10 & 0xFFFFFFFF;  // dividend into two digits.

    q1 = un64/vn1;            // Compute the first
    rhat = un64 - q1*vn1;     // quotient digit, q1.
again1:
    if (q1 >= b || q1*vn0 > b*rhat + un1) {
        q1 = q1 - 1;
        rhat = rhat + vn1;
        if (rhat < b) goto again1;}

    un21 = un64*b + un1 - q1*v;  // Multiply and subtract.

    q0 = un21/vn1;            // Compute the second
    rhat = un21 - q0*vn1;     // quotient digit, q0.
again2:
    if (q0 >= b || q0*vn0 > b*rhat + un0) {
        q0 = q0 - 1;
        rhat = rhat + vn1;
        if (rhat < b) goto again2;}

    if (r != NULL)            // If remainder is wanted,
        *r = (un21*b + un0 - q0*v) >> s;     // return it.
    return q1*b + q0;
}
#endif

#if LIBDIVIDE_ASSERTIONS_ON
#define LIBDIVIDE_ASSERT(x) do { if (! (x)) { fprintf(stderr, "Assertion failure on line %ld: %s\n", (long)__LINE__, #x); exit(-1); } } while (0)
#else
#define LIBDIVIDE_ASSERT(x)
#endif

#ifndef LIBDIVIDE_HEADER_ONLY

////////// UINT32

struct libdivide_u32_t libdivide_u32_gen(uint32_t d) {
    struct libdivide_u32_t result;
    if ((d & (d - 1)) == 0) {
        result.magic = 0;
        result.more = libdivide__count_trailing_zeros32(d) | LIBDIVIDE_U32_SHIFT_PATH;
    }
    else {
        const uint32_t floor_log_2_d = 31 - libdivide__count_leading_zeros32(d);

        uint8_t more;
        uint32_t rem, proposed_m;
        proposed_m = libdivide_64_div_32_to_32(1U << floor_log_2_d, 0, d, &rem);

        LIBDIVIDE_ASSERT(rem > 0 && rem < d);
        const uint32_t e = d - rem;

	/* This power works if e < 2**floor_log_2_d. */
	if (e < (1U << floor_log_2_d)) {
            /* This power works */
            more = floor_log_2_d;
        }
        else {
            /* We have to use the general 33-bit algorithm.  We need to compute (2**power) / d. However, we already have (2**(power-1))/d and its remainder.  By doubling both, and then correcting the remainder, we can compute the larger division. */
            proposed_m += proposed_m; //don't care about overflow here - in fact, we expect it
            const uint32_t twice_rem = rem + rem;
            if (twice_rem >= d || twice_rem < rem) proposed_m += 1;
            more = floor_log_2_d | LIBDIVIDE_ADD_MARKER;
        }
        result.magic = 1 + proposed_m;
        result.more = more;
        //result.more's shift should in general be ceil_log_2_d.  But if we used the smaller power, we subtract one from the shift because we're using the smaller power. If we're using the larger power, we subtract one from the shift because it's taken care of by the add indicator.  So floor_log_2_d happens to be correct in both cases.

    }
    return result;
}

uint32_t libdivide_u32_do(uint32_t numer, const struct libdivide_u32_t *denom) {
    uint8_t more = denom->more;
    if (more & LIBDIVIDE_U32_SHIFT_PATH) {
        return numer >> (more & LIBDIVIDE_32_SHIFT_MASK);
    }
    else {
        uint32_t q = libdivide__mullhi_u32(denom->magic, numer);
        if (more & LIBDIVIDE_ADD_MARKER) {
            uint32_t t = ((numer - q) >> 1) + q;
            return t >> (more & LIBDIVIDE_32_SHIFT_MASK);
        }
        else {
            return q >> more; //all upper bits are 0 - don't need to mask them off
        }
    }
}


int libdivide_u32_get_algorithm(const struct libdivide_u32_t *denom) {
    uint8_t more = denom->more;
    if (more & LIBDIVIDE_U32_SHIFT_PATH) return 0;
    else if (! (more & LIBDIVIDE_ADD_MARKER)) return 1;
    else return 2;
}

uint32_t libdivide_u32_do_alg0(uint32_t numer, const struct libdivide_u32_t *denom) {
    return numer >> (denom->more & LIBDIVIDE_32_SHIFT_MASK);
}

uint32_t libdivide_u32_do_alg1(uint32_t numer, const struct libdivide_u32_t *denom) {
    uint32_t q = libdivide__mullhi_u32(denom->magic, numer);
    return q >> denom->more;
}

uint32_t libdivide_u32_do_alg2(uint32_t numer, const struct libdivide_u32_t *denom) {
    // denom->add != 0
    uint32_t q = libdivide__mullhi_u32(denom->magic, numer);
    uint32_t t = ((numer - q) >> 1) + q;
    return t >> (denom->more & LIBDIVIDE_32_SHIFT_MASK);
}




#if LIBDIVIDE_USE_SSE2
__m128i libdivide_u32_do_vector(__m128i numers, const struct libdivide_u32_t *denom) {
    uint8_t more = denom->more;
    if (more & LIBDIVIDE_U32_SHIFT_PATH) {
        return _mm_srl_epi32(numers, libdivide_u32_to_m128i(more & LIBDIVIDE_32_SHIFT_MASK));
    }
    else {
        __m128i q = libdivide__mullhi_u32_flat_vector(numers, _mm_set1_epi32(denom->magic));
        if (more & LIBDIVIDE_ADD_MARKER) {
            //uint32_t t = ((numer - q) >> 1) + q;
            //return t >> denom->shift;
            __m128i t = _mm_add_epi32(_mm_srli_epi32(_mm_sub_epi32(numers, q), 1), q);
            return _mm_srl_epi32(t, libdivide_u32_to_m128i(more & LIBDIVIDE_32_SHIFT_MASK));

        }
        else {
            //q >> denom->shift
            return _mm_srl_epi32(q, libdivide_u32_to_m128i(more));
        }
    }
}

__m128i libdivide_u32_do_vector_alg0(__m128i numers, const struct libdivide_u32_t *denom) {
    return _mm_srl_epi32(numers, libdivide_u32_to_m128i(denom->more & LIBDIVIDE_32_SHIFT_MASK));
}

__m128i libdivide_u32_do_vector_alg1(__m128i numers, const struct libdivide_u32_t *denom) {
    __m128i q = libdivide__mullhi_u32_flat_vector(numers, _mm_set1_epi32(denom->magic));
    return _mm_srl_epi32(q, libdivide_u32_to_m128i(denom->more));
}

__m128i libdivide_u32_do_vector_alg2(__m128i numers, const struct libdivide_u32_t *denom) {
    __m128i q = libdivide__mullhi_u32_flat_vector(numers, _mm_set1_epi32(denom->magic));
    __m128i t = _mm_add_epi32(_mm_srli_epi32(_mm_sub_epi32(numers, q), 1), q);
    return _mm_srl_epi32(t, libdivide_u32_to_m128i(denom->more & LIBDIVIDE_32_SHIFT_MASK));
}

#endif

/////////// UINT64

struct libdivide_u64_t libdivide_u64_gen(uint64_t d) {
    struct libdivide_u64_t result;
    if ((d & (d - 1)) == 0) {
        result.more = libdivide__count_trailing_zeros64(d) | LIBDIVIDE_U64_SHIFT_PATH;
        result.magic = 0;
    }
    else {
        const uint32_t floor_log_2_d = 63 - libdivide__count_leading_zeros64(d);

        uint64_t proposed_m, rem;
        uint8_t more;
        proposed_m = libdivide_128_div_64_to_64(1ULL << floor_log_2_d, 0, d, &rem); //== (1 << (64 + floor_log_2_d)) / d

        LIBDIVIDE_ASSERT(rem > 0 && rem < d);
        const uint64_t e = d - rem;

	/* This power works if e < 2**floor_log_2_d. */
	if (e < (1ULL << floor_log_2_d)) {
            /* This power works */
            more = floor_log_2_d;
        }
        else {
            /* We have to use the general 65-bit algorithm.  We need to compute (2**power) / d. However, we already have (2**(power-1))/d and its remainder.  By doubling both, and then correcting the remainder, we can compute the larger division. */
            proposed_m += proposed_m; //don't care about overflow here - in fact, we expect it
            const uint64_t twice_rem = rem + rem;
            if (twice_rem >= d || twice_rem < rem) proposed_m += 1;
            more = floor_log_2_d | LIBDIVIDE_ADD_MARKER;
        }
        result.magic = 1 + proposed_m;
        result.more = more;
        //result.more's shift should in general be ceil_log_2_d.  But if we used the smaller power, we subtract one from the shift because we're using the smaller power. If we're using the larger power, we subtract one from the shift because it's taken care of by the add indicator.  So floor_log_2_d happens to be correct in both cases, which is why we do it outside of the if statement.
    }
    return result;
}

uint64_t libdivide_u64_do(uint64_t numer, const struct libdivide_u64_t *denom) {
    uint8_t more = denom->more;
    if (more & LIBDIVIDE_U64_SHIFT_PATH) {
        return numer >> (more & LIBDIVIDE_64_SHIFT_MASK);
    }
    else {
        uint64_t q = libdivide__mullhi_u64(denom->magic, numer);
        if (more & LIBDIVIDE_ADD_MARKER) {
            uint64_t t = ((numer - q) >> 1) + q;
            return t >> (more & LIBDIVIDE_64_SHIFT_MASK);
        }
        else {
            return q >> more; //all upper bits are 0 - don't need to mask them off
        }
    }
}


int libdivide_u64_get_algorithm(const struct libdivide_u64_t *denom) {
    uint8_t more = denom->more;
    if (more & LIBDIVIDE_U64_SHIFT_PATH) return 0;
    else if (! (more & LIBDIVIDE_ADD_MARKER)) return 1;
    else return 2;
}

uint64_t libdivide_u64_do_alg0(uint64_t numer, const struct libdivide_u64_t *denom) {
    return numer >> (denom->more & LIBDIVIDE_64_SHIFT_MASK);
}

uint64_t libdivide_u64_do_alg1(uint64_t numer, const struct libdivide_u64_t *denom) {
    uint64_t q = libdivide__mullhi_u64(denom->magic, numer);
    return q >> denom->more;
}

uint64_t libdivide_u64_do_alg2(uint64_t numer, const struct libdivide_u64_t *denom) {
    uint64_t q = libdivide__mullhi_u64(denom->magic, numer);
    uint64_t t = ((numer - q) >> 1) + q;
    return t >> (denom->more & LIBDIVIDE_64_SHIFT_MASK);
}

#if LIBDIVIDE_USE_SSE2
__m128i libdivide_u64_do_vector(__m128i numers, const struct libdivide_u64_t * denom) {
    uint8_t more = denom->more;
    if (more & LIBDIVIDE_U64_SHIFT_PATH) {
        return _mm_srl_epi64(numers, libdivide_u32_to_m128i(more & LIBDIVIDE_64_SHIFT_MASK));
    }
    else {
        __m128i q = libdivide_mullhi_u64_flat_vector(numers, libdivide__u64_to_m128(denom->magic));
        if (more & LIBDIVIDE_ADD_MARKER) {
            //uint32_t t = ((numer - q) >> 1) + q;
            //return t >> denom->shift;
            __m128i t = _mm_add_epi64(_mm_srli_epi64(_mm_sub_epi64(numers, q), 1), q);
            return _mm_srl_epi64(t, libdivide_u32_to_m128i(more & LIBDIVIDE_64_SHIFT_MASK));
        }
        else {
            //q >> denom->shift
            return _mm_srl_epi64(q, libdivide_u32_to_m128i(more));
        }
    }
}

__m128i libdivide_u64_do_vector_alg0(__m128i numers, const struct libdivide_u64_t *denom) {
    return _mm_srl_epi64(numers, libdivide_u32_to_m128i(denom->more & LIBDIVIDE_64_SHIFT_MASK));
}

__m128i libdivide_u64_do_vector_alg1(__m128i numers, const struct libdivide_u64_t *denom) {
    __m128i q = libdivide_mullhi_u64_flat_vector(numers, libdivide__u64_to_m128(denom->magic));
    return _mm_srl_epi64(q, libdivide_u32_to_m128i(denom->more));
}

__m128i libdivide_u64_do_vector_alg2(__m128i numers, const struct libdivide_u64_t *denom) {
    __m128i q = libdivide_mullhi_u64_flat_vector(numers, libdivide__u64_to_m128(denom->magic));
    __m128i t = _mm_add_epi64(_mm_srli_epi64(_mm_sub_epi64(numers, q), 1), q);
    return _mm_srl_epi64(t, libdivide_u32_to_m128i(denom->more & LIBDIVIDE_64_SHIFT_MASK));
}


#endif

/////////// SINT32


static inline int32_t libdivide__mullhi_s32(int32_t x, int32_t y) {
    int64_t xl = x, yl = y;
    int64_t rl = xl * yl;
    return (int32_t)(rl >> 32); //needs to be arithmetic shift
}

struct libdivide_s32_t libdivide_s32_gen(int32_t d) {
    struct libdivide_s32_t result;

    /* If d is a power of 2, or negative a power of 2, we have to use a shift.  This is especially important because the magic algorithm fails for -1.  To check if d is a power of 2 or its inverse, it suffices to check whether its absolute value has exactly one bit set.  This works even for INT_MIN, because abs(INT_MIN) == INT_MIN, and INT_MIN has one bit set and is a power of 2.  */
    uint32_t absD = (uint32_t)(d < 0 ? -d : d); //gcc optimizes this to the fast abs trick
    if ((absD & (absD - 1)) == 0) { //check if exactly one bit is set, don't care if absD is 0 since that's divide by zero
        result.magic = 0;
        result.more = libdivide__count_trailing_zeros32(absD) | (d < 0 ? LIBDIVIDE_NEGATIVE_DIVISOR : 0) | LIBDIVIDE_S32_SHIFT_PATH;
    }
    else {
        const uint32_t floor_log_2_d = 31 - libdivide__count_leading_zeros32(absD);
        LIBDIVIDE_ASSERT(floor_log_2_d >= 1);

        uint8_t more;
        //the dividend here is 2**(floor_log_2_d + 31), so the low 32 bit word is 0 and the high word is floor_log_2_d - 1
        uint32_t rem, proposed_m;
        proposed_m = libdivide_64_div_32_to_32(1U << (floor_log_2_d - 1), 0, absD, &rem);
        const uint32_t e = absD - rem;

        /* We are going to start with a power of floor_log_2_d - 1.  This works if works if e < 2**floor_log_2_d. */
        if (e < (1U << floor_log_2_d)) {
            /* This power works */
            more = floor_log_2_d - 1;
        }
        else {
            /* We need to go one higher.  This should not make proposed_m overflow, but it will make it negative when interpreted as an int32_t. */
            proposed_m += proposed_m;
            const uint32_t twice_rem = rem + rem;
            if (twice_rem >= absD || twice_rem < rem) proposed_m += 1;
            more = floor_log_2_d | LIBDIVIDE_ADD_MARKER | (d < 0 ? LIBDIVIDE_NEGATIVE_DIVISOR : 0); //use the general algorithm
        }
        proposed_m += 1;
        result.magic = (d < 0 ? -(int32_t)proposed_m : (int32_t)proposed_m);
        result.more = more;

    }
    return result;
}

int32_t libdivide_s32_do(int32_t numer, const struct libdivide_s32_t *denom) {
    uint8_t more = denom->more;
    if (more & LIBDIVIDE_S32_SHIFT_PATH) {
        uint8_t shifter = more & LIBDIVIDE_32_SHIFT_MASK;
        int32_t q = numer + ((numer >> 31) & ((1 << shifter) - 1));
        q = q >> shifter;
        int32_t shiftMask = (int8_t)more >> 7; //must be arithmetic shift and then sign-extend
        q = (q ^ shiftMask) - shiftMask;
        return q;
    }
    else {
        int32_t q = libdivide__mullhi_s32(denom->magic, numer);
        if (more & LIBDIVIDE_ADD_MARKER) {
            int32_t sign = (int8_t)more >> 7; //must be arithmetic shift and then sign extend
            q += ((numer ^ sign) - sign);
        }
        q >>= more & LIBDIVIDE_32_SHIFT_MASK;
        q += (q < 0);
        return q;
    }
}

int libdivide_s32_get_algorithm(const struct libdivide_s32_t *denom) {
    uint8_t more = denom->more;
    int positiveDivisor = ! (more & LIBDIVIDE_NEGATIVE_DIVISOR);
    if (more & LIBDIVIDE_S32_SHIFT_PATH) return (positiveDivisor ? 0 : 1);
    else if (more & LIBDIVIDE_ADD_MARKER) return (positiveDivisor ? 2 : 3);
    else return 4;
}

int32_t libdivide_s32_do_alg0(int32_t numer, const struct libdivide_s32_t *denom) {
    uint8_t shifter = denom->more & LIBDIVIDE_32_SHIFT_MASK;
    int32_t q = numer + ((numer >> 31) & ((1 << shifter) - 1));
    return q >> shifter;
}

int32_t libdivide_s32_do_alg1(int32_t numer, const struct libdivide_s32_t *denom) {
    uint8_t shifter = denom->more & LIBDIVIDE_32_SHIFT_MASK;
    int32_t q = numer + ((numer >> 31) & ((1 << shifter) - 1));
    return - (q >> shifter);
}

int32_t libdivide_s32_do_alg2(int32_t numer, const struct libdivide_s32_t *denom) {
    int32_t q = libdivide__mullhi_s32(denom->magic, numer);
    q += numer;
    q >>= denom->more & LIBDIVIDE_32_SHIFT_MASK;
    q += (q < 0);
    return q;
}

int32_t libdivide_s32_do_alg3(int32_t numer, const struct libdivide_s32_t *denom) {
    int32_t q = libdivide__mullhi_s32(denom->magic, numer);
    q -= numer;
    q >>= denom->more & LIBDIVIDE_32_SHIFT_MASK;
    q += (q < 0);
    return q;
}

int32_t libdivide_s32_do_alg4(int32_t numer, const struct libdivide_s32_t *denom) {
    int32_t q = libdivide__mullhi_s32(denom->magic, numer);
    q >>= denom->more & LIBDIVIDE_32_SHIFT_MASK;
    q += (q < 0);
    return q;
}

#if LIBDIVIDE_USE_SSE2
__m128i libdivide_s32_do_vector(__m128i numers, const struct libdivide_s32_t * denom) {
    uint8_t more = denom->more;
    if (more & LIBDIVIDE_S32_SHIFT_PATH) {
        uint32_t shifter = more & LIBDIVIDE_32_SHIFT_MASK;
        __m128i roundToZeroTweak = _mm_set1_epi32((1 << shifter) - 1); //could use _mm_srli_epi32 with an all -1 register
        __m128i q = _mm_add_epi32(numers, _mm_and_si128(_mm_srai_epi32(numers, 31), roundToZeroTweak)); //q = numer + ((numer >> 31) & roundToZeroTweak);
        q = _mm_sra_epi32(q, libdivide_u32_to_m128i(shifter)); // q = q >> shifter
        __m128i shiftMask = _mm_set1_epi32((int32_t)((int8_t)more >> 7)); //set all bits of shift mask = to the sign bit of more
        q = _mm_sub_epi32(_mm_xor_si128(q, shiftMask), shiftMask); //q = (q ^ shiftMask) - shiftMask;
        return q;
    }
    else {
        __m128i q = libdivide_mullhi_s32_flat_vector(numers, _mm_set1_epi32(denom->magic));
        if (more & LIBDIVIDE_ADD_MARKER) {
            __m128i sign = _mm_set1_epi32((int32_t)(int8_t)more >> 7); //must be arithmetic shift
            q = _mm_add_epi32(q, _mm_sub_epi32(_mm_xor_si128(numers, sign), sign)); // q += ((numer ^ sign) - sign);
        }
        q = _mm_sra_epi32(q, libdivide_u32_to_m128i(more & LIBDIVIDE_32_SHIFT_MASK)); //q >>= shift
        q = _mm_add_epi32(q, _mm_srli_epi32(q, 31)); // q += (q < 0)
        return q;
    }
}

__m128i libdivide_s32_do_vector_alg0(__m128i numers, const struct libdivide_s32_t *denom) {
    uint8_t shifter = denom->more & LIBDIVIDE_32_SHIFT_MASK;
    __m128i roundToZeroTweak = _mm_set1_epi32((1 << shifter) - 1);
    __m128i q = _mm_add_epi32(numers, _mm_and_si128(_mm_srai_epi32(numers, 31), roundToZeroTweak));
    return _mm_sra_epi32(q, libdivide_u32_to_m128i(shifter));
}

__m128i libdivide_s32_do_vector_alg1(__m128i numers, const struct libdivide_s32_t *denom) {
    uint8_t shifter = denom->more & LIBDIVIDE_32_SHIFT_MASK;
    __m128i roundToZeroTweak = _mm_set1_epi32((1 << shifter) - 1);
    __m128i q = _mm_add_epi32(numers, _mm_and_si128(_mm_srai_epi32(numers, 31), roundToZeroTweak));
    return _mm_sub_epi32(_mm_setzero_si128(), _mm_sra_epi32(q, libdivide_u32_to_m128i(shifter)));
}

__m128i libdivide_s32_do_vector_alg2(__m128i numers, const struct libdivide_s32_t *denom) {
    __m128i q = libdivide_mullhi_s32_flat_vector(numers, _mm_set1_epi32(denom->magic));
    q = _mm_add_epi32(q, numers);
    q = _mm_sra_epi32(q, libdivide_u32_to_m128i(denom->more & LIBDIVIDE_32_SHIFT_MASK));
    q = _mm_add_epi32(q, _mm_srli_epi32(q, 31));
    return q;
}

__m128i libdivide_s32_do_vector_alg3(__m128i numers, const struct libdivide_s32_t *denom) {
    __m128i q = libdivide_mullhi_s32_flat_vector(numers, _mm_set1_epi32(denom->magic));
    q = _mm_sub_epi32(q, numers);
    q = _mm_sra_epi32(q, libdivide_u32_to_m128i(denom->more & LIBDIVIDE_32_SHIFT_MASK));
    q = _mm_add_epi32(q, _mm_srli_epi32(q, 31));
    return q;
}

__m128i libdivide_s32_do_vector_alg4(__m128i numers, const struct libdivide_s32_t *denom) {
    __m128i q = libdivide_mullhi_s32_flat_vector(numers, _mm_set1_epi32(denom->magic));
    q = _mm_sra_epi32(q, libdivide_u32_to_m128i(denom->more)); //q >>= shift
    q = _mm_add_epi32(q, _mm_srli_epi32(q, 31)); // q += (q < 0)
    return q;
}
#endif

///////////// SINT64


struct libdivide_s64_t libdivide_s64_gen(int64_t d) {
    struct libdivide_s64_t result;

    /* If d is a power of 2, or negative a power of 2, we have to use a shift.  This is especially important because the magic algorithm fails for -1.  To check if d is a power of 2 or its inverse, it suffices to check whether its absolute value has exactly one bit set.  This works even for INT_MIN, because abs(INT_MIN) == INT_MIN, and INT_MIN has one bit set and is a power of 2.  */
    const uint64_t absD = (uint64_t)(d < 0 ? -d : d); //gcc optimizes this to the fast abs trick
    if ((absD & (absD - 1)) == 0) { //check if exactly one bit is set, don't care if absD is 0 since that's divide by zero
        result.more = libdivide__count_trailing_zeros64(absD) | (d < 0 ? LIBDIVIDE_NEGATIVE_DIVISOR : 0);
        result.magic = 0;
    }
    else {
        const uint32_t floor_log_2_d = 63 - libdivide__count_leading_zeros64(absD);

        //the dividend here is 2**(floor_log_2_d + 63), so the low 64 bit word is 0 and the high word is floor_log_2_d - 1
        uint8_t more;
        uint64_t rem, proposed_m;
        proposed_m = libdivide_128_div_64_to_64(1ULL << (floor_log_2_d - 1), 0, absD, &rem);
        const uint64_t e = absD - rem;

        /* We are going to start with a power of floor_log_2_d - 1.  This works if works if e < 2**floor_log_2_d. */
        if (e < (1ULL << floor_log_2_d)) {
            /* This power works */
            more = floor_log_2_d - 1;
        }
        else {
            /* We need to go one higher.  This should not make proposed_m overflow, but it will make it negative when interpreted as an int32_t. */
            proposed_m += proposed_m;
            const uint64_t twice_rem = rem + rem;
            if (twice_rem >= absD || twice_rem < rem) proposed_m += 1;
            more = floor_log_2_d | LIBDIVIDE_ADD_MARKER | (d < 0 ? LIBDIVIDE_NEGATIVE_DIVISOR : 0);
        }
        proposed_m += 1;
        result.more = more;
        result.magic = (d < 0 ? -(int64_t)proposed_m : (int64_t)proposed_m);
    }
    return result;
}

int64_t libdivide_s64_do(int64_t numer, const struct libdivide_s64_t *denom) {
    uint8_t more = denom->more;
    int64_t magic = denom->magic;
    if (magic == 0) { //shift path
        uint32_t shifter = more & LIBDIVIDE_64_SHIFT_MASK;
        int64_t q = numer + ((numer >> 63) & ((1LL << shifter) - 1));
        q = q >> shifter;
        int64_t shiftMask = (int8_t)more >> 7; //must be arithmetic shift and then sign-extend
        q = (q ^ shiftMask) - shiftMask;
        return q;
    }
    else {
        int64_t q = libdivide__mullhi_s64(magic, numer);
        if (more & LIBDIVIDE_ADD_MARKER) {
            int64_t sign = (int8_t)more >> 7; //must be arithmetic shift and then sign extend
            q += ((numer ^ sign) - sign);
        }
        q >>= more & LIBDIVIDE_64_SHIFT_MASK;
        q += (q < 0);
        return q;
    }
}


int libdivide_s64_get_algorithm(const struct libdivide_s64_t *denom) {
    uint8_t more = denom->more;
    int positiveDivisor = ! (more & LIBDIVIDE_NEGATIVE_DIVISOR);
    if (denom->magic == 0) return (positiveDivisor ? 0 : 1); //shift path
    else if (more & LIBDIVIDE_ADD_MARKER) return (positiveDivisor ? 2 : 3);
    else return 4;
}

int64_t libdivide_s64_do_alg0(int64_t numer, const struct libdivide_s64_t *denom) {
    uint32_t shifter = denom->more & LIBDIVIDE_64_SHIFT_MASK;
    int64_t q = numer + ((numer >> 63) & ((1LL << shifter) - 1));
    return q >> shifter;
}

int64_t libdivide_s64_do_alg1(int64_t numer, const struct libdivide_s64_t *denom) {
    //denom->shifter != -1 && demo->shiftMask != 0
    uint32_t shifter = denom->more & LIBDIVIDE_64_SHIFT_MASK;
    int64_t q = numer + ((numer >> 63) & ((1LL << shifter) - 1));
    return - (q >> shifter);
}

int64_t libdivide_s64_do_alg2(int64_t numer, const struct libdivide_s64_t *denom) {
    int64_t q = libdivide__mullhi_s64(denom->magic, numer);
    q += numer;
    q >>= denom->more & LIBDIVIDE_64_SHIFT_MASK;
    q += (q < 0);
    return q;
}

int64_t libdivide_s64_do_alg3(int64_t numer, const struct libdivide_s64_t *denom) {
    int64_t q = libdivide__mullhi_s64(denom->magic, numer);
    q -= numer;
    q >>= denom->more & LIBDIVIDE_64_SHIFT_MASK;
    q += (q < 0);
    return q;
}

int64_t libdivide_s64_do_alg4(int64_t numer, const struct libdivide_s64_t *denom) {
    int64_t q = libdivide__mullhi_s64(denom->magic, numer);
    q >>= denom->more;
    q += (q < 0);
    return q;
}


#if LIBDIVIDE_USE_SSE2
__m128i libdivide_s64_do_vector(__m128i numers, const struct libdivide_s64_t * denom) {
    uint8_t more = denom->more;
    int64_t magic = denom->magic;
    if (magic == 0) { //shift path
        uint32_t shifter = more & LIBDIVIDE_64_SHIFT_MASK;
        __m128i roundToZeroTweak = libdivide__u64_to_m128((1LL << shifter) - 1);
        __m128i q = _mm_add_epi64(numers, _mm_and_si128(libdivide_s64_signbits(numers), roundToZeroTweak)); //q = numer + ((numer >> 63) & roundToZeroTweak);
        q = libdivide_s64_shift_right_vector(q, shifter); // q = q >> shifter
        __m128i shiftMask = _mm_set1_epi32((int32_t)((int8_t)more >> 7));
        q = _mm_sub_epi64(_mm_xor_si128(q, shiftMask), shiftMask); //q = (q ^ shiftMask) - shiftMask;
        return q;
    }
    else {
        __m128i q = libdivide_mullhi_s64_flat_vector(numers, libdivide__u64_to_m128(magic));
        if (more & LIBDIVIDE_ADD_MARKER) {
            __m128i sign = _mm_set1_epi32((int32_t)((int8_t)more >> 7)); //must be arithmetic shift
            q = _mm_add_epi64(q, _mm_sub_epi64(_mm_xor_si128(numers, sign), sign)); // q += ((numer ^ sign) - sign);
        }
        q = libdivide_s64_shift_right_vector(q, more & LIBDIVIDE_64_SHIFT_MASK); //q >>= denom->mult_path.shift
        q = _mm_add_epi64(q, _mm_srli_epi64(q, 63)); // q += (q < 0)
        return q;
    }
}

__m128i libdivide_s64_do_vector_alg0(__m128i numers, const struct libdivide_s64_t *denom) {
    uint32_t shifter = denom->more & LIBDIVIDE_64_SHIFT_MASK;
    __m128i roundToZeroTweak = libdivide__u64_to_m128((1LL << shifter) - 1);
    __m128i q = _mm_add_epi64(numers, _mm_and_si128(libdivide_s64_signbits(numers), roundToZeroTweak));
    q = libdivide_s64_shift_right_vector(q, shifter);
    return q;
}

__m128i libdivide_s64_do_vector_alg1(__m128i numers, const struct libdivide_s64_t *denom) {
    uint32_t shifter = denom->more & LIBDIVIDE_64_SHIFT_MASK;
    __m128i roundToZeroTweak = libdivide__u64_to_m128((1LL << shifter) - 1);
    __m128i q = _mm_add_epi64(numers, _mm_and_si128(libdivide_s64_signbits(numers), roundToZeroTweak));
    q = libdivide_s64_shift_right_vector(q, shifter);
    return _mm_sub_epi64(_mm_setzero_si128(), q);
}

__m128i libdivide_s64_do_vector_alg2(__m128i numers, const struct libdivide_s64_t *denom) {
    __m128i q = libdivide_mullhi_s64_flat_vector(numers, libdivide__u64_to_m128(denom->magic));
    q = _mm_add_epi64(q, numers);
    q = libdivide_s64_shift_right_vector(q, denom->more & LIBDIVIDE_64_SHIFT_MASK);
    q = _mm_add_epi64(q, _mm_srli_epi64(q, 63)); // q += (q < 0)
    return q;
}

__m128i libdivide_s64_do_vector_alg3(__m128i numers, const struct libdivide_s64_t *denom) {
    __m128i q = libdivide_mullhi_s64_flat_vector(numers, libdivide__u64_to_m128(denom->magic));
    q = _mm_sub_epi64(q, numers);
    q = libdivide_s64_shift_right_vector(q, denom->more & LIBDIVIDE_64_SHIFT_MASK);
    q = _mm_add_epi64(q, _mm_srli_epi64(q, 63)); // q += (q < 0)
    return q;
}

__m128i libdivide_s64_do_vector_alg4(__m128i numers, const struct libdivide_s64_t *denom) {
    __m128i q = libdivide_mullhi_s64_flat_vector(numers, libdivide__u64_to_m128(denom->magic));
    q = libdivide_s64_shift_right_vector(q, denom->more);
    q = _mm_add_epi64(q, _mm_srli_epi64(q, 63));
    return q;
}

#endif

/////////// C++ stuff

#ifdef __cplusplus

/* The C++ template design here is a total mess.  This needs to be fixed by someone better at templates than I.  The current design is:

- The base is a template divider_base that takes the integer type, the libdivide struct, a generating function, a get algorithm function, a do function, and either a do vector function or a dummy int.
- The base has storage for the libdivide struct.  This is the only storage (so the C++ class should be no larger than the libdivide struct).

- Above that, there's divider_mid.  This is an empty struct by default, but it is specialized against our four int types.  divider_mid contains a template struct algo, that contains a typedef for a specialization of divider_base.  struct algo is specialized to take an "algorithm number," where -1 means to use the general algorithm.

- Publicly we have class divider, which inherits from divider_mid::algo.  This also take an algorithm number, which defaults to -1 (the general algorithm).
- divider has a operator / which allows you to use a divider as the divisor in a quotient expression.

*/

namespace libdivide_internal {

#if LIBDIVIDE_USE_SSE2
#define MAYBE_VECTOR(x) x
#define MAYBE_VECTOR_PARAM __m128i vector_func(__m128i, const DenomType *)
#else
#define MAYBE_VECTOR(x) 0
#define MAYBE_VECTOR_PARAM int vector_func
#endif

    /* Some bogus unswitch functions for unsigned types so the same (presumably templated) code can work for both signed and unsigned. */
    uint32_t crash_u32(uint32_t, const libdivide_u32_t *) { abort(); }
    uint64_t crash_u64(uint64_t, const libdivide_u64_t *) { abort(); }
#ifdef __APPLE__
    UInt64 crash_u64(UInt64, const libdivide_u64_t *) { abort(); }
#endif
#if LIBDIVIDE_USE_SSE2
    __m128i crash_u32_vector(__m128i, const libdivide_u32_t *) { abort(); }
    __m128i crash_u64_vector(__m128i, const libdivide_u64_t *) { abort(); }
#endif

    template<typename IntType, typename DenomType, DenomType gen_func(IntType), int get_algo(const DenomType *), IntType do_func(IntType, const DenomType *), MAYBE_VECTOR_PARAM>
    class divider_base {
    public:
        DenomType denom;
        divider_base(IntType d) : denom(gen_func(d)) { }
        divider_base(const DenomType & d) : denom(d) { }

        IntType perform_divide(IntType val) const { return do_func(val, &denom); }
#if LIBDIVIDE_USE_SSE2
        __m128i perform_divide_vector(__m128i val) const { return vector_func(val, &denom); }
#endif

        int get_algorithm() const { return get_algo(&denom); }
    };


    template<class T> struct divider_mid { };

    template<> struct divider_mid<uint32_t> {
        typedef uint32_t IntType;
        typedef struct libdivide_u32_t DenomType;
        template<IntType do_func(IntType, const DenomType *), MAYBE_VECTOR_PARAM> struct denom {
            typedef divider_base<IntType, DenomType, libdivide_u32_gen, libdivide_u32_get_algorithm, do_func, vector_func> divider;
        };

        template<int ALGO, int J = 0> struct algo { };
        template<int J> struct algo<-1, J> { typedef denom<libdivide_u32_do, MAYBE_VECTOR(libdivide_u32_do_vector)>::divider divider; };
        template<int J> struct algo<0, J>  { typedef denom<libdivide_u32_do_alg0, MAYBE_VECTOR(libdivide_u32_do_vector_alg0)>::divider divider; };
        template<int J> struct algo<1, J>  { typedef denom<libdivide_u32_do_alg1, MAYBE_VECTOR(libdivide_u32_do_vector_alg1)>::divider divider; };
        template<int J> struct algo<2, J>  { typedef denom<libdivide_u32_do_alg2, MAYBE_VECTOR(libdivide_u32_do_vector_alg2)>::divider divider; };

        /* Define two more bogus ones so that the same (templated, presumably) code can handle both signed and unsigned */
        template<int J> struct algo<3, J>  { typedef denom<crash_u32, MAYBE_VECTOR(crash_u32_vector)>::divider divider; };
        template<int J> struct algo<4, J>  { typedef denom<crash_u32, MAYBE_VECTOR(crash_u32_vector)>::divider divider; };

    };

    template<> struct divider_mid<int32_t> {
        typedef int32_t IntType;
        typedef struct libdivide_s32_t DenomType;
        template<IntType do_func(IntType, const DenomType *), MAYBE_VECTOR_PARAM> struct denom {
            typedef divider_base<IntType, DenomType, libdivide_s32_gen, libdivide_s32_get_algorithm, do_func, vector_func> divider;
        };


        template<int ALGO, int J = 0> struct algo { };
        template<int J> struct algo<-1, J> { typedef denom<libdivide_s32_do, MAYBE_VECTOR(libdivide_s32_do_vector)>::divider divider; };
        template<int J> struct algo<0, J>  { typedef denom<libdivide_s32_do_alg0, MAYBE_VECTOR(libdivide_s32_do_vector_alg0)>::divider divider; };
        template<int J> struct algo<1, J>  { typedef denom<libdivide_s32_do_alg1, MAYBE_VECTOR(libdivide_s32_do_vector_alg1)>::divider divider; };
        template<int J> struct algo<2, J>  { typedef denom<libdivide_s32_do_alg2, MAYBE_VECTOR(libdivide_s32_do_vector_alg2)>::divider divider; };
        template<int J> struct algo<3, J>  { typedef denom<libdivide_s32_do_alg3, MAYBE_VECTOR(libdivide_s32_do_vector_alg3)>::divider divider; };
        template<int J> struct algo<4, J>  { typedef denom<libdivide_s32_do_alg4, MAYBE_VECTOR(libdivide_s32_do_vector_alg4)>::divider divider; };

    };

#ifdef __APPLE__
    template<> struct divider_mid<Int64> {
        typedef Int64 IntType;
        typedef struct libdivide_s64_t DenomType;
        template<IntType do_func(IntType, const DenomType *), MAYBE_VECTOR_PARAM> struct denom {
            typedef divider_base<IntType, DenomType, libdivide_s64_gen, libdivide_s64_get_algorithm, do_func, vector_func> divider;
        };

        template<int ALGO, int J = 0> struct algo { };
        template<int J> struct algo<-1, J> { typedef denom<libdivide_s64_do, MAYBE_VECTOR(libdivide_s64_do_vector)>::divider divider; };
        template<int J> struct algo<0, J>  { typedef denom<libdivide_s64_do_alg0, MAYBE_VECTOR(libdivide_s64_do_vector_alg0)>::divider divider; };
        template<int J> struct algo<1, J>  { typedef denom<libdivide_s64_do_alg1, MAYBE_VECTOR(libdivide_s64_do_vector_alg1)>::divider divider; };
        template<int J> struct algo<2, J>  { typedef denom<libdivide_s64_do_alg2, MAYBE_VECTOR(libdivide_s64_do_vector_alg2)>::divider divider; };
        template<int J> struct algo<3, J>  { typedef denom<libdivide_s64_do_alg3, MAYBE_VECTOR(libdivide_s64_do_vector_alg3)>::divider divider; };
        template<int J> struct algo<4, J>  { typedef denom<libdivide_s64_do_alg4, MAYBE_VECTOR(libdivide_s64_do_vector_alg4)>::divider divider; };
    };

    template<> struct divider_mid<UInt64> {
        typedef UInt64 IntType;
        typedef struct libdivide_u64_t DenomType;
        template<IntType do_func(IntType, const DenomType *), MAYBE_VECTOR_PARAM> struct denom {
            typedef divider_base<IntType, DenomType, libdivide_u64_gen, libdivide_u64_get_algorithm, do_func, vector_func> divider;
        };

        template<int ALGO, int J = 0> struct algo { };
        template<int J> struct algo<-1, J> { typedef denom<libdivide_u64_do, MAYBE_VECTOR(libdivide_u64_do_vector)>::divider divider; };
        template<int J> struct algo<0, J>  { typedef denom<libdivide_u64_do_alg0, MAYBE_VECTOR(libdivide_u64_do_vector_alg0)>::divider divider; };
        template<int J> struct algo<1, J>  { typedef denom<libdivide_u64_do_alg1, MAYBE_VECTOR(libdivide_u64_do_vector_alg1)>::divider divider; };
        template<int J> struct algo<2, J>  { typedef denom<libdivide_u64_do_alg2, MAYBE_VECTOR(libdivide_u64_do_vector_alg2)>::divider divider; };

        /* Define two more bogus ones so that the same (templated, presumably) code can handle both signed and unsigned */
        template<int J> struct algo<3, J>  { typedef denom<crash_u64, MAYBE_VECTOR(crash_u64_vector)>::divider divider; };
        template<int J> struct algo<4, J>  { typedef denom<crash_u64, MAYBE_VECTOR(crash_u64_vector)>::divider divider; };


    };
#endif

    template<> struct divider_mid<uint64_t> {
        typedef uint64_t IntType;
        typedef struct libdivide_u64_t DenomType;
        template<IntType do_func(IntType, const DenomType *), MAYBE_VECTOR_PARAM> struct denom {
            typedef divider_base<IntType, DenomType, libdivide_u64_gen, libdivide_u64_get_algorithm, do_func, vector_func> divider;
        };

        template<int ALGO, int J = 0> struct algo { };
        template<int J> struct algo<-1, J> { typedef denom<libdivide_u64_do, MAYBE_VECTOR(libdivide_u64_do_vector)>::divider divider; };
        template<int J> struct algo<0, J>  { typedef denom<libdivide_u64_do_alg0, MAYBE_VECTOR(libdivide_u64_do_vector_alg0)>::divider divider; };
        template<int J> struct algo<1, J>  { typedef denom<libdivide_u64_do_alg1, MAYBE_VECTOR(libdivide_u64_do_vector_alg1)>::divider divider; };
        template<int J> struct algo<2, J>  { typedef denom<libdivide_u64_do_alg2, MAYBE_VECTOR(libdivide_u64_do_vector_alg2)>::divider divider; };

        /* Define two more bogus ones so that the same (templated, presumably) code can handle both signed and unsigned */
        template<int J> struct algo<3, J>  { typedef denom<crash_u64, MAYBE_VECTOR(crash_u64_vector)>::divider divider; };
        template<int J> struct algo<4, J>  { typedef denom<crash_u64, MAYBE_VECTOR(crash_u64_vector)>::divider divider; };


    };

    template<> struct divider_mid<int64_t> {
        typedef int64_t IntType;
        typedef struct libdivide_s64_t DenomType;
        template<IntType do_func(IntType, const DenomType *), MAYBE_VECTOR_PARAM> struct denom {
            typedef divider_base<IntType, DenomType, libdivide_s64_gen, libdivide_s64_get_algorithm, do_func, vector_func> divider;
        };

        template<int ALGO, int J = 0> struct algo { };
        template<int J> struct algo<-1, J> { typedef denom<libdivide_s64_do, MAYBE_VECTOR(libdivide_s64_do_vector)>::divider divider; };
        template<int J> struct algo<0, J>  { typedef denom<libdivide_s64_do_alg0, MAYBE_VECTOR(libdivide_s64_do_vector_alg0)>::divider divider; };
        template<int J> struct algo<1, J>  { typedef denom<libdivide_s64_do_alg1, MAYBE_VECTOR(libdivide_s64_do_vector_alg1)>::divider divider; };
        template<int J> struct algo<2, J>  { typedef denom<libdivide_s64_do_alg2, MAYBE_VECTOR(libdivide_s64_do_vector_alg2)>::divider divider; };
        template<int J> struct algo<3, J>  { typedef denom<libdivide_s64_do_alg3, MAYBE_VECTOR(libdivide_s64_do_vector_alg3)>::divider divider; };
        template<int J> struct algo<4, J>  { typedef denom<libdivide_s64_do_alg4, MAYBE_VECTOR(libdivide_s64_do_vector_alg4)>::divider divider; };
    };

}

template<typename T, int ALGO = -1>
class divider
{
    private:
    typename libdivide_internal::divider_mid<T>::template algo<ALGO>::divider sub;
    template<int NEW_ALGO, typename S> friend divider<S, NEW_ALGO> unswitch(const divider<S, -1> & d);
    divider(const typename libdivide_internal::divider_mid<T>::DenomType & denom) : sub(denom) { }

    public:

    /* Ordinary constructor, that takes the divisor as a parameter. */
    divider(T n) : sub(n) { }

    /* Default constructor, that divides by 1 */
    divider() : sub(1) { }

    /* Divides the parameter by the divisor, returning the quotient */
    T perform_divide(T val) const { return sub.perform_divide(val); }

#if LIBDIVIDE_USE_SSE2
    /* Treats the vector as either two or four packed values (depending on the size), and divides each of them by the divisor, returning the packed quotients. */
    __m128i perform_divide_vector(__m128i val) const { return sub.perform_divide_vector(val); }
#endif

    /* Returns the index of algorithm, for use in the unswitch function */
    int get_algorithm() const { return sub.get_algorithm(); } // returns the algorithm for unswitching

    /* operator== */
    bool operator==(const divider<T, ALGO> & him) const { return sub.denom.magic == him.sub.denom.magic && sub.denom.more == him.sub.denom.more; }

    bool operator!=(const divider<T, ALGO> & him) const { return ! (*this == him); }
};

/* Returns a divider specialized for the given algorithm. */
template<int NEW_ALGO, typename S>
divider<S, NEW_ALGO> unswitch(const divider<S, -1> & d) { return divider<S, NEW_ALGO>(d.sub.denom); }

/* Overload of the / operator for scalar division. */
template<typename int_type, int ALGO>
int_type operator/(int_type numer, const divider<int_type, ALGO> & denom) {
    return denom.perform_divide(numer);
}

#if  LIBDIVIDE_USE_SSE2
/* Overload of the / operator for vector division. */
template<typename int_type, int ALGO>
__m128i operator/(__m128i numer, const divider<int_type, ALGO> & denom) {
    return denom.perform_divide_vector(numer);
}
#endif


#endif //__cplusplus

#endif //LIBDIVIDE_HEADER_ONLY
#ifdef __cplusplus
} //close namespace libdivide
} //close anonymous namespace
#endif

#pragma GCC diagnostic pop
