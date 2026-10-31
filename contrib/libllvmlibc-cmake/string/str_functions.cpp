// Strong x86_64 `memchr` and `strlen`, overriding musl's generic C versions in
// the static link (musl ships x86_64 assembly only for memcpy/memmove/memset;
// LLVM-libc's are byte and word loops). glibc uses EVEX/AVX2 assembly for both,
// and the CSV parser showed musl's word-at-a-time `memchr` taking twice the
// samples.
//
// Two tiers from one body (str_functions_impl.h): the build's baseline (AVX2 at
// x86-64-v3, SSE2 in the compat build) and an AVX-512BW clone with 64-byte
// vectors and mask registers, selected once at runtime through a function
// pointer, as in x86_64_mem_functions.cpp. The 64-byte tier is what matches
// glibc's `__memchr_evex512` above 4 KiB; 32-byte vectors cap at 1.6x its time
// because of load width. Measured against glibc, musl, LLVM-libc, FreeBSD,
// bionic and StringZilla in libc-func-bench (2026-10-21, Zen 5).
//
// Not built under sanitizers: their interceptors define strong `memchr` /
// `strlen` and the wrapped libc versions carry the range checks.

#if defined(__x86_64__)

#include "x86_cpu_dispatch.h"

#include <immintrin.h>

#include <cstddef>
#include <cstdint>

namespace {

constexpr uintptr_t PAGE = 4096;

#define ALIGN_DOWN(p)                                                          \
  reinterpret_cast<const char *>(reinterpret_cast<uintptr_t>(p) &              \
                                 ~(uintptr_t{VEC} - 1))
#define ALIGN_DOWN4(p)                                                         \
  reinterpret_cast<const char *>(reinterpret_cast<uintptr_t>(p) &              \
                                 ~(uintptr_t{4 * VEC} - 1))

// ---------------------------------------------------------------------------
// AVX-512BW clone: 64-byte vectors, compares into k registers, masked loads
// with fault suppression for the head and tail.
// ---------------------------------------------------------------------------
#define CHSTR_AVX512 1
#define VEC 64
#define vec_t __m512i
#define mask_t __mmask64
#define SPLAT(c) _mm512_set1_epi8(static_cast<char>(c))
#define LOAD(p) _mm512_load_si512(static_cast<const void *>(p))
#define LOADU(p) _mm512_loadu_si512(static_cast<const void *>(p))
#define MLOADZ(k, p) _mm512_maskz_loadu_epi8((k), (p))
#define CMPEQ_M(a, b) _mm512_cmpeq_epi8_mask((a), (b))
#define CMPEQ_MM(k, a, b) _mm512_mask_cmpeq_epi8_mask((k), (a), (b))
#define MINU(a, b) _mm512_min_epu8((a), (b))
#define XOR(a, b) _mm512_xor_si512((a), (b))
#define MASK_LO(n) static_cast<mask_t>(_bzhi_u64(~0ull, static_cast<unsigned>(n)))
#define MASK_ALL (~mask_t{0})
#define CTZ(m) static_cast<size_t>(__builtin_ctzll(m))
#define CHSTR_ATTR CH_AVX512_CLONE
#define FN(name) name##_avx512
#include "str_functions_impl.h"
#undef FN
#undef CHSTR_ATTR
#undef CTZ
#undef MASK_ALL
#undef MASK_LO
#undef XOR
#undef MINU
#undef CMPEQ_MM
#undef CMPEQ_M
#undef MLOADZ
#undef LOADU
#undef LOAD
#undef SPLAT
#undef mask_t
#undef vec_t
#undef VEC
#undef CHSTR_AVX512

// ---------------------------------------------------------------------------
// Baseline: AVX2 (x86-64-v3 build) or SSE2 (compat build).
// ---------------------------------------------------------------------------
#if defined(__AVX2__)
#define VEC 32
#define vec_t __m256i
#define SPLAT(c) _mm256_set1_epi8(static_cast<char>(c))
#define LOAD(p) _mm256_load_si256(reinterpret_cast<const __m256i *>(p))
#define LOADU(p) _mm256_loadu_si256(reinterpret_cast<const __m256i *>(p))
#define CMPEQ(a, b) _mm256_cmpeq_epi8((a), (b))
#define MINU(a, b) _mm256_min_epu8((a), (b))
#define OR(a, b) _mm256_or_si256((a), (b))
#define MOVEMASK(v) static_cast<uint32_t>(_mm256_movemask_epi8(v))
#else
#define VEC 16
#define vec_t __m128i
#define SPLAT(c) _mm_set1_epi8(static_cast<char>(c))
#define LOAD(p) _mm_load_si128(reinterpret_cast<const __m128i *>(p))
#define LOADU(p) _mm_loadu_si128(reinterpret_cast<const __m128i *>(p))
#define CMPEQ(a, b) _mm_cmpeq_epi8((a), (b))
#define MINU(a, b) _mm_min_epu8((a), (b))
#define OR(a, b) _mm_or_si128((a), (b))
#define MOVEMASK(v) static_cast<uint32_t>(_mm_movemask_epi8(v))
#endif
#define mask_t uint32_t
#define CTZ(m) static_cast<size_t>(__builtin_ctz(m))
#if defined(__BMI2__)
#define MASK_LO(n) _bzhi_u32(~0u, static_cast<unsigned>(n))
#else
// Only used with n < VEC <= 16 here, so the shift is defined.
#define MASK_LO(n) ((1u << static_cast<unsigned>(n)) - 1u)
#endif
#define CHSTR_ATTR CH_NO_SSP
#define FN(name) name##_baseline
#include "str_functions_impl.h"
#undef FN
#undef CHSTR_ATTR
#undef MASK_LO
#undef CTZ
#undef mask_t
#undef MOVEMASK
#undef OR
#undef MINU
#undef CMPEQ
#undef LOADU
#undef LOAD
#undef SPLAT
#undef vec_t
#undef VEC
#undef ALIGN_DOWN4
#undef ALIGN_DOWN

// ---------------------------------------------------------------------------
// Dispatch: a function pointer initialised to a resolver that runs cpuid once
// and swaps itself out (glibc-IFUNC shape; LLVM will not tail-call from a v3
// function into a v4 clone, so an `if (avx512)` branch would cost a frame).
// ---------------------------------------------------------------------------
using memchr_fn = void *(*)(const void *, int, size_t) noexcept;
using strlen_fn = size_t (*)(const char *) noexcept;

void *memchr_resolve(const void *s, int c, size_t n) noexcept;
size_t strlen_resolve(const char *s) noexcept;

memchr_fn memchr_ptr = memchr_resolve;
strlen_fn strlen_ptr = strlen_resolve;

void *memchr_resolve(const void *s, int c, size_t n) noexcept {
  const memchr_fn fn =
      cpu_level() >= CPU_AVX512 ? memchr_avx512 : memchr_baseline;
  __atomic_store_n(&memchr_ptr, fn, __ATOMIC_RELAXED);
  return fn(s, c, n);
}

size_t strlen_resolve(const char *s) noexcept {
  const strlen_fn fn =
      cpu_level() >= CPU_AVX512 ? strlen_avx512 : strlen_baseline;
  __atomic_store_n(&strlen_ptr, fn, __ATOMIC_RELAXED);
  return fn(s);
}

} // namespace

extern "C" CH_NO_SSP void *memchr(const void *s, int c, size_t n) noexcept {
  return __atomic_load_n(&memchr_ptr, __ATOMIC_RELAXED)(s, c, n);
}

extern "C" CH_NO_SSP size_t strlen(const char *s) noexcept {
  return __atomic_load_n(&strlen_ptr, __ATOMIC_RELAXED)(s);
}

#endif // __x86_64__
