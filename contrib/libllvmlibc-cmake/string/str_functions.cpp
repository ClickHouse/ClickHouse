// Strong x86_64 `memchr` and `strlen`, overriding musl's generic C versions in
// the static link (musl ships x86_64 assembly only for memcpy/memmove/memset;
// LLVM-libc's are byte and word loops). glibc uses EVEX/AVX2 assembly for both,
// and the CSV parser showed musl's word-at-a-time `memchr` taking twice the
// samples.
//
// Two tiers from one body (str_functions_impl.h, instantiated by
// str_functions_tiers.h): the build's baseline (AVX2 at
// x86-64-v3, SSE2 in the compat build) and an AVX-512BW clone with 64-byte
// vectors and mask registers, selected once at runtime through a function
// pointer, as in x86_64_mem_functions.cpp. The 64-byte tier is what matches
// glibc's `__memchr_evex512` above 4 KiB; 32-byte vectors cap at 1.6x its time
// because of load width. Measured against glibc, musl, LLVM-libc, FreeBSD,
// bionic and StringZilla in libc-func-bench (2026-10-21, Zen 5).
//
// Not built under sanitizers: their interceptors define strong `memchr` /
// `strlen` and the wrapped libc versions carry the range checks. The unit test
// still exercises every tier there, through the uninstrumented, separately
// named copies in str_functions_test_candidates.cpp.

#if defined(__x86_64__)

// The build's baseline tier: AVX2 at x86-64-v3 and up, SSE2 in the compat build.
#if defined(__AVX2__)
#define CHSTR_AVX2_ATTR CH_NO_SSP
#define BASELINE(name) name##_avx2
#else
#define CHSTR_SSE2_ATTR CH_NO_SSP
#define BASELINE(name) name##_sse2
#endif
#include "str_functions_tiers.h"

namespace {

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
      cpu_level() >= CPU_AVX512 ? memchr_avx512 : BASELINE(memchr);
  __atomic_store_n(&memchr_ptr, fn, __ATOMIC_RELAXED);
  return fn(s, c, n);
}

size_t strlen_resolve(const char *s) noexcept {
  const strlen_fn fn =
      cpu_level() >= CPU_AVX512 ? strlen_avx512 : BASELINE(strlen);
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
