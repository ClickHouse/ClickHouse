// Strong x86_64 `memchr` and `strlen`, overriding musl's generic C versions in
// the static link (musl ships x86_64 assembly only for memcpy/memmove/memset;
// LLVM-libc's are byte loops). glibc uses EVEX/AVX2 assembly for both, and the
// CSV parser showed musl's word-at-a-time `memchr` taking twice the samples.
//
// Vector loads are done on aligned addresses only, and the unrolled loop on
// addresses aligned to its whole stride, so no load ever crosses a page
// boundary: reading past the buffer or past the first match stays inside a
// page that is known to be mapped, and the bytes outside [s, s + n) are masked
// off before the result is used. Not built
// under sanitizers: their interceptors define strong `memchr`/`strlen` and the
// wrapped libc versions carry the range checks.

#if defined(__x86_64__)

#include <immintrin.h>

#include <cstddef>
#include <cstdint>

namespace {

#if defined(__AVX2__)

constexpr size_t VEC = 32;
using vec_t = __m256i;

__attribute__((always_inline)) inline vec_t splat(unsigned char c) {
  return _mm256_set1_epi8(static_cast<char>(c));
}
__attribute__((always_inline)) inline vec_t eq_vec(const void *aligned,
                                                   vec_t needle) {
  vec_t v = _mm256_load_si256(static_cast<const vec_t *>(aligned));
  return _mm256_cmpeq_epi8(v, needle);
}
__attribute__((always_inline)) inline vec_t or_vec(vec_t a, vec_t b) {
  return _mm256_or_si256(a, b);
}
__attribute__((always_inline)) inline uint64_t to_mask(vec_t v) {
  return static_cast<uint32_t>(_mm256_movemask_epi8(v));
}

#else // x86-64-v2 compat build: SSE2

constexpr size_t VEC = 16;
using vec_t = __m128i;

__attribute__((always_inline)) inline vec_t splat(unsigned char c) {
  return _mm_set1_epi8(static_cast<char>(c));
}
__attribute__((always_inline)) inline vec_t eq_vec(const void *aligned,
                                                   vec_t needle) {
  vec_t v = _mm_load_si128(static_cast<const vec_t *>(aligned));
  return _mm_cmpeq_epi8(v, needle);
}
__attribute__((always_inline)) inline vec_t or_vec(vec_t a, vec_t b) {
  return _mm_or_si128(a, b);
}
__attribute__((always_inline)) inline uint64_t to_mask(vec_t v) {
  return static_cast<uint32_t>(_mm_movemask_epi8(v));
}

#endif

__attribute__((always_inline)) inline uint64_t eq_mask(const void *aligned,
                                                       vec_t needle) {
  return to_mask(eq_vec(aligned, needle));
}

// Scan 4 * VEC bytes starting at the aligned `a`: one branch per iteration on
// the OR of the four compares (glibc unrolls the same way), then locate the
// first match only when there is one. Returns nullptr if none.
__attribute__((always_inline)) inline const char *scan4(const char *a,
                                                        vec_t needle) {
  const vec_t e0 = eq_vec(a, needle);
  const vec_t e1 = eq_vec(a + VEC, needle);
  const vec_t e2 = eq_vec(a + 2 * VEC, needle);
  const vec_t e3 = eq_vec(a + 3 * VEC, needle);
  if (to_mask(or_vec(or_vec(e0, e1), or_vec(e2, e3))) == 0)
    return nullptr;
  if (uint64_t m = to_mask(e0))
    return a + __builtin_ctzll(m);
  if (uint64_t m = to_mask(e1))
    return a + VEC + __builtin_ctzll(m);
  if (uint64_t m = to_mask(e2))
    return a + 2 * VEC + __builtin_ctzll(m);
  return a + 3 * VEC + __builtin_ctzll(to_mask(e3));
}

__attribute__((always_inline)) inline const char *align_down(const char *p) {
  return reinterpret_cast<const char *>(reinterpret_cast<uintptr_t>(p) &
                                        ~(uintptr_t{VEC} - 1));
}

} // namespace

extern "C" void *memchr(const void *s, int c, size_t n) noexcept {
  if (n == 0)
    return nullptr;
  const char *p = static_cast<const char *>(s);
  const char *end = p + n;
  const vec_t needle = splat(static_cast<unsigned char>(c));

  // First block: drop the bytes before `p`.
  const char *a = align_down(p);
  uint64_t m = eq_mask(a, needle) >> (p - a);
  // Bytes at or past `end` are not part of the buffer. `end - p` can only be
  // < VEC here if the whole buffer fits into this first block.
  if (static_cast<size_t>(end - p) < VEC)
    m &= (uint64_t{1} << (end - p)) - 1;
  if (m)
    return const_cast<char *>(p + __builtin_ctzll(m));

  a += VEC;
  // Callers may pass an `n` that overstates the buffer and rely on the search
  // stopping at the first match (musl's strnlen passes the caller's limit,
  // realpath's is PATH_MAX past a shorter string), so `end` does not bound the
  // readable memory. Re-align to the 4 * VEC stride before the unrolled loop:
  // a 4 * VEC-aligned group never crosses a page, so it is only ever read
  // beyond the match within the page that holds the match.
  for (; a + VEC <= end && (reinterpret_cast<uintptr_t>(a) & (4 * VEC - 1));
       a += VEC) {
    m = eq_mask(a, needle);
    if (m)
      return const_cast<char *>(a + __builtin_ctzll(m));
  }
  for (; a + 4 * VEC <= end; a += 4 * VEC)
    if (const char *r = scan4(a, needle))
      return const_cast<char *>(r);
  for (; a + VEC <= end; a += VEC) {
    m = eq_mask(a, needle);
    if (m)
      return const_cast<char *>(a + __builtin_ctzll(m));
  }

  if (a < end) {
    m = eq_mask(a, needle) & ((uint64_t{1} << (end - a)) - 1);
    if (m)
      return const_cast<char *>(a + __builtin_ctzll(m));
  }
  return nullptr;
}

extern "C" size_t strlen(const char *s) noexcept {
  const vec_t zero = splat(0);
  const char *a = align_down(s);
  uint64_t m = eq_mask(a, zero) >> (s - a);
  if (m)
    return static_cast<size_t>(__builtin_ctzll(m));
  a += VEC;
  // Re-align to the 4 * VEC stride so the unrolled loads stay within one page.
  for (; reinterpret_cast<uintptr_t>(a) & (4 * VEC - 1); a += VEC) {
    m = eq_mask(a, zero);
    if (m)
      return static_cast<size_t>(a - s) + static_cast<size_t>(__builtin_ctzll(m));
  }
  for (;; a += 4 * VEC)
    if (const char *r = scan4(a, zero))
      return static_cast<size_t>(r - s);
}

#endif // __x86_64__
