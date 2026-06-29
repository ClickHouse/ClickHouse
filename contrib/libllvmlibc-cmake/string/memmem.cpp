// The twoway_memmem function below is ported from musl-libc.
//   https://www.musl-libc.org/
//   https://git.musl-libc.org/cgit/musl/tree/src/string/memmem.c
/*
musl as a whole is licensed under the following standard MIT license:

----------------------------------------------------------------------
Copyright © 2005-2014 Rich Felker, et al.

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
----------------------------------------------------------------------
*/

// Strong `memmem`, overriding musl's Two-Way in the static link. Lives in its
// own TU so a reference to it does not drag in mem_functions.cpp's weak `memcpy`.
//
// Short needles use a SIMD "two-byte filter": broadcast the first and last
// needle byte, compare both against the haystack per stride, AND the masks and
// verify each survivor with memcmp. This beats Two-Way 3-10x on the short
// found-needle workloads that dominate ClickHouse. The verification degenerates
// to O(haystack*needle) on low-selectivity needles, so needles >=
// TWO_WAY_THRESHOLD are routed to Two-Way (linear time, no worst case) instead.

#include <cstddef>
#include <cstdint>

namespace {

constexpr size_t TWO_WAY_THRESHOLD = 32;

// Two-Way (Crochemore-Perrin) string matching, ported from musl libc (MIT).
// Linear time with no degenerate case. Used for needles >= TWO_WAY_THRESHOLD,
// where the SIMD filter would lose its linear bound.
void *twoway_memmem(const unsigned char *h, const unsigned char *z,
                    const unsigned char *n, size_t l) {
  constexpr size_t WORD_BITS = 8 * sizeof(size_t);
  size_t k;
  size_t byteset[32 / sizeof(size_t)] = {0};
  size_t shift[256];

  // Compute the shift table and byteset.
  for (size_t i = 0; i < l; i++) {
    byteset[n[i] / WORD_BITS] |= size_t{1} << (n[i] % WORD_BITS);
    shift[n[i]] = i + 1;
  }

  // Maximal suffix.
  size_t ip = static_cast<size_t>(-1);
  size_t jp = 0;
  size_t p = 1;
  k = 1;
  while (jp + k < l) {
    if (n[ip + k] == n[jp + k]) {
      if (k == p) {
        jp += p;
        k = 1;
      } else {
        k++;
      }
    } else if (n[ip + k] > n[jp + k]) {
      jp += k;
      k = 1;
      p = jp - ip;
    } else {
      ip = jp++;
      k = p = 1;
    }
  }
  size_t ms = ip;
  const size_t p0 = p;

  // Maximal suffix with the opposite comparison.
  ip = static_cast<size_t>(-1);
  jp = 0;
  k = p = 1;
  while (jp + k < l) {
    if (n[ip + k] == n[jp + k]) {
      if (k == p) {
        jp += p;
        k = 1;
      } else {
        k++;
      }
    } else if (n[ip + k] < n[jp + k]) {
      jp += k;
      k = 1;
      p = jp - ip;
    } else {
      ip = jp++;
      k = p = 1;
    }
  }
  if (ip + 1 > ms + 1)
    ms = ip;
  else
    p = p0;

  // Periodic needle?
  size_t mem0 = 0;
  if (__builtin_memcmp(n, n + p, ms + 1) != 0)
    p = (ms > l - ms - 1 ? ms : l - ms - 1) + 1;
  else
    mem0 = l - p;
  size_t mem = 0;

  for (;;) {
    if (static_cast<size_t>(z - h) < l)
      return nullptr;

    // Check the last byte first; advance by the shift on a mismatch.
    const unsigned char last = h[l - 1];
    if (byteset[last / WORD_BITS] & (size_t{1} << (last % WORD_BITS))) {
      k = l - shift[last];
      if (k) {
        if (k < mem)
          k = mem;
        h += k;
        mem = 0;
        continue;
      }
    } else {
      h += l;
      mem = 0;
      continue;
    }

    // Compare the right half.
    for (k = (ms + 1 > mem ? ms + 1 : mem); k < l && n[k] == h[k]; k++)
      ;
    if (k < l) {
      h += k - ms;
      mem = 0;
      continue;
    }
    // Compare the left half.
    for (k = ms + 1; k > mem && n[k - 1] == h[k - 1]; k--)
      ;
    if (k <= mem)
      return const_cast<unsigned char *>(h);
    h += p;
    mem = mem0;
  }
}

} // namespace

#if defined(__x86_64__)

#include "x86_cpu_dispatch.h"

#include <immintrin.h>

namespace {

// AVX-512BW: 64 candidate positions per iteration via mask registers.
CH_AVX512_CLONE void *memmem_avx512(const char *h, const char *n, size_t nlen,
                                    size_t end) {
  const __m512i first = _mm512_set1_epi8(n[0]);
  const __m512i last = _mm512_set1_epi8(n[nlen - 1]);
  size_t i = 0;
  while (i + 64 <= end) {
    __m512i bf = _mm512_loadu_si512(reinterpret_cast<const __m512i *>(h + i));
    __m512i bl =
        _mm512_loadu_si512(reinterpret_cast<const __m512i *>(h + i + nlen - 1));
    __mmask64 mask =
        _mm512_cmpeq_epi8_mask(bf, first) & _mm512_cmpeq_epi8_mask(bl, last);
    while (mask) {
      const int pos = __builtin_ctzll(mask);
      if (nlen <= 2 || __builtin_memcmp(h + i + pos + 1, n + 1, nlen - 2) == 0)
        return const_cast<char *>(h + i + pos);
      mask &= mask - 1;
    }
    i += 64;
  }
  for (; i < end; ++i) {
    if (h[i] == n[0] && h[i + nlen - 1] == n[nlen - 1] &&
        (nlen <= 2 || __builtin_memcmp(h + i + 1, n + 1, nlen - 2) == 0))
      return const_cast<char *>(h + i);
  }
  return nullptr;
}

// AVX2: 32 candidate positions per iteration via vpmovmskb (the v3 default).
__attribute__((target("arch=x86-64-v3"), noinline, flatten)) void *
memmem_avx2(const char *h, const char *n, size_t nlen, size_t end) {
  const __m256i first = _mm256_set1_epi8(n[0]);
  const __m256i last = _mm256_set1_epi8(n[nlen - 1]);
  size_t i = 0;
  while (i + 32 <= end) {
    __m256i bf = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(h + i));
    __m256i bl =
        _mm256_loadu_si256(reinterpret_cast<const __m256i *>(h + i + nlen - 1));
    uint32_t mask = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_and_si256(
        _mm256_cmpeq_epi8(bf, first), _mm256_cmpeq_epi8(bl, last))));
    while (mask) {
      const int pos = __builtin_ctz(mask);
      if (nlen <= 2 || __builtin_memcmp(h + i + pos + 1, n + 1, nlen - 2) == 0)
        return const_cast<char *>(h + i + pos);
      mask &= mask - 1;
    }
    i += 32;
  }
  for (; i < end; ++i) {
    if (h[i] == n[0] && h[i + nlen - 1] == n[nlen - 1] &&
        (nlen <= 2 || __builtin_memcmp(h + i + 1, n + 1, nlen - 2) == 0))
      return const_cast<char *>(h + i);
  }
  return nullptr;
}

// Scalar fallback: pre-AVX2 hardware or calls before CPU detection runs.
void *memmem_scalar(const char *h, const char *n, size_t nlen, size_t end) {
  for (size_t i = 0; i < end; ++i) {
    if (h[i] == n[0] && h[i + nlen - 1] == n[nlen - 1] &&
        (nlen <= 2 || __builtin_memcmp(h + i + 1, n + 1, nlen - 2) == 0))
      return const_cast<char *>(h + i);
  }
  return nullptr;
}

void *memmem_filter(const char *h, const char *n, size_t nlen, size_t end) {
  const uint32_t level = cpu_level();
  if (level >= CPU_AVX512)
    return memmem_avx512(h, n, nlen, end);
  if (level >= CPU_AVX2)
    return memmem_avx2(h, n, nlen, end);
  return memmem_scalar(h, n, nlen, end);
}

} // namespace

#elif defined(__aarch64__) && defined(__ARM_NEON)

#include <arm_neon.h>

namespace {

// Collapse a uint8x16_t lane mask (each lane 0x00 or 0xFF) to a uint64_t where
// each nibble represents one input byte (Lemire's shrn trick). ctzll on the
// result gives 4 * lane_index of the first set bit.
__attribute__((always_inline)) inline uint64_t neon_mask_to_u64(uint8x16_t m) {
  uint8x8_t narrowed = vshrn_n_u16(vreinterpretq_u16_u8(m), 4);
  return vget_lane_u64(vreinterpret_u64_u8(narrowed), 0);
}

void *memmem_filter(const char *h, const char *n, size_t nlen, size_t end) {
  const uint8x16_t first = vdupq_n_u8(static_cast<unsigned char>(n[0]));
  const uint8x16_t last = vdupq_n_u8(static_cast<unsigned char>(n[nlen - 1]));
  size_t i = 0;
  while (i + 16 <= end) {
    uint8x16_t bf = vld1q_u8(reinterpret_cast<const uint8_t *>(h + i));
    uint8x16_t bl = vld1q_u8(reinterpret_cast<const uint8_t *>(h + i + nlen - 1));
    uint8x16_t mask = vandq_u8(vceqq_u8(bf, first), vceqq_u8(bl, last));
    uint64_t m = neon_mask_to_u64(mask);
    while (m) {
      int pos = __builtin_ctzll(m) >> 2; // 4 bits per input byte
      if (nlen <= 2 || __builtin_memcmp(h + i + pos + 1, n + 1, nlen - 2) == 0)
        return const_cast<char *>(h + i + pos);
      m &= ~(uint64_t{0xF} << (pos * 4)); // clear this nibble
    }
    i += 16;
  }
  for (; i < end; ++i) {
    if (h[i] == n[0] && h[i + nlen - 1] == n[nlen - 1] &&
        (nlen <= 2 || __builtin_memcmp(h + i + 1, n + 1, nlen - 2) == 0))
      return const_cast<char *>(h + i);
  }
  return nullptr;
}

} // namespace

#else

namespace {

void *memmem_filter(const char *h, const char *n, size_t nlen, size_t end) {
  for (size_t i = 0; i < end; ++i) {
    if (h[i] == n[0] && h[i + nlen - 1] == n[nlen - 1] &&
        (nlen <= 2 || __builtin_memcmp(h + i + 1, n + 1, nlen - 2) == 0))
      return const_cast<char *>(h + i);
  }
  return nullptr;
}

} // namespace

#endif

extern "C" void *memmem(const void *haystack, size_t hlen, const void *needle,
                        size_t nlen) noexcept {
  if (nlen == 0)
    return const_cast<void *>(haystack);
  if (hlen < nlen)
    return nullptr;
  const char *h = static_cast<const char *>(haystack);
  const char *n = static_cast<const char *>(needle);
  if (nlen == 1)
    return __builtin_memchr(haystack, n[0], hlen);
  // Long needles go straight to Two-Way; the SIMD filter's per-candidate
  // verification would lose its linear bound on low-selectivity needles.
  if (nlen >= TWO_WAY_THRESHOLD)
    return twoway_memmem(reinterpret_cast<const unsigned char *>(h),
                         reinterpret_cast<const unsigned char *>(h) + hlen,
                         reinterpret_cast<const unsigned char *>(n), nlen);
  return memmem_filter(h, n, nlen, hlen - nlen + 1);
}
