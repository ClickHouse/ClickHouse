// Instantiates the vector tiers of the x86_64 `memchr` and `strlen` from
// str_functions_impl.h as functions `memchr_<tier>` / `strlen_<tier>` in an
// anonymous namespace. Included by str_functions.cpp (the exported libc
// symbols) and by str_functions_test_candidates.cpp (every tier under its own
// name, for the unit test), so both test and ship the same bodies.
//
// The includer selects the tiers by defining, before the include:
//   always            `_avx512`: 64-byte vectors and mask registers, built as a
//                     `CH_AVX512_CLONE` whatever the target of the TU.
//   CHSTR_AVX2_ATTR   `_avx2`: 32-byte vectors, with these function attributes
//                     (they must enable AVX2 unless the TU already does).
//   CHSTR_SSE2_ATTR   `_sse2`: 16-byte vectors, with these function attributes.

#pragma once

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
// SSE2 and AVX2: movemask into a general-purpose register.
// ---------------------------------------------------------------------------
#define mask_t uint32_t
#define CTZ(m) static_cast<size_t>(__builtin_ctz(m))
#if defined(__BMI2__)
#define MASK_LO(n) _bzhi_u32(~0u, static_cast<unsigned>(n))
#else
// Only used with n < VEC <= 32 here, so the shift is defined.
#define MASK_LO(n) ((1u << static_cast<unsigned>(n)) - 1u)
#endif

#if defined(CHSTR_AVX2_ATTR)
#define VEC 32
#define vec_t __m256i
#define SPLAT(c) _mm256_set1_epi8(static_cast<char>(c))
#define LOAD(p) _mm256_load_si256(reinterpret_cast<const __m256i *>(p))
#define LOADU(p) _mm256_loadu_si256(reinterpret_cast<const __m256i *>(p))
#define CMPEQ(a, b) _mm256_cmpeq_epi8((a), (b))
#define MINU(a, b) _mm256_min_epu8((a), (b))
#define OR(a, b) _mm256_or_si256((a), (b))
#define MOVEMASK(v) static_cast<uint32_t>(_mm256_movemask_epi8(v))
#define CHSTR_ATTR CHSTR_AVX2_ATTR
#define FN(name) name##_avx2
#include "str_functions_impl.h"
#undef FN
#undef CHSTR_ATTR
#undef MOVEMASK
#undef OR
#undef MINU
#undef CMPEQ
#undef LOADU
#undef LOAD
#undef SPLAT
#undef vec_t
#undef VEC
#endif

#if defined(CHSTR_SSE2_ATTR)
#define VEC 16
#define vec_t __m128i
#define SPLAT(c) _mm_set1_epi8(static_cast<char>(c))
#define LOAD(p) _mm_load_si128(reinterpret_cast<const __m128i *>(p))
#define LOADU(p) _mm_loadu_si128(reinterpret_cast<const __m128i *>(p))
#define CMPEQ(a, b) _mm_cmpeq_epi8((a), (b))
#define MINU(a, b) _mm_min_epu8((a), (b))
#define OR(a, b) _mm_or_si128((a), (b))
#define MOVEMASK(v) static_cast<uint32_t>(_mm_movemask_epi8(v))
#define CHSTR_ATTR CHSTR_SSE2_ATTR
#define FN(name) name##_sse2
#include "str_functions_impl.h"
#undef FN
#undef CHSTR_ATTR
#undef MOVEMASK
#undef OR
#undef MINU
#undef CMPEQ
#undef LOADU
#undef LOAD
#undef SPLAT
#undef vec_t
#undef VEC
#endif

#undef MASK_LO
#undef CTZ
#undef mask_t
#undef ALIGN_DOWN4
#undef ALIGN_DOWN

} // namespace
