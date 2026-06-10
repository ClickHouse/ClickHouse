// ClickHouse-side aarch64 implementations of `memcpy`, `memmove`, `memset`.
//
// Upstream's aarch64 dispatchers (in `src/string/memory_utils/aarch64/`) align
// SRC and choose blocks/tiers that under-use the cache line. Two changes:
//
//   * Align DST instead of SRC for the large-copy main loop. Stores benefit
//     from alignment more than loads on Neoverse / Apple cores (store-buffer
//     coalescing, no STP cracking). Mirrors musl's `aarch64/memcpy.S` and our
//     x86 fix.
//   * Add a 128-byte head-tail tier ahead of the main loop, so copies in the
//     128–256 B range avoid the loop's startup overhead.
//
// Everything is `always_inline` so the public `memcpy` / `memmove` / `memset`
// entries inline the head-tail tiers and the main-loop alignment block
// directly — without that, sizes 128–256 B pay a needless function call.

#include "src/__support/common.h"
#include "src/__support/macros/config.h"
#include "src/__support/macros/null_check.h"
#include "src/string/memcpy.h"
#include "src/string/memmove.h"
#include "src/string/memset.h"
#include "src/string/memory_utils/inline_memmove.h" // is_disjoint
#include "src/string/memory_utils/op_aarch64.h"
#include "src/string/memory_utils/op_builtin.h"
#include "src/string/memory_utils/op_generic.h"
#include "src/string/memory_utils/utils.h"

#include <stddef.h>

namespace LIBC_NAMESPACE_DECL {

// ============================================================================
// memcpy
// ============================================================================

// Same small-size tiers as upstream `inline_memcpy_aarch64`. The only change is
// the main-loop preamble: align `dst` to 32 bytes (Arg::Dst) instead of `src`
// to 16 (Arg::Src). Stores benefit from alignment more than loads on Neoverse
// / Apple cores; this matches what musl's hand-written aarch64 memcpy does.
__attribute__((flatten, always_inline)) LIBC_INLINE void
ch_inline_memcpy_aarch64(Ptr __restrict dst, CPtr __restrict src,
                         size_t count) {
  if (count == 0)
    return;
  if (count == 1)
    return builtin::Memcpy<1>::block(dst, src);
  if (count == 2)
    return builtin::Memcpy<2>::block(dst, src);
  if (count == 3)
    return builtin::Memcpy<3>::block(dst, src);
  if (count == 4)
    return builtin::Memcpy<4>::block(dst, src);
  if (count < 8)
    return builtin::Memcpy<4>::head_tail(dst, src, count);
  if (count < 16)
    return builtin::Memcpy<8>::head_tail(dst, src, count);
  if (count < 32)
    return builtin::Memcpy<16>::head_tail(dst, src, count);
  if (count < 64)
    return builtin::Memcpy<32>::head_tail(dst, src, count);
  if (count < 128)
    return builtin::Memcpy<64>::head_tail(dst, src, count);
  // count >= 128: align dst to 32 bytes, then 64-B/iter main loop.
  builtin::Memcpy<32>::block(dst, src);
  align_to_next_boundary<32, Arg::Dst>(dst, src, count);
  return builtin::Memcpy<64>::loop_and_tail(dst, src, count);
}

LLVM_LIBC_FUNCTION(void *, memcpy,
                   (void *__restrict dst, const void *__restrict src,
                    size_t size)) {
  if (size) {
    LIBC_CRASH_ON_NULLPTR(dst);
    LIBC_CRASH_ON_NULLPTR(src);
  }
  ch_inline_memcpy_aarch64(reinterpret_cast<Ptr>(dst),
                           reinterpret_cast<CPtr>(src), size);
  return dst;
}

// ============================================================================
// memset
// ============================================================================

__attribute__((flatten, always_inline)) LIBC_INLINE void
ch_inline_memset_aarch64(Ptr dst, uint8_t value, size_t count) {
  using uint128_t = generic_v128;
  using uint256_t = generic_v256;
  using uint512_t = generic_v512;
  if (count == 0)
    return;
  if (count <= 3) {
    generic::Memset<uint8_t>::block(dst, value);
    if (count > 1)
      generic::Memset<uint16_t>::tail(dst, value, count);
    return;
  }
  if (count <= 8)
    return generic::Memset<uint32_t>::head_tail(dst, value, count);
  if (count <= 16)
    return generic::Memset<uint64_t>::head_tail(dst, value, count);
  if (count <= 32)
    return generic::Memset<uint128_t>::head_tail(dst, value, count);
  if (count <= 64)
    return generic::Memset<uint256_t>::head_tail(dst, value, count);
  if (count <= 128)
    return generic::Memset<uint512_t>::head_tail(dst, value, count);
#if defined(__ARM_NEON)
  // Use neon `dc zva` for big aligned zero-fills when available.
  if (count >= 448 && value == 0 && aarch64::neon::hasZva()) {
    generic::Memset<uint512_t>::block(dst, 0);
    align_to_next_boundary<64>(dst, count);
    return aarch64::neon::BzeroCacheLine::loop_and_tail(dst, 0, count);
  }
#endif
  // count > 128: align dst to 32 bytes, then 64-B/iter ZMM-equivalent loop.
  generic::Memset<uint256_t>::block(dst, value);
  align_to_next_boundary<32>(dst, count);
  return generic::Memset<uint512_t>::loop_and_tail(dst, value, count);
}

LLVM_LIBC_FUNCTION(void *, memset,
                   (void *dst, int value, size_t count)) {
  if (count)
    LIBC_CRASH_ON_NULLPTR(dst);
  ch_inline_memset_aarch64(reinterpret_cast<Ptr>(dst),
                           static_cast<uint8_t>(value), count);
  return dst;
}

// ============================================================================
// memmove
// ============================================================================

// Upstream aligns SRC to 32 bytes before the main loop; we align DST for the
// same store-side reason as memcpy.
//
// Alignment SIZE must be the *smaller* of the two (use uint256_t / 32 B)
// because `align_forward`/`align_backward` consume up to 2*SIZE bytes from
// `count`. Using uint512_t (64 B) would consume up to 128 B and leave the
// main loop with count < SIZE — corrupting the trailing bytes via a
// do-while iteration that runs once even when the loop condition is false.
__attribute__((flatten, always_inline)) LIBC_INLINE void
ch_inline_memmove_follow_up_aarch64(Ptr dst, CPtr src, size_t count) {
  using uint256_t = generic_v256;
  using uint512_t = generic_v512;
  if (dst < src) {
    generic::Memmove<uint256_t>::align_forward<Arg::Dst>(dst, src, count);
    return generic::Memmove<uint512_t>::loop_and_tail_forward(dst, src, count);
  } else {
    generic::Memmove<uint256_t>::align_backward<Arg::Dst>(dst, src, count);
    return generic::Memmove<uint512_t>::loop_and_tail_backward(dst, src, count);
  }
}

LLVM_LIBC_FUNCTION(void *, memmove,
                   (void *dst, const void *src, size_t count)) {
  using uint128_t = generic_v128;
  using uint256_t = generic_v256;
  using uint512_t = generic_v512;
  if (count) {
    LIBC_CRASH_ON_NULLPTR(dst);
    LIBC_CRASH_ON_NULLPTR(src);
  }
  Ptr dst_p = reinterpret_cast<Ptr>(dst);
  CPtr src_p = reinterpret_cast<CPtr>(src);
  // Small sizes — same tiers as upstream (already optimal for aarch64).
  if (count == 0)
    return dst;
  if (count == 1) {
    generic::Memmove<uint8_t>::block(dst_p, src_p);
    return dst;
  }
  if (count <= 4) {
    generic::Memmove<uint16_t>::head_tail(dst_p, src_p, count);
    return dst;
  }
  if (count <= 8) {
    generic::Memmove<uint32_t>::head_tail(dst_p, src_p, count);
    return dst;
  }
  if (count <= 16) {
    generic::Memmove<uint64_t>::head_tail(dst_p, src_p, count);
    return dst;
  }
  if (count <= 32) {
    generic::Memmove<uint128_t>::head_tail(dst_p, src_p, count);
    return dst;
  }
  if (count <= 64) {
    generic::Memmove<uint256_t>::head_tail(dst_p, src_p, count);
    return dst;
  }
  if (count <= 128) {
    generic::Memmove<uint512_t>::head_tail(dst_p, src_p, count);
    return dst;
  }
  // Disjoint case routes through our (improved) memcpy to pick up the new
  // dst-aligned main loop.
  if (is_disjoint(dst, src, count))
    ch_inline_memcpy_aarch64(dst_p, src_p, count);
  else
    ch_inline_memmove_follow_up_aarch64(dst_p, src_p, count);
  return dst;
}

} // namespace LIBC_NAMESPACE_DECL

// ============================================================================
// memmem (NEON-accelerated two-byte filter)
// ============================================================================
//
// Same algorithm as the x86 AVX-512 path in x86_64_mem_functions.cpp, ported
// to aarch64 NEON. Operates on 16-byte chunks (one Q-reg), uses a
// `shrn`-based mask trick (per Daniel Lemire's "shuf2" technique) to collapse
// the 16-byte cmpeq result to a 64-bit mask that we can `ctz` over.
//
// Strong symbol — overrides musl's `memmem` in the static link.

#if defined(__ARM_NEON)
#include <arm_neon.h>

// Collapse a uint8x16_t mask (each lane is 0x00 or 0xFF) to a uint64_t where
// each nibble represents one input byte (Lemire). __builtin_ctzll on the
// result gives 4 * lane_index of the first set bit.
__attribute__((always_inline)) static inline uint64_t
neon_mask_to_u64(uint8x16_t m) noexcept
{
    uint8x8_t narrowed = vshrn_n_u16(vreinterpretq_u16_u8(m), 4);
    return vget_lane_u64(vreinterpret_u64_u8(narrowed), 0);
}

extern "C" void *memmem(const void *haystack, size_t hlen,
                        const void *needle, size_t nlen) noexcept
{
    if (nlen == 0)
        return const_cast<void *>(haystack);
    if (hlen < nlen)
        return nullptr;
    const char *h = static_cast<const char *>(haystack);
    const char *n = static_cast<const char *>(needle);
    if (nlen == 1)
        return const_cast<void *>(__builtin_memchr(haystack, n[0], hlen));

    const uint8x16_t first = vdupq_n_u8(static_cast<unsigned char>(n[0]));
    const uint8x16_t last  = vdupq_n_u8(static_cast<unsigned char>(n[nlen - 1]));
    const size_t end = hlen - nlen + 1;
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
            m &= m - 1;
            m &= m - 1;
            m &= m - 1;
            m &= m - 1; // clear all 4 bits of this nibble
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
#endif // __ARM_NEON
