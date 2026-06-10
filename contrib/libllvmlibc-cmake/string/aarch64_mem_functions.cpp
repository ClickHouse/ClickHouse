// ClickHouse-side aarch64 implementations of `memmove` and `memmem`.
//
// `memcpy` and `memset` deliberately do NOT come from this file: musl ships
// the Arm Optimized Routines assembly for them
// (`src/string/aarch64/{memcpy,memset}.S`) — the same code glibc builds as
// `__memcpy_generic` — and it beats LLVM-libc's generic C++ tiers by 10-30%
// on the small/medium copies that dominate ClickHouse column operations
// (`position_empty_needle`, `column_array_replicate`, `int_parsing` CI perf
// regressions). See contrib/musl-cmake/CMakeLists.txt.
//
// musl has no aarch64 assembly for `memmove` (and its C fallback is a byte
// loop), so memmove is provided here: overlap-safe NEON head-tail tiers up to
// 128 bytes, a tail call to the AOR `memcpy` for larger disjoint buffers, and
// a DST-aligned 64-byte-per-iteration NEON loop for genuine overlaps.

#include "src/__support/common.h"
#include "src/__support/macros/config.h"
#include "src/__support/macros/null_check.h"
#include "src/string/memmove.h"
#include "src/string/memory_utils/inline_memmove.h" // is_disjoint
#include "src/string/memory_utils/op_aarch64.h"
#include "src/string/memory_utils/op_builtin.h"
#include "src/string/memory_utils/op_generic.h"
#include "src/string/memory_utils/utils.h"

#include <stddef.h>

// musl's Arm Optimized Routines assembly (the only aarch64 definition).
extern "C" void *memcpy(void *__restrict, const void *__restrict, size_t);

namespace LIBC_NAMESPACE_DECL {

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
  // Disjoint buffers take the plain copy path — a tail call into musl's Arm
  // Optimized Routines `memcpy` (the only `memcpy` definition on aarch64;
  // the TU is compiled with -fno-builtin so this is a real call, and memcpy
  // never calls memmove back).
  if (is_disjoint(dst, src, count))
    return ::memcpy(dst, src, count);
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
