// ClickHouse-side x86_64 implementations of `memcpy`, `memmove`, `memset`.
//
// Adds AVX-512 (ZMM) hot paths that the upstream dispatchers in
// `src/string/memory_utils/x86_64/inline_{memcpy,memmove,memset}.h` lack:
//
//   * memcpy: a `_avx512_ge64` variant with 64-byte head-tail tiers up to
//             512 bytes and a 64-byte-aligned 256-byte-per-iteration main loop.
//   * memset: a `_avx512_gt128` variant that aligns dst to 64 bytes and uses
//             `Memset<uint512_t>::loop_and_tail` (ZMM stores) — upstream main
//             loop is `Memset<uint256_t>` (YMM) even when AVX-512 is on.
//   * memmove (count > 128, non-disjoint): align dst to 64 bytes (upstream
//             aligns src to 32 bytes) and run the ZMM `loop_and_tail_forward`/
//             `_backward`. For disjoint copies we go through our own memcpy
//             path rather than the upstream `inline_memcpy`.
//
// For non-AVX-512 targets (X86_ARCH_LEVEL=2/3) the dispatchers fall back to
// the upstream `_avx_ge64` / `_sse2_ge64` helpers so v2/v3 codegen is
// byte-equivalent to the stock build.
//
// Everything that could be inlined into the public `memcpy` / `memmove` /
// `memset` entries is marked `always_inline` — without that, sizes 64..256 B
// pay a function call and an extra `vzeroupper` on top of the entry epilogue,
// erasing most of the win.

#include "src/__support/common.h"
#include "src/__support/macros/config.h"
#include "src/__support/macros/null_check.h"
#include "src/string/memcpy.h"
#include "src/string/memmove.h"
#include "src/string/memset.h"
#include "src/string/memory_utils/inline_memmove.h" // is_disjoint
#include "src/string/memory_utils/op_builtin.h"
#include "src/string/memory_utils/op_generic.h"
#include "src/string/memory_utils/op_x86.h"
#include "src/string/memory_utils/utils.h"
// Pulls the upstream `_avx_ge64` / `_sse2_ge64` helpers we delegate to when
// AVX-512 is unavailable, and the upstream `inline_memmove_small_size_x86` /
// `inline_memmove_follow_up_x86` definitions.
#include "src/string/memory_utils/x86_64/inline_memcpy.h"
#include "src/string/memory_utils/x86_64/inline_memmove.h"
#include "src/string/memory_utils/x86_64/inline_memset.h"

#include <stddef.h>

namespace LIBC_NAMESPACE_DECL {

// ============================================================================
// memcpy
// ============================================================================

#if defined(__AVX512F__)
[[gnu::always_inline]] LIBC_INLINE void
inline_memcpy_x86_avx512_ge64(Ptr __restrict dst, CPtr __restrict src,
                              size_t count) {
  if (count <= 128)
    return builtin::Memcpy<64>::head_tail(dst, src, count);
  if (count <= 256)
    return builtin::Memcpy<128>::head_tail(dst, src, count);
  if (count < 512)
    return builtin::Memcpy<256>::head_tail(dst, src, count);
  builtin::Memcpy<64>::block(dst, src);
  align_to_next_boundary<64, Arg::Dst>(dst, src, count);
  return builtin::Memcpy<256>::loop_and_tail(dst, src, count);
}
#endif

// Mirror of upstream `inline_memcpy_x86` with an AVX-512 branch inserted ahead
// of the AVX-only branch. Small-size tiers are unchanged from upstream.
[[gnu::flatten, gnu::always_inline]] LIBC_INLINE void
ch_inline_memcpy_x86(Ptr __restrict dst, CPtr __restrict src, size_t count) {
#if defined(__AVX512F__)
  constexpr size_t VECTOR_SIZE = 64;
#elif defined(__AVX__)
  constexpr size_t VECTOR_SIZE = 32;
#elif defined(__SSE2__)
  constexpr size_t VECTOR_SIZE = 16;
#else
  constexpr size_t VECTOR_SIZE = 8;
#endif
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
  if (VECTOR_SIZE >= 16 ? count < 16 : count <= 16)
    return builtin::Memcpy<8>::head_tail(dst, src, count);
  if (VECTOR_SIZE >= 32 ? count < 32 : count <= 32)
    return builtin::Memcpy<16>::head_tail(dst, src, count);
  if (VECTOR_SIZE >= 64 ? count < 64 : count <= 64)
    return builtin::Memcpy<32>::head_tail(dst, src, count);
#if defined(__AVX512F__)
  return inline_memcpy_x86_avx512_ge64(dst, src, count);
#else
  if constexpr (x86::K_AVX)
    return inline_memcpy_x86_avx_ge64(dst, src, count);
  else
    return inline_memcpy_x86_sse2_ge64(dst, src, count);
#endif
}

LLVM_LIBC_FUNCTION(void *, memcpy,
                   (void *__restrict dst, const void *__restrict src,
                    size_t size)) {
  if (size) {
    LIBC_CRASH_ON_NULLPTR(dst);
    LIBC_CRASH_ON_NULLPTR(src);
  }
  ch_inline_memcpy_x86(reinterpret_cast<Ptr>(dst),
                       reinterpret_cast<CPtr>(src), size);
  return dst;
}

// ============================================================================
// memset
// ============================================================================

#if defined(__AVX512F__)
// `Memset<uint512_t>::block` with `uint512_t = generic_v512` materializes as a
// single 64-byte ZMM store. Aligning dst to 64 bytes first gets us aligned
// stores in the main loop — upstream aligns to 32 bytes which leaves every
// other store crossing a cacheline boundary.
[[gnu::always_inline]] LIBC_INLINE void
inline_memset_x86_avx512_gt128(Ptr dst, uint8_t value, size_t count) {
  generic::Memset<uint512_t>::block(dst, value);
  align_to_next_boundary<64>(dst, count);
  return generic::Memset<uint512_t>::loop_and_tail(dst, value, count);
}
#endif

[[gnu::flatten, gnu::always_inline]] LIBC_INLINE void
ch_inline_memset_x86(Ptr dst, uint8_t value, size_t count) {
  if (count == 0)
    return;
  if (count == 1)
    return generic::Memset<uint8_t>::block(dst, value);
  if (count == 2)
    return generic::Memset<uint16_t>::block(dst, value);
  if (count == 3)
    return generic::MemsetSequence<uint16_t, uint8_t>::block(dst, value);
  if (count <= 8)
    return generic::Memset<uint32_t>::head_tail(dst, value, count);
  if (count <= 16)
    return generic::Memset<uint64_t>::head_tail(dst, value, count);
  if (count <= 32)
    return generic::Memset<uint128_t>::head_tail(dst, value, count);
  if (count <= 64)
    return generic::Memset<uint256_t>::head_tail(dst, value, count);
  if constexpr (x86::K_USE_SOFTWARE_PREFETCHING_MEMSET)
    return inline_memset_x86_gt64_sw_prefetching(dst, value, count);
  if (count <= 128)
    return generic::Memset<uint512_t>::head_tail(dst, value, count);
#if defined(__AVX512F__)
  return inline_memset_x86_avx512_gt128(dst, value, count);
#else
  generic::Memset<uint256_t>::block(dst, value);
  align_to_next_boundary<32>(dst, count);
  return generic::Memset<uint256_t>::loop_and_tail(dst, value, count);
#endif
}

LLVM_LIBC_FUNCTION(void *, memset,
                   (void *dst, int value, size_t count)) {
  if (count)
    LIBC_CRASH_ON_NULLPTR(dst);
  ch_inline_memset_x86(reinterpret_cast<Ptr>(dst),
                       static_cast<uint8_t>(value), count);
  return dst;
}

// ============================================================================
// memmove
// ============================================================================

#if defined(__AVX512F__)
// Aligns DST (not SRC like upstream) to 64 bytes so the ZMM stores in the
// loop are aligned. Stores benefit from alignment more than loads on modern
// x86 cores, and 32-byte alignment is no longer "natural" once the unit of
// work is 64 bytes.
[[gnu::always_inline]] LIBC_INLINE void
inline_memmove_follow_up_avx512_x86(Ptr dst, CPtr src, size_t count) {
  if (dst < src) {
    generic::Memmove<uint512_t>::align_forward<Arg::Dst>(dst, src, count);
    return generic::Memmove<uint512_t>::loop_and_tail_forward(dst, src, count);
  } else {
    generic::Memmove<uint512_t>::align_backward<Arg::Dst>(dst, src, count);
    return generic::Memmove<uint512_t>::loop_and_tail_backward(dst, src, count);
  }
}
#endif

[[gnu::flatten, gnu::always_inline]] LIBC_INLINE void
ch_inline_memmove_follow_up_x86(Ptr dst, CPtr src, size_t count) {
#if defined(__AVX512F__)
  return inline_memmove_follow_up_avx512_x86(dst, src, count);
#else
  return inline_memmove_follow_up_x86(dst, src, count);
#endif
}

LLVM_LIBC_FUNCTION(void *, memmove,
                   (void *dst, const void *src, size_t count)) {
  if (count) {
    LIBC_CRASH_ON_NULLPTR(dst);
    LIBC_CRASH_ON_NULLPTR(src);
  }
  Ptr dst_p = reinterpret_cast<Ptr>(dst);
  CPtr src_p = reinterpret_cast<CPtr>(src);
  // Small sizes (≤128 B) are upper-bounded by `Memmove<uint512_t>::head_tail`
  // when AVX-512 is available — already optimal in upstream, so just delegate.
  if (inline_memmove_small_size_x86(dst_p, src_p, count))
    return dst;
  // Disjoint case can go through plain memcpy. Route through *our* dispatcher
  // so the AVX-512 hot path is reached (upstream's `inline_memcpy` would
  // pick the AVX-only `_ge64` variant even on an AVX-512 build).
  if (is_disjoint(dst, src, count))
    ch_inline_memcpy_x86(dst_p, src_p, count);
  else
    ch_inline_memmove_follow_up_x86(dst_p, src_p, count);
  return dst;
}

} // namespace LIBC_NAMESPACE_DECL
