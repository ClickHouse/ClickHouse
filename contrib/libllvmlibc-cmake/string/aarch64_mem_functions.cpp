// aarch64 `memmove`. `memcpy`/`memset` come from musl's Arm Optimized Routines
// assembly (see contrib/musl-cmake/CMakeLists.txt); musl has no aarch64
// `memmove`, so it is provided here, tail-calling the AOR `memcpy` for large
// disjoint buffers and using a DST-aligned NEON loop for genuine overlaps.

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

// Align DST (stores benefit more). The alignment type must be the smaller
// uint256_t: aligning with the 64-byte type can consume enough of count to
// leave the main loop with count < SIZE and corrupt the tail.
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
  // Disjoint buffers take the plain copy path — a tail call into musl's AOR
  // `memcpy` (the TU is built with -fno-builtin, so this is a real call, and
  // memcpy never calls memmove back).
  if (is_disjoint(dst, src, count))
    return ::memcpy(dst, src, count);
  ch_inline_memmove_follow_up_aarch64(dst_p, src_p, count);
  return dst;
}

} // namespace LIBC_NAMESPACE_DECL
