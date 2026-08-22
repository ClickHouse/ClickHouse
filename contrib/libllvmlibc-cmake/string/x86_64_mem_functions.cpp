// x86_64 `memcpy`/`memmove`/`memset` with runtime AVX-512 dispatch.
//
// The binary baseline is x86-64-v3 (raising it to v4 costs 2-4x elsewhere — see
// PR #97452), so the AVX-512 variants are v4-attributed clones selected at
// runtime. Each large-size op goes through a function pointer initialised to a
// resolver that runs cpuid once, swaps the pointer and forwards (glibc-IFUNC
// shape). A pointer, not an `if (avx512)` branch, because LLVM won't tail-call
// from a v3 function to a v4 callee and `flatten` rejects a direct reference.

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
// Upstream baseline (non-AVX-512) building blocks.
#include "src/string/memory_utils/x86_64/inline_memcpy.h"
#include "src/string/memory_utils/x86_64/inline_memmove.h"
#include "src/string/memory_utils/x86_64/inline_memset.h"

#include "x86_cpu_dispatch.h"

#include <stddef.h>
#include <stdint.h>

namespace LIBC_NAMESPACE_DECL {

// ============================================================================
// memcpy
// ============================================================================

// AVX-512 (ZMM): 64-byte head-tail tiers up to 512 bytes, then a
// 64-byte-aligned 256-byte-per-iteration main loop (align DST, like glibc).
CH_AVX512_CLONE static void *
memcpy_ge64_avx512(Ptr __restrict dst, CPtr __restrict src, size_t count) {
  void *const ret = dst;
  if (count <= 128) {
    builtin::Memcpy<64>::head_tail(dst, src, count);
    return ret;
  }
  if (count <= 256) {
    builtin::Memcpy<128>::head_tail(dst, src, count);
    return ret;
  }
  if (count < 512) {
    builtin::Memcpy<256>::head_tail(dst, src, count);
    return ret;
  }
  builtin::Memcpy<64>::block(dst, src);
  align_to_next_boundary<64, Arg::Dst>(dst, src, count);
  builtin::Memcpy<256>::loop_and_tail(dst, src, count);
  return ret;
}

// Baseline: upstream YMM (or SSE2) implementation.
CH_NO_SSP static void *
memcpy_ge64_baseline(Ptr __restrict dst, CPtr __restrict src, size_t count) {
  void *const ret = dst;
  if constexpr (x86::K_AVX)
    inline_memcpy_x86_avx_ge64(dst, src, count);
  else
    inline_memcpy_x86_sse2_ge64(dst, src, count);
  return ret;
}

using mem_copy_fn = void *(*)(Ptr __restrict, CPtr __restrict, size_t);

static void *memcpy_ge64_resolve(Ptr __restrict dst, CPtr __restrict src,
                                 size_t count);

static mem_copy_fn memcpy_ge64_ptr = memcpy_ge64_resolve;

static void *memcpy_ge64_resolve(Ptr __restrict dst, CPtr __restrict src,
                                 size_t count) {
  const mem_copy_fn fn = cpu_level() >= CPU_AVX512 ? memcpy_ge64_avx512
                                                   : memcpy_ge64_baseline;
  __atomic_store_n(&memcpy_ge64_ptr, fn, __ATOMIC_RELAXED);
  return fn(dst, src, count);
}

// The >=64 B check comes first: calls reaching the libc symbol are dominated by
// runtime-sized bulk copies (compilers inline small fixed-size memcpy).
CH_NO_SSP LLVM_LIBC_FUNCTION(void *, memcpy,
                             (void *__restrict dst_v,
                              const void *__restrict src_v, size_t count)) {
  if (count) {
    LIBC_CRASH_ON_NULLPTR(dst_v);
    LIBC_CRASH_ON_NULLPTR(src_v);
  }
  Ptr dst = reinterpret_cast<Ptr>(dst_v);
  CPtr src = reinterpret_cast<CPtr>(src_v);
  if (count >= 64)
    return __atomic_load_n(&memcpy_ge64_ptr, __ATOMIC_RELAXED)(dst, src, count);
  if (count == 0)
    return dst_v;
  if (count == 1) {
    builtin::Memcpy<1>::block(dst, src);
    return dst_v;
  }
  if (count == 2) {
    builtin::Memcpy<2>::block(dst, src);
    return dst_v;
  }
  if (count == 3) {
    builtin::Memcpy<3>::block(dst, src);
    return dst_v;
  }
  if (count == 4) {
    builtin::Memcpy<4>::block(dst, src);
    return dst_v;
  }
  if (count < 8) {
    builtin::Memcpy<4>::head_tail(dst, src, count);
    return dst_v;
  }
  if (count < 16) {
    builtin::Memcpy<8>::head_tail(dst, src, count);
    return dst_v;
  }
  if (count < 32) {
    builtin::Memcpy<16>::head_tail(dst, src, count);
    return dst_v;
  }
  builtin::Memcpy<32>::head_tail(dst, src, count);
  return dst_v;
}

// ============================================================================
// memset
// ============================================================================

// AVX-512: align dst to 64 bytes, 64-byte ZMM stores.
CH_AVX512_CLONE static void *memset_gt128_avx512(Ptr dst, uint8_t value,
                                                 size_t count) {
  void *const ret = dst;
  builtin::Memset<64>::block(dst, value);
  align_to_next_boundary<64>(dst, count);
  if (count <= 256) {
    builtin::Memset<64>::loop_and_tail(dst, value, count);
    return ret;
  }
  builtin::Memset<256>::loop_and_tail(dst, value, count);
  return ret;
}

// Baseline: upstream 32-byte-aligned YMM loop.
CH_NO_SSP static void *memset_gt128_baseline(Ptr dst, uint8_t value,
                                             size_t count) {
  void *const ret = dst;
  generic::Memset<uint256_t>::block(dst, value);
  align_to_next_boundary<32>(dst, count);
  generic::Memset<uint256_t>::loop_and_tail(dst, value, count);
  return ret;
}

using mem_set_fn = void *(*)(Ptr, uint8_t, size_t);

static void *memset_gt128_resolve(Ptr dst, uint8_t value, size_t count);

static mem_set_fn memset_gt128_ptr = memset_gt128_resolve;

static void *memset_gt128_resolve(Ptr dst, uint8_t value, size_t count) {
  const mem_set_fn fn = cpu_level() >= CPU_AVX512 ? memset_gt128_avx512
                                                  : memset_gt128_baseline;
  __atomic_store_n(&memset_gt128_ptr, fn, __ATOMIC_RELAXED);
  return fn(dst, value, count);
}

CH_NO_SSP LLVM_LIBC_FUNCTION(void *, memset,
                             (void *dst_v, int value_v, size_t count)) {
  if (count)
    LIBC_CRASH_ON_NULLPTR(dst_v);
  Ptr dst = reinterpret_cast<Ptr>(dst_v);
  const uint8_t value = static_cast<uint8_t>(value_v);
  if (count > 128)
    return __atomic_load_n(&memset_gt128_ptr, __ATOMIC_RELAXED)(dst, value,
                                                                count);
  if (count == 0)
    return dst_v;
  if (count == 1) {
    generic::Memset<uint8_t>::block(dst, value);
    return dst_v;
  }
  if (count == 2) {
    generic::Memset<uint16_t>::block(dst, value);
    return dst_v;
  }
  if (count == 3) {
    generic::MemsetSequence<uint16_t, uint8_t>::block(dst, value);
    return dst_v;
  }
  if (count <= 8) {
    generic::Memset<uint32_t>::head_tail(dst, value, count);
    return dst_v;
  }
  if (count <= 16) {
    generic::Memset<uint64_t>::head_tail(dst, value, count);
    return dst_v;
  }
  if (count <= 32) {
    generic::Memset<generic_v128>::head_tail(dst, value, count);
    return dst_v;
  }
  if (count <= 64) {
    generic::Memset<generic_v256>::head_tail(dst, value, count);
    return dst_v;
  }
  generic::Memset<generic_v512>::head_tail(dst, value, count);
  return dst_v;
}

// ============================================================================
// memmove
// ============================================================================

// AVX-512 overlapping move, aligning DST. The alignment type must be the
// smaller generic_v256: aligning with the 64-byte type can consume enough of
// count to leave the generic_v512 loop with count < SIZE and corrupt the tail.
CH_AVX512_CLONE static void *memmove_overlap_avx512(Ptr dst, CPtr src,
                                                    size_t count) {
  void *const ret = dst;
  if (dst < src) {
    generic::Memmove<generic_v256>::align_forward<Arg::Dst>(dst, src, count);
    generic::Memmove<generic_v512>::loop_and_tail_forward(dst, src, count);
  } else {
    generic::Memmove<generic_v256>::align_backward<Arg::Dst>(dst, src, count);
    generic::Memmove<generic_v512>::loop_and_tail_backward(dst, src, count);
  }
  return ret;
}

// Baseline: upstream YMM implementation.
CH_NO_SSP static void *memmove_overlap_baseline(Ptr dst, CPtr src,
                                                size_t count) {
  void *const ret = dst;
  inline_memmove_follow_up_x86(dst, src, count);
  return ret;
}

using mem_move_fn = void *(*)(Ptr, CPtr, size_t);

static void *memmove_overlap_resolve(Ptr dst, CPtr src, size_t count);

static mem_move_fn memmove_overlap_ptr = memmove_overlap_resolve;

static void *memmove_overlap_resolve(Ptr dst, CPtr src, size_t count) {
  const mem_move_fn fn = cpu_level() >= CPU_AVX512 ? memmove_overlap_avx512
                                                   : memmove_overlap_baseline;
  __atomic_store_n(&memmove_overlap_ptr, fn, __ATOMIC_RELAXED);
  return fn(dst, src, count);
}

CH_NO_SSP LLVM_LIBC_FUNCTION(void *, memmove,
                             (void *dst, const void *src, size_t count)) {
  if (count) {
    LIBC_CRASH_ON_NULLPTR(dst);
    LIBC_CRASH_ON_NULLPTR(src);
  }
  Ptr dst_p = reinterpret_cast<Ptr>(dst);
  CPtr src_p = reinterpret_cast<CPtr>(src);
  if (count > 128) {
    // Disjoint buffers take the plain copy path (count > 128 satisfies the
    // copy pointer's >=64 B requirement). Both are tail calls.
    if (is_disjoint(dst, src, count))
      return __atomic_load_n(&memcpy_ge64_ptr, __ATOMIC_RELAXED)(dst_p, src_p,
                                                                 count);
    return __atomic_load_n(&memmove_overlap_ptr, __ATOMIC_RELAXED)(dst_p, src_p,
                                                                   count);
  }
  // Sizes <= 128 B: overlap-safe head-tail tiers using native vector types (the
  // upstream cpp::array<v256, 2> temporaries spill; generic_v512 stays in regs).
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
    generic::Memmove<generic_v128>::head_tail(dst_p, src_p, count);
    return dst;
  }
  if (count <= 64) {
    generic::Memmove<generic_v256>::head_tail(dst_p, src_p, count);
    return dst;
  }
  generic::Memmove<generic_v512>::head_tail(dst_p, src_p, count);
  return dst;
}

} // namespace LIBC_NAMESPACE_DECL
