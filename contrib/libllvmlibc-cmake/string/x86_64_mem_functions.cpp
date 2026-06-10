// ClickHouse-side x86_64 implementations of `memcpy`, `memmove`, `memset`,
// `memmem` with **runtime** AVX-512 dispatch.
//
// Why runtime dispatch: the binary baseline is x86-64-v3 (raising
// `X86_ARCH_LEVEL` to 4 disables `ENABLE_MULTITARGET_CODE` in
// src/CMakeLists.txt and deletes the `TargetSpecific::x86_64_v4` kernels,
// causing 2-4x scalar fallbacks across hashes/distances — see PR #97452 perf
// history). So instead of compile-time `__AVX512F__` gating, the AVX-512
// variants here are `__attribute__((target("arch=x86-64-v4,...")))` clones
// selected via a `cpuid` check performed lazily on the first large-size call.
//
// How the clones get ZMM codegen on a v3 TU: `generic_v512` /
// `builtin::Memcpy<N>` lower through target-neutral IR (64-byte clang vector
// types and `__builtin_memcpy_inline`); `__attribute__((flatten))` inlines the
// llvm-libc helpers into the attributed clone, and instruction selection then
// runs with the clone's subtarget (`no-prefer-256-bit` makes clang actually
// pick ZMM over 2xYMM). Verified by disassembly — if you touch these
// functions, re-check that the clones still contain `zmm` registers.
//
// Dispatch cost: one relaxed pointer load plus a predicted indirect jump,
// only on the large-size paths (>=64 B for memcpy, >128 B for
// memset/memmove) — same shape as a resolved glibc IFUNC call.
//
// The AVX-512 shapes mirror glibc's `__memmove_avx512_unaligned_erms` family:
// 64-byte head-tail tiers and a 64-byte-aligned 256-byte-per-iteration main
// loop, aligning DST (stores benefit from alignment more than loads).
//
// memmem replaces musl's `twoway_memmem` (3-10x slower than glibc on short
// found-needle workloads: `cutURLParameter`, `position`, LIKE '%x%') with a
// "two-byte filter" SIMD scan: broadcast first and last needle byte, compare
// both against the haystack per stride, AND the masks, verify candidates with
// memcmp. Loses to Two-Way only on long misses with >=32-byte needles.

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
// Upstream `_avx_ge64` / `_sse2_ge64` helpers used as the baseline paths, and
// the upstream `inline_memmove_small_size_x86` / `inline_memmove_follow_up_x86`
// / `inline_memset_x86` building blocks.
#include "src/string/memory_utils/x86_64/inline_memcpy.h"
#include "src/string/memory_utils/x86_64/inline_memmove.h"
#include "src/string/memory_utils/x86_64/inline_memset.h"

#include <immintrin.h>
#include <stddef.h>
#include <stdint.h>

// ============================================================================
// Runtime CPU feature detection
// ============================================================================

namespace {

// Detected lazily on the first large-size call (no static initializer, so
// mem* needs no .init_array support and the very first call already gets the
// fast path). The benign race — several threads detecting concurrently — has
// all writers storing the same value; relaxed atomics keep it well-defined.
enum CpuLevel : uint32_t {
  CPU_UNKNOWN = 0,
  CPU_BASELINE = 1, // pre-AVX2
  CPU_AVX2 = 2,     // AVX2 + OS YMM state
  CPU_AVX512 = 3,   // AVX-512 F/DQ/CD/BW/VL + OS ZMM state
};

uint32_t cpu_level_storage = CPU_UNKNOWN;

inline void cpuid_ex(uint32_t leaf, uint32_t subleaf, uint32_t out[4]) {
  __asm__ __volatile__("cpuid"
                       : "=a"(out[0]), "=b"(out[1]), "=c"(out[2]), "=d"(out[3])
                       : "0"(leaf), "2"(subleaf));
}

__attribute__((noinline, cold)) uint32_t detect_cpu_level() {
  uint32_t level = CPU_BASELINE;
  uint32_t r[4];
  cpuid_ex(0, 0, r);
  if (r[0] >= 7) {
    cpuid_ex(1, 0, r);
    if (r[2] & (1u << 27)) { // OSXSAVE
      uint32_t xcr0_lo = 0;
      uint32_t xcr0_hi = 0;
      __asm__ __volatile__("xgetbv" : "=a"(xcr0_lo), "=d"(xcr0_hi) : "c"(0));
      const bool os_ymm = (xcr0_lo & 0x06) == 0x06; // XMM + YMM state
      const bool os_zmm =
          (xcr0_lo & 0xE6) == 0xE6; // + opmask, ZMM_Hi256, Hi16_ZMM
      cpuid_ex(7, 0, r);
      const bool avx2 = r[1] & (1u << 5);
      const bool avx512_f = r[1] & (1u << 16);
      const bool avx512_dq = r[1] & (1u << 17);
      const bool avx512_cd = r[1] & (1u << 28);
      const bool avx512_bw = r[1] & (1u << 30);
      const bool avx512_vl = r[1] & (1u << 31);
      if (os_ymm && avx2)
        level = CPU_AVX2;
      if (os_zmm && avx512_f && avx512_dq && avx512_cd && avx512_bw &&
          avx512_vl)
        level = CPU_AVX512;
    }
  }
  __atomic_store_n(&cpu_level_storage, level, __ATOMIC_RELAXED);
  return level;
}

inline uint32_t cpu_level() {
  const uint32_t v = __atomic_load_n(&cpu_level_storage, __ATOMIC_RELAXED);
  if (__builtin_expect(v == CPU_UNKNOWN, 0))
    return detect_cpu_level();
  return v;
}

} // namespace

// `no_stack_protector` everywhere on this path: a stack-protector canary in
// the entry forces a frame and turns the dispatch tail-jmp into a full call
// plus canary re-check (memmove measured ~1.5 ns/call slower). glibc compiles
// its string routines with -fno-stack-protector for the same reason.
#define CH_AVX512_CLONE                                                        \
  __attribute__((target("arch=x86-64-v4,no-prefer-256-bit"), noinline,         \
                 flatten, no_stack_protector))
#define CH_NO_SSP __attribute__((no_stack_protector))

namespace LIBC_NAMESPACE_DECL {

// ============================================================================
// Dispatch plumbing (manual IFUNC)
// ============================================================================
//
// Each large-size operation goes through a function pointer that is statically
// initialized to a resolver; the resolver runs `cpuid` once, swaps the pointer
// to the chosen implementation and forwards the first call. This is the same
// shape glibc uses (IFUNC): after the first call, the cost is one predicted
// indirect `jmp` on the large-size path only.
//
// Two non-obvious constraints force the pointer indirection (instead of an
// `if (avx512) clone(...)` branch):
//  * LLVM refuses to emit a *direct* tail call from a v3 function to a v4
//    target-attributed callee (feature-mismatch conservatism), so the branch
//    form pays a full call/ret frame — measurably halving 64-512 B throughput.
//    Indirect tail calls have no such restriction: `return ptr(...)` lowers
//    to `jmp *%rax`.
//  * `flatten` on the public entries hard-errors on any direct reference to a
//    higher-feature callee; through a pointer there is nothing to inline.
//
// The implementations return the original `dst` so the entries can tail-call.
// Plain relaxed atomics on the pointer: racing resolvers all store the same
// value. No static initializers — the pointer init is a constant.

// ============================================================================
// memcpy
// ============================================================================

// AVX-512 (ZMM) large-copy path: 64-byte head-tail tiers up to 512 bytes,
// then a 64-byte-aligned 256-byte-per-iteration main loop.
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

// Baseline large-copy path: upstream YMM (or SSE2) implementation.
CH_NO_SSP static void *memcpy_ge64_baseline(Ptr __restrict dst, CPtr __restrict src,
                                  size_t count) {
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

// The >=64 B check comes first (one compare, then a tail call through the
// resolved pointer): calls that reach the libc symbol are dominated by
// runtime-sized bulk copies — compilers inline small fixed-size memcpy at the
// call site. Small-size tiers below mirror upstream `inline_memcpy_x86`.
CH_NO_SSP LLVM_LIBC_FUNCTION(void *, memcpy,
                   (void *__restrict dst_v, const void *__restrict src_v,
                    size_t count)) {
  if (count) {
    LIBC_CRASH_ON_NULLPTR(dst_v);
    LIBC_CRASH_ON_NULLPTR(src_v);
  }
  Ptr dst = reinterpret_cast<Ptr>(dst_v);
  CPtr src = reinterpret_cast<CPtr>(src_v);
  if (count >= 64)
    return __atomic_load_n(&memcpy_ge64_ptr, __ATOMIC_RELAXED)(dst, src,
                                                               count);
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

// AVX-512 large-fill path: align dst to 64 bytes, 64-byte ZMM stores.
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

// Baseline large-fill path: upstream 32-byte-aligned YMM loop.
CH_NO_SSP static void *memset_gt128_baseline(Ptr dst, uint8_t value, size_t count) {
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

CH_NO_SSP LLVM_LIBC_FUNCTION(void *, memset, (void *dst_v, int value_v, size_t count)) {
  if (count)
    LIBC_CRASH_ON_NULLPTR(dst_v);
  Ptr dst = reinterpret_cast<Ptr>(dst_v);
  const uint8_t value = static_cast<uint8_t>(value_v);
  // Large-size check first — see the memcpy entry comment.
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

// AVX-512 overlapping-move path. Aligns DST (upstream aligns SRC).
//
// Alignment SIZE must be the *smaller* type (generic_v256 / 32 B) because
// `align_forward`/`align_backward` consume up to 2*SIZE bytes from `count`;
// aligning with the 64-byte type can leave the `generic_v512` loop with
// count < SIZE and corrupt trailing bytes (do-while runs once regardless).
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

// Baseline overlapping-move path: upstream YMM implementation.
CH_NO_SSP static void *memmove_overlap_baseline(Ptr dst, CPtr src, size_t count) {
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

CH_NO_SSP LLVM_LIBC_FUNCTION(void *, memmove, (void *dst, const void *src,
                                     size_t count)) {
  if (count) {
    LIBC_CRASH_ON_NULLPTR(dst);
    LIBC_CRASH_ON_NULLPTR(src);
  }
  Ptr dst_p = reinterpret_cast<Ptr>(dst);
  CPtr src_p = reinterpret_cast<CPtr>(src);
  // Large-size check first — see the memcpy entry comment.
  if (count > 128) {
    // Disjoint buffers take the plain copy path (count > 128 satisfies the
    // >=64 B requirement of the copy pointer). Both are tail calls.
    if (is_disjoint(dst, src, count))
      return __atomic_load_n(&memcpy_ge64_ptr, __ATOMIC_RELAXED)(dst_p, src_p,
                                                                 count);
    return __atomic_load_n(&memmove_overlap_ptr, __ATOMIC_RELAXED)(
        dst_p, src_p, count);
  }
  // Sizes <= 128 B: overlap-safe head-tail tiers using native vector types
  // (the upstream ladder's cpp::array<v256, 2> temporaries spill to the
  // stack; `generic_v512` stays in registers).
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

// ============================================================================
// memmem (SIMD two-byte filter; strong symbol overriding musl's Two-Way)
// ============================================================================

namespace {

// AVX-512BW: 64 candidate positions per iteration via mask registers.
CH_AVX512_CLONE void *memmem_avx512(const char *h, const char *n, size_t nlen,
                                    size_t end) {
  const __m512i first = _mm512_set1_epi8(n[0]);
  const __m512i last = _mm512_set1_epi8(n[nlen - 1]);
  size_t i = 0;
  while (i + 64 <= end) {
    __m512i bf = _mm512_loadu_si512(reinterpret_cast<const __m512i *>(h + i));
    __m512i bl = _mm512_loadu_si512(
        reinterpret_cast<const __m512i *>(h + i + nlen - 1));
    __mmask64 mask = _mm512_cmpeq_epi8_mask(bf, first) &
                     _mm512_cmpeq_epi8_mask(bl, last);
    while (mask) {
      const int pos = __builtin_ctzll(mask);
      if (nlen <= 2 ||
          __builtin_memcmp(h + i + pos + 1, n + 1, nlen - 2) == 0)
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

// AVX2: 32 candidate positions per iteration via vpmovmskb. This is the
// default on x86-64-v3 hardware without AVX-512.
__attribute__((target("arch=x86-64-v3"), noinline, flatten)) void *
memmem_avx2(const char *h, const char *n, size_t nlen, size_t end) {
  const __m256i first = _mm256_set1_epi8(n[0]);
  const __m256i last = _mm256_set1_epi8(n[nlen - 1]);
  size_t i = 0;
  while (i + 32 <= end) {
    __m256i bf =
        _mm256_loadu_si256(reinterpret_cast<const __m256i *>(h + i));
    __m256i bl = _mm256_loadu_si256(
        reinterpret_cast<const __m256i *>(h + i + nlen - 1));
    uint32_t mask =
        static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_and_si256(
            _mm256_cmpeq_epi8(bf, first), _mm256_cmpeq_epi8(bl, last))));
    while (mask) {
      const int pos = __builtin_ctz(mask);
      if (nlen <= 2 ||
          __builtin_memcmp(h + i + pos + 1, n + 1, nlen - 2) == 0)
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

} // namespace

extern "C" void *memmem(const void *haystack, size_t hlen, const void *needle,
                        size_t nlen) noexcept {
  if (nlen == 0)
    return const_cast<void *>(haystack);
  if (hlen < nlen)
    return nullptr;
  const char *h = static_cast<const char *>(haystack);
  const char *n = static_cast<const char *>(needle);
  if (nlen == 1)
    return const_cast<void *>(__builtin_memchr(haystack, n[0], hlen));
  const size_t end = hlen - nlen + 1; // last valid start position + 1
  const uint32_t level = cpu_level();
  if (level >= CPU_AVX512)
    return memmem_avx512(h, n, nlen, end);
  if (level >= CPU_AVX2)
    return memmem_avx2(h, n, nlen, end);
  return memmem_scalar(h, n, nlen, end);
}
