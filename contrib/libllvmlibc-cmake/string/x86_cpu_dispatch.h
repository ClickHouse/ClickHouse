// Runtime AVX-512/AVX2 detection shared by the x86_64 mem* dispatchers and
// memmem. Detected lazily on the first large call; the benign race has all
// writers storing the same value, so relaxed atomics suffice.
#pragma once

#include <cstdint>

enum CpuLevel : uint32_t {
  CPU_UNKNOWN = 0,
  CPU_BASELINE = 1, // pre-AVX2
  CPU_AVX2 = 2,
  CPU_AVX512 = 3, // F/DQ/CD/BW/VL + OS ZMM state
};

static uint32_t cpu_level_storage = CPU_UNKNOWN;

static inline void cpuid_ex(uint32_t leaf, uint32_t subleaf, uint32_t out[4]) {
  __asm__ __volatile__("cpuid"
                       : "=a"(out[0]), "=b"(out[1]), "=c"(out[2]), "=d"(out[3])
                       : "0"(leaf), "2"(subleaf));
}

__attribute__((noinline, cold)) static uint32_t detect_cpu_level() {
  uint32_t level = CPU_BASELINE;
  uint32_t r[4];
  cpuid_ex(0, 0, r);
  if (r[0] >= 7) {
    cpuid_ex(1, 0, r);
    if (r[2] & (1u << 27)) { // OSXSAVE
      uint32_t xcr0_lo = 0;
      uint32_t xcr0_hi = 0;
      __asm__ __volatile__("xgetbv" : "=a"(xcr0_lo), "=d"(xcr0_hi) : "c"(0));
      const bool os_ymm = (xcr0_lo & 0x06) == 0x06;
      const bool os_zmm = (xcr0_lo & 0xE6) == 0xE6;
      cpuid_ex(7, 0, r);
      if (os_ymm && (r[1] & (1u << 5)))
        level = CPU_AVX2;
      if (os_zmm && (r[1] & (1u << 16)) && (r[1] & (1u << 17)) &&
          (r[1] & (1u << 28)) && (r[1] & (1u << 30)) && (r[1] & (1u << 31)))
        level = CPU_AVX512;
    }
  }
  __atomic_store_n(&cpu_level_storage, level, __ATOMIC_RELAXED);
  return level;
}

static inline uint32_t cpu_level() {
  const uint32_t v = __atomic_load_n(&cpu_level_storage, __ATOMIC_RELAXED);
  if (__builtin_expect(v == CPU_UNKNOWN, 0))
    return detect_cpu_level();
  return v;
}

// v4-attributed clone: gets ZMM codegen on a v3 TU, `no-prefer-256-bit` picks
// ZMM over 2xYMM, `no_stack_protector` keeps the dispatch tail-call a plain jmp.
#define CH_AVX512_CLONE                                                        \
  __attribute__((target("arch=x86-64-v4,no-prefer-256-bit"), noinline,         \
                 flatten, no_stack_protector))
#define CH_NO_SSP __attribute__((no_stack_protector))
