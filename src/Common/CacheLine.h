#pragma once

#include <cstddef>

namespace DB
{

/// This constant is meant to replace `std::hardware_destructive_interference_size` that we've tried to use, but faced some issues,
/// see https://github.com/ClickHouse/ClickHouse/pull/97357#issuecomment-3969653804.
/// The values below repeat the ones from LLVM (`hardwareInterferenceSizes`) almost 100%.
/// Most notable divergence is for AArch64, where LLVM sets `hardware_destructive_interference_size` to 256,
/// see https://github.com/llvm/llvm-project/commit/bce2cc15133a1458e4ad053085e58c7423c365b4.
/// In reality though, most of the server (not Apple M-series) processors (i.e., Neoverse) have 64-byte cache lines,
/// see https://github.com/pytorch/cpuinfo/blob/45a761d6c4c52e2a5276985c89ee7a2ab15d7006/src/arm/cache.c#L1315-L1331.
///
#if defined(__powerpc64__)
constexpr size_t CH_CACHE_LINE_SIZE = 128;
#elif defined(__powerpc__)
constexpr size_t CH_CACHE_LINE_SIZE = 32;
#elif defined(__s390x__)
constexpr size_t CH_CACHE_LINE_SIZE = 256;
#elif defined(__loongarch64) || defined(__riscv)
constexpr size_t CH_CACHE_LINE_SIZE = 64;
#elif defined(__e2k__)
constexpr size_t CH_CACHE_LINE_SIZE = 64;
#else // x86_64 and AArch64
constexpr size_t CH_CACHE_LINE_SIZE = 64;
#endif

}
