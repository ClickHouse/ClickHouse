#pragma once

#include <base/types.h>

#include <cstddef>

namespace DB
{

/// Map each 32-bit hash uniformly to [0, range_size) using the multiply-and-shift method:
///
///     result[i] = (uint64_t{hashes[i]} * range_size) >> 32
///
/// This is strictly more uniform than `h % n` (no bias for non-power-of-2 ranges)
/// and avoids the division instruction.
///
/// SIMD-multi-versioned (x86_64_v4 / scalar baseline) via MULTITARGET_FUNCTION_X86_V4.
/// The inner multiply is expressed as a uint32 × uint32 → uint64 widening product so the
/// compiler emits vpmuludq (1 µop, 0.5c throughput) instead of vpmullq (3 µops).
///
/// range_size must fit in uint32_t (any realistic partition count does).
///
/// Two output-width overloads share the same kernel shape:
///   - UInt64 output feeds an IColumn::Selector directly (no widening copy).
///   - UInt32 output halves the selector bandwidth where a 32-bit index suffices.
void mapToRange(const UInt32 * hashes, size_t n, UInt32 range_size, UInt64 * result);
void mapToRange(const UInt32 * hashes, size_t n, UInt32 range_size, UInt32 * result);

}
