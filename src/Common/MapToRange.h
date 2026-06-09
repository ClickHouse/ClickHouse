#pragma once

#include <base/types.h>

#include <cstddef>

namespace DB
{

/// Map each 32-bit hash uniformly to [0, range_size) using the multiply-and-shift method:
///     result[i] = (uint64_t{hashes[i]} * range_size) >> 32
/// This is strictly more uniform than `h % n` (no bias for non-power-of-2 ranges) and avoids
/// the division instruction. `range_size` must fit in UInt32 (any realistic partition count does).
/// The UInt64 output feeds an `IColumn::Selector` directly (no widening copy).
void mapToRange(const UInt32 * hashes, size_t n, UInt32 range_size, UInt64 * result);

}
