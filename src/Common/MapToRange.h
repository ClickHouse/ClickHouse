#pragma once

#include <base/types.h>

#include <cstddef>

namespace DB
{

/// Map each 32-bit hash uniformly to [0, range_size) via multiply-and-shift (more uniform than
/// `h % n`, no division). `range_size` must fit in `UInt32`. `Id` is `UInt64` for an `IColumn::Selector`
/// or `UInt16` for an `IColumn::SmallSelector`. With `UInt16`, `range_size` must be at most 65536.
template <typename Id>
void mapToRange(const UInt32 * hashes, size_t n, UInt32 range_size, Id * result);

}
