#pragma once

#include <base/types.h>

#include <cstddef>

namespace DB
{

/// Map each 32-bit hash uniformly to [0, range_size) via multiply-and-shift (more uniform than
/// `h % n`, no division). `range_size` must fit in UInt32; the UInt64 output feeds an IColumn::Selector.
void mapToRange(const UInt32 * hashes, size_t n, UInt32 range_size, UInt64 * result);

}
