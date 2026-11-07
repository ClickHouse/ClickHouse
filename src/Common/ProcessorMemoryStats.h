#pragma once

#include <base/types.h>

namespace DB
{
struct ProcessorMemoryStats
{
    // The total spillable memory in the processor
    Int64 spillable_memory_bytes = 0;
    // To avoid this processor cause OOM, at least `reserved_memory_bytes` should be reserved.
    // including auxiliary memory to finish the spilling process.
    Int64 need_reserved_memory_bytes = 0;
};
}
