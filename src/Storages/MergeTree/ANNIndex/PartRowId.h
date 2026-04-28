#pragma once

#include <base/types.h>
#include <cstddef>
#include <tuple>

namespace DB
{

/// Stable row identity used for vector search indexes built across parts.
/// Triple of:
///   - partition_hash: UInt64 hash of `_partition_id` (hash algorithm + seed recorded in meta.json).
///   - block_number:   value of the `_block_number` virtual column.
///   - block_offset:   value of the `_block_offset` virtual column.
/// Layout is fixed at 24 bytes so that it can be stored directly as the record body of `id_map.bin`.
struct PartRowId
{
    UInt64 partition_hash;
    UInt64 block_number;
    UInt64 block_offset;

    bool operator==(const PartRowId & rhs) const noexcept = default;

    /// Lexicographic ordering (`partition_hash`, `block_number`, `block_offset`).
    bool operator<(const PartRowId & rhs) const noexcept
    {
        return std::tie(partition_hash, block_number, block_offset)
             < std::tie(rhs.partition_hash, rhs.block_number, rhs.block_offset);
    }
};

static_assert(sizeof(PartRowId) == 24, "PartRowId must be 24 bytes (id_map.bin record layout)");
static_assert(alignof(PartRowId) == 8, "PartRowId must be 8-byte aligned");

}
