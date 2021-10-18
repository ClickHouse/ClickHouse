#pragma once

#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>

namespace DB
{
/// Per-part info about rows deleted by lightweight mutations.
// TODO Current implementation is naive, may research other compression formats
struct MergeTreeDataPartDeletedMask
{
    static constexpr std::string_view name = "deleted_mask_{}.bin"; // {} substituted by block number
    static constexpr std::hash<size_t> hasher;

    std::vector<size_t> deleted_rows;

    // Pre-calculated hash for fast update
    size_t deleted_rows_hash {0};

    void read(ReadBuffer & in);
    void write(WriteBuffer & out) const;
};
};
