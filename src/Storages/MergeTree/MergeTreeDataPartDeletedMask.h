#pragma once

#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>

namespace DB
{
/// Per-part info about rows deleted by lightweight mutations.
struct MergeTreeDataPartDeletedMask
{
    static constexpr auto FILE_NAME = "deleted_mask.bin";
    static constexpr std::hash<size_t> hasher;

    std::vector<size_t> deleted_rows;

    // Pre-calculated hash for fast update
    size_t deleted_rows_hash {0};

    void read(ReadBuffer & in);
    void write(WriteBuffer & out) const;

    // Assumption: already deleted rows can't be deleted, so
    // intersection(deleted_rows, other.deleted_rows) = empty.
    void update(const MergeTreeDataPartDeletedMask& other);

    // Assumption: already deleted rows can't be deleted, so
    // intersection(deleted_rows, other.deleted_rows) = empty.
    // Updated current state and updates the underlying file
    void updateWrite(const MergeTreeDataPartDeletedMask& other, WriteBufferFromFile & out);
};
};
