#pragma once

#include <Common/HashTable/HashSet.h>
#include <Common/HashTable/Hash.h>
#include <Common/SipHash.h>
#include <Core/Block.h>
#include <Core/Names.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnConst.h>

#include <atomic>
#include <shared_mutex>
#include <vector>


namespace DB
{

class MergeTreeData;
class IMergeTreeDataPart;

/// Ephemeral, in-memory set of 128-bit SipHash values for UNIQUE constraint enforcement.
///
/// Design (from Alexey Milovidov's RFC #70589):
///   - The set is NOT persistent. On server restart, it is reconstructed from table data.
///   - Uses sipHash128 of the unique columns tuple.
///   - For non-replicated MergeTree: serialize INSERTs with a mutex.
///   - For replicated MergeTree: optimistic transactions with retries.
///   - Serialization should be done independently per partition.
///
/// Thread safety:
///   - Multiple concurrent readers allowed (shared lock).
///   - Exclusive write access required for modifications.
///   - `ready` flag is atomic: false during reconstruction, true when set is usable.
///
class UniqueKeySet
{
public:
    using Hash128 = UInt128;

    UniqueKeySet() = default;

    /// Check which rows in the block have keys that already exist in the set.
    /// Returns a UInt8 column where 1 = duplicate (exists in set), 0 = unique (not in set).
    /// Thread-safe: acquires shared lock.
    ColumnUInt8::MutablePtr findDuplicates(const Block & block, const Names & key_columns) const;

    /// Check for duplicates both against the set AND within the block itself (intra-block dedup).
    /// A row is marked as duplicate if:
    ///   1. Its key hash already exists in the set, OR
    ///   2. An earlier row in the same block has the same key hash.
    /// Returns a UInt8 column where 1 = duplicate, 0 = unique.
    /// Thread-safe: acquires shared lock.
    ColumnUInt8::MutablePtr findDuplicatesWithSelf(const Block & block, const Names & key_columns) const;

    /// Add all key hashes from a block to the set.
    /// Called after a data part is successfully committed.
    /// Thread-safe: acquires exclusive lock.
    void addFromBlock(const Block & block, const Names & key_columns);

    /// Remove all key hashes for rows in a block from the set.
    /// Called when a data part is dropped, merged, or mutated.
    /// Thread-safe: acquires exclusive lock.
    void removeFromBlock(const Block & block, const Names & key_columns);

    /// Full rebuild: clear the set and populate from all active data parts.
    /// Blocks INSERTs while rebuilding (ready = false).
    /// Thread-safe: acquires exclusive lock.
    void rebuild(const MergeTreeData & storage, const Names & key_columns);

    /// Snapshot I/O for fast restart.
    /// saveSnapshot() writes the hash set to a write buffer.
    /// loadSnapshot() reads it back; returns false if the snapshot is invalid.
    void saveSnapshot(WriteBuffer & out) const;
    bool loadSnapshot(ReadBuffer & in, const String & parts_checksum);

    /// Returns true when the set has been fully populated and is ready for use.
    /// INSERTs should block or throw while this is false.
    bool isReady() const { return ready.load(std::memory_order_acquire); }

    /// Mark the set as ready for use (called after initialization/rebuild).
    void markReady() { ready.store(true, std::memory_order_release); }

    /// Number of unique key hashes in the set.
    size_t size() const;

    /// Total memory used by the hash set.
    size_t getBufferSizeInBytes() const;

    /// The per-partition mutex for serializing INSERTs (non-replicated MergeTree).
    /// The caller (MergeTreeSink) is responsible for acquiring this.
    std::mutex insert_mutex;

private:
    /// Compute SipHash128 of a single row's unique key columns.
    /// Uses IColumn::updateHashWithValue which handles ALL data types correctly.
    static Hash128 hashRow(const Block & block, const Names & key_columns, size_t row_idx);

    /// Compute all row hashes for a block.
    static std::vector<Hash128> hashAllRows(const Block & block, const Names & key_columns);

    /// The core data structure: HashSet of 128-bit SipHash values.
    /// Uses ClickHouse's high-performance open-addressing hash table.
    mutable std::shared_mutex rw_mutex;
    HashSet<Hash128, UInt128Hash> set;

    /// Status flags
    std::atomic<bool> ready{false};
    std::atomic<bool> rebuilding{false};
};

} // namespace DB
