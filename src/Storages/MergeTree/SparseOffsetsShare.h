#pragma once

#include <Columns/IColumn.h>
#include <Common/SharedMutex.h>
#include <Storages/MergeTree/MarkRange.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>


namespace DB
{

struct SubstreamsCacheSparseOffsetsElement;

/// Decompressed sparse-offsets read by the granule analyzer, kept around so the data
/// scan that follows can serve its own reads of the same column without going back to
/// disk. The analyzer reads each `MarkRange` as one block; the scan reads in smaller
/// blocks (usually `max_block_size` rows). We store the analyzer's per-range column
/// unchanged and slice a fresh cache element per scan `readRows` call.
struct SparseOffsetsRange
{
    /// Mark range covered by `offsets` (`[begin, end)`).
    MarkRange range;
    /// Absolute starting row of `range.begin` in the part. Matches the scan's
    /// `getMarkStartingRow(from_mark)`; lets us translate scan reads into local indices.
    size_t start_row_in_part = 0;
    /// Total rows in this range, i.e. `getRowsCountInRange(range)`.
    size_t total_rows = 0;
    /// Positions of non-default rows, relative to `start_row_in_part` (position 0
    /// corresponds to the first row of `range`).
    ColumnPtr offsets;
};

/// Per-query storage of analyzer-produced offsets indexed by `(part_name, column_name)`.
/// Held by `MergeTreeIndexReadResultPool` so its lifetime coincides with one query.
class SparseOffsetsShare
{
public:
    /// Public so callers can hold a stable pointer; see `findBucket`.
    struct Bucket
    {
        std::vector<SparseOffsetsRange> ranges;
    };

    void insert(
        const std::string & part_name,
        const std::string & column_name,
        MarkRange range,
        size_t start_row_in_part,
        size_t total_rows,
        ColumnPtr offsets);

    /// Resolve `(part, column)` to a stable pointer into the share. The pointer remains
    /// valid for the lifetime of `*this` (entries are never erased and `unordered_map`
    /// guarantees pointer stability under rehash). Acquires the shared lock once; callers
    /// should cache the result and call `sliceFromBucket` on the cached pointer to skip
    /// the lock on every subsequent slice.
    const Bucket * findBucket(const std::string & part_name, const std::string & column_name) const;

    /// Build a cache element for the row window
    /// `[abs_row_start, abs_row_start + rows_offset + limit)` from a previously-resolved
    /// bucket:
    ///   - The first `rows_offset` rows are the "skip" zone; their non-defaults contribute
    ///     to `skipped_values_rows` but are not emitted in the offsets column.
    ///   - The next `limit` rows are the "produce" zone; their non-default positions are
    ///     emitted, shifted into the consumer's frame `[frame_prev_size, frame_prev_size + limit)`.
    /// Returns `nullptr` when the request isn't covered by a single stored range, in which
    /// case the consumer falls back to a normal disk read.
    /// Lock-free: the bucket's contents are immutable once visible to scan-side readers.
    static std::unique_ptr<SubstreamsCacheSparseOffsetsElement> sliceFromBucket(
        const Bucket & bucket,
        size_t abs_row_start,
        size_t rows_offset,
        size_t limit,
        size_t frame_prev_size = 0);

private:
    /// `insert` happens during the analyzer's `getOrBuildIndexReadResult` (`data_read`) or
    /// inside `filterMarkRangesBySparsityInfo` (`planning`); `findBucket`/`sliceFromBucket`
    /// are called from many scan threads later. Only `findBucket` needs the mutex; once
    /// it returns a `Bucket *`, further slicing is lock-free.
    mutable SharedMutex mutex;
    std::unordered_map<std::string, std::unordered_map<std::string, Bucket>> store;
};

using SparseOffsetsSharePtr = std::shared_ptr<SparseOffsetsShare>;

}
