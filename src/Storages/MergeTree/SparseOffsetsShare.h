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
///
/// Lifecycle is per `(part, column)` key, not share-wide: for each key all
/// `insert`/`retain*` calls must complete before any `findBucket` for that same key.
/// Different keys progress independently and may be at different phases at the same
/// time.
///
///   - `planning` mode: `filterMarkRangesBySparsityInfo` populates every key and the
///     caller runs `retainSurvivingRanges` once, all before any scan reader exists.
///     Effectively share-wide phasing.
///   - `data_read` mode: `MergeTreeSparsityReader::read(part)` populates and calls
///     `retainRangesForPart(part)` for one part; its scan reader only calls
///     `findBucket(part, ...)` after `getOrBuildIndexReadResult(part)` returns.
///     Other parts may still be in `read()` at that point — that is fine because
///     each part touches only its own key and inserts on other keys don't invalidate
///     the returned `Bucket *` (`std::unordered_map` insertion does not invalidate
///     pointers to existing elements).
class SparseOffsetsShare
{
public:
    /// Public so callers can hold a stable pointer; see `findBucket`.
    struct Bucket
    {
        std::vector<SparseOffsetsRange> ranges;
    };

    /// Append one analyzer chunk to `(part, column)`'s bucket. Must not race with a
    /// `findBucket` on the same key.
    void insert(
        const std::string & part_name,
        const std::string & column_name,
        MarkRange range,
        size_t start_row_in_part,
        size_t total_rows,
        ColumnPtr offsets);

    /// Resolve `(part, column)` to a stable pointer into the share. Callers must ensure
    /// no further `insert`/`retain*` runs for this key after they hold the returned
    /// pointer; other keys may continue to be populated. Acquires the shared lock once;
    /// callers should cache the result and call `sliceFromBucket` on the cached pointer
    /// to skip the lock on every subsequent slice.
    const Bucket * findBucket(const std::string & part_name, const std::string & column_name) const;

    /// True when no bucket has been inserted (no sparse columns / no recognised conjuncts).
    bool empty() const;

    /// Per-part cleanup: drop ranges in `part_name`'s bucket that no surviving
    /// `MarkRange` overlaps; remove the bucket if every range was dropped. Buckets for
    /// other parts are untouched. Must not race with a `findBucket` for `part_name`.
    /// Used by `MergeTreeSparsityReader::read` once the verdict is known.
    void retainRangesForPart(const std::string & part_name, const MarkRanges & surviving_ranges);

    /// Whole-share cleanup, safe when no scan reader has resolved any bucket yet:
    ///   - Parts absent from `per_part_surviving_ranges` are dropped entirely.
    ///   - For surviving parts, `retainRangesForPart` is applied.
    /// Used after `planning`-mode pruning has settled the surviving mark ranges.
    void retainSurvivingRanges(const std::unordered_map<std::string, MarkRanges> & per_part_surviving_ranges);

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
    /// All store mutations (`insert`, `retain*`) take the unique lock; lookups (`findBucket`,
    /// `empty`) take the shared lock. `sliceFromBucket` runs on the `Bucket *` returned by
    /// `findBucket` and is lock-free; the per-key lifecycle contract (see class doc) is
    /// what keeps that pointer stable.
    mutable SharedMutex mutex;
    std::unordered_map<std::string, std::unordered_map<std::string, Bucket>> store;
};

using SparseOffsetsSharePtr = std::shared_ptr<SparseOffsetsShare>;

}
