#pragma once

#include <Columns/IColumn.h>
#include <Common/SharedMutex.h>
#include <Storages/MergeTree/MarkRange.h>
#include <base/defines.h>

#include <atomic>
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
/// Lifecycle (callers must run the phases in order; mixing them is unsafe because
/// `findBucket` hands out raw pointers into the store that mutations would invalidate):
///   1. Populate: the analyzer fills the share via `insert`.
///   2. Prune:    once the surviving mark ranges are known, the caller drops dead
///                entries with `retainSurvivingRanges` (planning, whole-share) or
///                `retainRangesForPart` (data_read, per-part).
///   3. Consume:  scan readers resolve buckets via `findBucket` and slice from them.
///
/// In `planning` mode phases 1+2 happen back-to-back in `selectRangesToRead`, then
/// scans run phase 3. In `data_read` mode the three phases happen per part inside the
/// per-part `getOrBuildIndexReadResult`; the scan reader for that part runs phase 3
/// only after its `getOrBuildIndexReadResult` returns.
///
/// Debug and sanitizer builds enforce "no phase 1/2 after phase 3" with a `chassert`.
class SparseOffsetsShare
{
public:
    /// Public so callers can hold a stable pointer; see `findBucket`.
    struct Bucket
    {
        std::vector<SparseOffsetsRange> ranges;
    };

    /// Phase 1: append one analyzer chunk to `(part, column)`'s bucket.
    void insert(
        const std::string & part_name,
        const std::string & column_name,
        MarkRange range,
        size_t start_row_in_part,
        size_t total_rows,
        ColumnPtr offsets);

    /// Phase 3: resolve `(part, column)` to a stable pointer into the share. The pointer
    /// stays valid for the share's lifetime because phases 1 and 2 are already done by
    /// the time any reader calls this. Acquires the shared lock once; callers should
    /// cache the result and call `sliceFromBucket` on the cached pointer to skip the
    /// lock on every subsequent slice.
    const Bucket * findBucket(const std::string & part_name, const std::string & column_name) const;

    /// True when no bucket has been inserted (no sparse columns / no recognised conjuncts).
    bool empty() const;

    /// Phase 2 (per-part): drop ranges in `part_name`'s bucket that no surviving
    /// `MarkRange` overlaps; remove the bucket if every range was dropped. Buckets for
    /// other parts are untouched. Used by the `data_read` analyzer once it has computed
    /// its verdict for the part.
    void retainRangesForPart(const std::string & part_name, const MarkRanges & surviving_ranges);

    /// Phase 2 (whole-share): one-shot cleanup before any scan reader looks up buckets:
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
    /// `findBucket` and is lock-free; the lifecycle contract (see class doc) is what makes
    /// that pointer stable.
    mutable SharedMutex mutex;
    std::unordered_map<std::string, std::unordered_map<std::string, Bucket>> store;

#ifdef DEBUG_OR_SANITIZER_BUILD
    /// Flipped to true the first time `findBucket` runs. Any later phase-1/2 call
    /// (`insert`/`retain*`) triggers a `chassert` failure.
    mutable std::atomic<bool> consumed{false};
#endif
};

using SparseOffsetsSharePtr = std::shared_ptr<SparseOffsetsShare>;

}
