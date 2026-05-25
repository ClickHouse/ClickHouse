#pragma once

#include <Columns/IColumn.h>
#include <Storages/MergeTree/MarkRange.h>

#include <memory>
#include <mutex>
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
    void insert(
        const std::string & part_name,
        const std::string & column_name,
        MarkRange range,
        size_t start_row_in_part,
        size_t total_rows,
        ColumnPtr offsets);

    /// Build a cache element that serves rows `[abs_row_start, abs_row_start + num_rows)`
    /// for `(part_name, column_name)`. The cached positions are emitted in
    /// `[frame_prev_size, frame_prev_size + num_rows)` so they line up with the consumer's
    /// growing result column. Pass `frame_prev_size = 0` for a fresh `readRows`.
    /// Returns `nullptr` when the request isn't covered by a single stored range, in which
    /// case the scan falls back to a normal disk read.
    std::unique_ptr<SubstreamsCacheSparseOffsetsElement> slice(
        const std::string & part_name,
        const std::string & column_name,
        size_t abs_row_start,
        size_t num_rows,
        size_t frame_prev_size = 0) const;

private:
    struct Bucket
    {
        std::vector<SparseOffsetsRange> ranges;
    };

    mutable std::mutex mutex;
    std::unordered_map<std::string, std::unordered_map<std::string, Bucket>> store;
};

using SparseOffsetsSharePtr = std::shared_ptr<SparseOffsetsShare>;

}
