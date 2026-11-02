#pragma once

#include <Core/ColumnNumbers.h>
#include <Core/Names.h>
#include <DataTypes/IDataType_fwd.h>
#include <Interpreters/AggregatedDataVariants.h>
#include <Processors/Chunk.h>
#include <Processors/Transforms/ChunkRowRange.h>
#include <Processors/Transforms/SeenGroupExtractor.h>
#include <Common/PODArray.h>

#include <functional>
#include <memory>
#include <vector>

namespace DB
{

class IProcessor;

/// Positions of the specified `LIMIT BY` keys whose header columns are not constant. An empty list of
/// names yields no keys, so every input row belongs to one group.
ColumnNumbers calculateLimitByKeyColumnsPositions(const Block & header, const Names & column_names);

/// The exclusive end of the kept per-group interval `[offset, offset + length)`, saturated at the maximum
/// `UInt64`.
UInt64 computeGroupLimitEnd(UInt64 length, UInt64 offset);

/// Returns the chunk-local portion of a same-group run that falls into the kept per-group interval
/// `[group_offset, group_limit_end)`. `run_start_row` and `run_row_count` describe the run inside the current
/// chunk. `group_rows_seen_before_run` is the total number of rows already seen for this group before the run
/// starts. `length == 0` means the run contributes no rows.
ChunkRowRange shrinkRunToLimitWindow(
    UInt64 run_start_row, UInt64 run_row_count, UInt64 group_rows_seen_before_run, UInt64 group_offset, UInt64 group_limit_end);

/// Hash-based state of `LIMIT n OFFSET m BY keys`: counts the input rows of each group and keeps the rows
/// whose rank within their group falls into `[group_offset, group_limit_end)`. Groups are identified by the
/// exact keys of a `GROUP BY` hash method. A cell maps to a stable index into the group counts, which keeps
/// the consecutive-keys cache of the method valid. Rows of one group that follow each other in a chunk form a
/// run, handled at once. `LimitByTransform` uses the filter for streaming execution;
/// `ExternalLimitByTransform` also extracts the groups with their counts when it spills.
///
/// A method that serializes its keys (generic keys) stores bytes that only that method can compare. A filter
/// that keeps fingerprints therefore also keeps, for each such group, the 128-bit fingerprint of its key
/// columns, as the `hashed` method of `DISTINCT` does; spill runs compare the fingerprints instead of the
/// keys.
///
/// The row loop observes cancellation of `owner_`: a cancelled filter returns the chunk without rows, and its
/// counts no longer describe the consumed input.
class LimitByGroupFilter
{
public:
    LimitByGroupFilter(
        const Block & header,
        const Names & column_names,
        UInt64 group_offset_,
        UInt64 group_limit_end_,
        const IProcessor & owner_,
        bool keep_fingerprints_ = false);

    /// Positions of the non-constant grouping key columns in the header.
    const ColumnNumbers & getKeyColumnsPositions() const { return grouping_key_positions; }
    bool hasKeyColumns() const { return !grouping_key_positions.empty(); }

    /// Spilled groups compare their fingerprints when the filter keeps them, and their typed key columns
    /// otherwise.
    SpillKeyRepresentation getKeyRepresentation() const
    {
        return retains_fingerprints ? SpillKeyRepresentation::Hash128 : SpillKeyRepresentation::Columns;
    }

    /// The number of groups seen so far.
    size_t getTotalRowCount() const { return group_counts.size(); }

    /// The memory of the hash table, its key arena, the group counts, and the fingerprints.
    size_t getTotalByteCount() const;

    /// Whether no later row can be kept: every key is constant, so all rows form one group, and its window is
    /// full.
    bool isExhausted() const;

    /// Materializes the grouping key columns in place, so that estimates see their full size. The output
    /// keeps the materialized values.
    void prepareForInsert(Chunk & chunk) const;

    /// Estimates peak additional memory assuming every row of the prepared chunk starts a new group:
    /// hash-table resize peaks, key-arena growth, and the growth of the group counts and fingerprints.
    /// Saturates at the maximum of `size_t`.
    size_t estimateGrowthMemory(const Chunk & chunk) const;

    /// Estimates the workspace for run slices, packed keys, and an output copy of the prepared chunk.
    size_t estimateFilteringMemory(const Chunk & chunk) const;

    /// Counts the rows of the chunk and returns those inside the window of their group, preserving chunk
    /// information. A chunk without kept rows is returned without columns.
    Chunk filter(Chunk chunk);

    struct FilterResult
    {
        Chunk chunk;
        size_t processed_rows;
    };

    /// Called before inserting a new group, with its estimated table, arena, count, and fingerprint growth in
    /// bytes.
    using InsertionCheck = std::function<bool(size_t)>;

    /// Like `filter`, but checks membership first and stops before the first new group that the check
    /// rejects. Returns the kept rows of the processed prefix and the number of processed input rows; the
    /// rejected row is not counted. Requires columns prepared by `prepareForInsert` and at least one key
    /// column.
    FilterResult filterWithInsertionCheck(Chunk chunk, const InsertionCheck & can_insert);

    /// Moves the groups into an extractor that returns their keys in the representation of
    /// `getKeyRepresentation`, with the count of each group (see `group_counts`). Typed keys follow
    /// `getKeyColumnsPositions`; the hash table stays alive until the last group is materialized or the
    /// extractor is destroyed early. Fingerprints are returned in insertion order, and the hash table is
    /// released at once. Requires at least one group and one key column.
    std::unique_ptr<SeenGroupExtractor> extractSeenGroups() &&;

private:
    /// Once a group has filled its window every later run for it is a no-op, so keep that test at the
    /// call site: the call costs more than the work.
    void processRun(UInt64 run_start_row, UInt64 run_row_count, size_t group_idx)
    {
        chassert(group_idx < group_counts.size());
        const UInt64 group_rows_seen_before_run = group_counts[group_idx];
        if (group_rows_seen_before_run < group_limit_end)
            processRunInsideWindow(run_start_row, run_row_count, group_idx, group_rows_seen_before_run);
    }

    NO_INLINE void
    processRunInsideWindow(UInt64 run_start_row, UInt64 run_row_count, size_t group_idx, UInt64 group_rows_seen_before_run);

    /// Appends the fingerprint of the key at `row` to `group_fingerprints`. Only a spilling filter with
    /// generic keys keeps fingerprints, so the call stays out of the row loop.
    NO_INLINE void addGroupFingerprint(size_t row, const ColumnRawPtrs & key_columns);

    /// Splits the rows into runs and counts them; returns false when cancellation stopped the loop. With
    /// `checked_insertion`, looks keys up first and stops before the first new group that `can_insert`
    /// rejects. Stores the number of processed rows in `processed_rows`.
    template <bool checked_insertion, typename Method>
    bool consumeImpl(
        Method & hash_method, const ColumnRawPtrs & grouping_key_columns, UInt64 row_count,
        const InsertionCheck * can_insert, UInt64 & processed_rows);

    /// Moves the state into an extractor for a single-level method that maps keys to group indices.
    template <bool is_two_level, typename Method>
    std::unique_ptr<SeenGroupExtractor> createGroupExtractor(Method & method);

    template <typename Method>
    size_t estimateGrowthMemoryImpl(const Method & hash_method, const ColumnRawPtrs & key_columns, size_t start_row, size_t num_rows) const;

    /// The growth of the group counts and fingerprints after `new_groups` more groups.
    size_t estimateGroupStateGrowth(size_t new_groups) const;

    ColumnRawPtrs getKeyColumns(const Columns & columns) const;
    Chunk filterImpl(Chunk chunk, const InsertionCheck * can_insert, UInt64 & processed_rows);

    /// Positions of the non-constant grouping key columns in the chunk header.
    ColumnNumbers grouping_key_positions;
    /// Types of the grouping key columns, for the key extraction.
    DataTypes key_types;

    /// Kept per-group interval is `[group_offset, group_limit_end)`.
    const UInt64 group_offset;
    const UInt64 group_limit_end;

    /// Owns the hash table and its key arena until the filter is destroyed or extraction takes ownership.
    std::unique_ptr<AggregatedDataVariants> data;
    ColumnsHashing::HashMethodContextPtr hash_method_context;

    /// The number of input rows counted for each group, which decides whether the next row of that group is
    /// returned. A group stops counting once its window is full, so its count is exact below the window end
    /// and at least the window end otherwise.
    std::vector<UInt64> group_counts;

    /// Whether every group keeps the fingerprint of its key columns: set for a filter that keeps fingerprints
    /// and a method that serializes its keys.
    bool retains_fingerprints = false;
    /// Fingerprints of the groups, indexed like `group_counts`.
    std::vector<UInt128> group_fingerprints;

    /// Slices from the current chunk that will be returned.
    PODArray<ChunkRowRange> output_slices;

    const IProcessor & owner;
};

}
