#pragma once

#include <deque>
#include <list>
#include <Columns/IColumn.h>
#include <Interpreters/AggregatedDataVariants.h>
#include <Processors/IAccumulatingTransform.h>
#include <Processors/IInflatingTransform.h>


namespace DB
{

/// A contiguous slice of rows from a chunk that share the same grouping key.
struct ChunkSlice
{
    using ColumnsPtr = std::shared_ptr<const Columns>;
    ColumnsPtr columns;
    UInt64 start = 0;
    UInt64 length = 0;
};


/// Default variant: runs when the input isn't sorted by the LIMIT BY key,
/// so rows from different groups may interleave, e.g.
///
///     SELECT number, number % 3 AS g
///     FROM numbers(15)
///     ORDER BY number
///     LIMIT -2 BY g
///
/// Since groups aren't contiguous, no group's "last rows" can be decided until the whole input
/// has been seen. Maintain a global candidate list of `ChunkSlice` entries in input order, plus
/// per group a sliding window of iterators into that list covering the group's last
/// `length + offset` rows. New slices are appended to the global list, and the corresponding
/// iterator is pushed onto the group's window so that, if needed, during per-group eviction, we
/// can easily remove the entry from the global list in O(1). Once a group's window overflows,
/// the oldest slice is either erased entirely (popping the iterator and removing it from the
/// global list) or trimmed in place by advancing its start. At the end of input, we drop the trailing
/// `offset` rows per group. Whatever remains in the candidate list afterwards is all the rows we want
/// to output. Since the candidate list is already in input order, traversing from the front and
/// creating chunks to emit is enough.
///
/// Memory: on average, O(sum over groups of min(group_size, length + offset)) rows.
/// Note: whole source chunks stay pinned while any of their rows survive, so in the worst-case
/// scenario, we may have all the chunks in memory.
class NegativeLimitByTransform final : public IAccumulatingTransform
{
public:
    NegativeLimitByTransform(SharedHeader header, UInt64 group_length_, UInt64 group_offset_, const Names & column_names);

    String getName() const override { return "NegativeLimitByTransform"; }

private:
    void consume(Chunk chunk) override;
    Chunk generate() override;

    using CandidateList = std::list<ChunkSlice>;
    using CandidateIt = CandidateList::iterator;

    struct GroupWindow
    {
        std::deque<CandidateIt> slices;
        UInt64 window_rows = 0;
    };

    void dropExcessRows(GroupWindow & window);
    void dropOffsetRows(GroupWindow & window);

    /// Append a new run of same-grouping-key rows to `candidate_list` and the group's window.
    void appendRun(const ChunkSlice::ColumnsPtr & columns_ptr, UInt64 start, UInt64 length, size_t group_idx);

    template <typename Method>
    void consumeImpl(Method & method, const ColumnRawPtrs & key_columns, const ChunkSlice::ColumnsPtr & columns_ptr, UInt64 num_rows);

    std::vector<size_t> key_positions;
    const UInt64 group_offset;
    const UInt64 group_window_size;

    /// Holds the ChunkSlices that will be part of the output, in input order.
    CandidateList candidate_list;

    /// Holds the references to the ChunkSlices inside `candidate_list` per grouping key.
    std::vector<GroupWindow> group_windows;

    AggregatedDataVariants data;
    ColumnsHashing::HashMethodContextPtr hash_method_context;

    /// Whether the trailing offset rows have been removed from each group's window.
    bool offset_rows_dropped = false;
};


/// SortedStream variant: runs when the input is already sorted by the LIMIT BY key
/// (or a prefix of it), e.g.
///
///     SELECT number, number % 3 AS g
///     FROM numbers(15)
///     ORDER BY g, number
///     LIMIT -2 BY g
///
/// Because each group's rows are contiguous, only one group needs to be tracked
/// at a time. As rows arrive, keep the last `length + offset` rows of the current
/// group in a sliding window; when the key changes, the group is complete - drop
/// the trailing `offset` rows and emit the rest, then start fresh for the next
/// group.
///
/// Memory: O(length + offset) rows for the single active group.
class NegativeLimitBySortedStreamTransform final : public IInflatingTransform
{
public:
    NegativeLimitBySortedStreamTransform(SharedHeader header, UInt64 group_length_, UInt64 group_offset_, const Names & column_names);

    String getName() const override { return "NegativeLimitBySortedStreamTransform"; }

private:
    void consume(Chunk chunk) override;
    bool canGenerate() override;
    Chunk generate() override;
    Chunk getRemaining() override;

    struct GroupWindow
    {
        std::deque<ChunkSlice> slices;
        UInt64 window_rows = 0;
    };

    bool sameAsPrevChunkKey(const Columns & cols, UInt64 row) const;
    bool sameAsRowBefore(const Columns & cols, UInt64 row) const;
    void rememberKey(const Columns & cols, UInt64 row);
    void dropExcessRows(GroupWindow & window) const;
    void dropOffsetRows(GroupWindow & window) const;
    void finalizeWindow(GroupWindow & window);

    std::vector<size_t> key_positions;
    const UInt64 group_offset;
    const UInt64 group_window_size;

    GroupWindow current_group_window;
    MutableColumns prev_key_columns;

    /// Slices staged for output. `generate()` coalesces consecutive slices
    /// sharing one source chunk into a single output chunk.
    std::deque<ChunkSlice> pending;
};

}
