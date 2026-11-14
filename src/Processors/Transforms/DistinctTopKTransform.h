#pragma once

#include <Columns/IColumn.h>
#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashMap.h>
#include <Core/ColumnNumbers.h>
#include <Core/SortDescription.h>
#include <Processors/IAccumulatingTransform.h>
#include <Processors/TopKThresholdTracker.h>

namespace DB
{

/// The preliminary `DISTINCT` of `SELECT DISTINCT keys ... ORDER BY s LIMIT K` (see `DistinctStep::applyTopK`).
///
/// Keeps, for each `DISTINCT` key, the row with the best value of the sort description `s` seen so far,
/// and only the `K` keys with the best such values. A key ranks by its best row, so its rank only improves
/// as rows arrive (a decrease-key). The memory is O(`K`) instead of O(number of distinct keys).
///
/// No heap is maintained: once the set has been trimmed to `K` keys, the value of the worst of them is the
/// boundary, and a row not better than the boundary can never reach the result: `K` keys are already at least
/// as good. Every other row either improves its key or admits a new key, and is appended to the kept columns.
/// When the kept rows exceed `K` by half (the stale rows of improved keys count too), the keys are ranked with
/// one `nth_element`, the `K` best survive and the boundary tightens.
///
/// The output of each stream is its `K` best keys, each with its best row, in no particular order. A key among
/// the global `K` best ranks among the `K` best of the stream holding its best row: every key better there is
/// better globally too. So the sorting and the final `DISTINCT` above, which keeps the first row of each key in
/// the sorted order, produce the `K` best keys of the whole input, each with its best row.
///
/// The result differs from the plain plan only in the choice the `DISTINCT` semantics leave open: when the sort
/// columns are not `DISTINCT` keys, the plain plan sorts by the value of an arbitrary row of each key, while here
/// it is the best row.
class DistinctTopKTransform final : public IAccumulatingTransform
{
public:
    DistinctTopKTransform(
        SharedHeader header_,
        const Names & columns_,
        size_t k_,
        const SortDescription & sort_description_,
        TopKThresholdTrackerPtr threshold_tracker_);

    String getName() const override { return "DistinctTopKTransform"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    /// Negative when the row `lhs_row` is better than the row `rhs_row` in the sort description. The arguments
    /// map the index of a sort column to the column.
    template <typename LhsColumn, typename RhsColumn>
    int compareRows(LhsColumn && lhs_column, size_t lhs_row, RhsColumn && rhs_column, size_t rhs_row) const;
    bool isBelowBoundary(const Columns & columns, size_t row) const;
    /// Fills `compare_results` with the sign of the comparison of each row with the boundary, by the leading sort
    /// columns without a collation; zero for the rows equal to the boundary in them.
    void compareWithBoundary(const Columns & columns, size_t num_rows);

    UInt128 getKey(size_t row) const;
    void appendRow(const Columns & columns, size_t row, UInt128 key);
    /// If at least `k` keys are kept, moves the boundary to the `k`-th best of them. Leaves the rows that are not stale
    /// in `trim_rows`, the `k` best ones first.
    void updateBoundary();
    /// Drops the stale rows of improved keys, and if more than `k` keys are kept, all but the `k` best ones.
    void trim();

    const ColumnNumbers key_positions;
    /// The positions of the sort columns in the header, with the direction and collation of each.
    std::vector<size_t> sort_positions;
    const SortDescription sort_description;
    const size_t k;
    /// The kept rows may exceed `k` by this much before a trim. With a small `k` and the rows arriving in the best
    /// order first, every row improves the set, and a trim per few rows would cost O(`k`) per row.
    static constexpr size_t min_trim_slack = 1024;
    const size_t max_kept_rows;

    /// All columns of the header, full; row `i` is a candidate row, the best one of its key unless `stale[i]`.
    MutableColumns kept;
    /// The key of each row of `kept`, see `index`.
    std::vector<UInt128> kept_keys;
    std::vector<UInt8> stale;
    size_t num_stale = 0;

    /// The key of a row -> its row in `kept`. The key columns are packed into the key when they are fixed-size and fit
    /// (like `SetMethodKeysFixed` of the hash `DISTINCT`); otherwise their 128-bit hash stands for them (like `SetMethodHashed`).
    HashMap<UInt128, size_t, UInt128HashCRC32> index;
    /// Set when the key columns are packed; the size of the value of each key column.
    bool packed_keys = false;
    std::vector<size_t> key_sizes;
    /// The key columns of the current chunk.
    ColumnRawPtrs key_columns;

    /// One row of the sort columns: the worst of the `k` keys that survived the last trim, set by the first trim that evicts.
    MutableColumns boundary;
    bool has_boundary = false;
    /// The first column of the boundary is published here each time the boundary moves. Every stream's boundary is
    /// a valid one for the whole input: the stream already holds `k` keys, and the best row of each of them in the
    /// whole input is at least as good. May be null.
    TopKThresholdTrackerPtr threshold_tracker;
    /// Incremented each time the boundary moves.
    size_t boundary_version = 0;
    /// Rows appended to `kept` since the boundary was last updated.
    size_t appended_since_boundary = 0;

    /// Scratch buffers of `compareWithBoundary`.
    PaddedPODArray<UInt64> rows_to_compare;
    PaddedPODArray<Int8> compare_results;
    /// The rows of the chunk not worse than the boundary in the columns `compareWithBoundary` compares.
    PaddedPODArray<UInt32> candidate_rows;

    /// Whether `consume` visits the rows of a chunk in the sort order when many of them pass the boundary.
    bool use_sorted_visit = true;

    /// Scratch buffers of `consume`.
    IColumn::Permutation permutation;
    IColumn::Filter visited_rows;

    /// Scratch buffers of `trim`.
    std::vector<size_t> trim_rows;
    IColumn::Filter trim_filter;
};

}
