#pragma once

#include <memory>
#include <vector>

#include <base/types.h>
#include <Common/PODArray.h>
#include <Core/TypeId.h>
#include <Interpreters/AggregateDescription.h>

/// The aggregations whose aggregate functions all pick one row of their group by one ordering column, which
/// lets a frozen producer combine the misses of a key into one record (see `AdaptiveRowSelection`): which
/// aggregates qualify, and the ranks by which the producer finds the best of their rows.

namespace DB
{

class IColumn;

/// When every aggregate function selects one row of its group by the same ordering column, the misses of a
/// key that a frozen producer combines into its partition's tail (see `AdaptivePartitionTails`) stage a
/// single record, of their best row: `argMax(x, ts)`, `max(ts)` and `any(x)` keep the row with the largest
/// `ts`, and `argMin(x, ts)`, `min(ts)` and `any(x)` the row with the smallest. The earlier row stays on a
/// tie, as in `argMax`. A `count` among them takes the combined rows from the record's multiplicity (see
/// `AdaptiveArgumentLayout::multiplicity_offset`). See `Aggregator::executeFrozenImpl`.
struct AdaptiveRowSelection
{
    /// The aggregate, and its argument, that read the ordering column, and the column's type.
    size_t ordering_aggregate = 0;
    size_t ordering_argument = 0;
    TypeIndex ordering_type = TypeIndex::Nothing;
    /// Whether the best row has the largest ordering value, or the smallest.
    bool largest = true;
    /// The `count` aggregates, in query order.
    std::vector<size_t> count_aggregates;

    /// Whether a record carries the number of rows it stands for, which a `count` needs.
    [[nodiscard]] bool countsRows() const { return !count_aggregates.empty(); }
};

/// The row selection of `aggregates`, or null when they do not all select one row by the same ordering column
/// of a type stored as an integer of up to 64 bits: an integer, `Enum`, `IPv4`, date, time, interval,
/// `Decimal32` or `Decimal64`. Each aggregate function states which rows its result depends on (see
/// `IAggregateFunction::getRowSelection`), so another function joins by stating it. The caller guarantees
/// that every aggregate carries its argument names.
std::unique_ptr<const AdaptiveRowSelection> adaptiveRowSelectionOf(const AggregateDescriptions & aggregates);

/// Fills `ranks[row_begin, row_end)` with the rank of each row: its value of `column`, the ordering argument
/// of `selection`, mapped to an `Int64` that is larger for the row the selection prefers. The ranks follow
/// the values for a selection of the largest and run against them for a selection of the smallest, and equal
/// values get equal ranks, so the best of several rows is their first row of the largest rank. For example,
/// the values 5, 9, 9 of `max` rank as 5, 9, 9, so the best row is the second; of `min`, as -6, -10, -10, so
/// the best row is the first. The column may be `Sparse`, because an aggregate of one argument, such as
/// `max(ts)`, keeps a sparse argument as it is.
/// The caller guarantees that `ranks` has `row_end` elements or more.
void fillRowSelectionRanks(
    const AdaptiveRowSelection & selection, const IColumn & column, size_t row_begin, size_t row_end, PaddedPODArray<Int64> & ranks);

/// The ranks of a block's rows under a row selection, filled as far as a kernel asks for them: a block
/// whose misses neither look the tails of their partitions up nor repeat the previous miss compares no
/// rows, so it needs no ranks (see `AdaptiveRunCombiner`), and a block that goes through the tails only for
/// its first rows needs only theirs.
class AdaptiveRowSelectionRanks
{
public:
    /// Starts a block: the ranks are those of the rows from `row_begin_` to `row_end_` of `column_`, the
    /// ordering argument of `selection_`. The caller guarantees that both outlive the block.
    void startBlock(const AdaptiveRowSelection & selection_, const IColumn & column_, size_t row_begin_, size_t row_end_)
    {
        selection = &selection_;
        column = &column_;
        filled_end = row_begin_;
        row_end = row_end_;
        /// The ranks take their final place now, so that the pointer to them stays valid through the block.
        ranks.resize(row_end);
    }

    /// Ranks the rows of the block before `end` that are not ranked yet.
    void rankUpTo(size_t end)
    {
        if (filled_end < end)
        {
            fillRowSelectionRanks(*selection, *column, filled_end, end, ranks);
            filled_end = end;
        }
    }

    /// Ranks all the rows of the block that are not ranked yet.
    void rankAll() { rankUpTo(row_end); }

    /// The ranks, indexed by row, which hold the ranks of the rows that are ranked. The pointer stays the
    /// same through the block.
    [[nodiscard]] const Int64 * data() const { return ranks.data(); }

private:
    const AdaptiveRowSelection * selection = nullptr;
    const IColumn * column = nullptr;
    size_t filled_end = 0;
    size_t row_end = 0;
    PaddedPODArray<Int64> ranks;
};

}
