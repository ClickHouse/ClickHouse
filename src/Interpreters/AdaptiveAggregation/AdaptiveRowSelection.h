#pragma once

#include <memory>
#include <vector>

#include <base/types.h>
#include <Common/PODArray_fwd.h>
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
    bool countsRows() const { return !count_aggregates.empty(); }
};

/// The row selection of `aggregates`, or null when they do not all select one row by the same ordering column
/// of a type stored as an integer of up to 64 bits: an integer, `Enum`, `IPv4`, date, time, interval,
/// `Decimal32` or `Decimal64`. The caller guarantees that every aggregate carries its argument names.
std::unique_ptr<const AdaptiveRowSelection> adaptiveRowSelectionOf(const AggregateDescriptions & aggregates);

/// Fills `ranks[row_begin, row_end)` with the rank of each row: its value of `column`, the ordering argument
/// of `selection`, mapped to an `Int64` that is larger for the row the selection prefers. The ranks follow
/// the values for a selection of the largest and run against them for a selection of the smallest, and equal
/// values get equal ranks, so the best of several rows is their first row of the largest rank. For example,
/// the values 5, 9, 9 of `max` rank as 5, 9, 9, so the best row is the second; of `min`, as -6, -10, -10, so
/// the best row is the first. The column may be `Sparse`, because an aggregate of one argument, such as
/// `max(ts)`, keeps a sparse argument as it is.
void fillRowSelectionRanks(
    const AdaptiveRowSelection & selection, const IColumn & column, size_t row_begin, size_t row_end, PaddedPODArray<Int64> & ranks);

}
