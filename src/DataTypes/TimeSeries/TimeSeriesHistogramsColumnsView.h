#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumns.h>

#include <array>
#include <span>
#include <vector>


namespace DB
{
class Block;

/// Typed read-only view of the columns of histogram samples, one row per sample: `timestamp` and the payload elements of
/// TimeSeriesHistogramsColumns. The constructors check the types of the columns and throw ILLEGAL_COLUMN naming the element.
/// Every consumer of the stored histograms goes through this view and never reads the tuple directly, so that another encoding
/// of the counts can later put a decode step behind it. The static accessors return the pairs or the values of one row of an
/// array element as contiguous ranges.
class TimeSeriesHistogramsColumnsView
{
public:
    /// The buckets of one side of a histogram: bucket `i` has the index `indexes[i]` and the count `counts[i]`; the indexes increase.
    template <typename Count>
    struct Pairs
    {
        std::span<const Int32> indexes;
        std::span<const Count> counts;

        size_t size() const { return indexes.size(); }
    };

    using IntPairs = Pairs<Int64>;
    using FloatPairs = Pairs<Float64>;

    /// Over the sub-columns of a tuple of the stored shape `Tuple(timestamp, flags, ..., custom_values)`, e.g. the data of the
    /// `histograms` column of a samples table or of the result of `timeSeriesSelector`. The elements are found by position
    /// (see `TimeSeriesHistogramsColumns::getTuplePosition`).
    explicit TimeSeriesHistogramsColumnsView(const ColumnTuple & tuple);

    /// Over the nested columns of the outer `histograms.*` columns of `block` (see `TimeSeriesHistogramsColumns::getOuterColumnName`):
    /// each is an array column, and all of them share one offsets column, which `outer_offsets` exposes. Throws ILLEGAL_COLUMN if an
    /// array has other offsets than `histograms.timestamp`.
    explicit TimeSeriesHistogramsColumnsView(const Block & block);

    /// The number of samples.
    size_t size() const { return timestamps.size(); }

    const IColumn & timestamps;
    const ColumnUInt8 & flags;
    const ColumnUInt8 & counter_reset_hint;
    const ColumnInt8 & schema;
    const ColumnFloat64 & zero_threshold;
    const ColumnFloat64 & sum;
    const ColumnInt64 & count_int;
    const ColumnInt64 & zero_count_int;
    const ColumnFloat64 & count_float;
    const ColumnFloat64 & zero_count_float;
    const ColumnArray & positive_int;
    const ColumnArray & negative_int;
    const ColumnArray & positive_float;
    const ColumnArray & negative_float;
    const ColumnArray & custom_values;

    /// The offsets shared by the outer arrays, one entry per row of the block; null for a view over a tuple.
    const ColumnArray::Offsets * const outer_offsets = nullptr;

    /// The columns of the view in the order of the elements of the stored tuple (see `TimeSeriesHistogramsColumns::getTimestampPosition`
    /// and `getTuplePosition`): `timestamps` first, then the payload elements. For building a tuple of the stored shape from the view.
    using StoredColumns = std::array<const IColumn *, TimeSeriesHistogramsColumns::getAll().size() + 1>;
    StoredColumns getColumnsInStoredOrder() const;

    /// Whether sample `row` is a float histogram (bit `IsFloat` of its flags), otherwise it is an integer one.
    bool isFloatAt(size_t row) const { return TimeSeriesHistogramFlags::isFloat(flags.getData()[row]); }

    /// The total count of the observations of sample `row` of either flavour, as `Float64`.
    Float64 countAt(size_t row) const
    {
        return isFloatAt(row) ? count_float.getData()[row] : static_cast<Float64>(count_int.getData()[row]);
    }

    /// The buckets of row `row` of `positive_int` or `negative_int`.
    static IntPairs intPairsAt(const ColumnArray & pairs, size_t row);

    /// The buckets of row `row` of `positive_float` or `negative_float`.
    static FloatPairs floatPairsAt(const ColumnArray & pairs, size_t row);

    /// The values of row `row` of `custom_values`.
    static std::span<const Float64> floatsAt(const ColumnArray & values, size_t row);

private:
    /// `Source` gives the columns: `timestamps()`, `element(TimeSeriesHistogramsColumn)` and `outerOffsets()`.
    template <typename Source>
    explicit TimeSeriesHistogramsColumnsView(const Source & source);
};


/// The mutable counterpart of TimeSeriesHistogramsColumnsView, for building the columns of histogram samples.
class TimeSeriesHistogramsColumnsMutableView
{
public:
    /// The payload columns, one per element of the registry in the canonical order (indexed by the enum).
    using Columns = std::array<MutableColumnPtr, TimeSeriesHistogramsColumns::getAll().size()>;

    /// Creates empty payload columns of the types of the registry.
    static Columns createPayloadColumns();

    /// Over the columns being built: `timestamps_` and the payload columns, all of them non-null.
    TimeSeriesHistogramsColumnsMutableView(IColumn & timestamps_, const Columns & payload);

    /// Over the sub-columns of a tuple of the stored shape (see the read-only view).
    explicit TimeSeriesHistogramsColumnsMutableView(ColumnTuple & tuple);

    /// The number of samples.
    size_t size() const { return timestamps.size(); }

    IColumn & timestamps;
    ColumnUInt8 & flags;
    ColumnUInt8 & counter_reset_hint;
    ColumnInt8 & schema;
    ColumnFloat64 & zero_threshold;
    ColumnFloat64 & sum;
    ColumnInt64 & count_int;
    ColumnInt64 & zero_count_int;
    ColumnFloat64 & count_float;
    ColumnFloat64 & zero_count_float;
    ColumnArray & positive_int;
    ColumnArray & negative_int;
    ColumnArray & positive_float;
    ColumnArray & negative_float;
    ColumnArray & custom_values;

private:
    /// `Source` gives the columns: `timestamps()` and `element(TimeSeriesHistogramsColumn)`.
    template <typename Source>
    explicit TimeSeriesHistogramsColumnsMutableView(const Source & source);
};

}
