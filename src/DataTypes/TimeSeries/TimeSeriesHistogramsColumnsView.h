#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumns.h>

#include <array>
#include <span>
#include <type_traits>


namespace DB
{
class Block;

/// The buckets of one side of a histogram: bucket `i` has the index `indexes[i]` and the count `counts[i]`; the indexes increase.
template <typename Count>
struct TimeSeriesHistogramBucketPairs
{
    std::span<const Int32> indexes;
    std::span<const Count> counts;

    size_t size() const { return indexes.size(); }
};

/// Typed view of the payload columns of histogram samples, one row per sample: the elements of TimeSeriesHistogramsColumns after
/// `timestamp` (positions 1 to 14 of the stored tuple; position 0 is never looked at), read-only when `is_const`, otherwise over
/// columns being built. The constructors throw ILLEGAL_COLUMN on a column of another type than the registry's.
template <bool is_const>
class TimeSeriesHistogramPayloadColumnsViewImpl
{
public:
    template <typename T>
    using Ref = std::conditional_t<is_const, const T &, T &>;

    template <typename T>
    using Ptr = std::conditional_t<is_const, const T *, T *>;

    template <typename Count>
    using Pairs = TimeSeriesHistogramBucketPairs<Count>;

    using IntPairs = Pairs<Int64>;
    using FloatPairs = Pairs<Float64>;

    static constexpr size_t NUM_PAYLOAD_COLUMNS = TimeSeriesHistogramsColumns::getAll().size();

    /// The payload columns being built, indexed by TimeSeriesHistogramsColumn.
    using Columns = std::array<MutableColumnPtr, NUM_PAYLOAD_COLUMNS>;

    static Columns createPayloadColumns() requires (!is_const);

    /// Over a tuple of the stored shape (see `TimeSeriesHistogramsColumns::getTuplePosition`), e.g. the data of a `histograms` column.
    explicit TimeSeriesHistogramPayloadColumnsViewImpl(Ref<ColumnTuple> tuple);

    /// Over the payload columns being built, all of them non-null.
    explicit TimeSeriesHistogramPayloadColumnsViewImpl(const Columns & payload) requires (!is_const);

    /// The read-only view of a mutable view. Explicit because it checks the columns again: a loop must not convert per row.
    explicit TimeSeriesHistogramPayloadColumnsViewImpl(const TimeSeriesHistogramPayloadColumnsViewImpl<!is_const> & other) requires is_const;

    size_t size() const { return flags.size(); }

    Ref<ColumnUInt8> flags;
    Ref<ColumnUInt8> counter_reset_hint;
    Ref<ColumnInt8> schema;
    Ref<ColumnFloat64> zero_threshold;
    Ref<ColumnFloat64> sum;
    Ref<ColumnInt64> count_int;
    Ref<ColumnInt64> zero_count_int;
    Ref<ColumnFloat64> count_float;
    Ref<ColumnFloat64> zero_count_float;
    Ref<ColumnArray> positive_int;
    Ref<ColumnArray> negative_int;
    Ref<ColumnArray> positive_float;
    Ref<ColumnArray> negative_float;
    Ref<ColumnArray> custom_values;

    Ref<IColumn> getPayloadColumn(TimeSeriesHistogramsColumn element) const;

    /// The payload columns in the order of the stored tuple: `flags` first.
    using PayloadColumns = std::array<Ptr<IColumn>, NUM_PAYLOAD_COLUMNS>;
    PayloadColumns getPayloadColumnsInStoredOrder() const;

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

    /// Appends the payload of sample `row` of `source`.
    void insertPayloadFrom(const TimeSeriesHistogramPayloadColumnsViewImpl<true> & source, size_t row) const requires (!is_const);

protected:
    /// Tag of the constructors over a source of columns: without it the constructor template would accept any argument, e.g. a view
    /// of the other constness, and win over the converting constructor.
    struct FromSource
    {
    };

    /// `Source` gives the columns: `element(TimeSeriesHistogramsColumn)`.
    template <typename Source>
    TimeSeriesHistogramPayloadColumnsViewImpl(FromSource, const Source & source);
};


/// The payload view with the timestamps of the samples: over the whole stored tuple, or over the outer `histograms.*` columns of a
/// block. The constructors throw ILLEGAL_COLUMN unless the timestamps have a timestamp type of a TimeSeries table.
template <bool is_const>
class TimeSeriesHistogramsColumnsViewImpl : public TimeSeriesHistogramPayloadColumnsViewImpl<is_const>
{
public:
    using Base = TimeSeriesHistogramPayloadColumnsViewImpl<is_const>;
    using typename Base::Columns;

    template <typename T>
    using Ref = typename Base::template Ref<T>;

    template <typename T>
    using Ptr = typename Base::template Ptr<T>;

    explicit TimeSeriesHistogramsColumnsViewImpl(Ref<ColumnTuple> tuple);

    /// Over the outer `histograms.*` array columns of `block` (see `TimeSeriesHistogramsColumns::getOuterColumnName`), which must
    /// share their offsets.
    explicit TimeSeriesHistogramsColumnsViewImpl(const Block & block) requires is_const;

    /// Over the columns being built, all of them non-null.
    TimeSeriesHistogramsColumnsViewImpl(IColumn & timestamps_, const Columns & payload) requires (!is_const);

    /// The read-only view of a mutable view (see the base class).
    explicit TimeSeriesHistogramsColumnsViewImpl(const TimeSeriesHistogramsColumnsViewImpl<!is_const> & other) requires is_const;

    Ref<IColumn> timestamps;

    /// The offsets of the outer arrays of the block, one entry per row; null for a view over anything but a block.
    const ColumnArray::Offsets * const outer_offsets = nullptr;

    /// All the columns in the order of the stored tuple: `timestamps` first, then the payload.
    using StoredColumns = std::array<Ptr<IColumn>, Base::NUM_PAYLOAD_COLUMNS + 1>;
    StoredColumns getColumnsInStoredOrder() const;

private:
    /// `Source` gives the columns: `timestamps()`, `element(TimeSeriesHistogramsColumn)` and `outerOffsets()`.
    template <typename Source>
    TimeSeriesHistogramsColumnsViewImpl(typename Base::FromSource, const Source & source);
};


using TimeSeriesHistogramPayloadColumnsView = TimeSeriesHistogramPayloadColumnsViewImpl<true>;
using TimeSeriesHistogramPayloadColumnsMutableView = TimeSeriesHistogramPayloadColumnsViewImpl<false>;
using TimeSeriesHistogramsColumnsView = TimeSeriesHistogramsColumnsViewImpl<true>;
using TimeSeriesHistogramsColumnsMutableView = TimeSeriesHistogramsColumnsViewImpl<false>;

}
