#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <Storages/TimeSeries/TimeSeriesHistogramsColumns.h>

#include <functional>
#include <span>


namespace DB
{
class Block;

/// Read-only typed views of the payload columns of histogram samples, one row per sample, in the shape of the "histograms"
/// inner table (see TimeSeriesHistogramsColumns).
struct TimeSeriesHistogramsColumnsView
{
    /// Read-only view of a column with numeric values.
    template <typename T>
    class ScalarView
    {
    public:
        explicit ScalarView(const IColumn & column) : data(assert_cast<const ColumnVector<T> &>(column).getData()) { }

        T operator[](size_t row) const { return data[row]; }

    private:
        const PaddedPODArray<T> & data;
    };

    /// Read-only view of an `Array(T)` column with numeric elements.
    template <typename T>
    class ArrayView
    {
    public:
        explicit ArrayView(const IColumn & column)
            : array(assert_cast<const ColumnArray &>(column))
            , data(assert_cast<const ColumnVector<T> &>(array.getData()).getData())
        {
        }

        size_t sizeAt(size_t row) const { return array.getSize(row); }
        std::span<const T> operator[](size_t row) const { return {data.data() + array.getOffset(row), array.getSize(row)}; }

    private:
        const ColumnArray & array;
        const PaddedPODArray<T> & data;
    };

    /// The spans of one side of a histogram: `offsets[i]` and `lengths[i]` describe span `i`.
    struct Spans
    {
        std::span<const Int32> offsets;
        std::span<const UInt32> lengths;

        size_t size() const { return offsets.size(); }
    };

    /// Read-only view of an `Array(Tuple(offset Int32, length UInt32))` column.
    class SpansView
    {
    public:
        explicit SpansView(const IColumn & column)
            : array(assert_cast<const ColumnArray &>(column))
        {
            const auto & tuple = assert_cast<const ColumnTuple &>(array.getData());
            offsets = &assert_cast<const ColumnInt32 &>(tuple.getColumn(0)).getData();
            lengths = &assert_cast<const ColumnUInt32 &>(tuple.getColumn(1)).getData();
        }

        size_t sizeAt(size_t row) const { return array.getSize(row); }

        Spans operator[](size_t row) const
        {
            const size_t start = array.getOffset(row);
            const size_t size = array.getSize(row);
            return {{offsets->data() + start, size}, {lengths->data() + start, size}};
        }

    private:
        const ColumnArray & array;
        const PaddedPODArray<Int32> * offsets;
        const PaddedPODArray<UInt32> * lengths;
    };

    /// From a block with the columns named as the columns of the "histograms" inner table.
    explicit TimeSeriesHistogramsColumnsView(const Block & block);

    /// From a tuple with the columns as elements in the canonical order, e.g. the elements of the `histogram` column
    /// returned by `timeSeriesSelector` (see `TimeSeriesHistogramsColumns::getHistogramColumnType`).
    explicit TimeSeriesHistogramsColumnsView(const ColumnTuple & tuple);

    /// The number of buckets of a side is the size of the values array of the flavour the row uses.
    size_t numPositiveBuckets(size_t row) const { return is_float[row] ? positive_values_float.sizeAt(row) : positive_values_int.sizeAt(row); }
    size_t numNegativeBuckets(size_t row) const { return is_float[row] ? negative_values_float.sizeAt(row) : negative_values_int.sizeAt(row); }
    bool zeroCountIsZero(size_t row) const { return is_float[row] ? (zero_count_float[row] == 0) : (zero_count_int[row] == 0); }

    /// The count of the observations, of the flavour the row uses.
    Float64 getCount(size_t row) const { return is_float[row] ? count_float[row] : static_cast<Float64>(count_int[row]); }

    /// The views, one per column of the registry: a column added there must be added here too.
    const ScalarView<UInt8> is_float;
    const ScalarView<UInt8> counter_reset_hint;
    const ScalarView<Int8> schema;
    const ScalarView<Float64> zero_threshold;
    const ScalarView<Float64> sum;
    const SpansView positive_spans;
    const SpansView negative_spans;
    const ArrayView<Float64> custom_values;
    const ScalarView<UInt64> count_int;
    const ScalarView<UInt64> zero_count_int;
    const ArrayView<UInt64> positive_values_int;
    const ArrayView<UInt64> negative_values_int;
    const ScalarView<Float64> count_float;
    const ScalarView<Float64> zero_count_float;
    const ArrayView<Float64> positive_values_float;
    const ArrayView<Float64> negative_values_float;

private:
    using GetColumn = std::function<const IColumn & (TimeSeriesHistogramsColumn)>;
    explicit TimeSeriesHistogramsColumnsView(const GetColumn & get_column);
};

}
