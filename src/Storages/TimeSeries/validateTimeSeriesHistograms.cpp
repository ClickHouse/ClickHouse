#include <Storages/TimeSeries/validateTimeSeriesHistograms.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBufferFromString.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesHistogramsColumns.h>

#include <cmath>
#include <limits>
#include <span>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{
    /// The schema values Prometheus accepts.
    constexpr Int8 exponential_schema_min = -4;
    constexpr Int8 exponential_schema_max = 8;
    constexpr Int8 custom_buckets_schema = -53;

    constexpr UInt8 max_counter_reset_hint = 3;

    const IColumn & getColumn(const Block & block, TimeSeriesHistogramsColumn column)
    {
        return *block.getByName(String{TimeSeriesHistogramsColumns::getName(column)}).column;
    }

    /// Read-only view of a column with numeric values.
    template <typename T>
    class ScalarView
    {
    public:
        ScalarView(const Block & block, TimeSeriesHistogramsColumn column)
            : data(assert_cast<const ColumnVector<T> &>(getColumn(block, column)).getData())
        {
        }

        T operator[](size_t row) const { return data[row]; }

    private:
        const PaddedPODArray<T> & data;
    };

    /// Read-only view of an `Array(T)` column with numeric elements.
    template <typename T>
    class ArrayView
    {
    public:
        ArrayView(const Block & block, TimeSeriesHistogramsColumn column)
            : array(assert_cast<const ColumnArray &>(getColumn(block, column)))
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
        SpansView(const Block & block, TimeSeriesHistogramsColumn column)
            : array(assert_cast<const ColumnArray &>(getColumn(block, column)))
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

    /// Throws INCORRECT_DATA about the histogram sample in the specified row.
    class InvalidHistogramThrower
    {
    public:
        explicit InvalidHistogramThrower(const Block & block) : timestamp(block.getByName(TimeSeriesColumnNames::Timestamp)) { }

        template <typename... Args>
        [[noreturn]] void fail(size_t row, fmt::format_string<Args...> format, Args &&... args) const
        {
            WriteBufferFromOwnString timestamp_text;
            timestamp.type->getDefaultSerialization()->serializeText(*timestamp.column, row, timestamp_text, {});
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid histogram sample #{} with timestamp {}: {}",
                row, timestamp_text.str(), fmt::format(format, std::forward<Args>(args)...));
        }

    private:
        const ColumnWithTypeAndName & timestamp;
    };

    /// The rules, each applied to one row. `fail` is `InvalidHistogramThrower::fail` bound to the row.

    /// The reset hint is one of unknown, yes, no, gauge.
    template <typename Fail>
    void checkCounterResetHint(UInt8 counter_reset_hint, const Fail & fail)
    {
        if (counter_reset_hint > max_counter_reset_hint)
        {
            fail("counter_reset_hint {} is out of the range 0..{}",
                static_cast<UInt32>(counter_reset_hint),
                static_cast<UInt32>(max_counter_reset_hint));
        }
    }

    bool isCustomBucketsSchema(Int8 schema)
    {
        return schema == custom_buckets_schema;
    }

    /// The schema is either exponential or custom buckets.
    template <typename Fail>
    void checkSchema(Int8 schema, const Fail & fail)
    {
        if (!isCustomBucketsSchema(schema) && ((schema < exponential_schema_min) || (schema > exponential_schema_max)))
            fail("schema {} is invalid: expected {}..{} for exponential buckets or {} for custom buckets",
                schema, exponential_schema_min, exponential_schema_max, custom_buckets_schema);
    }

    /// `histograms_max_buckets` limits the number of buckets of both sides together.
    template <typename Fail>
    void checkMaxBuckets(size_t num_positive_buckets, size_t num_negative_buckets, UInt64 max_buckets, const Fail & fail)
    {
        if (max_buckets && (num_positive_buckets + num_negative_buckets > max_buckets))
            fail("{} buckets exceed the limit of {} buckets per histogram set by the `histograms_max_buckets` setting",
                num_positive_buckets + num_negative_buckets, max_buckets);
    }

    /// Prometheus's `checkHistogramSpans` for one side of a histogram with exponential buckets: the spans describe exactly
    /// the given buckets and a span after the first doesn't go backwards. The bucket indexes must also fit in Int32:
    /// they are relative in the spans and absolute at evaluation time.
    template <typename Fail>
    void checkExponentialSpans(const Spans & spans, size_t num_buckets, std::string_view side, const Fail & fail)
    {
        size_t total_length = 0;
        Int64 bucket_index = 0;
        for (size_t i = 0; i != spans.size(); ++i)
        {
            if ((i > 0) && (spans.offsets[i] < 0))
                fail("{} side: span #{} has a negative offset {}", side, i, spans.offsets[i]);
            bucket_index += spans.offsets[i];
            bucket_index += spans.lengths[i];
            if ((bucket_index < std::numeric_limits<Int32>::min()) || (bucket_index > std::numeric_limits<Int32>::max()))
                fail("{} side: the bucket indexes overflow Int32", side);
            total_length += spans.lengths[i];
        }
        if (total_length != num_buckets)
            fail("{} side: the spans need {} buckets, but {} buckets are given", side, total_length, num_buckets);
    }

    /// Prometheus's `checkHistogramCustomBounds`: the bounds are finite and strictly increasing without an explicit +Inf,
    /// the spans of the (only) positive side describe exactly the given buckets going forwards from index 0, and the bounds
    /// cover them - the last bucket goes up to +Inf and has no explicit bound, so n bounds define n + 1 buckets.
    template <typename Fail>
    void checkCustomBounds(std::span<const Float64> bounds, const Spans & spans, size_t num_buckets, const Fail & fail)
    {
        for (size_t i = 0; i != bounds.size(); ++i)
        {
            if (std::isnan(bounds[i]))
                fail("custom_values must not contain NaN");
            if ((i > 0) && (bounds[i] <= bounds[i - 1]))
                fail("custom_values must be strictly increasing, but custom_values[{}] = {} follows {}", i, bounds[i], bounds[i - 1]);
        }
        if (!bounds.empty() && (bounds.back() == std::numeric_limits<Float64>::infinity()))
            fail("the last +Inf bound must not be explicitly defined in custom_values");

        size_t total_length = 0;
        size_t total_span = 0;
        for (size_t i = 0; i != spans.size(); ++i)
        {
            if (spans.offsets[i] < 0)
                fail("positive side: span #{} has a negative offset {}", i, spans.offsets[i]);
            total_length += spans.lengths[i];
            total_span += static_cast<size_t>(spans.offsets[i]) + spans.lengths[i];
        }
        if (total_length != num_buckets)
            fail("positive side: the spans need {} buckets, but {} buckets are given", total_length, num_buckets);
        if (bounds.size() + 1 < total_span)
            fail("only {} custom bounds are given, which is insufficient to cover the total span length of {}", bounds.size(), total_span);
    }

    /// A histogram with custom buckets has no negative side and no zero bucket.
    template <typename Fail>
    void checkCustomBucketsHaveNoNegativeSide(size_t num_negative_spans, size_t num_negative_buckets, const Fail & fail)
    {
        if (num_negative_spans || num_negative_buckets)
            fail("a histogram with custom buckets must not have negative buckets");
    }

    template <typename Fail>
    void checkCustomBucketsHaveNoZeroBucket(bool zero_count_is_zero, Float64 zero_threshold, const Fail & fail)
    {
        if (!zero_count_is_zero)
            fail("a histogram with custom buckets must have a zero count of 0");
        if (zero_threshold != 0)
            fail("a histogram with custom buckets must have a zero threshold of 0");
    }

    /// A histogram with exponential buckets has no custom bounds.
    template <typename Fail>
    void checkExponentialBucketsHaveNoCustomBounds(size_t num_custom_values, const Fail & fail)
    {
        if (num_custom_values)
            fail("a histogram with exponential buckets must not have custom_values");
    }

    /// Prometheus's `checkHistogramBuckets` for a float histogram: no negative counts.
    /// There is no check of `count_float` against the sum of the buckets: Prometheus skips it too, because
    /// floating-point precision would give false positives.
    template <typename Fail>
    void checkFloatBuckets(std::span<const Float64> buckets, std::string_view side, const Fail & fail)
    {
        for (size_t i = 0; i != buckets.size(); ++i)
        {
            if (buckets[i] < 0)
                fail("{} side: bucket #{} has a negative count {}", side, i, buckets[i]);
        }
    }

    template <typename Fail>
    void checkFloatZeroCount(Float64 zero_count, const Fail & fail)
    {
        if (zero_count < 0)
            fail("the zero bucket has a negative count {}", zero_count);
    }

    /// The count check of Prometheus's `Histogram.Validate`: the buckets and the zero bucket sum up to `count` exactly,
    /// or to at most `count` when `sum` is NaN (NaN observations are counted but fall into no bucket).
    /// The buckets are UInt64 here, so they can't be negative, but their sum can overflow.
    template <typename Fail>
    void checkIntCount(
        UInt64 count, UInt64 zero_count, std::span<const UInt64> positive_buckets, std::span<const UInt64> negative_buckets, Float64 sum,
        const Fail & fail)
    {
        UInt64 sum_of_buckets = zero_count;
        const auto add = [&](UInt64 value)
        {
            if (__builtin_add_overflow(sum_of_buckets, value, &sum_of_buckets))
                fail("the sum of the bucket counts overflows UInt64");
        };
        for (const UInt64 value : positive_buckets)
            add(value);
        for (const UInt64 value : negative_buckets)
            add(value);

        if (std::isnan(sum) ? (sum_of_buckets > count) : (sum_of_buckets != count))
            fail("{} observations are found in the buckets, but count_int is {}", sum_of_buckets, count);
    }

    /// All the typed views of the columns of a block with the shape of the "histograms" table.
    struct HistogramsColumns
    {
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

        explicit HistogramsColumns(const Block & block)
            : is_float(block, TimeSeriesHistogramsColumn::IsFloat)
            , counter_reset_hint(block, TimeSeriesHistogramsColumn::CounterResetHint)
            , schema(block, TimeSeriesHistogramsColumn::Schema)
            , zero_threshold(block, TimeSeriesHistogramsColumn::ZeroThreshold)
            , sum(block, TimeSeriesHistogramsColumn::Sum)
            , positive_spans(block, TimeSeriesHistogramsColumn::PositiveSpans)
            , negative_spans(block, TimeSeriesHistogramsColumn::NegativeSpans)
            , custom_values(block, TimeSeriesHistogramsColumn::CustomValues)
            , count_int(block, TimeSeriesHistogramsColumn::CountInt)
            , zero_count_int(block, TimeSeriesHistogramsColumn::ZeroCountInt)
            , positive_values_int(block, TimeSeriesHistogramsColumn::PositiveValuesInt)
            , negative_values_int(block, TimeSeriesHistogramsColumn::NegativeValuesInt)
            , count_float(block, TimeSeriesHistogramsColumn::CountFloat)
            , zero_count_float(block, TimeSeriesHistogramsColumn::ZeroCountFloat)
            , positive_values_float(block, TimeSeriesHistogramsColumn::PositiveValuesFloat)
            , negative_values_float(block, TimeSeriesHistogramsColumn::NegativeValuesFloat)
        {
        }

        /// The number of buckets of a side is the size of the values array of the flavour the row uses.
        size_t numPositiveBuckets(size_t row) const { return is_float[row] ? positive_values_float.sizeAt(row) : positive_values_int.sizeAt(row); }
        size_t numNegativeBuckets(size_t row) const { return is_float[row] ? negative_values_float.sizeAt(row) : negative_values_int.sizeAt(row); }
        bool zeroCountIsZero(size_t row) const { return is_float[row] ? (zero_count_float[row] == 0) : (zero_count_int[row] == 0); }
    };

    /// Binds `InvalidHistogramThrower::fail` to a row.
    class FailAtRow
    {
    public:
        FailAtRow(const InvalidHistogramThrower & thrower_, size_t row_) : thrower(thrower_), row(row_) { }

        template <typename... Args>
        [[noreturn]] void operator()(fmt::format_string<Args...> format, Args &&... args) const
        {
            thrower.fail(row, format, std::forward<Args>(args)...);
        }

    private:
        const InvalidHistogramThrower & thrower;
        const size_t row;
    };
}


void validateTimeSeriesHistograms(const Block & histograms_block, UInt64 max_buckets)
{
    const size_t num_rows = histograms_block.rows();
    if (!num_rows)
        return;

    const HistogramsColumns columns{histograms_block};
    const InvalidHistogramThrower thrower{histograms_block};

    for (size_t row = 0; row != num_rows; ++row)
    {
        const FailAtRow fail{thrower, row};

        checkCounterResetHint(columns.counter_reset_hint[row], fail);
        checkSchema(columns.schema[row], fail);

        const bool is_float = columns.is_float[row];
        const size_t num_positive_buckets = columns.numPositiveBuckets(row);
        const size_t num_negative_buckets = columns.numNegativeBuckets(row);
        checkMaxBuckets(num_positive_buckets, num_negative_buckets, max_buckets, fail);

        if (isCustomBucketsSchema(columns.schema[row]))
        {
            checkCustomBounds(columns.custom_values[row], columns.positive_spans[row], num_positive_buckets, fail);
            checkCustomBucketsHaveNoNegativeSide(columns.negative_spans.sizeAt(row), num_negative_buckets, fail);
            checkCustomBucketsHaveNoZeroBucket(columns.zeroCountIsZero(row), columns.zero_threshold[row], fail);
        }
        else
        {
            checkExponentialSpans(columns.positive_spans[row], num_positive_buckets, "positive", fail);
            checkExponentialSpans(columns.negative_spans[row], num_negative_buckets, "negative", fail);
            checkExponentialBucketsHaveNoCustomBounds(columns.custom_values.sizeAt(row), fail);
        }

        if (is_float)
        {
            checkFloatBuckets(columns.positive_values_float[row], "positive", fail);
            checkFloatBuckets(columns.negative_values_float[row], "negative", fail);
            checkFloatZeroCount(columns.zero_count_float[row], fail);
        }
        else
        {
            checkIntCount(columns.count_int[row], columns.zero_count_int[row],
                columns.positive_values_int[row], columns.negative_values_int[row], columns.sum[row], fail);
        }
    }
}

}
