#include <Storages/TimeSeries/validateTimeSeriesHistograms.h>

#include <Columns/ColumnArray.h>
#include <Common/NaNUtils.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBufferFromString.h>
#include <DataTypes/TimeSeries/TimeSeriesColumnNames.h>
#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumnsView.h>

#include <algorithm>
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

    using ColumnsView = TimeSeriesHistogramsColumnsView;

    /// Throws INCORRECT_DATA about a histogram sample: its number among the samples of the block, the row of the block it belongs
    /// to (found from the offsets of the outer arrays) and its timestamp, serialized with the type of the `histograms.timestamp` column.
    class InvalidHistogramThrower
    {
    public:
        InvalidHistogramThrower(const Block & block, const ColumnsView & columns)
            : timestamps(columns.timestamps)
            , offsets(*columns.outer_offsets)
            , timestamp_type(assert_cast<const DataTypeArray &>(
                *block.getByName(TimeSeriesHistogramsColumns::getOuterColumnName(TimeSeriesColumnNames::Timestamp)).type).getNestedType())
        {
        }

        template <typename... Args>
        [[noreturn]] void fail(size_t sample, fmt::format_string<Args...> format, Args &&... args) const
        {
            WriteBufferFromOwnString timestamp_text;
            timestamp_type->getDefaultSerialization()->serializeText(timestamps, sample, timestamp_text, {});
            /// `offsets[i]` is the end of the samples of row `i`, so the row of a sample is the first offset greater than its number.
            const size_t row = std::upper_bound(offsets.begin(), offsets.end(), sample) - offsets.begin();
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid histogram sample #{} in row #{} with timestamp {}: {}",
                sample, row, timestamp_text.str(), fmt::format(format, std::forward<Args>(args)...));
        }

    private:
        const IColumn & timestamps;
        const ColumnArray::Offsets & offsets;
        const DataTypePtr timestamp_type;
    };

    /// The rules, each applied to one row. `fail` is `InvalidHistogramThrower::fail` bound to the row.

    /// Only the flavour bit may be set.
    template <typename Fail>
    void checkFlags(UInt8 flags, const Fail & fail)
    {
        if (TimeSeriesHistogramFlags::hasReservedBits(flags))
            fail("flags {} has reserved bits set, only bit 0 (a float histogram) may be set", static_cast<UInt32>(flags));
    }

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

    /// `histograms_max_buckets` limits the number of buckets of both sides together, like Prometheus's `native_histogram_bucket_limit`.
    template <typename Fail>
    void checkMaxBuckets(size_t num_positive_buckets, size_t num_negative_buckets, UInt64 max_buckets, const Fail & fail)
    {
        if (max_buckets && (num_positive_buckets + num_negative_buckets > max_buckets))
            fail("{} buckets exceed the limit of {} buckets per histogram set by the `histograms_max_buckets` setting",
                num_positive_buckets + num_negative_buckets, max_buckets);
    }

    /// The buckets of one side: the indexes increase strictly (so they are unique and the pairs are sorted) and no count is negative
    /// (Prometheus's `checkHistogramBuckets`). A count of 0 is allowed, as in Prometheus: the buckets are stored as sent.
    /// Prometheus addresses the buckets with spans whose offset from the previous span is an `Int32`: a wider gap between two indexes
    /// can't be returned over remote read, so it is rejected (remote write can't produce it, a direct INSERT can).
    template <typename Count, typename Fail>
    void checkBuckets(const ColumnsView::Pairs<Count> & buckets, std::string_view side, const Fail & fail)
    {
        for (size_t i = 0; i != buckets.size(); ++i)
        {
            if (i > 0)
            {
                if (buckets.indexes[i] <= buckets.indexes[i - 1])
                    fail("{} side: the bucket indexes must be strictly increasing, but bucket #{} has index {} after index {}",
                        side, i, buckets.indexes[i], buckets.indexes[i - 1]);
                if (static_cast<Int64>(buckets.indexes[i]) - buckets.indexes[i - 1] - 1 > std::numeric_limits<Int32>::max())
                    fail("{} side: the gap between the bucket indexes {} and {} is too wide for a span offset",
                        side, buckets.indexes[i - 1], buckets.indexes[i]);
            }
            if (buckets.counts[i] < 0)
                fail("{} side: bucket #{} with index {} has a negative count {}", side, i, buckets.indexes[i], buckets.counts[i]);
        }
    }

    /// Prometheus's `checkHistogramCustomBounds`: the bounds are finite and strictly increasing without an explicit +Inf, and they
    /// cover the (only) positive side: n bounds define n + 1 buckets with the indexes 0..n, the last bucket goes up to +Inf and has
    /// no explicit bound.
    template <typename Fail>
    void checkCustomBounds(std::span<const Float64> bounds, std::span<const Int32> indexes, const Fail & fail)
    {
        for (size_t i = 0; i != bounds.size(); ++i)
        {
            if (!std::isfinite(bounds[i]))
                fail("custom_values must be finite, but custom_values[{}] = {}", i, bounds[i]);
            if ((i > 0) && (bounds[i] <= bounds[i - 1]))
                fail("custom_values must be strictly increasing, but custom_values[{}] = {} follows {}", i, bounds[i], bounds[i - 1]);
        }

        if (indexes.empty())
            return;
        if (indexes.front() < 0)
            fail("positive side: a bucket has the negative index {}, but the custom buckets start at index 0", indexes.front());
        if (static_cast<size_t>(indexes.back()) > bounds.size())
            fail("only {} custom bounds are given, which is insufficient for a bucket with index {}: {} bounds define the buckets 0..{}",
                bounds.size(), indexes.back(), bounds.size(), bounds.size());
    }

    /// A histogram with custom buckets has no negative side and no zero bucket.
    template <typename Fail>
    void checkCustomBucketsHaveNoNegativeSide(size_t num_negative_buckets, const Fail & fail)
    {
        if (num_negative_buckets)
            fail("a histogram with custom buckets must not have negative buckets");
    }

    template <typename Count, typename Fail>
    void checkCustomBucketsHaveNoZeroBucket(Count zero_count, Float64 zero_threshold, const Fail & fail)
    {
        if (zero_count != 0)
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

    /// The total count and the zero bucket can't be negative either.
    template <typename Count, typename Fail>
    void checkCount(Count count, std::string_view error_message, const Fail & fail)
    {
        if (count < 0)
            fail("{} {}", error_message, count);
    }

    /// Prometheus's check of the total count of an integer histogram: the buckets and the zero bucket add up to `count_int`, or to
    /// at most `count_int` for a stale marker (`sum` is NaN), whose count may be bigger than its buckets. There is no such check for
    /// a float histogram: Prometheus skips it too, because floating-point precision would give false positives.
    template <typename Fail>
    void checkIntegerCountMatchesBuckets(
        Int64 count, Int64 zero_count, const ColumnsView::IntPairs & positive, const ColumnsView::IntPairs & negative, Float64 sum, const Fail & fail)
    {
        Int64 total = zero_count;
        const auto add_counts = [&](const ColumnsView::IntPairs & buckets)
        {
            for (const Int64 bucket_count : buckets.counts)
                if (__builtin_add_overflow(total, bucket_count, &total))
                    fail("the counts of the buckets overflow Int64");
        };
        add_counts(positive);
        add_counts(negative);

        if (isNaN(sum))
        {
            if (total > count)
                fail("{} observations found in the buckets, but count_int is {}, which is less", total, count);
        }
        else if (total != count)
        {
            fail("{} observations found in the buckets, but count_int is {}", total, count);
        }
    }

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

    /// The rules on the buckets and the counts of the flavour of the sample: `positive` and `negative` are the sides of that flavour,
    /// `count` and `zero_count` its scalars.
    template <typename Count, typename Fail>
    void checkFlavour(
        const ColumnsView::Pairs<Count> & positive,
        const ColumnsView::Pairs<Count> & negative,
        Count count,
        Count zero_count,
        Int8 schema,
        Float64 zero_threshold,
        std::span<const Float64> custom_values,
        UInt64 max_buckets,
        const Fail & fail)
    {
        checkMaxBuckets(positive.size(), negative.size(), max_buckets, fail);
        checkBuckets(positive, "positive", fail);
        checkBuckets(negative, "negative", fail);

        if (isCustomBucketsSchema(schema))
        {
            checkCustomBounds(custom_values, positive.indexes, fail);
            checkCustomBucketsHaveNoNegativeSide(negative.size(), fail);
            checkCustomBucketsHaveNoZeroBucket(zero_count, zero_threshold, fail);
        }
        else
        {
            checkExponentialBucketsHaveNoCustomBounds(custom_values.size(), fail);
        }

        checkCount(count, "count is negative:", fail);
        checkCount(zero_count, "the zero bucket has a negative count", fail);
    }
}


void validateTimeSeriesHistograms(const Block & histograms_block, UInt64 max_buckets)
{
    if (!histograms_block.rows())
        return;

    const ColumnsView columns{histograms_block};
    const InvalidHistogramThrower thrower{histograms_block, columns};
    const auto & schema = columns.schema.getData();
    const auto & zero_threshold = columns.zero_threshold.getData();
    const auto & sum = columns.sum.getData();

    const size_t num_samples = columns.size();
    for (size_t row = 0; row != num_samples; ++row)
    {
        const FailAtRow fail{thrower, row};

        checkFlags(columns.flags.getData()[row], fail);
        checkCounterResetHint(columns.counter_reset_hint.getData()[row], fail);
        checkSchema(schema[row], fail);

        /// The elements of the other flavour are not checked: remote write leaves them 0 and empty by construction, a direct insert
        /// is the user's responsibility, and the readers ignore them anyway.
        const auto custom_values = ColumnsView::floatsAt(columns.custom_values, row);
        if (columns.isFloatAt(row))
        {
            checkFlavour(
                ColumnsView::floatPairsAt(columns.positive_float, row), ColumnsView::floatPairsAt(columns.negative_float, row),
                columns.count_float.getData()[row], columns.zero_count_float.getData()[row],
                schema[row], zero_threshold[row], custom_values, max_buckets, fail);
        }
        else
        {
            const auto positive = ColumnsView::intPairsAt(columns.positive_int, row);
            const auto negative = ColumnsView::intPairsAt(columns.negative_int, row);
            const Int64 count = columns.count_int.getData()[row];
            const Int64 zero_count = columns.zero_count_int.getData()[row];
            checkFlavour(positive, negative, count, zero_count, schema[row], zero_threshold[row], custom_values, max_buckets, fail);
            checkIntegerCountMatchesBuckets(count, zero_count, positive, negative, sum[row], fail);
        }
    }
}

}
