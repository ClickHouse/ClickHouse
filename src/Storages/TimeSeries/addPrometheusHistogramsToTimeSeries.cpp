#include <Storages/TimeSeries/addPrometheusHistogramsToTimeSeries.h>

#if USE_PROMETHEUS_PROTOBUFS

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Common/assert_cast.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumnsView.h>

#include <type_traits>


namespace DB
{

namespace
{
    using ColumnsView = TimeSeriesHistogramsColumnsView;

    /// Writes the buckets of one side (the positive or the negative one) of an integer histogram as Prometheus sends them:
    /// the deltas, each bucket relative to the previous one (the first one to 0).
    template <bool positive>
    class IntBucketsWriter
    {
    public:
        explicit IntBucketsWriter(prometheus::Histogram & histogram_) : histogram(histogram_) { }

        prometheus::BucketSpan * addSpan() const
        {
            if constexpr (positive)
                return histogram.add_positive_spans();
            else
                return histogram.add_negative_spans();
        }

        void addBucket(Int64 count)
        {
            if constexpr (positive)
                histogram.add_positive_deltas(count - previous_count);
            else
                histogram.add_negative_deltas(count - previous_count);
            previous_count = count;
        }

    private:
        prometheus::Histogram & histogram;
        Int64 previous_count = 0;
    };

    /// Writes the buckets of one side of a float histogram: the counts as they are.
    template <bool positive>
    class FloatBucketsWriter
    {
    public:
        explicit FloatBucketsWriter(prometheus::Histogram & histogram_) : histogram(histogram_) { }

        prometheus::BucketSpan * addSpan() const
        {
            if constexpr (positive)
                return histogram.add_positive_spans();
            else
                return histogram.add_negative_spans();
        }

        void addBucket(Float64 count) const
        {
            if constexpr (positive)
                histogram.add_positive_counts(count);
            else
                histogram.add_negative_counts(count);
        }

    private:
        prometheus::Histogram & histogram;
    };

    /// Writes the buckets of one side (the positive or the negative one): the spans, each covering a run of consecutive indexes with
    /// the offset from the index after the previous span (the first one from 0), and one value per bucket in the order of the spans.
    /// The type of the counts decides the writer of the values: `Int64` buckets are written as deltas, `Float64` ones as counts.
    template <bool positive, typename Count>
    void addBuckets(const ColumnsView::Pairs<Count> & buckets, prometheus::Histogram & histogram)
    {
        using Writer = std::conditional_t<std::is_same_v<Count, Int64>, IntBucketsWriter<positive>, FloatBucketsWriter<positive>>;
        Writer writer{histogram};
        Int64 next_index = 0;
        prometheus::BucketSpan * span = nullptr;
        for (size_t i = 0; i != buckets.size(); ++i)
        {
            const Int64 index = buckets.indexes[i];
            if (!span || (index != next_index))
            {
                span = writer.addSpan();
                span->set_offset(static_cast<Int32>(index - next_index));
            }
            span->set_length(span->length() + 1);
            next_index = index + 1;
            writer.addBucket(buckets.counts[i]);
        }
    }

    /// The reset hint a Prometheus server returns over remote read is `GAUGE`, `NO` or `UNKNOWN`, never `YES`: a reset, explicit or
    /// detected, starts a new chunk in its TSDB, and the first sample of a chunk is returned with an unknown hint because the previous
    /// chunk may be gone (`counterResetHint` in `tsdb/chunkenc/histogram_meta.go`), so the readers detect resets from the data.
    /// PromQL trusts `YES` without checking the data, so a stored reset is returned as unknown here too.
    prometheus::Histogram_ResetHint toPrometheusResetHint(const UInt8 stored_hint)
    {
        if (stored_hint == TimeSeriesHistogramCounterResetHint::Gauge)
            return prometheus::Histogram::GAUGE;
        if (stored_hint == TimeSeriesHistogramCounterResetHint::NoReset)
            return prometheus::Histogram::NO;
        return prometheus::Histogram::UNKNOWN;
    }

    void addHistogram(const ColumnsView & columns, const size_t row, const Int64 timestamp_ms, prometheus::Histogram & histogram)
    {
        if (columns.isFloatAt(row))
        {
            histogram.set_count_float(columns.count_float.getData()[row]);
            histogram.set_zero_count_float(columns.zero_count_float.getData()[row]);
            addBuckets<true>(ColumnsView::floatPairsAt(columns.positive_float, row), histogram);
            addBuckets<false>(ColumnsView::floatPairsAt(columns.negative_float, row), histogram);
        }
        else
        {
            histogram.set_count_int(columns.count_int.getData()[row]);
            histogram.set_zero_count_int(columns.zero_count_int.getData()[row]);
            addBuckets<true>(ColumnsView::intPairsAt(columns.positive_int, row), histogram);
            addBuckets<false>(ColumnsView::intPairsAt(columns.negative_int, row), histogram);
        }

        histogram.set_sum(columns.sum.getData()[row]);
        histogram.set_schema(columns.schema.getData()[row]);
        histogram.set_zero_threshold(columns.zero_threshold.getData()[row]);
        histogram.set_reset_hint(toPrometheusResetHint(columns.counter_reset_hint.getData()[row]));
        histogram.set_timestamp(timestamp_ms);
        for (const Float64 bound : ColumnsView::floatsAt(columns.custom_values, row))
            histogram.add_custom_values(bound);
    }
}


void addPrometheusHistogramsToTimeSeries(
    const IColumn & histograms_column, const size_t row, const UInt32 timestamp_scale, prometheus::TimeSeries & time_series)
{
    const auto & histograms = assert_cast<const ColumnArray &>(histograms_column);
    const ColumnsView columns{assert_cast<const ColumnTuple &>(histograms.getData())};

    /// The float samples are sorted by timestamp, like the histogram samples.
    const auto & float_samples = time_series.samples();
    int float_index = 0;

    /// The predecessor of the first histogram isn't in the response: it is in an earlier insert, whose hint of the first sample the
    /// sink keeps as sent without checking it, or before the time range. So the first histogram gets an unknown counter hint, like
    /// the first sample of a chunk in Prometheus, which never trusts a sender's `NO` either.
    bool previous_histogram_skipped = true;

    const size_t begin = histograms.getOffsets()[static_cast<ssize_t>(row) - 1];
    const size_t end = begin + histograms.getSize(row);
    for (size_t i = begin; i != end; ++i)
    {
        const Int64 timestamp_ms = DecimalUtils::convertTo<Decimal64>(3, Decimal64{columns.timestamps.getInt(i)}, timestamp_scale).value;

        /// A float sample beats a histogram sample.
        while ((float_index < float_samples.size()) && (float_samples[float_index].timestamp() < timestamp_ms))
            ++float_index;
        if ((float_index < float_samples.size()) && (float_samples[float_index].timestamp() == timestamp_ms))
        {
            previous_histogram_skipped = true;
            continue;
        }

        auto & histogram = *time_series.add_histograms();
        addHistogram(columns, i, timestamp_ms, histogram);
        /// A counter hint describes the original predecessor; `GAUGE` describes the sample itself.
        if (previous_histogram_skipped && histogram.reset_hint() != prometheus::Histogram::GAUGE)
            histogram.set_reset_hint(prometheus::Histogram::UNKNOWN);
        previous_histogram_skipped = false;
    }
}

}

#endif
