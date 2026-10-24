#include <Storages/TimeSeries/addPrometheusHistogramsToTimeSeries.h>

#if USE_PROMETHEUS_PROTOBUFS

#include <base/DecomposedFloat.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Common/Exception.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/assert_cast.h>
#include <Core/CompareHelper.h>
#include <Core/DecimalFunctions.h>
#include <Storages/TimeSeries/TimeSeriesHistogramsColumnsView.h>

#include <algorithm>
#include <limits>
#include <optional>
#include <span>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{
    template <typename AddSpan>
    void addSpans(const TimeSeriesHistogramsColumnsView::Spans & spans, const AddSpan & add_span)
    {
        for (size_t i = 0; i != spans.size(); ++i)
        {
            auto & span = *add_span();
            span.set_offset(spans.offsets[i]);
            span.set_length(spans.lengths[i]);
        }
    }

    /// Encodes the absolute bucket counts of an integer histogram as the deltas Prometheus sends.
    void addDeltas(const std::span<const UInt64> counts, google::protobuf::RepeatedField<Int64> & deltas)
    {
        Int64 previous = 0;
        for (const UInt64 count : counts)
        {
            /// The insertion rejects such counts (see `validateTimeSeriesHistograms`).
            if (count > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                                "Cannot read a histogram with a bucket count {} above the maximum {}", count, std::numeric_limits<Int64>::max());
            deltas.Add(static_cast<Int64>(count) - previous);
            previous = static_cast<Int64>(count);
        }
    }

    void addHistogram(const TimeSeriesHistogramsColumnsView & columns, const size_t row, const Int64 timestamp_ms, prometheus::Histogram & histogram)
    {
        if (columns.is_float[row])
        {
            histogram.set_count_float(columns.count_float[row]);
            histogram.set_zero_count_float(columns.zero_count_float[row]);
            for (const Float64 count : columns.positive_values_float[row])
                histogram.add_positive_counts(count);
            for (const Float64 count : columns.negative_values_float[row])
                histogram.add_negative_counts(count);
        }
        else
        {
            histogram.set_count_int(columns.count_int[row]);
            histogram.set_zero_count_int(columns.zero_count_int[row]);
            addDeltas(columns.positive_values_int[row], *histogram.mutable_positive_deltas());
            addDeltas(columns.negative_values_int[row], *histogram.mutable_negative_deltas());
        }

        histogram.set_sum(columns.sum[row]);
        histogram.set_schema(columns.schema[row]);
        histogram.set_zero_threshold(columns.zero_threshold[row]);
        addSpans(columns.positive_spans[row], [&] { return histogram.add_positive_spans(); });
        addSpans(columns.negative_spans[row], [&] { return histogram.add_negative_spans(); });
        histogram.set_reset_hint(static_cast<prometheus::Histogram_ResetHint>(columns.counter_reset_hint[row]));
        histogram.set_timestamp(timestamp_ms);
        for (const Float64 bound : columns.custom_values[row])
            histogram.add_custom_values(bound);
    }
}


void addPrometheusHistogramsToTimeSeries(
    const IColumn & histograms_column, const size_t row, const UInt32 timestamp_scale, prometheus::TimeSeries & time_series)
{
    /// Array(Tuple(timestamp, histogram)), where each `histogram` is an array holding one histogram sample.
    const auto & samples = assert_cast<const ColumnArray &>(histograms_column);
    const auto & samples_tuple = assert_cast<const ColumnTuple &>(samples.getData());
    const auto & timestamps = samples_tuple.getColumn(0);
    const auto & histograms = assert_cast<const ColumnArray &>(samples_tuple.getColumn(1));
    const auto & payload = assert_cast<const ColumnTuple &>(histograms.getData());
    const TimeSeriesHistogramsColumnsView columns{payload};

    struct Sample
    {
        Int64 timestamp_ms;
        size_t payload_row;
    };

    /// Proportional to the histogram samples of the time series, so tracked against the memory limit of the query.
    VectorWithMemoryTracking<Sample> sorted_samples;
    sorted_samples.reserve(samples.getSize(row));
    const size_t samples_begin = samples.getOffset(row);
    const size_t samples_end = samples_begin + samples.getSize(row);
    for (size_t i = samples_begin; i != samples_end; ++i)
    {
        chassert(histograms.getSize(i) == 1);
        const Int64 timestamp_ms = DecimalUtils::convertTo<Decimal64>(3, Decimal64{timestamps.getInt(i)}, timestamp_scale).value;
        sorted_samples.push_back({timestamp_ms, histograms.getOffset(i)});
    }

    const auto compare_different_type_counts = [&columns](const Sample & float_sample, const Sample & int_sample)
    {
        chassert(columns.is_float[float_sample.payload_row]);
        chassert(!columns.is_float[int_sample.payload_row]);

        const Float64 float_count = columns.count_float[float_sample.payload_row];
        if (isNaN(float_count))
            return -1;

        const UInt64 int_count = columns.count_int[int_sample.payload_row];

        return DecomposedFloat64(float_count).compare(int_count);
    };

    const auto compare_count = [&](const Sample & lhs, const Sample & rhs)
    {
        const size_t lhs_row = lhs.payload_row;
        const size_t rhs_row = rhs.payload_row;
        if (columns.is_float[lhs_row])
        {
            /// Both sides have float counts, so we compare them.
            if (columns.is_float[rhs_row])
            {
                return CompareHelper<Float64>::compare(
                    columns.count_float[lhs_row],
                    columns.count_float[rhs_row],
                    /* nan_direction_hint = */ -1);
            }

            /// `lhs` is float, `rhs` is int
            return compare_different_type_counts(lhs, rhs);
        }

        /// `lhs` is int, `rhs` is float
        if (columns.is_float[rhs_row])
        {
            return -compare_different_type_counts(rhs, lhs);
        }

        /// Both sides are int
        return CompareHelper<UInt64>::compare(columns.count_int[lhs_row], columns.count_int[rhs_row], /* nan_direction_hint = */ 0);
    };

    /// By timestamp, and for each timestamp the winning sample first: the greatest count, then the greatest in the order of the columns.
    std::sort(sorted_samples.begin(), sorted_samples.end(), [&](const Sample & lhs, const Sample & rhs)
    {
        if (lhs.timestamp_ms != rhs.timestamp_ms)
            return lhs.timestamp_ms < rhs.timestamp_ms;
        const int count_comparison = compare_count(lhs, rhs);
        if (count_comparison != 0)
            return count_comparison > 0;

        /// The first column to be compared is `is_float`, so a float histogram wins against an integer one with the same count.
        /// The choice is arbitrary, but the ordering should be deterministic and should not depend on the samples order.
        return payload.compareAt(lhs.payload_row, rhs.payload_row, payload, /* nan_direction_hint = */ -1) > 0;
    });

    /// The float samples are sorted by timestamp too.
    const auto & float_samples = time_series.samples();
    int float_index = 0;
    std::optional<Int64> previous_timestamp_ms;

    for (const auto & sample : sorted_samples)
    {
        /// Only the winning sample of each timestamp is used.
        if (sample.timestamp_ms == previous_timestamp_ms)
            continue;
        previous_timestamp_ms = sample.timestamp_ms;

        /// A float sample beats a histogram sample.
        while ((float_index < float_samples.size()) && (float_samples[float_index].timestamp() < sample.timestamp_ms))
            ++float_index;
        if ((float_index < float_samples.size()) && (float_samples[float_index].timestamp() == sample.timestamp_ms))
            continue;

        addHistogram(columns, sample.payload_row, sample.timestamp_ms, *time_series.add_histograms());
    }
}

}

#endif
