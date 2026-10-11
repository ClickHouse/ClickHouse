#include "config.h"

#if USE_PROMETHEUS_PROTOBUFS

#include <Storages/TimeSeries/addPrometheusHistogramsToTimeSeries.h>

#include <Common/assert_cast.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeTuple.h>
#include <Storages/TimeSeries/TimeSeriesHistogramsColumns.h>
#include <base/EnumReflection.h>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <numeric>
#include <variant>
#include <vector>

using namespace DB;

namespace
{

class AddPrometheusHistogramsToTimeSeriesTest : public ::testing::Test
{
protected:
    static constexpr UInt32 timestamp_scale = 3;

    /// A histogram sample with the count of its flavour and `sum`, the first column that decides between different histograms
    /// with equal counts. The other columns keep their defaults.
    struct HistogramSample
    {
        Int64 timestamp_ms;
        std::variant<UInt64, Float64> count;
        Float64 sum = 0;
    };

    struct FloatSample
    {
        Int64 timestamp_ms;
        Float64 value;
    };

    static ColumnPtr makeHistogramsColumn(const std::vector<HistogramSample> & histogram_samples)
    {
        Array histograms_data;
        for (const auto & [timestamp_ms, count, sum] : histogram_samples)
        {
            Tuple histogram = getDefaultHistogram();
            if (const auto * const int_count = std::get_if<UInt64>(&count))
            {
                histogram[getIndex(TimeSeriesHistogramsColumn::CountInt)] = *int_count;
            }
            else
            {
                histogram[getIndex(TimeSeriesHistogramsColumn::IsFloat)] = static_cast<UInt64>(1);
                histogram[getIndex(TimeSeriesHistogramsColumn::CountFloat)] = std::get<Float64>(count);
            }
            histogram[getIndex(TimeSeriesHistogramsColumn::Sum)] = sum;
            histograms_data.push_back(Tuple{DecimalField<DateTime64>(DateTime64(timestamp_ms), timestamp_scale), Array{std::move(histogram)}});
        }

        auto column = getHistogramsColumnType()->createColumn();
        column->insert(histograms_data);
        return column;
    }

    static prometheus::TimeSeries makeTimeSeriesMessage(const std::vector<FloatSample> & float_samples)
    {
        prometheus::TimeSeries time_series;
        for (const auto & [timestamp_ms, value] : float_samples)
        {
            auto & sample = *time_series.add_samples();
            sample.set_timestamp(timestamp_ms);
            sample.set_value(value);
        }
        return time_series;
    }

    static void checkHistograms(const prometheus::TimeSeries & time_series, const std::vector<HistogramSample> & expected)
    {
        ASSERT_EQ(time_series.histograms_size(), expected.size());
        for (size_t i = 0; i != expected.size(); ++i)
        {
            SCOPED_TRACE(fmt::format("Histogram {}", i));
            const auto & histogram = time_series.histograms(static_cast<int>(i));
            ASSERT_EQ(histogram.timestamp(), expected[i].timestamp_ms);
            if (const auto * const int_count = std::get_if<UInt64>(&expected[i].count))
            {
                ASSERT_TRUE(histogram.has_count_int());
                ASSERT_EQ(histogram.count_int(), *int_count);
            }
            else
            {
                ASSERT_TRUE(histogram.has_count_float());
                ASSERT_THAT(histogram.count_float(), ::testing::NanSensitiveDoubleNear(std::get<Float64>(expected[i].count), 0));
            }
            ASSERT_THAT(histogram.sum(), ::testing::NanSensitiveDoubleNear(expected[i].sum, 0));
        }
    }

    static void checkPermutations(
        const std::vector<HistogramSample> & histogram_samples,
        const std::vector<FloatSample> & float_samples,
        const std::vector<HistogramSample> & expected)
    {
        std::vector<size_t> order(histogram_samples.size());
        std::iota(order.begin(), order.end(), 0);
        while (std::next_permutation(order.begin(), order.end()))
        {
            SCOPED_TRACE(fmt::format("Order of the histogram samples: {}", fmt::join(order, ", ")));
            std::vector<HistogramSample> permuted;
            for (const size_t index : order)
                permuted.push_back(histogram_samples[index]);

            const ColumnPtr histograms_column = makeHistogramsColumn(permuted);
            prometheus::TimeSeries time_series = makeTimeSeriesMessage(float_samples);
            addPrometheusHistogramsToTimeSeries(*histograms_column, /* row = */ 0, timestamp_scale, time_series);
            ASSERT_NO_FATAL_FAILURE(checkHistograms(time_series, expected));
        }
    }

private:
    static size_t getIndex(const TimeSeriesHistogramsColumn column) { return magic_enum::enum_index(column).value(); }

    static const Tuple & getDefaultHistogram()
    {
        static const Tuple histogram
            = assert_cast<const DataTypeArray &>(*TimeSeriesHistogramsColumns::getHistogramColumnType()).getNestedType()->getDefault().safeGet<Tuple>();
        return histogram;
    }

    /// `groupArrayIf((timestamp, histogram), notEmpty(histogram))`
    static const DataTypePtr & getHistogramsColumnType()
    {
        static const DataTypePtr data_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
            DataTypes{std::make_shared<DataTypeDateTime64>(timestamp_scale), TimeSeriesHistogramsColumns::getHistogramColumnType()}));
        return data_type;
    }
};

}


TEST_F(AddPrometheusHistogramsToTimeSeriesTest, GreaterIntCountWinsBeyondFloat64Precision)
{
    const UInt64 count = (1ULL << 53) + 1;
    const HistogramSample smaller{.timestamp_ms = 1000, .count = count - 1, .sum = 1};
    const HistogramSample greater{.timestamp_ms = 1000, .count = count, .sum = 0};
    const std::vector<HistogramSample> histogram_samples{smaller, greater};
    const std::vector<FloatSample> float_samples;
    const ColumnPtr histograms_column = makeHistogramsColumn(histogram_samples);
    prometheus::TimeSeries time_series = makeTimeSeriesMessage(float_samples);

    addPrometheusHistogramsToTimeSeries(*histograms_column, /* row = */ 0, timestamp_scale, time_series);

    const std::vector<HistogramSample> expected{greater};
    ASSERT_NO_FATAL_FAILURE(checkHistograms(time_series, expected));
    ASSERT_NO_FATAL_FAILURE(checkPermutations(histogram_samples, float_samples, expected));
}

TEST_F(AddPrometheusHistogramsToTimeSeriesTest, GreaterFloatCountWins)
{
    const HistogramSample smaller{.timestamp_ms = 1000, .count = 2.5, .sum = 1};
    const HistogramSample greater{.timestamp_ms = 1000, .count = 3.5, .sum = 0};
    const std::vector<HistogramSample> histogram_samples{smaller, greater};
    const std::vector<FloatSample> float_samples;
    const ColumnPtr histograms_column = makeHistogramsColumn(histogram_samples);
    prometheus::TimeSeries time_series = makeTimeSeriesMessage(float_samples);

    addPrometheusHistogramsToTimeSeries(*histograms_column, /* row = */ 0, timestamp_scale, time_series);

    const std::vector<HistogramSample> expected{greater};
    ASSERT_NO_FATAL_FAILURE(checkHistograms(time_series, expected));
    ASSERT_NO_FATAL_FAILURE(checkPermutations(histogram_samples, float_samples, expected));
}

TEST_F(AddPrometheusHistogramsToTimeSeriesTest, GreaterIntCountWinsOverFloatCountBeyondFloat64Precision)
{
    const HistogramSample smaller{.timestamp_ms = 1000, .count = 0x1p53, .sum = 0};
    const HistogramSample greater{.timestamp_ms = 1000, .count = (1ULL << 53) + 1, .sum = 0};
    const std::vector<HistogramSample> histogram_samples{smaller, greater};
    const std::vector<FloatSample> float_samples;
    const ColumnPtr histograms_column = makeHistogramsColumn(histogram_samples);
    prometheus::TimeSeries time_series = makeTimeSeriesMessage(float_samples);

    addPrometheusHistogramsToTimeSeries(*histograms_column, /* row = */ 0, timestamp_scale, time_series);

    const std::vector<HistogramSample> expected{greater};
    ASSERT_NO_FATAL_FAILURE(checkHistograms(time_series, expected));
    ASSERT_NO_FATAL_FAILURE(checkPermutations(histogram_samples, float_samples, expected));
}

TEST_F(AddPrometheusHistogramsToTimeSeriesTest, NaNFloatCountLosesToZeroIntCount)
{
    const HistogramSample nan{.timestamp_ms = 1000, .count = std::numeric_limits<Float64>::quiet_NaN(), .sum = 0};
    const HistogramSample zero{.timestamp_ms = 1000, .count = UInt64{0}, .sum = 0};
    const std::vector<HistogramSample> histogram_samples{nan, zero};
    const std::vector<FloatSample> float_samples;
    const ColumnPtr histograms_column = makeHistogramsColumn(histogram_samples);
    prometheus::TimeSeries time_series = makeTimeSeriesMessage(float_samples);

    addPrometheusHistogramsToTimeSeries(*histograms_column, /* row = */ 0, timestamp_scale, time_series);

    const std::vector<HistogramSample> expected{zero};
    ASSERT_NO_FATAL_FAILURE(checkHistograms(time_series, expected));
    ASSERT_NO_FATAL_FAILURE(checkPermutations(histogram_samples, float_samples, expected));
}

TEST_F(AddPrometheusHistogramsToTimeSeriesTest, NaNFloatCountLosesToZeroFloatCount)
{
    const HistogramSample nan{.timestamp_ms = 1000, .count = std::numeric_limits<Float64>::quiet_NaN(), .sum = 1};
    const HistogramSample zero{.timestamp_ms = 1000, .count = 0.0, .sum = 0};
    const std::vector<HistogramSample> histogram_samples{nan, zero};
    const std::vector<FloatSample> float_samples;
    const ColumnPtr histograms_column = makeHistogramsColumn(histogram_samples);
    prometheus::TimeSeries time_series = makeTimeSeriesMessage(float_samples);

    addPrometheusHistogramsToTimeSeries(*histograms_column, /* row = */ 0, timestamp_scale, time_series);

    const std::vector<HistogramSample> expected{zero};
    ASSERT_NO_FATAL_FAILURE(checkHistograms(time_series, expected));
    ASSERT_NO_FATAL_FAILURE(checkPermutations(histogram_samples, float_samples, expected));
}

TEST_F(AddPrometheusHistogramsToTimeSeriesTest, IdenticalSamplesCollapse)
{
    const HistogramSample sample{.timestamp_ms = 1000, .count = UInt64{3}, .sum = 1};
    const std::vector<HistogramSample> histogram_samples{sample, sample};
    const std::vector<FloatSample> float_samples;
    const ColumnPtr histograms_column = makeHistogramsColumn(histogram_samples);
    prometheus::TimeSeries time_series = makeTimeSeriesMessage(float_samples);

    addPrometheusHistogramsToTimeSeries(*histograms_column, /* row = */ 0, timestamp_scale, time_series);

    const std::vector<HistogramSample> expected{sample};
    ASSERT_NO_FATAL_FAILURE(checkHistograms(time_series, expected));
}

TEST_F(AddPrometheusHistogramsToTimeSeriesTest, FloatHistogramWinsEqualCountOverIntHistogram)
{
    const HistogramSample int_histogram{.timestamp_ms = 1000, .count = UInt64{3}, .sum = 1};
    const HistogramSample float_histogram{.timestamp_ms = 1000, .count = 3.0, .sum = 0};
    const std::vector<HistogramSample> histogram_samples{int_histogram, float_histogram};
    const std::vector<FloatSample> float_samples;
    const ColumnPtr histograms_column = makeHistogramsColumn(histogram_samples);
    prometheus::TimeSeries time_series = makeTimeSeriesMessage(float_samples);

    addPrometheusHistogramsToTimeSeries(*histograms_column, /* row = */ 0, timestamp_scale, time_series);

    const std::vector<HistogramSample> expected{float_histogram};
    ASSERT_NO_FATAL_FAILURE(checkHistograms(time_series, expected));
    ASSERT_NO_FATAL_FAILURE(checkPermutations(histogram_samples, float_samples, expected));
}

TEST_F(AddPrometheusHistogramsToTimeSeriesTest, IntCountWinsOverSlightlySmallerFloatCount)
{
    const HistogramSample smaller{.timestamp_ms = 1000, .count = std::nextafter(3.0, 0.0), .sum = 0};
    const HistogramSample greater{.timestamp_ms = 1000, .count = UInt64{3}, .sum = 0};
    const std::vector<HistogramSample> histogram_samples{smaller, greater};
    const std::vector<FloatSample> float_samples;
    const ColumnPtr histograms_column = makeHistogramsColumn(histogram_samples);
    prometheus::TimeSeries time_series = makeTimeSeriesMessage(float_samples);

    addPrometheusHistogramsToTimeSeries(*histograms_column, /* row = */ 0, timestamp_scale, time_series);

    const std::vector<HistogramSample> expected{greater};
    ASSERT_NO_FATAL_FAILURE(checkHistograms(time_series, expected));
    ASSERT_NO_FATAL_FAILURE(checkPermutations(histogram_samples, float_samples, expected));
}

TEST_F(AddPrometheusHistogramsToTimeSeriesTest, FloatSampleWinsOverHistogramAtSameTimestamp)
{
    const HistogramSample before_floats{.timestamp_ms = 1000, .count = UInt64{1}, .sum = 0};
    const HistogramSample at_first_float{.timestamp_ms = 2000, .count = UInt64{1}, .sum = 0};
    const HistogramSample between_floats{.timestamp_ms = 3000, .count = UInt64{1}, .sum = 0};
    const HistogramSample after_floats{.timestamp_ms = 5000, .count = UInt64{1}, .sum = 0};
    const std::vector<HistogramSample> histogram_samples{before_floats, at_first_float, between_floats, after_floats};
    const std::vector<FloatSample> float_samples{{.timestamp_ms = 2000, .value = 1}, {.timestamp_ms = 4000, .value = 2}};
    const ColumnPtr histograms_column = makeHistogramsColumn(histogram_samples);
    prometheus::TimeSeries time_series = makeTimeSeriesMessage(float_samples);

    addPrometheusHistogramsToTimeSeries(*histograms_column, /* row = */ 0, timestamp_scale, time_series);

    const std::vector<HistogramSample> expected{before_floats, between_floats, after_floats};
    ASSERT_NO_FATAL_FAILURE(checkHistograms(time_series, expected));
    ASSERT_NO_FATAL_FAILURE(checkPermutations(histogram_samples, float_samples, expected));
}

#endif
