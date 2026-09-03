#include <algorithm>

#include <Functions/FunctionFactory.h>

#include <Functions/TimeSeries/TimeSeriesHistogramFunctionHelpers.h>


namespace DB
{

namespace
{
    /// timeSeriesHistogramQuantile(histogram, phi) mirrors `HistogramQuantile` in promql/quantile.go: buckets are walked to the rank
    /// (forward for NaN sum or phi < 0.5, reverse otherwise), then interpolated (linear for custom and zero buckets, log-scale for exponential).
    class FunctionTimeSeriesHistogramQuantile final : public FunctionTimeSeriesHistogramWithScalarParams
    {
    public:
        static constexpr auto name = "timeSeriesHistogramQuantile";

        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSeriesHistogramQuantile>(); }

        FunctionTimeSeriesHistogramQuantile() : FunctionTimeSeriesHistogramWithScalarParams(name, 1) {}

    private:
        Float64 computeRow(
            const ColumnTuple & tuple_column,
            const TimeSeriesHistogramPayloadPositions & element_positions,
            const ColumnsWithTypeAndName & arguments,
            size_t row) const override
        {
            const Float64 phi = arguments[1].column->getFloat64(row);

            if (phi < 0)
                return -std::numeric_limits<Float64>::infinity();
            if (phi > 1)
                return std::numeric_limits<Float64>::infinity();

            const Float64 histogram_count
                = tuple_column.getColumn(element_positions[TimeSeriesHistogramPayloadTupleIndex::Count]).getFloat64(row);
            const Float64 histogram_sum
                = tuple_column.getColumn(element_positions[TimeSeriesHistogramPayloadTupleIndex::Sum]).getFloat64(row);

            if (histogram_count == 0 || std::isnan(phi))
                return std::numeric_limits<Float64>::quiet_NaN();

            /// With NaN observations (the sum is NaN) or below the median the buckets are walked
            /// forward, otherwise in reverse.
            const bool forward = std::isnan(histogram_sum) || phi < 0.5;
            const auto walk = walkTimeSeriesHistogramBuckets(tuple_column, element_positions, row, /*reverse_order=*/!forward);

            Float64 rank = forward ? phi * histogram_count : (1 - phi) * histogram_count;

            TimeSeriesHistogramResolvedBucket bucket{};
            Float64 count = 0;
            for (const auto & walk_bucket : walk.buckets)
            {
                bucket = walk_bucket;
                if (walk_bucket.count == 0)
                    continue;
                count += walk_bucket.count;
                if (count >= rank)
                    break;
            }

            if (!walk.custom_buckets && bucket.lower < 0 && bucket.upper > 0)
            {
                /// The result is in the zero bucket: with only positive buckets the natural lower
                /// bound is 0; with only negative buckets the natural upper bound is 0.
                if (!walk.has_negative_buckets && walk.has_positive_buckets)
                    bucket.lower = 0;
                else if (!walk.has_positive_buckets && walk.has_negative_buckets)
                    bucket.upper = 0;
            }
            else if (walk.custom_buckets)
            {
                if (bucket.lower == -std::numeric_limits<Float64>::infinity())
                {
                    /// The first bucket, with lower bound -Inf.
                    if (bucket.upper <= 0)
                        return bucket.upper;
                    bucket.lower = 0;
                }
                else if (bucket.upper == std::numeric_limits<Float64>::infinity())
                {
                    /// The last bucket, with upper bound +Inf.
                    return bucket.lower;
                }
            }

            /// Due to numerical inaccuracies the walked count could exceed the total count.
            count = std::min(count, histogram_count);

            /// The walk could hit the last bucket without reaching the rank (only with NaN observations,
            /// when the sum is also NaN - see https://github.com/prometheus/prometheus/issues/16578).
            if (count < rank)
            {
                if (std::isnan(histogram_sum))
                    return std::numeric_limits<Float64>::quiet_NaN();
                /// Otherwise this is a precision issue or a corrupted histogram: return the upper
                /// bound of the highest bucket, like upstream.
                return bucket.upper;
            }

            if (forward)
                rank -= count - bucket.count;
            else
                rank = count - rank;

            /// The fraction of how far we are into the current bucket.
            const Float64 fraction = rank / bucket.count;

            /// Linear interpolation for custom buckets and for quantiles that end up in the zero
            /// bucket.
            if (walk.custom_buckets || (bucket.lower <= 0 && bucket.upper >= 0))
                return bucket.lower + (bucket.upper - bucket.lower) * fraction;

            /// For exponential buckets the interpolation is done on the logarithmic scale, where
            /// the exponential bucket boundaries (for any schema) become linear.
            const Float64 log_lower = std::log2(std::abs(bucket.lower));
            const Float64 log_upper = std::log2(std::abs(bucket.upper));
            if (bucket.lower > 0)
                return std::exp2(log_lower + (log_upper - log_lower) * fraction);
            /// Negative buckets are mirrored.
            return -std::exp2(log_upper + (log_lower - log_upper) * (1 - fraction));
        }
    };
}

REGISTER_FUNCTION(TimeSeriesHistogramQuantile)
{
    FunctionDocumentation::Description description = R"(
Returns the phi-quantile of the observations stored in a native histogram, mirroring PromQL
`histogram_quantile` over native histograms (`HistogramQuantile` in Prometheus promql/quantile.go):
the buckets are walked until the accumulated count reaches the rank, and the result is interpolated
within the bucket - linearly for custom buckets and the zero bucket, on the logarithmic scale for
exponential buckets. `phi` must be a constant number; a `phi` below 0 returns -Inf, above 1 returns
+Inf, NaN returns NaN, and a histogram with 0 observations returns NaN. If the histogram is NULL
the function returns NULL.
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesHistogramQuantile(histogram, phi)";
    FunctionDocumentation::Arguments arguments = {
        {"histogram", "A native histogram sample: the payload tuple of the `histograms` target table of a `TimeSeries` table.", {"Tuple"}},
        {"phi", "The quantile to compute: a constant number in [0, 1] (values outside the range return -Inf/+Inf).", {"Float64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the phi-quantile of the histogram observations, or NULL if the histogram is NULL.", {"Nullable(Float64)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
SELECT timeSeriesHistogramQuantile((0, 0, 0., 4., 6., 0., [(1, 2)], [2., 2.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)), 0.5) AS median
        )",
        R"(
┌─median─┐
│      2 │
└────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesHistogramQuantile>(documentation);
}

}
