#include <Functions/FunctionFactory.h>

#include <Functions/TimeSeries/TimeSeriesHistogramFunctionHelpers.h>


namespace DB
{

namespace
{
    /// The estimated observations at or below `v` inside one bucket, added to `rank` (the cumulative count before it):
    /// mirrors `interpolateLinearly` (`Bucket.FractionBelow` linear=true); a -Inf-bound bucket contributes its whole count.
    Float64 interpolateLinearly(Float64 bucket_lower, Float64 bucket_upper, Float64 bucket_count, Float64 rank, Float64 v)
    {
        if (bucket_lower == -std::numeric_limits<Float64>::infinity())
            return bucket_count;
        return rank + bucket_count * ((v - bucket_lower) / (bucket_upper - bucket_lower));
    }

    /// The same for exponential buckets, interpolating on the logarithmic scale (mirrors
    /// `Bucket.FractionBelow` with linear=false); negative buckets are mirrored.
    Float64 interpolateExponentially(Float64 bucket_lower, Float64 bucket_upper, Float64 bucket_count, Float64 rank, Float64 v)
    {
        const Float64 log_lower = std::log2(std::abs(bucket_lower));
        const Float64 log_upper = std::log2(std::abs(bucket_upper));
        const Float64 log_v = std::log2(std::abs(v));
        if (v > 0)
            return rank + bucket_count * ((log_v - log_lower) / (log_upper - log_lower));
        return rank + bucket_count * (1 - ((log_v - log_upper) / (log_lower - log_upper)));
    }

    /// timeSeriesHistogramFraction(histogram, lower, upper) mirrors `HistogramFraction` in promql/quantile.go: the ranks of the
    /// bounds are interpolated in their buckets (linear for custom and zero buckets, log-scale for exponential); (upper_rank - lower_rank) / count.
    class FunctionTimeSeriesHistogramFraction final : public FunctionTimeSeriesHistogramWithScalarParams
    {
    public:
        static constexpr auto name = "timeSeriesHistogramFraction";

        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSeriesHistogramFraction>(); }

        FunctionTimeSeriesHistogramFraction() : FunctionTimeSeriesHistogramWithScalarParams(name, 2) {}

    private:
        Float64 computeRow(
            const ColumnTuple & tuple_column,
            const TimeSeriesHistogramPayloadPositions & element_positions,
            const ColumnsWithTypeAndName & arguments,
            size_t row) const override
        {
            const Float64 lower = arguments[1].column->getFloat64(row);
            const Float64 upper = arguments[2].column->getFloat64(row);

            const Float64 histogram_count
                = tuple_column.getColumn(element_positions[TimeSeriesHistogramPayloadTupleIndex::Count]).getFloat64(row);
            const Float64 histogram_sum
                = tuple_column.getColumn(element_positions[TimeSeriesHistogramPayloadTupleIndex::Sum]).getFloat64(row);

            if (histogram_count == 0 || std::isnan(lower) || std::isnan(upper))
                return std::numeric_limits<Float64>::quiet_NaN();
            if (lower >= upper)
                return 0;

            const auto walk = walkTimeSeriesHistogramBuckets(tuple_column, element_positions, row, /*reverse_order=*/false);

            Float64 count = 0;
            Float64 rank = 0;
            Float64 lower_rank = 0;
            Float64 upper_rank = 0;
            bool lower_set = false;
            bool upper_set = false;

            for (const auto & walk_bucket : walk.buckets)
            {
                count += walk_bucket.count;

                Float64 bucket_lower = walk_bucket.lower;
                Float64 bucket_upper = walk_bucket.upper;

                /// The zero bucket (and any bucket spanning zero) interpolates linearly; with only positive
                /// buckets the natural lower bound is 0, with only negative buckets the natural upper bound is 0.
                const bool zero_bucket = (bucket_lower <= 0 && bucket_upper >= 0);
                if (zero_bucket)
                {
                    if (!walk.has_negative_buckets && walk.has_positive_buckets)
                        bucket_lower = 0;
                    else if (!walk.has_positive_buckets && walk.has_negative_buckets)
                        bucket_upper = 0;
                }

                if (!lower_set && bucket_lower >= lower)
                {
                    /// `lower` coincides with the lower boundary of this bucket.
                    lower_rank = rank;
                    lower_set = true;
                }
                if (!upper_set && bucket_lower >= upper)
                {
                    /// `upper` coincides with the lower boundary of this bucket.
                    upper_rank = rank;
                    upper_set = true;
                }
                if (lower_set && upper_set)
                    break;

                if (!lower_set && bucket_lower < lower && bucket_upper > lower)
                {
                    /// `lower` is inside this bucket.
                    lower_rank = (walk.custom_buckets || zero_bucket)
                        ? interpolateLinearly(bucket_lower, bucket_upper, walk_bucket.count, rank, lower)
                        : interpolateExponentially(bucket_lower, bucket_upper, walk_bucket.count, rank, lower);
                    lower_set = true;
                }
                if (!upper_set && bucket_lower < upper && bucket_upper > upper)
                {
                    /// `upper` is inside this bucket.
                    upper_rank = (walk.custom_buckets || zero_bucket)
                        ? interpolateLinearly(bucket_lower, bucket_upper, walk_bucket.count, rank, upper)
                        : interpolateExponentially(bucket_lower, bucket_upper, walk_bucket.count, rank, upper);
                    upper_set = true;
                }
                if (lower_set && upper_set)
                    break;
                rank += walk_bucket.count;
            }

            /// With no NaN observations the walked count is exactly the total count.
            if (!std::isnan(histogram_sum))
                count = histogram_count;

            if (!lower_set || lower_rank > count)
                lower_rank = count;
            if (!upper_set || upper_rank > count)
                upper_rank = count;

            return (upper_rank - lower_rank) / histogram_count;
        }
    };
}

REGISTER_FUNCTION(TimeSeriesHistogramFraction)
{
    FunctionDocumentation::Description description = R"(
Returns the estimated fraction of the observations of a native histogram between `lower` and
`upper`, mirroring PromQL `histogram_fraction` over native histograms (`HistogramFraction` in
Prometheus promql/quantile.go): the ranks of `lower` and `upper` are interpolated within the
buckets containing them - linearly for custom buckets and the zero bucket, on the logarithmic
scale for exponential buckets - and the result is the difference of the ranks divided by the total
count. `lower` and `upper` must be constant numbers; a NaN bound or a histogram with 0 observations
returns NaN, and `lower >= upper` returns 0. If the histogram is NULL the function returns NULL.
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesHistogramFraction(histogram, lower, upper)";
    FunctionDocumentation::Arguments arguments = {
        {"histogram", "A native histogram sample: the payload tuple of the `histograms` target table of a `TimeSeries` table.", {"Tuple"}},
        {"lower", "The lower bound of the interval: a constant number (-Inf counts all observations below `upper`).", {"Float64"}},
        {"upper", "The upper bound of the interval: a constant number (+Inf counts all observations above `lower`).", {"Float64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the estimated fraction of the histogram observations between `lower` and `upper`, or NULL if the histogram is NULL.", {"Nullable(Float64)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
SELECT timeSeriesHistogramFraction((0, 0, 0., 4., 6., 0., [(1, 2)], [2., 2.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)), 1, 3) AS fraction
        )",
        R"(
┌─fraction──────────┐
│ 0.792481250360578 │
└───────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesHistogramFraction>(documentation);
}

}
