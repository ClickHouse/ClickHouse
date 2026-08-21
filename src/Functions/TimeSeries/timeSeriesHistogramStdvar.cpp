#include <Functions/FunctionFactory.h>

#include <Functions/TimeSeries/TimeSeriesHistogramFunctionHelpers.h>


namespace DB
{

namespace
{
    /// timeSeriesHistogramStdvar(histogram) returns the variance of a native histogram's observations
    /// (see FunctionTimeSeriesHistogramVariance), mirroring PromQL `histogram_stdvar`.
    class FunctionTimeSeriesHistogramStdvar final : public FunctionTimeSeriesHistogramVariance
    {
    public:
        static constexpr auto name = "timeSeriesHistogramStdvar";

        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSeriesHistogramStdvar>(); }

        FunctionTimeSeriesHistogramStdvar() : FunctionTimeSeriesHistogramVariance(name) {}

    private:
        Float64 transformResult(Float64 variance) const override { return variance; }
    };
}

REGISTER_FUNCTION(TimeSeriesHistogramStdvar)
{
    FunctionDocumentation::Description description = R"(
Returns the variance of the observations stored in a native histogram, mirroring PromQL `histogram_stdvar`:
every populated bucket counts as `count` observations at a representative value (the geometric mean of the
bucket bounds for exponential buckets, the arithmetic mean for custom buckets, and 0 for the zero bucket),
and the squared deviations from `sum`/`count` of the payload tuple (see `getTimeSeriesHistogramPayloadTupleType`)
are averaged with compensated summation. If the argument is NULL the function returns NULL.
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesHistogramStdvar(histogram)";
    FunctionDocumentation::Arguments arguments = {
        {"histogram", "A native histogram sample: the payload tuple of the `histograms` target table of a `TimeSeries` table.", {"Tuple"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the variance of the histogram (NaN when `count` is 0), or NULL if the argument is NULL.", {"Nullable(Float64)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
SELECT timeSeriesHistogramStdvar((0, -53, 0., 4., 8., 0., [(1, 2)], [3., 1.], [], [], [1., 3., 5.])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64))) AS stdvar
        )",
        R"(
┌─stdvar─┐
│      1 │
└────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesHistogramStdvar>(documentation);
}

}
