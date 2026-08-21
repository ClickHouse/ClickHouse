#include <Functions/FunctionFactory.h>

#include <Functions/TimeSeries/TimeSeriesHistogramFunctionHelpers.h>


namespace DB
{

namespace
{
    /// timeSeriesHistogramStddev(histogram) returns the standard deviation of a native histogram's
    /// observations (the square root of the variance), mirroring PromQL `histogram_stddev`.
    class FunctionTimeSeriesHistogramStddev final : public FunctionTimeSeriesHistogramVariance
    {
    public:
        static constexpr auto name = "timeSeriesHistogramStddev";

        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSeriesHistogramStddev>(); }

        FunctionTimeSeriesHistogramStddev() : FunctionTimeSeriesHistogramVariance(name) {}

    private:
        Float64 transformResult(Float64 variance) const override { return std::sqrt(variance); }
    };
}

REGISTER_FUNCTION(TimeSeriesHistogramStddev)
{
    FunctionDocumentation::Description description = R"(
Returns the standard deviation of the observations stored in a native histogram (the square root of the
variance, see `timeSeriesHistogramStdvar`), mirroring PromQL `histogram_stddev`. If the argument is NULL
the function returns NULL.
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesHistogramStddev(histogram)";
    FunctionDocumentation::Arguments arguments = {
        {"histogram", "A native histogram sample: the payload tuple of the `histograms` target table of a `TimeSeries` table.", {"Tuple"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the standard deviation of the histogram (NaN when `count` is 0), or NULL if the argument is NULL.", {"Nullable(Float64)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
SELECT timeSeriesHistogramStddev((0, -53, 0., 4., 8., 0., [(1, 2)], [3., 1.], [], [], [1., 3., 5.])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64))) AS stddev
        )",
        R"(
┌─stddev─┐
│      1 │
└────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesHistogramStddev>(documentation);
}

}
