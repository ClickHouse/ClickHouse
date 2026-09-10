#include <Functions/FunctionFactory.h>

#include <Functions/TimeSeries/TimeSeriesHistogramMathFunctionHelpers.h>


namespace DB
{

namespace
{
    /// timeSeriesHistogramDivByScalar(histogram, scalar): the PromQL `/` operator between a
    /// native-histogram sample and a float (FloatHistogram.Div, then Compact(0)).
    class FunctionTimeSeriesHistogramDivByScalar final : public FunctionTimeSeriesHistogramScalarMath
    {
    public:
        static constexpr auto name = "timeSeriesHistogramDivByScalar";

        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSeriesHistogramDivByScalar>(); }

        FunctionTimeSeriesHistogramDivByScalar() : FunctionTimeSeriesHistogramScalarMath(name) {}

    protected:
        void apply(TimeSeriesFloatHistogram & histogram, Float64 scalar) const override
        {
            histogram.div(scalar);
        }
    };
}

REGISTER_FUNCTION(TimeSeriesHistogramDivByScalar)
{
    FunctionDocumentation::Description description = R"(
Divides a native histogram by a scalar: the count, the sum, the zero bucket count and every bucket
count are divided by the scalar, like the PromQL `/` operator between a native-histogram sample and
a float (see `FloatHistogram.Div` in Prometheus). Division by zero removes all buckets (the scalar
fields still get divided, ending up as +-Inf or NaN). A negative scalar marks the result as a
gauge. The result is `Compact(0)`-ed. Returns NULL when either argument is NULL.
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesHistogramDivByScalar(histogram, scalar)";
    FunctionDocumentation::Arguments arguments = {
        {"histogram", "A native histogram sample: the payload tuple of the `histograms` target table of a `TimeSeries` table.", {"Tuple"}},
        {"scalar", "The scalar to divide the histogram by.", {"Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the payload tuple of the divided histogram, or NULL if either argument is NULL.", {"Nullable(Tuple)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
SELECT timeSeriesHistogramDivByScalar((0, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)), 2)
        )",
        R"(
┌─timeSeriesHistogramDivByScalar(...)───────────────────────────────────────┐
│ (0,0,0,2,5,0,[(0,2)],[0.5,1.5],[],[],[])                                  │
└───────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesHistogramDivByScalar>(documentation);
}

}
