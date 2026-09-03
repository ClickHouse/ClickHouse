#include <Functions/FunctionFactory.h>

#include <Functions/TimeSeries/TimeSeriesHistogramMathFunctionHelpers.h>


namespace DB
{

namespace
{
    /// timeSeriesHistogramMulByScalar(histogram, scalar): the PromQL `*` operator between a float and
    /// a native-histogram sample (FloatHistogram.Mul, then Compact(0)).
    class FunctionTimeSeriesHistogramMulByScalar final : public FunctionTimeSeriesHistogramScalarMath
    {
    public:
        static constexpr auto name = "timeSeriesHistogramMulByScalar";

        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSeriesHistogramMulByScalar>(); }

        FunctionTimeSeriesHistogramMulByScalar() : FunctionTimeSeriesHistogramScalarMath(name) {}

    protected:
        void apply(TimeSeriesFloatHistogram & histogram, Float64 scalar) const override
        {
            histogram.mul(scalar);
        }
    };
}

REGISTER_FUNCTION(TimeSeriesHistogramMulByScalar)
{
    FunctionDocumentation::Description description = R"(
Scales a native histogram by a scalar factor: the count, the sum, the zero bucket count and every
bucket count are multiplied by the factor, like the PromQL `*` operator between a float and a
native-histogram sample (see `FloatHistogram.Mul` in Prometheus). A negative factor marks the
result as a gauge. The result is `Compact(0)`-ed. Returns NULL when either argument is NULL.
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesHistogramMulByScalar(histogram, factor)";
    FunctionDocumentation::Arguments arguments = {
        {"histogram", "A native histogram sample: the payload tuple of the `histograms` target table of a `TimeSeries` table.", {"Tuple"}},
        {"factor", "The scalar factor to scale the histogram by.", {"Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the payload tuple of the scaled histogram, or NULL if either argument is NULL.", {"Nullable(Tuple)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
SELECT timeSeriesHistogramMulByScalar((0, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)), 2)
        )",
        R"(
┌─timeSeriesHistogramMulByScalar(...)───────────────────────────────────────┐
│ (0,0,0,8,20,0,[(0,2)],[2,6],[],[],[])                                     │
└───────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesHistogramMulByScalar>(documentation);
}

}
