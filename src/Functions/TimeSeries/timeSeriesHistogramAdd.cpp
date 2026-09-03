#include <Functions/FunctionFactory.h>

#include <Functions/TimeSeries/TimeSeriesHistogramMathFunctionHelpers.h>


namespace DB
{

namespace
{
    /// timeSeriesHistogramAdd(histogram, histogram): the PromQL `+` operator over two native-histogram
    /// samples (FloatHistogram.Add, then Compact(0)); NULL on NULL input or incompatible schemas.
    class FunctionTimeSeriesHistogramAdd final : public FunctionTimeSeriesHistogramBinaryMath
    {
    public:
        static constexpr auto name = "timeSeriesHistogramAdd";

        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSeriesHistogramAdd>(); }

        FunctionTimeSeriesHistogramAdd() : FunctionTimeSeriesHistogramBinaryMath(name) {}

    protected:
        void apply(TimeSeriesFloatHistogram & lhs, const TimeSeriesFloatHistogram & rhs) const override
        {
            lhs.add(rhs);
        }
    };
}

REGISTER_FUNCTION(TimeSeriesHistogramAdd)
{
    FunctionDocumentation::Description description = R"(
Adds two native histograms bucket-by-bucket, reconciling the zero threshold and the bucket schema,
like the PromQL `+` operator over two native-histogram samples (see `FloatHistogram.Add` in
Prometheus). The result is `Compact(0)`-ed. Returns NULL when either argument is NULL or the two
histograms have incompatible schemas (exponential vs custom buckets; PromQL drops the sample there).
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesHistogramAdd(histogram, other_histogram)";
    FunctionDocumentation::Arguments arguments = {
        {"histogram", "A native histogram sample: the payload tuple of the `histograms` target table of a `TimeSeries` table.", {"Tuple"}},
        {"other_histogram", "A native histogram sample: the payload tuple of the `histograms` target table of a `TimeSeries` table.", {"Tuple"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the payload tuple of the sum of the two histograms, or NULL if either argument is NULL or the schemas are incompatible.", {"Nullable(Tuple)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
SELECT timeSeriesHistogramAdd(h, h) FROM (SELECT (0, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)) AS h)
        )",
        R"(
┌─timeSeriesHistogramAdd(h, h)──────────────────────────────────────────────┐
│ (0,0,0,8,20,0,[(0,2)],[2,6],[],[],[])                                     │
└───────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesHistogramAdd>(documentation);
}

}
