#include <Functions/FunctionFactory.h>

#include <Functions/TimeSeries/TimeSeriesHistogramFunctionHelpers.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>


namespace DB
{

namespace
{
    /// Function timeSeriesHistogramCount(histogram) returns the `count` element of a native-histogram
    /// payload tuple (see getTimeSeriesHistogramPayloadTupleType).
    class FunctionTimeSeriesHistogramCount final : public FunctionTimeSeriesHistogramElement
    {
    public:
        static constexpr auto name = "timeSeriesHistogramCount";

        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSeriesHistogramCount>(); }

        FunctionTimeSeriesHistogramCount() : FunctionTimeSeriesHistogramElement(name, TimeSeriesColumnNames::Count) {}
    };
}

REGISTER_FUNCTION(TimeSeriesHistogramCount)
{
    FunctionDocumentation::Description description = R"(
Returns the total number of observations stored in a native histogram: the `count` element of its payload tuple
(see `getTimeSeriesHistogramPayloadTupleType`). If the argument is NULL the function returns NULL.
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesHistogramCount(histogram)";
    FunctionDocumentation::Arguments arguments = {
        {"histogram", "A native histogram sample: the payload tuple of the `histograms` target table of a `TimeSeries` table.", {"Tuple"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the `count` element of the histogram payload tuple, or NULL if the argument is NULL.", {"Nullable(Float64)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
SELECT timeSeriesHistogramCount((0, 0, 0., 12., 30.5, 1., [], [], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64))) AS count
        )",
        R"(
┌─count─┐
│    12 │
└───────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesHistogramCount>(documentation);
}

}
