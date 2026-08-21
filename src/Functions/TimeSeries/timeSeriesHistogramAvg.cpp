#include <Functions/FunctionFactory.h>

#include <Functions/TimeSeries/TimeSeriesHistogramFunctionHelpers.h>


namespace DB
{

namespace
{
    /// timeSeriesHistogramAvg(histogram) returns the mean observation of a native histogram: the ratio of
    /// the `sum` and `count` payload tuple elements (mirrors `funcHistogramAvg` in promql/functions.go).
    class FunctionTimeSeriesHistogramAvg final : public FunctionTimeSeriesHistogramStatistic
    {
    public:
        static constexpr auto name = "timeSeriesHistogramAvg";

        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSeriesHistogramAvg>(); }

        FunctionTimeSeriesHistogramAvg() : FunctionTimeSeriesHistogramStatistic(name) {}

    private:
        Float64 computeRow(
            const ColumnTuple & tuple_column,
            const TimeSeriesHistogramPayloadPositions & element_positions,
            size_t row) const override
        {
            const Float64 sum
                = tuple_column.getColumn(element_positions[TimeSeriesHistogramPayloadTupleIndex::Sum]).getFloat64(row);
            const Float64 count
                = tuple_column.getColumn(element_positions[TimeSeriesHistogramPayloadTupleIndex::Count]).getFloat64(row);
            return sum / count;
        }
    };
}

REGISTER_FUNCTION(TimeSeriesHistogramAvg)
{
    FunctionDocumentation::Description description = R"(
Returns the mean of all observations stored in a native histogram: the ratio of the `sum` and `count`
elements of its payload tuple (see `getTimeSeriesHistogramPayloadTupleType`), mirroring PromQL
`histogram_avg`. If the argument is NULL the function returns NULL.
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesHistogramAvg(histogram)";
    FunctionDocumentation::Arguments arguments = {
        {"histogram", "A native histogram sample: the payload tuple of the `histograms` target table of a `TimeSeries` table.", {"Tuple"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `sum`/`count` of the histogram payload tuple (NaN when `count` is 0), or NULL if the argument is NULL.", {"Nullable(Float64)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
SELECT timeSeriesHistogramAvg((0, 0, 0., 12., 30.5, 1., [], [], [], [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64))) AS avg
        )",
        R"(
┌─avg────────────────┐
│ 2.5416666666666665 │
└────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesHistogramAvg>(documentation);
}

}
