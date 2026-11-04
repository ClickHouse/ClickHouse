#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeSeriesGroupArrayHistograms.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumns.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
    AggregateFunctionPtr createAggregateFunctionTimeSeriesGroupArrayHistograms(
        const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        /// Not gated by the `enable_time_series_aggregate_functions` setting for the same reason as `timeSeriesGroupArray`:
        /// the TimeSeries table engine uses it in the `SimpleAggregateFunction` type of its inner tables, so the type must be
        /// parsed in any context.
        assertNoParameters(name, parameters);
        assertUnary(name, argument_types);

        /// The elements are addressed by the positions of the registry, so the argument must have exactly the shape of the
        /// `histograms` column of a TimeSeries table.
        const DataTypePtr timestamp_type = TimeSeriesHistogramsColumns::tryGetTimestampType(argument_types[0]);
        if (!timestamp_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument for aggregate function {}, expected the type of the `histograms` column of a TimeSeries table: {}",
                argument_types[0]->getName(), name, TimeSeriesHistogramsColumns::describeArrayTypeForDocs("<timestamp type>"));

        if (isDateTime64(timestamp_type))
            return std::make_shared<AggregateFunctionTimeSeriesGroupArrayHistograms<DateTime64>>(name, argument_types);
        if (isDateTime(timestamp_type) || isUInt32(timestamp_type))
            return std::make_shared<AggregateFunctionTimeSeriesGroupArrayHistograms<UInt32>>(name, argument_types);

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of the `timestamp` element of the argument for aggregate function {}, expected DateTime64, DateTime or UInt32",
            timestamp_type->getName(), name);
    }
}

void registerAggregateFunctionTimeSeriesGroupArrayHistograms(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeSeriesGroupArrayHistograms(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Merges arrays of native histogram samples sorted by timestamp into one array sorted by timestamp and deduplicated by timestamp.

Every row holds an array of histogram samples of the shape of the `histograms` column of the samples tables of the `TimeSeries`
table engine (`Array(Tuple(timestamp, flags, counter_reset_hint, schema, zero_threshold, sum, count_int, zero_count_int, count_float,
zero_count_float, positive_int, negative_int, positive_float, negative_float, custom_values))`, see the documentation of the engine).
If several samples have the same timestamp, only one of them is kept: the one with the greatest total count (`count_int` for an integer
histogram, `count_float` for a float one; a NaN count loses to any other count), on ties the one with the greatest `sum`, then the other
elements decide, the gauge marker among them: a gauge histogram beats a counter one with the same count and sum. The other counter
reset hints only break ties between otherwise identical payloads, with unknown preferred over known hints.
The winning histogram payload does not depend on the order of merging. Where merging changes a sample's predecessor or joins separate
input arrays, its counter reset hint is detected again against the new predecessor by the rules of the inserts into a `TimeSeries` table
(a decrease of a count is a reset, a change of the schema or of the zero threshold makes the hint unknown); explicit reset hints and gauge
markers are kept. The result type is the type of the argument, so the function can be used in the `SimpleAggregateFunction` data type.
)";
    FunctionDocumentation::Syntax syntax = "timeSeriesGroupArrayHistograms(histograms)";
    FunctionDocumentation::Arguments arguments = {
        {"histograms", "Histogram samples of a time series sorted by timestamp, with the type of the `histograms` column of a TimeSeries table.", {"Array(Tuple(DateTime64, ...))", "Array(Tuple(DateTime, ...))", "Array(Tuple(UInt32, ...))"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array of the same type sorted by timestamp.", {"Array(Tuple(T1, ...))"}};
    FunctionDocumentation::Examples examples = {
    {
        "Merging two arrays of integer histograms",
        R"(
SELECT timeSeriesGroupArrayHistograms(h)
FROM values('h Array(Tuple(timestamp UInt32, flags UInt8, counter_reset_hint UInt8, schema Int8, zero_threshold Float64, sum Float64, count_int Int64, zero_count_int Int64, count_float Float64, zero_count_float Float64, positive_int Array(Tuple(index Int32, count Int64)), negative_int Array(Tuple(index Int32, count Int64)), positive_float Array(Tuple(index Int32, count Float64)), negative_float Array(Tuple(index Int32, count Float64)), custom_values Array(Float64)))',
    [(100, 0, 0, 0, 0., 1., 1, 0, 0., 0., [(0, 1)], [], [], [], []), (120, 0, 0, 0, 0., 3., 2, 0, 0., 0., [(0, 2)], [], [], [], [])],
    [(110, 0, 0, 0, 0., 2., 1, 0, 0., 0., [(1, 1)], [], [], [], []), (120, 0, 0, 0, 0., 7., 3, 0, 0., 0., [(0, 3)], [], [], [], [])]);
        )",
        R"(
┌─timeSeriesGroupArrayHistograms(h)─────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ [(100,0,0,0,0,1,1,0,0,0,[(0,1)],[],[],[],[]),(110,0,0,0,0,2,1,0,0,0,[(1,1)],[],[],[],[]),(120,0,0,0,0,7,3,0,0,0,[(0,3)],[],[],[],[])] │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction("timeSeriesGroupArrayHistograms", {createAggregateFunctionTimeSeriesGroupArrayHistograms, documentation});
}

}
