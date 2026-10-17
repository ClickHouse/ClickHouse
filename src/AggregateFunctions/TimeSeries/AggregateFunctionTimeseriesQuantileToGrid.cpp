#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesQuantileToGrid.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesHelpers.h>

#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


void registerAggregateFunctionTimeseriesQuantileToGrid(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesQuantileToGrid(AggregateFunctionFactory & factory)
{
    /// timeSeriesQuantileToGrid documentation
    FunctionDocumentation::Description description_timeSeriesQuantileToGrid = R"(
Aggregate function that takes time series data as pairs of timestamps and values and calculates the [PromQL `quantile_over_time`](https://prometheus.io/docs/prometheus/latest/querying/functions/#quantile_over_time) function on a regular time grid described by start timestamp, end timestamp and step. For each point on the grid the samples for calculating the quantile are considered within the specified time window. The quantile is computed using the R-7 (inclusive) method, like `quantileExactInclusive`. NaN samples are not skipped the way `quantileExactInclusive` skips them: like in Prometheus they are kept and sorted before every real value, so a window of `[1, NaN, 2]` has median `1`, and a window whose samples are all NaN gives NaN.

The quantile level follows the samples as the last argument: either one number used at every grid point, or an array with one number per grid point. It must be the same in every row. Like in Prometheus, a level below 0 gives `-Inf`, a level above 1 gives `+Inf` and a NaN level gives NaN for every grid point whose window has samples.

:::note
This function is in private preview, enable it by setting `enable_time_series_aggregate_functions=true`.
:::
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesQuantileToGrid = R"(
timeSeriesQuantileToGrid(start_timestamp, end_timestamp, grid_step, staleness)(timestamp, value, phi)
timeSeriesQuantileToGrid(start_timestamp, end_timestamp, grid_step, staleness)(samples, phi)
    )";
    FunctionDocumentation::Parameters parameters_timeSeriesQuantileToGrid = {
        {"start_timestamp", "Specifies start of the grid. It can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"end_timestamp", "Specifies end of the grid. It can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"grid_step", "Specifies step of the grid in seconds. It can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}},
        {"staleness", "Specifies the maximum \"staleness\" in seconds of the considered samples. The staleness window is a left-open and right-closed interval. It can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}}
    };
    FunctionDocumentation::Arguments arguments_timeSeriesQuantileToGrid = {
        {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {"UInt32", "DateTime", "DateTime64", "Array(UInt32)", "Array(DateTime)", "Array(DateTime64)"}},
        {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {"Float*", "Array(Float*)"}},
        {"samples", "Samples of the time series passed as an array of tuples `(timestamp, value)`, where the tuple elements have the timestamp and value types listed above. An alternative to passing the timestamps and the values as two separate arguments.", {"Array(Tuple(T1, T2))"}},
        {"phi", "Quantile level, normally in the range [0, 1]: either one number for the whole grid or an array with one number per grid point. Must be the same in every row.", {"Float*", "UInt8/16/32/64", "Int8/16/32/64", "Array(Float*)", "Array(UInt8/16/32/64)", "Array(Int8/16/32/64)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSeriesQuantileToGrid = {"Returns the phi-quantile of values on the specified grid. The returned array contains one value for each time grid point. The value is NULL if there are no samples within the window for a particular grid point.", {"Array(Nullable(Float64))"}};
    FunctionDocumentation::Examples examples_timeSeriesQuantileToGrid = {};
    FunctionDocumentation::IntroducedIn introduced_in_timeSeriesQuantileToGrid = {26, 9};
    FunctionDocumentation::Category category_timeSeriesQuantileToGrid = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_timeSeriesQuantileToGrid = {description_timeSeriesQuantileToGrid, syntax_timeSeriesQuantileToGrid, arguments_timeSeriesQuantileToGrid, parameters_timeSeriesQuantileToGrid, returned_value_timeSeriesQuantileToGrid, examples_timeSeriesQuantileToGrid, introduced_in_timeSeriesQuantileToGrid, category_timeSeriesQuantileToGrid};

    factory.registerFunction("timeSeriesQuantileToGrid",
        {[](const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings) -> AggregateFunctionPtr
        {
            assertTimeseriesParametersCount(name, parameters, 4, "start_timestamp, end_timestamp, step, window");

            auto make_function = [&]<typename TimestampType, typename ValueType>(DateTime64 start, DateTime64 end, Decimal64 step, Decimal64 window, UInt32 grid_scale, UInt32 column_timestamp_scale) -> AggregateFunctionPtr
            {
                /// The quantile level follows the samples: a number for the whole grid, or an array with a number for each grid point.
                const auto & phi_type = argument_types.back();
                const auto * array_type = typeid_cast<const DataTypeArray *>(phi_type.get());
                if (!isNativeNumber(array_type ? array_type->getNestedType() : phi_type))
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of the last argument for aggregate function {}, expected a number or an array of numbers",
                        phi_type->getName(), name);

                return std::make_shared<AggregateFunctionTimeseriesQuantileToGrid<TimestampType, ValueType>>(argument_types, parameters, start, end, step, window, grid_scale, column_timestamp_scale);
            };
            return createAggregateFunctionTimeseries(name, argument_types, parameters, settings, make_function,
                AggregateFunctionTimeseriesQuantileToGrid<DateTime64, Float64>::num_extra_arguments);
        },
        documentation_timeSeriesQuantileToGrid});
}

}
