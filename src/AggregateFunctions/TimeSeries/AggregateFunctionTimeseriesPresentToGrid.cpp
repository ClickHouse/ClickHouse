#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesPresentToGrid.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesHelpers.h>


namespace DB
{

void registerAggregateFunctionTimeseriesPresentToGrid(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesPresentToGrid(AggregateFunctionFactory & factory)
{
    /// timeSeriesPresentToGrid documentation
    FunctionDocumentation::Description description_timeSeriesPresentToGrid = R"(
Aggregate function that checks whether time series data is present on the specified grid. For each point on the grid the function returns 1 if there is at least one sample within the specified time window, otherwise NULL.

:::note
This function is in private preview, enable it by setting `enable_time_series_aggregate_functions=true`.
:::
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesPresentToGrid = R"(
timeSeriesPresentToGrid(start_timestamp, end_timestamp, grid_step, staleness)(timestamp, value)
timeSeriesPresentToGrid(start_timestamp, end_timestamp, grid_step, staleness)(samples)
    )";
    FunctionDocumentation::Parameters parameters_timeSeriesPresentToGrid = {
        {"start_timestamp", "Specifies start of the grid. It can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"end_timestamp", "Specifies end of the grid. It can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"grid_step", "Specifies step of the grid in seconds. It can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}},
        {"staleness", "Specifies the maximum \"staleness\" in seconds of the considered samples. The staleness window is a left-open and right-closed interval. It can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}}
    };
    FunctionDocumentation::Arguments arguments_timeSeriesPresentToGrid = {
        {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {"UInt32", "DateTime", "DateTime64", "Array(UInt32)", "Array(DateTime)", "Array(DateTime64)"}},
        {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {"Float*", "Array(Float*)"}},
        {"samples", "Samples of the time series passed as an array of tuples `(timestamp, value)`, where the tuple elements have the timestamp and value types listed above. An alternative to passing the timestamps and the values as two separate arguments.", {"Array(Tuple(T1, T2))"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSeriesPresentToGrid = {"Returns 1 for each grid point whose window contains at least one sample, otherwise NULL.", {"Array(Nullable(UInt8))"}};
    FunctionDocumentation::Examples examples_timeSeriesPresentToGrid = {};
    FunctionDocumentation::IntroducedIn introduced_in_timeSeriesPresentToGrid = {26, 9};
    FunctionDocumentation::Category category_timeSeriesPresentToGrid = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_timeSeriesPresentToGrid = {description_timeSeriesPresentToGrid, syntax_timeSeriesPresentToGrid, arguments_timeSeriesPresentToGrid, parameters_timeSeriesPresentToGrid, returned_value_timeSeriesPresentToGrid, examples_timeSeriesPresentToGrid, introduced_in_timeSeriesPresentToGrid, category_timeSeriesPresentToGrid};

    factory.registerFunction("timeSeriesPresentToGrid",
        {[](const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings) -> AggregateFunctionPtr
        {
            assertTimeseriesParametersCount(name, parameters, 4, "start_timestamp, end_timestamp, step, window");
            auto make_function = [&]<typename TimestampType, typename ValueType>(DateTime64 start, DateTime64 end, Decimal64 step, Decimal64 window, UInt32 grid_scale, UInt32 column_timestamp_scale) -> AggregateFunctionPtr
            {
                return std::make_shared<AggregateFunctionTimeseriesPresentToGrid<TimestampType, ValueType>>(argument_types, parameters, start, end, step, window, grid_scale, column_timestamp_scale);
            };
            return createAggregateFunctionTimeseries(name, argument_types, parameters, settings, make_function);
        },
        documentation_timeSeriesPresentToGrid});
}

}
