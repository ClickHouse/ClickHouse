#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesQuantileToGrid.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesHelpers.h>

#include <algorithm>
#include <cmath>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/castTypeToEither.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

namespace
{
    /// A NaN level is allowed (Prometheus defines `quantile_over_time(NaN, v)` as NaN), so NaN must compare equal to itself.
    bool samePhi(Float64 lhs, Float64 rhs)
    {
        return (lhs == rhs) || (std::isnan(lhs) && std::isnan(rhs));
    }
}


void AggregateFunctionTimeseriesQuantileToGridPhi::captureOrCheck(
    size_t grid_size, size_t row_begin, size_t row_end, const IColumn & column, const UInt8 * flags, bool flag_value_to_include)
{
    auto is_excluded = [&](size_t row) { return flags && ((flags[row] != 0) != flag_value_to_include); };

    size_t row = row_begin;
    while (row < row_end && is_excluded(row))
        ++row;
    if (row == row_end)
        return;

    const auto * array_column = typeid_cast<const ColumnArray *>(&column);
    const IColumn & number_column = array_column ? array_column->getData() : column;

    /// `[begin, begin + size)` are the elements of the current `row` in the nested column (`offsets[-1]` is 0).
    auto array_begin = [&] { return array_column->getOffsets()[row - 1]; };
    auto array_size = [&] { return array_column->getOffsets()[row] - array_column->getOffsets()[row - 1]; };

    /// The argument holds any native number type (checked when the function is created).
    const bool dispatched = castTypeToEither<
        ColumnVector<UInt8>, ColumnVector<UInt16>, ColumnVector<UInt32>, ColumnVector<UInt64>,
        ColumnVector<Int8>, ColumnVector<Int16>, ColumnVector<Int32>, ColumnVector<Int64>,
        ColumnVector<Float32>, ColumnVector<Float64>>(&number_column, [&](const auto & number_column_typed)
    {
        const auto & data = number_column_typed.getData();

        if (values.empty())
        {
            if (array_column)
            {
                const size_t size = array_size();
                if (size != grid_size)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Aggregate function timeSeriesQuantileToGrid requires the array argument `phi` to have one value per grid point ({}), got {} values",
                        grid_size, size);

                const auto * row_values = data.data() + array_begin();
                values.resize(size);
                for (size_t i = 0; i < size; ++i)
                    values[i] = static_cast<Float64>(row_values[i]);

                /// An array with the same value at every grid point is kept as that single value.
                if (std::all_of(values.begin(), values.end(), [&](Float64 value) { return samePhi(value, values[0]); }))
                    values.resize(1);
            }
            else
            {
                values.push_back(static_cast<Float64>(data[row]));
            }
            ++row;
        }

        /// The other rows must carry the captured `values`.
        for (; row < row_end; ++row)
        {
            if (is_excluded(row))
                continue;

            bool same = false;
            if (!array_column)
            {
                same = (values.size() == 1) && samePhi(static_cast<Float64>(data[row]), values[0]);
            }
            else if (array_size() == grid_size)
            {
                const auto * row_values = data.data() + array_begin();
                if (values.size() == 1)
                    same = std::all_of(row_values, row_values + grid_size, [&](auto value) { return samePhi(static_cast<Float64>(value), values[0]); });
                else
                    same = std::equal(values.begin(), values.end(), row_values, [](Float64 lhs, auto rhs) { return samePhi(lhs, static_cast<Float64>(rhs)); });
            }

            if (!same)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Aggregate function timeSeriesQuantileToGrid requires the same value of the argument `phi` in every row");
        }
        return true;
    });

    if (!dispatched)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column {} for the argument `phi`", number_column.getName());
}


void AggregateFunctionTimeseriesQuantileToGridPhi::merge(const AggregateFunctionTimeseriesQuantileToGridPhi & other)
{
    if (values.empty())
    {
        values = other.values;
        return;
    }

    if (other.values.empty())
        return;

    if (values.size() != other.values.size() || !std::equal(values.begin(), values.end(), other.values.begin(), samePhi))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cannot merge states of aggregate function timeSeriesQuantileToGrid created with different values of the argument `phi`");
}


void AggregateFunctionTimeseriesQuantileToGridPhi::serialize(WriteBuffer & buf) const
{
    writeBinaryLittleEndian(static_cast<UInt64>(values.size()), buf);
    for (const Float64 value : values)
        writeBinaryLittleEndian(value, buf);
}


void AggregateFunctionTimeseriesQuantileToGridPhi::deserialize(ReadBuffer & buf, size_t grid_size)
{
    UInt64 size = 0;
    readBinaryLittleEndian(size, buf);
    if (size > 1 && size != grid_size)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Cannot deserialize data with {} values of the argument `phi`, expected 0, 1 or {}", size, grid_size);

    values.resize(size);
    for (auto & value : values)
        readBinaryLittleEndian(value, buf);
}


Float64 AggregateFunctionTimeseriesQuantileToGridPhi::at(size_t grid_index) const
{
    if (values.empty())
        return 0;
    return (values.size() == 1) ? values[0] : values[grid_index];
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
