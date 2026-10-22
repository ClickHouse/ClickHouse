#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnsNumber.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <WindowFunctions/Common/WindowArguments.h>
#include <WindowFunctions/Common/WindowBounds.h>
#include <WindowFunctions/General/WindowFunctionState.h>
#include <WindowFunctions/WindowFunction.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

struct NonNegativeDerivativeState
{
    Float64 previous_metric = 0;
    Float64 previous_timestamp = 0;
};

struct NonNegativeDerivativeParams
{
    static constexpr size_t ARGUMENT_METRIC = 0;
    static constexpr size_t ARGUMENT_TIMESTAMP = 1;
    static constexpr size_t ARGUMENT_INTERVAL = 2;

    Float64 interval_length = 1;
    bool interval_specified = false;
    Int64 ts_scale_multiplier = 0;

    NonNegativeDerivativeParams(const String & name_, const DataTypes & argument_types, const Array & parameters)
    {
        if (!parameters.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} cannot be parameterized", name_);

        if (argument_types.size() != 2 && argument_types.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} takes 2 or 3 arguments", name_);

        if (!isNumber(argument_types[ARGUMENT_METRIC]))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument {} must be a number, '{}' given", ARGUMENT_METRIC, argument_types[ARGUMENT_METRIC]->getName());

        if (!isDateTime(argument_types[ARGUMENT_TIMESTAMP]) && !isDateTime64(argument_types[ARGUMENT_TIMESTAMP]))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument {} must be DateTime or DateTime64, '{}' given", ARGUMENT_TIMESTAMP, argument_types[ARGUMENT_TIMESTAMP]->getName());

        if (isDateTime64(argument_types[ARGUMENT_TIMESTAMP]))
        {
            const auto & datetime64_type = assert_cast<const DataTypeDateTime64 &>(*argument_types[ARGUMENT_TIMESTAMP]);
            ts_scale_multiplier = DecimalUtils::scaleMultiplier<DateTime64>(datetime64_type.getScale());
        }

        if (argument_types.size() == 3)
        {
            const DataTypeInterval * interval_datatype = checkAndGetDataType<DataTypeInterval>(argument_types[ARGUMENT_INTERVAL].get());
            if (!interval_datatype)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument {} must be an INTERVAL, '{}' given", ARGUMENT_INTERVAL, argument_types[ARGUMENT_INTERVAL]->getName());

            if (!interval_datatype->getKind().isFixedLength())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The INTERVAL must be a week or shorter, '{}' given", argument_types[ARGUMENT_INTERVAL]->getName());

            interval_length = interval_datatype->getKind().toSeconds();
            interval_specified = true;
        }
    }
};

class WindowFunctionNonNegativeDerivative final : public WindowFunctionState<NonNegativeDerivativeState>
{
public:
    WindowFunctionNonNegativeDerivative(const String & name_, const DataTypes & argument_types_, const Array & parameters_)
        : WindowFunctionState(name_, argument_types_, parameters_, std::make_shared<DataTypeFloat64>())
        , params(name_, argument_types_, parameters_)
    {
    }

    void evaluate(char * place, const WindowBounds & bounds, const WindowArguments & arguments, IColumn & result) const override
    {
        NonNegativeDerivativeState & state = stateAt(place);
        const Float64 interval_duration = params.interval_specified
            ? params.interval_length * arguments.at(NonNegativeDerivativeParams::ARGUMENT_INTERVAL, bounds.row[0]).getFloat64()
            : 1;
        const Float64 timestamp_scale = params.ts_scale_multiplier ? static_cast<Float64>(params.ts_scale_multiplier) : 1;
        auto & data = assert_cast<ColumnFloat64 &>(result).getData();
        for (size_t i = 0; i < bounds.rows; ++i)
        {
            const Int64 row = bounds.row[i];
            const Float64 metric = arguments.at(NonNegativeDerivativeParams::ARGUMENT_METRIC, row).getFloat64();
            const ArgumentValue timestamp_value = arguments.at(NonNegativeDerivativeParams::ARGUMENT_TIMESTAMP, row);
            const Float64 timestamp = params.ts_scale_multiplier ? static_cast<Float64>(timestamp_value.getInt()) : timestamp_value.getFloat64();
            const Float64 time_elapsed = timestamp - state.previous_timestamp;
            Float64 derivative = time_elapsed > 0 ? (metric - state.previous_metric) * timestamp_scale / time_elapsed * interval_duration : 0;
            if (row == 0)
                derivative = 0;
            state.previous_metric = metric;
            state.previous_timestamp = timestamp;
            data.push_back(derivative >= 0 ? derivative : 0);
        }
    }

private:
    const NonNegativeDerivativeParams params;
};

}

void registerWindowFunctionNonNegativeDerivative(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionNonNegativeDerivative(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties)
{
    factory.registerFunction("nonNegativeDerivative", {[](const std::string & name,
           const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return std::make_shared<WindowFunction>(std::make_shared<WindowFunctionNonNegativeDerivative>(name, argument_types, parameters));
        }, {.description = R"DOCS_MD(
Computes the non-negative derivative of `metric_column` with respect to `timestamp_column`.
This is a ClickHouse-specific window function, not part of standard SQL.

For each row, the derivative is computed against the *previous row in the window's evaluation order*, which is determined by the window's `ORDER BY` clause - not by `timestamp_column`.
The `timestamp_column` argument is read only to measure the elapsed time between the current row and that previous row; it does not order the rows itself.

<Warning>
`nonNegativeDerivative` does not order rows by `timestamp_column`; the window's `ORDER BY` does.
For the formula below to apply, `timestamp_column` must be strictly increasing in the window's evaluation order, so you should normally order the window by `timestamp_column` ascending (for example `... OVER (ORDER BY ts ASC)` together with `nonNegativeDerivative(metric, ts)`).
Whenever the elapsed time between the current row and the previous row is non-positive - which happens with `ORDER BY timestamp_column DESC` or with duplicate (equal) timestamps - the function returns `0` for that row instead of following the formula.
</Warning>

The result is the rate of change of the metric per `INTERVAL`, with any negative value clamped to `0`.
This is useful for monotonically increasing metrics, such as counters, where a decrease usually indicates a reset rather than a real negative rate.

**Syntax**

```sql
nonNegativeDerivative(metric_column, timestamp_column[, INTERVAL X UNITS])
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        [ROWS, RANGE, or GROUPS expression_to_bound_rows_within_the_group]] | [window_name])
FROM table_name
WINDOW window_name AS ([PARTITION BY grouping_column] [ORDER BY sorting_column] [ROWS, RANGE, or GROUPS expression_to_bound_rows_within_the_group])
```

For more detail on window function syntax see: [Window Functions - Syntax](/reference/functions/window-functions/index#syntax).

**Arguments**

- `metric_column` — The column whose derivative is computed. [(U)Int*](/reference/data-types/int-uint) or [Float*](/reference/data-types/float).
- `timestamp_column` — The column used to measure the elapsed time between the current row and the previous row in the window order. It does not order the rows; the window's `ORDER BY` does, and should normally use this same column. [DateTime](/reference/data-types/datetime) or [DateTime64](/reference/data-types/datetime64).
- `INTERVAL X UNITS` — Optional. The time unit the result is scaled to. Defaults to `INTERVAL 1 SECOND`. Only fixed-length units are supported (`NANOSECOND`, `MICROSECOND`, `MILLISECOND`, `SECOND`, `MINUTE`, `HOUR`, `DAY`, `WEEK`); variable-length units (`MONTH`, `QUARTER`, `YEAR`) raise an exception.

**Returned value**

For each row, the value is computed as:

- `0` for the first row;
- `0` for any row whose elapsed time since the previous row is non-positive (that is, $\text{timestamp}_i - \text{timestamp}_{i-1} \le 0$, as happens with descending order or duplicate timestamps); and
- ${\text{metric}_i - \text{metric}_{i-1} \over \text{timestamp}_i - \text{timestamp}_{i-1}} * \text{interval}$ otherwise.

If the computed value would be negative, it is clamped to `0`. The return type is [Float64](/reference/data-types/float).

**Example**

The following example computes the per-second rate of change of a sensor reading.
Note that the third row drops from `110` to `105`, so its derivative is clamped to `0`.

```sql title="Query"
CREATE TABLE sensor_readings
(
    `sensor_id` UInt32,
    `ts`        DateTime,
    `reading`   Float64
)
ENGINE = Memory;

INSERT INTO sensor_readings VALUES
    (1, '2024-01-01 00:00:00', 100),
    (1, '2024-01-01 00:00:10', 110),
    (1, '2024-01-01 00:00:20', 105),
    (1, '2024-01-01 00:00:30', 130);
```

```sql title="Query"
SELECT
    ts,
    reading,
    nonNegativeDerivative(reading, ts) OVER (ORDER BY ts ASC) AS deriv_per_second
FROM sensor_readings
ORDER BY ts ASC;
```

```response title="Response"
   ┌──────────────────ts─┬─reading─┬─deriv_per_second─┐
1. │ 2024-01-01 00:00:00 │     100 │                0 │
2. │ 2024-01-01 00:00:10 │     110 │                1 │
3. │ 2024-01-01 00:00:20 │     105 │                0 │
4. │ 2024-01-01 00:00:30 │     130 │              2.5 │
   └─────────────────────┴─────────┴──────────────────┘
```
)DOCS_MD", .category = FunctionDocumentation::Category::AggregateFunction}, properties});
}

}
