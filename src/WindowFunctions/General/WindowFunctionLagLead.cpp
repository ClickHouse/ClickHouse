#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/IColumn.h>
#include <WindowFunctions/Common/WindowArguments.h>
#include <WindowFunctions/Common/WindowBounds.h>
#include <WindowFunctions/IGeneralWindowFunction.h>
#include <WindowFunctions/WindowFunction.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
}

namespace
{

DataTypePtr lagLeadResultType(const String & name, const DataTypes & argument_types)
{
    if (argument_types.empty())
        throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Function {} takes at least one argument", name);

    return argument_types[0];
}

Int64 checkedOffset(const String & name, Int64 offset)
{
    if (offset < 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The offset for function {} must be in (0, {}], {} given", name, INT64_MAX, offset);

    return offset;
}

class WindowFunctionLagLead final : public IGeneralWindowFunction
{
public:
    WindowFunctionLagLead(const String & name_, const DataTypes & argument_types_, const Array & parameters_, bool is_lead_, bool in_frame_)
        : IGeneralWindowFunction(name_, argument_types_, parameters_, lagLeadResultType(name_, argument_types_))
        , is_lead(is_lead_)
        , in_frame(in_frame_)
    {
        if (!parameters.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} cannot be parameterized", name_);

        if (argument_types.size() == 1)
            return;

        if (!isInt64OrUInt64FieldType(argument_types[1]->getDefault().getType()))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Offset must be an integer, '{}' given", argument_types[1]->getName());

        if (argument_types.size() == 2)
            return;

        if (argument_types.size() > 3)
            throw Exception(ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION, "Function '{}' accepts at most 3 arguments, {} given", name, argument_types.size());

        if (!argument_types[0]->equals(*argument_types[2]))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The default value type '{}' must be the same as the argument type '{}'", argument_types[2]->getName(), argument_types[0]->getName());
    }

    WindowFunctionRequirements getRequirements(const WindowDescription &) const override
    {
        if (in_frame)
            return WindowFunctionRequirements{.frame = true};

        return WindowFunctionRequirements{.partition = true};
    }

    std::optional<WindowFrame> getDefaultFrame() const override
    {
        if (in_frame)
            return {};

        WindowFrame frame;
        frame.type = WindowFrame::FrameType::ROWS;
        frame.end_type = WindowFrame::BoundaryType::Unbounded;
        return frame;
    }

    void evaluate(char *, const WindowBounds & bounds, const WindowArguments & arguments, IColumn & result) const override
    {
        for (size_t i = 0; i < bounds.rows; ++i)
        {
            const Int64 row = bounds.row[i];
            const Int64 offset = arguments.size() > 1 ? checkedOffset(getName(), arguments.at(1, row).getInt()) : 1;
            const Int64 distance = is_lead ? offset : -offset;
            if (bounds.frame_start[i] - row <= distance && distance < bounds.frame_end[i] - row)
                arguments.at(0, row + distance).appendTo(result);
            else if (arguments.size() > 2)
                arguments.at(2, row).appendTo(result);
            else
                result.insertDefault();
        }
    }

private:
    const bool is_lead;
    const bool in_frame;
};

}

void registerWindowFunctionsLagLead(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionsLagLead(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties)
{
    factory.registerFunction("lagInFrame", {[](const std::string & name,
            const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return std::make_shared<WindowFunction>(std::make_shared<WindowFunctionLagLead>(name, argument_types, parameters, false, true));
        }, {.description = R"DOCS_MD(
Returns a value evaluated at the row that is at a specified physical offset row before the current row within the ordered frame.

<Warning>
`lagInFrame` behavior differs from the standard SQL `lag` window function.
ClickHouse window function `lagInFrame` respects the window frame.
To get behavior identical to the `lag`, use `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.
</Warning>

**Syntax**

```sql
lagInFrame(x[, offset[, default]])
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        [ROWS, RANGE, or GROUPS expression_to_bound_rows_withing_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

For more detail on window function syntax see: [Window Functions - Syntax](/reference/functions/window-functions/index#syntax).

**Parameters**
- `x` — Column name.
- `offset` — Offset to apply. [(U)Int*](/reference/data-types/int-uint). (Optional - `1` by default).
- `default` — Value to return if calculated row exceeds the boundaries of the window frame. (Optional - default value of column type when omitted).

**Returned value**

- Value evaluated at the row that is at a specified physical offset before the current row within the ordered frame.

**Example**

This example looks at historical data for a specific stock and uses the `lagInFrame` function to calculate a day-to-day delta and percentage change in the closing price of the stock.

```sql title="Query"
CREATE TABLE stock_prices
(
    `date`   Date,
    `open`   Float32, -- opening price
    `high`   Float32, -- daily high
    `low`    Float32, -- daily low
    `close`  Float32, -- closing price
    `volume` UInt32   -- trade volume
)
Engine = Memory;

INSERT INTO stock_prices FORMAT Values
    ('2024-06-03', 113.62, 115.00, 112.00, 115.00, 438392000),
    ('2024-06-04', 115.72, 116.60, 114.04, 116.44, 403324000),
    ('2024-06-05', 118.37, 122.45, 117.47, 122.44, 528402000),
    ('2024-06-06', 124.05, 125.59, 118.32, 121.00, 664696000),
    ('2024-06-07', 119.77, 121.69, 118.02, 120.89, 412386000);
```

```sql title="Query"
SELECT
    date,
    close,
    lagInFrame(close, 1, close) OVER (ORDER BY date ASC
       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
     ) AS previous_day_close,
    COALESCE(ROUND(close - previous_day_close, 2)) AS delta,
    COALESCE(ROUND((delta / previous_day_close) * 100, 2)) AS percent_change
FROM stock_prices
ORDER BY date DESC
```

```response title="Response"
   ┌───────date─┬──close─┬─previous_day_close─┬─delta─┬─percent_change─┐
1. │ 2024-06-07 │ 120.89 │                121 │ -0.11 │          -0.09 │
2. │ 2024-06-06 │    121 │             122.44 │ -1.44 │          -1.18 │
3. │ 2024-06-05 │ 122.44 │             116.44 │     6 │           5.15 │
4. │ 2024-06-04 │ 116.44 │                115 │  1.44 │           1.25 │
5. │ 2024-06-03 │    115 │                115 │     0 │              0 │
   └────────────┴────────┴────────────────────┴───────┴────────────────┘
```
)DOCS_MD", .category = FunctionDocumentation::Category::AggregateFunction}, properties});

    factory.registerFunction("lag", {[](const std::string & name,
            const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return std::make_shared<WindowFunction>(std::make_shared<WindowFunctionLagLead>(name, argument_types, parameters, false, false));
        }, {.description = R"DOCS_MD(
Returns a value evaluated at the row that is at a specified physical offset before the current row within the ordered frame.
This function is similar to [`lagInFrame`](/reference/functions/window-functions/lagInFrame), but always uses the `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` frame.

**Syntax**

```sql
lag(x[, offset[, default]])
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

For more detail on window function syntax see: [Window Functions - Syntax](/reference/functions/window-functions/index#syntax).

**Parameters**

- `x` — Column name.
- `offset` — Offset to apply. [(U)Int*](/reference/data-types/int-uint). (Optional - `1` by default).
- `default` — Value to return if calculated row exceeds the boundaries of the window frame. (Optional - default value of column type when omitted).

**Returned value**

- Value evaluated at the row that is at a specified physical offset before the current row within the ordered frame.

**Example**

This example looks at historical data for a specific stock and uses the `lag` function to calculate a day-to-day delta and percentage change in the closing price of the stock.

```sql title="Query"
CREATE TABLE stock_prices
(
    `date`   Date,
    `open`   Float32, -- opening price
    `high`   Float32, -- daily high
    `low`    Float32, -- daily low
    `close`  Float32, -- closing price
    `volume` UInt32   -- trade volume
)
Engine = Memory;

INSERT INTO stock_prices FORMAT Values
    ('2024-06-03', 113.62, 115.00, 112.00, 115.00, 438392000),
    ('2024-06-04', 115.72, 116.60, 114.04, 116.44, 403324000),
    ('2024-06-05', 118.37, 122.45, 117.47, 122.44, 528402000),
    ('2024-06-06', 124.05, 125.59, 118.32, 121.00, 664696000),
    ('2024-06-07', 119.77, 121.69, 118.02, 120.89, 412386000);
```

```sql title="Query"
SELECT
    date,
    close,
    lag(close, 1, close) OVER (ORDER BY date ASC) AS previous_day_close,
    COALESCE(ROUND(close - previous_day_close, 2)) AS delta,
    COALESCE(ROUND((delta / previous_day_close) * 100, 2)) AS percent_change
FROM stock_prices
ORDER BY date DESC
```

```response title="Response"
   ┌───────date─┬──close─┬─previous_day_close─┬─delta─┬─percent_change─┐
1. │ 2024-06-07 │ 120.89 │                121 │ -0.11 │          -0.09 │
2. │ 2024-06-06 │    121 │             122.44 │ -1.44 │          -1.18 │
3. │ 2024-06-05 │ 122.44 │             116.44 │     6 │           5.15 │
4. │ 2024-06-04 │ 116.44 │                115 │  1.44 │           1.25 │
5. │ 2024-06-03 │    115 │                115 │     0 │              0 │
   └────────────┴────────┴────────────────────┴───────┴────────────────┘
```
)DOCS_MD", .category = FunctionDocumentation::Category::AggregateFunction}, properties}, AggregateFunctionFactory::Case::Insensitive);

    factory.registerFunction("leadInFrame", {[](const std::string & name,
            const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return std::make_shared<WindowFunction>(std::make_shared<WindowFunctionLagLead>(name, argument_types, parameters, true, true));
        }, {.description = R"DOCS_MD(
Returns a value evaluated at the row that is offset rows after the current row within the ordered frame.

<Warning>
`leadInFrame` behavior differs from the standard SQL `lead` window function.
ClickHouse window function `leadInFrame` respects the window frame.
To get behavior identical to the `lead`, use `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.
</Warning>

**Syntax**

```sql
leadInFrame(x[, offset[, default]])
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        [ROWS, RANGE, or GROUPS expression_to_bound_rows_withing_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

For more detail on window function syntax see: [Window Functions - Syntax](/reference/functions/window-functions/index#syntax).

**Parameters**
- `x` — Column name.
- `offset` — Offset to apply. [(U)Int*](/reference/data-types/int-uint). (Optional - `1` by default).
- `default` — Value to return if calculated row exceeds the boundaries of the window frame. (Optional - default value of column type when omitted).

**Returned value**

- value evaluated at the row that is offset rows after the current row within the ordered frame.

**Example**

This example looks at [historical data](https://www.kaggle.com/datasets/sazidthe1/nobel-prize-data) for Nobel Prize winners and uses the `leadInFrame` function to return a list of successive winners in the physics category.

```sql title="Query"
CREATE OR REPLACE VIEW nobel_prize_laureates
AS SELECT *
FROM file('nobel_laureates_data.csv');
```

```sql title="Query"
SELECT
    fullName,
    leadInFrame(year, 1, year) OVER (PARTITION BY category ORDER BY year ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS year,
    category,
    motivation
FROM nobel_prize_laureates
WHERE category = 'physics'
ORDER BY year DESC
LIMIT 9
```

```response title="Response"
   ┌─fullName─────────┬─year─┬─category─┬─motivation─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
1. │ Anne L Huillier  │ 2023 │ physics  │ for experimental methods that generate attosecond pulses of light for the study of electron dynamics in matter                     │
2. │ Pierre Agostini  │ 2023 │ physics  │ for experimental methods that generate attosecond pulses of light for the study of electron dynamics in matter                     │
3. │ Ferenc Krausz    │ 2023 │ physics  │ for experimental methods that generate attosecond pulses of light for the study of electron dynamics in matter                     │
4. │ Alain Aspect     │ 2022 │ physics  │ for experiments with entangled photons establishing the violation of Bell inequalities and  pioneering quantum information science │
5. │ Anton Zeilinger  │ 2022 │ physics  │ for experiments with entangled photons establishing the violation of Bell inequalities and  pioneering quantum information science │
6. │ John Clauser     │ 2022 │ physics  │ for experiments with entangled photons establishing the violation of Bell inequalities and  pioneering quantum information science │
7. │ Giorgio Parisi   │ 2021 │ physics  │ for the discovery of the interplay of disorder and fluctuations in physical systems from atomic to planetary scales                │
8. │ Klaus Hasselmann │ 2021 │ physics  │ for the physical modelling of Earths climate quantifying variability and reliably predicting global warming                        │
9. │ Syukuro Manabe   │ 2021 │ physics  │ for the physical modelling of Earths climate quantifying variability and reliably predicting global warming                        │
   └──────────────────┴──────┴──────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```
)DOCS_MD", .category = FunctionDocumentation::Category::AggregateFunction}, properties});

    factory.registerFunction("lead", {[](const std::string & name,
            const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return std::make_shared<WindowFunction>(std::make_shared<WindowFunctionLagLead>(name, argument_types, parameters, true, false));
        }, {.description = R"DOCS_MD(
Returns a value evaluated at the row that is offset rows after the current row within the ordered frame.
This function is similar to [`leadInFrame`](/reference/functions/window-functions/leadInFrame), but always uses the `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` frame.

**Syntax**

```sql
lead(x[, offset[, default]])
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

For more detail on window function syntax see: [Window Functions - Syntax](/reference/functions/window-functions/index#syntax).

**Parameters**

- `x` — Column name.
- `offset` — Offset to apply. [(U)Int*](/reference/data-types/int-uint). (Optional - `1` by default).
- `default` — Value to return if calculated row exceeds the boundaries of the window frame. (Optional - default value of column type when omitted).

**Returned value**

- value evaluated at the row that is offset rows after the current row within the ordered frame.

**Example**

This example looks at [historical data](https://www.kaggle.com/datasets/sazidthe1/nobel-prize-data) for Nobel Prize winners and uses the `lead` function to return a list of successive winners in the physics category.

```sql title="Query"
CREATE OR REPLACE VIEW nobel_prize_laureates
AS SELECT *
FROM file('nobel_laureates_data.csv');
```

```sql title="Query"
SELECT
    fullName,
    lead(year, 1, year) OVER (PARTITION BY category ORDER BY year ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS year,
    category,
    motivation
FROM nobel_prize_laureates
WHERE category = 'physics'
ORDER BY year DESC
LIMIT 9
```

```response title="Query"
   ┌─fullName─────────┬─year─┬─category─┬─motivation─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
1. │ Anne L Huillier  │ 2023 │ physics  │ for experimental methods that generate attosecond pulses of light for the study of electron dynamics in matter                     │
2. │ Pierre Agostini  │ 2023 │ physics  │ for experimental methods that generate attosecond pulses of light for the study of electron dynamics in matter                     │
3. │ Ferenc Krausz    │ 2023 │ physics  │ for experimental methods that generate attosecond pulses of light for the study of electron dynamics in matter                     │
4. │ Alain Aspect     │ 2022 │ physics  │ for experiments with entangled photons establishing the violation of Bell inequalities and  pioneering quantum information science │
5. │ Anton Zeilinger  │ 2022 │ physics  │ for experiments with entangled photons establishing the violation of Bell inequalities and  pioneering quantum information science │
6. │ John Clauser     │ 2022 │ physics  │ for experiments with entangled photons establishing the violation of Bell inequalities and  pioneering quantum information science │
7. │ Giorgio Parisi   │ 2021 │ physics  │ for the discovery of the interplay of disorder and fluctuations in physical systems from atomic to planetary scales                │
8. │ Klaus Hasselmann │ 2021 │ physics  │ for the physical modelling of Earths climate quantifying variability and reliably predicting global warming                        │
9. │ Syukuro Manabe   │ 2021 │ physics  │ for the physical modelling of Earths climate quantifying variability and reliably predicting global warming                        │
   └──────────────────┴──────┴──────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```
)DOCS_MD", .category = FunctionDocumentation::Category::AggregateFunction}, properties}, AggregateFunctionFactory::Case::Insensitive);
}

}
