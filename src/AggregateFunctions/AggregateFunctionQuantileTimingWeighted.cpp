#include <AggregateFunctions/AggregateFunctionQuantile.h>
#include <AggregateFunctions/QuantileTiming.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Core/Field.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <typename Value, bool _> using FuncQuantileTimingWeighted = AggregateFunctionQuantile<Value, QuantileTiming<Value>, NameQuantileTimingWeighted, UInt64, Float32, false, false>;
template <typename Value, bool _> using FuncQuantilesTimingWeighted = AggregateFunctionQuantile<Value, QuantileTiming<Value>, NameQuantilesTimingWeighted, UInt64, Float32, true, false>;

template <template <typename, bool> class Function>
AggregateFunctionPtr createAggregateFunctionQuantile(
    const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    if (argument_types.empty())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires at least one argument", name);

    const DataTypePtr & argument_type = argument_types[0];
    WhichDataType which(argument_type);

#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return std::make_shared<Function<TYPE, true>>(argument_types, params);
    FOR_BASIC_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Date) return std::make_shared<Function<DataTypeDate::FieldType, false>>(argument_types, params);
    if (which.idx == TypeIndex::DateTime) return std::make_shared<Function<DataTypeDateTime::FieldType, false>>(argument_types, params);

    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}",
                    argument_type->getName(), name);
}

}

void registerAggregateFunctionsQuantileTimingWeighted(AggregateFunctionFactory & factory)
{
    /// For aggregate functions returning array we cannot return NULL on empty set.
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true };

    FunctionDocumentation::Description description = R"(
With the determined precision computes the [quantile](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence according to the weight of each sequence member.

The result is deterministic (it does not depend on the query processing order). The function is optimized for working with sequences which describe distributions like loading web pages times or backend response times.

When using multiple `quantile*` functions with different levels in a query, the internal states are not combined (that is, the query works less efficiently than it could). In this case, use the [`quantiles`](/sql-reference/aggregate-functions/reference/quantiles#quantiles) function.

**Accuracy**

The calculation is accurate if:

- Total number of values does not exceed 5670.
- Total number of values exceeds 5670, but the page loading time is less than 1024ms.

Otherwise, the result of the calculation is rounded to the nearest multiple of 16 ms.

:::note
For calculating page loading time quantiles, this function is more effective and accurate than [`quantile`](/sql-reference/aggregate-functions/reference/quantile).
:::

:::note
If no values are passed to the function (when using `quantileTimingIf`), [NaN](/sql-reference/data-types/float#nan-and-inf) is returned. The purpose of this is to differentiate these cases from cases that result in zero. See [ORDER BY clause](/sql-reference/statements/select/order-by) for notes on sorting `NaN` values.
:::
    )";
    FunctionDocumentation::Syntax syntax = R"(
quantileTimingWeighted(level)(expr, weight)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"expr", "Expression over a column values returning a Float*-type number. If negative values are passed to the function, the behavior is undefined. If the value is greater than 30,000 (a page loading time of more than 30 seconds), it is assumed to be 30,000.", {"Float*"}},
        {"weight", "Column with weights of sequence elements. Weight is a number of value occurrences.", {"UInt*"}}
    };
    FunctionDocumentation::Parameters parameters = {
        {"level", "Optional. Level of quantile. Constant floating-point number from 0 to 1. We recommend using a `level` value in the range of `[0.01, 0.99]`. Default value: 0.5. At `level=0.5` the function calculates median.", {"Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Quantile of the specified level.", {"Float32"}};
    FunctionDocumentation::Examples examples = {
    {
        "Computing weighted timing quantile",
        R"(
CREATE TABLE t (response_time UInt32, weight UInt32) ENGINE = Memory;
INSERT INTO t VALUES (68, 1), (104, 2), (112, 3), (126, 2), (138, 1), (162, 1);

SELECT quantileTimingWeighted(response_time, weight) FROM t;
        )",
        R"(
┌─quantileTimingWeighted(response_time, weight)─┐
│                                           112 │
└───────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction(NameQuantileTimingWeighted::name, {createAggregateFunctionQuantile<FuncQuantileTimingWeighted>, documentation});

    FunctionDocumentation::Description description_quantiles = R"(
Computes multiple [quantiles](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence at different levels simultaneously with determined precision, taking into account the weight of each sequence member.

This function is equivalent to [`quantileTimingWeighted`](/sql-reference/aggregate-functions/reference/quantiletimingweighted) but allows computing multiple quantile levels in a single pass, which is more efficient than calling individual quantile functions.

The result is deterministic (it does not depend on the query processing order). The function is optimized for working with sequences which describe distributions like loading web pages times or backend response times.

**Accuracy**

The calculation is accurate if:

- Total number of values does not exceed 5670.
- Total number of values exceeds 5670, but the page loading time is less than 1024ms.

Otherwise, the result of the calculation is rounded to the nearest multiple of 16 ms.

:::note
For calculating page loading time quantiles, this function is more effective and accurate than [`quantiles`](/sql-reference/aggregate-functions/reference/quantiles).
:::
    )";
    FunctionDocumentation::Syntax syntax_quantiles = R"(
quantilesTimingWeighted(level1, level2, ...)(expr, weight)
    )";
    FunctionDocumentation::Arguments arguments_quantiles = {
        {"expr", "Expression over a column values returning a Float*-type number. If negative values are passed to the function, the behavior is undefined. If the value is greater than 30,000 (a page loading time of more than 30 seconds), it is assumed to be 30,000.", {"Float*"}},
        {"weight", "Column with weights of sequence elements. Weight is a number of value occurrences.", {"UInt*"}}
    };
    FunctionDocumentation::Parameters parameters_quantiles = {
        {"level", "Levels of quantiles. One or more constant floating-point numbers from 0 to 1. We recommend using `level` values in the range of `[0.01, 0.99]`.", {"Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_quantiles = {"Array of quantiles of the specified levels in the same order as the levels were specified.", {"Array(Float32)"}};
    FunctionDocumentation::Examples examples_quantiles = {
    {
        "Computing multiple weighted timing quantiles",
        R"(
SELECT quantilesTimingWeighted(0.5, 0.99)(response_time, weight) FROM t;
        )",
        R"(
┌─quantilesTimingWeighted(0.5, 0.99)(response_time, weight)─┐
│ [112, 162]                                                │
└───────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_quantiles = {1, 1};
    FunctionDocumentation::Category category_quantiles = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_quantiles = {description_quantiles, syntax_quantiles, arguments_quantiles, parameters_quantiles, returned_value_quantiles, examples_quantiles, introduced_in_quantiles, category_quantiles};

    factory.registerFunction(NameQuantilesTimingWeighted::name, {createAggregateFunctionQuantile<FuncQuantilesTimingWeighted>, documentation_quantiles, properties});

    /// 'median' is an alias for 'quantile'
    factory.registerAlias("medianTimingWeighted", NameQuantileTimingWeighted::name);
}

}
