#include <AggregateFunctions/AggregateFunctionQuantile.h>
#include <AggregateFunctions/QuantileExact.h>
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
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <typename Value, bool _> using FuncQuantileExactInclusive = AggregateFunctionQuantile<Value, QuantileExactInclusive<Value>, NameQuantileExactInclusive, void, Float64, false, false>;
template <typename Value, bool _> using FuncQuantilesExactInclusive = AggregateFunctionQuantile<Value, QuantileExactInclusive<Value>, NameQuantilesExactInclusive, void, Float64, true, false>;

template <template <typename, bool> class Function>
AggregateFunctionPtr createAggregateFunctionQuantile(
    const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    if (argument_types.empty())
        throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Aggregate function {} requires at least one argument", name);

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

void registerAggregateFunctionsQuantileExactInclusive(AggregateFunctionFactory & factory)
{
    /// For aggregate functions returning array we cannot return NULL on empty set.
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true };

    FunctionDocumentation::Description description = R"(
Similar to [`quantileExact`](/sql-reference/aggregate-functions/reference/quantileexact), this computes the exact [quantile](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence.

This function is equivalent to [`quantileExact`](/sql-reference/aggregate-functions/reference/quantileexact) but uses the inclusive method for calculating quantiles, as described in the [R-7 method](https://en.wikipedia.org/wiki/Quantile#Estimating_quantiles_from_a_sample).

When using this function, the quantile is calculated such that the interpolation formula for a given quantile p takes the form: `x[floor((n-1)*p)] + ((n-1)*p - floor((n-1)*p)) * (x[floor((n-1)*p)+1] - x[floor((n-1)*p)])`, where x is the sorted array.

To get the exact value, all the passed values are combined into an array, which is then fully sorted.
The sorting algorithm's complexity is `O(N·log(N))`, where `N = std::distance(first, last)` comparisons.

When using multiple `quantile*` functions with different levels in a query, the internal states are not combined (that is, the query works less efficiently than it could).
In this case, use the [quantiles](/sql-reference/aggregate-functions/reference/quantiles) function.
    )";
    FunctionDocumentation::Syntax syntax = R"(
quantileExactInclusive(level)(expr)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"expr", "Expression over the column values resulting in numeric data types, Date or DateTime.", {"(U)Int*", "Float*", "Decimal*", "Date", "DateTime"}}
    };
    FunctionDocumentation::Parameters parameters = {
        {"level", "Level of quantile. Constant floating-point number from 0 to 1 (inclusive). We recommend using a `level` value in the range of `[0.01, 0.99]`.", {"Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the quantile of the specified level.", {"Float64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Computing exact inclusive quantile",
        R"(
SELECT quantileExactInclusive(0.25)(number) FROM numbers(5);
        )",
        R"(
┌─quantileExactInclusive(0.25)(number)─┐
│                                    1 │
└──────────────────────────────────────┘
        )"
    },
    {
        "Computing multiple quantile levels",
        R"(
SELECT quantileExactInclusive(0.1)(number), quantileExactInclusive(0.9)(number) FROM numbers(10);
        )",
        R"(
┌─quantileExactInclusive(0.1)(number)─┬─quantileExactInclusive(0.9)(number)─┐
│                                 0.9 │                                 8.1 │
└─────────────────────────────────────┴─────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction(NameQuantileExactInclusive::name, { createAggregateFunctionQuantile<FuncQuantileExactInclusive>, documentation});

    FunctionDocumentation::Description description_quantiles = R"(
Exactly computes multiple [quantiles](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence at different levels simultaneously using the inclusive method.

This function is equivalent to [`quantileExactInclusive`](/sql-reference/aggregate-functions/reference/quantileExactInclusive) but allows computing multiple quantile levels in a single pass, which is more efficient than calling individual quantile functions.

This function uses the inclusive method for calculating quantiles, as described in the [R-7 method](https://en.wikipedia.org/wiki/Quantile#Estimating_quantiles_from_a_sample).
This is equivalent to [PERCENTILE.INC](https://support.microsoft.com/en-us/office/percentile-inc-function-680f9539-45eb-410b-9a5e-c1355e5fe2ed) Excel function.

To get exact values, all the passed values are combined into an array, which is then partially sorted.
The sorting algorithm's complexity is `O(N·log(N))`, where `N = std::distance(first, last)` comparisons.
    )";
    FunctionDocumentation::Syntax syntax_quantiles = R"(
quantilesExactInclusive(level1, level2, ...)(expr)
    )";
    FunctionDocumentation::Arguments arguments_quantiles = {
        {"expr", "Expression over the column values resulting in numeric data types, Date or DateTime.", {"(U)Int*", "Float*", "Decimal*", "Date", "DateTime"}}
    };
    FunctionDocumentation::Parameters parameters_quantiles = {
        {"level", "Levels of quantiles. Constant floating-point numbers from 0 to 1 (inclusive). We recommend using `level` values in the range of `[0.01, 0.99]`.", {"Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_quantiles = {"Array of quantiles of the specified levels in the same order as the levels were specified.", {"Array(Float64)"}};
    FunctionDocumentation::Examples examples_quantiles = {
    {
        "Computing multiple exact inclusive quantiles",
        R"(
CREATE TABLE num AS numbers(1000);
SELECT quantilesExactInclusive(0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999)(number) FROM num;
        )",
        R"(
┌─quantilesExactInclusive(0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999)(number)─┐
│ [249.75,499.5,749.25,899.1,949.05,989.01,998.001]                        │
└──────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_quantiles = {20, 1};
    FunctionDocumentation::Category category_quantiles = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_quantiles = {description_quantiles, syntax_quantiles, arguments_quantiles, parameters_quantiles, returned_value_quantiles, examples_quantiles, introduced_in_quantiles, category_quantiles};

    factory.registerFunction(NameQuantilesExactInclusive::name, { createAggregateFunctionQuantile<FuncQuantilesExactInclusive>, documentation_quantiles, properties });
}

}
