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

template <typename Value, bool _> using FuncQuantileExactHigh = AggregateFunctionQuantile<Value, QuantileExactHigh<Value>, NameQuantileExactHigh, void, void, false, false>;
template <typename Value, bool _> using FuncQuantilesExactHigh = AggregateFunctionQuantile<Value, QuantileExactHigh<Value>, NameQuantilesExactHigh, void, void, true, false>;


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
    if (which.idx == TypeIndex::DateTime64) return std::make_shared<Function<DateTime64, false>>(argument_types, params);

    if (which.idx == TypeIndex::Decimal32) return std::make_shared<Function<Decimal32, false>>(argument_types, params);
    if (which.idx == TypeIndex::Decimal64) return std::make_shared<Function<Decimal64, false>>(argument_types, params);
    if (which.idx == TypeIndex::Decimal128) return std::make_shared<Function<Decimal128, false>>(argument_types, params);
    if (which.idx == TypeIndex::Decimal256) return std::make_shared<Function<Decimal256, false>>(argument_types, params);

    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}",
                    argument_type->getName(), name);
}

}

void registerAggregateFunctionsQuantileExactHigh(AggregateFunctionFactory & factory);
void registerAggregateFunctionsQuantileExactHigh(AggregateFunctionFactory & factory)
{
    /// For aggregate functions returning array we cannot return NULL on empty set.
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true };

    FunctionDocumentation::Description description = R"(
Similar to [`quantileExact`](/sql-reference/aggregate-functions/reference/quantileexact), this computes the exact [quantile](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence.

To get the exact value, all the passed values are combined into an array, which is then fully sorted.
The sorting algorithm's complexity is `O(N·log(N))`, where `N = std::distance(first, last)` comparisons.

The return value depends on the quantile level and the number of elements in the selection, i.e. if the level is 0.5, then the function returns the higher median value for an even number of elements and the middle median value for an odd number of elements.
Median is calculated similarly to the [`median_high`](https://docs.python.org/3/library/statistics.html#statistics.median_high) implementation which is used in python.

For all other levels, the element at the index corresponding to the value of `level * size_of_array` is returned.

When using multiple `quantile*` functions with different levels in a query, the internal states are not combined (that is, the query works less efficiently than it could).
In this case, use the [quantiles](/sql-reference/aggregate-functions/reference/quantiles) function.
    )";
    FunctionDocumentation::Syntax syntax = R"(
quantileExactHigh(level)(expr)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"expr", "Expression over the column values resulting in numeric data types, `Date`, `DateTime` or `DateTime64`.", {"(U)Int*", "Float*", "Decimal*", "Date", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::Parameters parameters = {
        {"level", "Optional. Level of quantile. Constant floating-point number from 0 to 1. We recommend using a `level` value in the range of `[0.01, 0.99]`. Default value: 0.5. At `level=0.5` the function calculates median.", {"Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the quantile of the specified level. For numeric data types the output format matches the input format.", {"(U)Int*", "Float*", "Decimal*", "Date", "DateTime", "DateTime64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Computing exact high quantile",
        R"(
SELECT quantileExactHigh(number) FROM numbers(10);
        )",
        R"(
┌─quantileExactHigh(number)─┐
│                         5 │
└───────────────────────────┘
        )"
    },
    {
        "Computing specific quantile level",
        R"(
SELECT quantileExactHigh(0.1)(number) FROM numbers(10);
        )",
        R"(
┌─quantileExactHigh(0.1)(number)─┐
│                              1 │
└────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction(NameQuantileExactHigh::name, {createAggregateFunctionQuantile<FuncQuantileExactHigh>, documentation});

    FunctionDocumentation::Description description_quantiles = R"(
Exactly computes multiple [quantiles](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence at different levels simultaneously. For an even number of elements at level `0.5` it returns the higher of the two middle values.

This function is equivalent to [`quantileExactHigh`](/sql-reference/aggregate-functions/reference/quantileExactHigh) but allows computing multiple quantile levels in a single pass, which is more efficient than calling individual quantile functions.
    )";
    FunctionDocumentation::Syntax syntax_quantiles = R"(
quantilesExactHigh(level1, level2, ...)(expr)
    )";
    FunctionDocumentation::Arguments arguments_quantiles = {
        {"expr", "Expression over the column values resulting in numeric data types, `Date`, `DateTime` or `DateTime64`.", {"(U)Int*", "Float*", "Decimal*", "Date", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::Parameters parameters_quantiles = {
        {"level", "Levels of quantiles. One or more constant floating-point numbers from 0 to 1. We recommend using `level` values in the range of `[0.01, 0.99]`.", {"Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_quantiles = {"Array of quantiles of the specified levels in the same order as the levels were specified. For numeric data types the output format matches the input format.", {"Array((U)Int*)", "Array(Float*)", "Array(Decimal*)", "Array(Date)", "Array(DateTime)", "Array(DateTime64)"}};
    FunctionDocumentation::Examples examples_quantiles = {
    {
        "Computing multiple exact high quantiles",
        R"(
SELECT quantilesExactHigh(0.25, 0.5, 0.75)(number) FROM numbers(10)
        )",
        R"(
┌─quantilesExactHigh(0.25, 0.5, 0.75)(number)─┐
│ [2,5,7]                                     │
└─────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_quantiles = {20, 8};
    FunctionDocumentation::Category category_quantiles = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_quantiles = {description_quantiles, syntax_quantiles, arguments_quantiles, parameters_quantiles, returned_value_quantiles, examples_quantiles, introduced_in_quantiles, category_quantiles};

    factory.registerFunction(NameQuantilesExactHigh::name, { createAggregateFunctionQuantile<FuncQuantilesExactHigh>, documentation_quantiles, properties });

    /// 'median' is an alias for 'quantile'
    factory.registerAlias("medianExactHigh", NameQuantileExactHigh::name);
}

}
