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
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <typename Value, bool _> using FuncQuantileExact = AggregateFunctionQuantile<Value, QuantileExact<Value>, NameQuantileExact, void, void, false, false>;
template <typename Value, bool _> using FuncQuantilesExact = AggregateFunctionQuantile<Value, QuantileExact<Value>, NameQuantilesExact, void, void, true, false>;


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

    if (which.idx == TypeIndex::Decimal32) return std::make_shared<Function<Decimal32, false>>(argument_types, params);
    if (which.idx == TypeIndex::Decimal64) return std::make_shared<Function<Decimal64, false>>(argument_types, params);
    if (which.idx == TypeIndex::Decimal128) return std::make_shared<Function<Decimal128, false>>(argument_types, params);
    if (which.idx == TypeIndex::Decimal256) return std::make_shared<Function<Decimal256, false>>(argument_types, params);
    if (which.idx == TypeIndex::DateTime64) return std::make_shared<Function<DateTime64, false>>(argument_types, params);

    if (which.idx == TypeIndex::Int128) return std::make_shared<Function<Int128, true>>(argument_types, params);
    if (which.idx == TypeIndex::UInt128) return std::make_shared<Function<UInt128, true>>(argument_types, params);
    if (which.idx == TypeIndex::Int256) return std::make_shared<Function<Int256, true>>(argument_types, params);
    if (which.idx == TypeIndex::UInt256) return std::make_shared<Function<UInt256, true>>(argument_types, params);

    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}",
                    argument_type->getName(), name);
}

}

void registerAggregateFunctionsQuantileExact(AggregateFunctionFactory & factory);
void registerAggregateFunctionsQuantileExact(AggregateFunctionFactory & factory)
{
    /// For aggregate functions returning array we cannot return NULL on empty set.
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true };

    FunctionDocumentation::Description description = R"(
Exactly computes the [quantile](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence.

To get exact value, all the passed values are combined into an array, which is then partially sorted.
Therefore, the function consumes `O(n)` memory, where `n` is a number of values that were passed.
However, for a small number of values, the function is very effective.

When using multiple `quantile*` functions with different levels in a query, the internal states are not combined (that is, the query works less efficiently than it could).
In this case, use the [`quantiles`](/sql-reference/aggregate-functions/reference/quantiles#quantiles) function.
    )";
    FunctionDocumentation::Syntax syntax = R"(
quantileExact(level)(expr)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"expr", "Expression over the column values resulting in numeric data types, `Date`, `DateTime` or `DateTime64`.", {"(U)Int*", "Int128", "UInt128", "Int256", "UInt256", "Float*", "Decimal*", "Date", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::Parameters parameters = {
        {"level", "Optional. Level of quantile. Constant floating-point number from 0 to 1. We recommend using a `level` value in the range of `[0.01, 0.99]`. Default value: 0.5. At `level=0.5` the function calculates median.", {"Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Quantile of the specified level. For numeric data types the output format will be the same as the input format.", {"(U)Int*", "Int128", "UInt128", "Int256", "UInt256", "Float*", "Decimal*", "Date", "DateTime", "DateTime64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Computing exact quantile",
        R"(
SELECT quantileExact(number) FROM numbers(10);
        )",
        R"(
┌─quantileExact(number)─┐
│                     5 │
└───────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction(NameQuantileExact::name, {createAggregateFunctionQuantile<FuncQuantileExact>, documentation});

    FunctionDocumentation::Description description_quantiles = R"(
Exactly computes multiple [quantiles](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence at different levels simultaneously.

This function is equivalent to [`quantileExact`](/sql-reference/aggregate-functions/reference/quantileexact) but allows computing multiple quantile levels in a single pass, which is more efficient than calling individual quantile functions.

To get exact values, all the passed values are combined into an array, which is then partially sorted. Therefore, the function consumes `O(n)` memory, where `n` is the number of values passed.
    )";
    FunctionDocumentation::Syntax syntax_quantiles = R"(
quantilesExact(level1, level2, ...)(expr)
    )";
    FunctionDocumentation::Arguments arguments_quantiles = {
        {"expr", "Expression over the column values resulting in numeric data types, `Date`, `DateTime` or `DateTime64`.", {"(U)Int*", "Int128", "UInt128", "Int256", "UInt256", "Float*", "Decimal*", "Date", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::Parameters parameters_quantiles = {
        {"level", "Levels of quantiles. One or more constant floating-point numbers from 0 to 1. We recommend using `level` values in the range of `[0.01, 0.99]`.", {"Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_quantiles = {"Array of quantiles of the specified levels in the same order as the levels were specified. For numeric data types the output format matches the input format.", {"Array((U)Int*)", "Array(Int128)", "Array(UInt128)", "Array(Int256)", "Array(UInt256)", "Array(Float*)", "Array(Decimal*)", "Array(Date)", "Array(DateTime)", "Array(DateTime64)"}};
    FunctionDocumentation::Examples examples_quantiles = {
    {
        "Computing multiple exact quantiles",
        R"(
SELECT quantilesExact(0.25, 0.5, 0.75)(number) FROM numbers(10)
        )",
        R"(
┌─quantilesExact(0.25, 0.5, 0.75)(number)─┐
│ [2,5,7]                                 │
└─────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_quantiles = {1, 1};
    FunctionDocumentation::Category category_quantiles = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_quantiles = {description_quantiles, syntax_quantiles, arguments_quantiles, parameters_quantiles, returned_value_quantiles, examples_quantiles, introduced_in_quantiles, category_quantiles};

    factory.registerFunction(NameQuantilesExact::name, { createAggregateFunctionQuantile<FuncQuantilesExact>, documentation_quantiles, properties });

    /// 'median' is an alias for 'quantile'
    factory.registerAlias("medianExact", NameQuantileExact::name);
}

}
