#include <AggregateFunctions/AggregateFunctionQuantile.h>
#include <AggregateFunctions/QuantileTDigest.h>
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

template <typename Value, bool float_return> using FuncQuantileTDigestWeighted = AggregateFunctionQuantile<Value, QuantileTDigest<Value>, NameQuantileTDigestWeighted, UInt64, std::conditional_t<float_return, Float32, void>, false, false>;
template <typename Value, bool float_return> using FuncQuantilesTDigestWeighted = AggregateFunctionQuantile<Value, QuantileTDigest<Value>, NameQuantilesTDigestWeighted, UInt64, std::conditional_t<float_return, Float32, void>, true, false>;

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

void registerAggregateFunctionsQuantileTDigestWeighted(AggregateFunctionFactory & factory)
{
    /// For aggregate functions returning array we cannot return NULL on empty set.
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true };

    FunctionDocumentation::Description description = R"(
Computes an approximate [quantile](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence using the [t-digest](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf) algorithm.
The function takes into account the weight of each sequence member.

The maximum error is 1%.
Memory consumption is `log(n)`, where `n` is a number of values.

The performance of the function is lower than performance of [`quantile`](/sql-reference/aggregate-functions/reference/quantile) or [`quantileTiming`](/sql-reference/aggregate-functions/reference/quantiletiming).
In terms of the ratio of State size to precision, this function is much better than `quantile`.

The result depends on the order of running the query, and is nondeterministic.

When using multiple `quantile*` functions with different levels in a query, the internal states are not combined (that is, the query works less efficiently than it could).
In this case, use the [`quantiles`](/sql-reference/aggregate-functions/reference/quantiles#quantiles) function.

:::note
Using `quantileTDigestWeighted` [is not recommended for tiny data sets](https://github.com/tdunning/t-digest/issues/167#issuecomment-828650275) and can lead to significant error.
In this case, consider possibility of using [`quantileTDigest`](/sql-reference/aggregate-functions/reference/quantiletdigest) instead.
:::
    )";
    FunctionDocumentation::Syntax syntax = R"(
quantileTDigestWeighted(level)(expr, weight)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"expr", "Expression over the column values resulting in numeric data types, Date or DateTime.", {"(U)Int*", "Float*", "Decimal*", "Date", "DateTime"}},
        {"weight", "Column with weights of sequence elements. Weight is a number of value occurrences.", {"UInt*"}}
    };
    FunctionDocumentation::Parameters parameters = {
        {"level", "Optional. Level of quantile. Constant floating-point number from 0 to 1. We recommend using a `level` value in the range of `[0.01, 0.99]`. Default value: 0.5. At `level=0.5` the function calculates median.", {"Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Approximate quantile of the specified level.", {"Float64", "Date", "DateTime"}};
    FunctionDocumentation::Examples examples = {
    {
        "Computing weighted quantile with t-digest",
        R"(
SELECT quantileTDigestWeighted(number, 1) FROM numbers(10);
        )",
        R"(
┌─quantileTDigestWeighted(number, 1)─┐
│                                4.5 │
└────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction(NameQuantileTDigestWeighted::name, {createAggregateFunctionQuantile<FuncQuantileTDigestWeighted>, documentation});

    FunctionDocumentation::Description description_quantiles = R"(
Computes multiple approximate [quantiles](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence at different levels simultaneously using the [t-digest](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf) algorithm.
The function takes into account the weight of each sequence member.

This function is equivalent to [`quantileTDigestWeighted`](/sql-reference/aggregate-functions/reference/quantiletdigestweighted) but allows computing multiple quantile levels in a single pass, which is more efficient than calling individual quantile functions.

The maximum error is 1%. Memory consumption is `log(n)`, where `n` is a number of values.

The performance of the function is lower than performance of [`quantiles`](/sql-reference/aggregate-functions/reference/quantiles) or [`quantilesTiming`](/sql-reference/aggregate-functions/reference/quantilestiming).
In terms of the ratio of State size to precision, this function is much better than `quantiles`.

The result depends on the order of running the query, and is nondeterministic.

:::note
Using `quantilesTDigestWeighted` [is not recommended for tiny data sets](https://github.com/tdunning/t-digest/issues/167#issuecomment-828650275) and can lead to significant error.
In this case, consider possibility of using [`quantilesTDigest`](/sql-reference/aggregate-functions/reference/quantilestdigest) instead.
:::
    )";
    FunctionDocumentation::Syntax syntax_quantiles = R"(
quantilesTDigestWeighted(level1, level2, ...)(expr, weight)
    )";
    FunctionDocumentation::Arguments arguments_quantiles = {
        {"expr", "Expression over the column values resulting in numeric data types, Date or DateTime.", {"(U)Int*", "Float*", "Decimal*", "Date", "DateTime"}},
        {"weight", "Column with weights of sequence elements. Weight is a number of value occurrences.", {"UInt*"}}
    };
    FunctionDocumentation::Parameters parameters_quantiles = {
        {"level", "Levels of quantiles. One or more constant floating-point numbers from 0 to 1. We recommend using `level` values in the range of `[0.01, 0.99]`.", {"Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_quantiles = {"Array of approximate quantiles of the specified levels in the same order as the levels were specified.", {"Array(Float64)", "Array(Date)", "Array(DateTime)"}};
    FunctionDocumentation::Examples examples_quantiles = {
    {
        "Computing multiple weighted quantiles with t-digest",
        R"(
SELECT quantilesTDigestWeighted(0.25, 0.5, 0.75)(number, 1) FROM numbers(100);
        )",
        R"(
┌─quantilesTDigestWeighted(0.25, 0.5, 0.75)(number, 1)─┐
│ [24.75,49.5,74.25]                                   │
└───────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_quantiles = {20, 1};
    FunctionDocumentation::Category category_quantiles = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_quantiles = {description_quantiles, syntax_quantiles, arguments_quantiles, parameters_quantiles, returned_value_quantiles, examples_quantiles, introduced_in_quantiles, category_quantiles};

    factory.registerFunction(NameQuantilesTDigestWeighted::name, {createAggregateFunctionQuantile<FuncQuantilesTDigestWeighted>, documentation_quantiles, properties});

    /// 'median' is an alias for 'quantile'
    factory.registerAlias("medianTDigestWeighted", NameQuantileTDigestWeighted::name);
}

}
