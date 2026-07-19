#include <AggregateFunctions/AggregateFunctionQuantile.h>
#include <AggregateFunctions/QuantileBFloat16Histogram.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
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

template <typename Value, bool float_return> using FuncQuantileBFloat16Weighted = AggregateFunctionQuantile<Value, QuantileBFloat16Histogram<Value>, NameQuantileBFloat16Weighted, UInt64, std::conditional_t<float_return, Float64, void>, false, false>;
template <typename Value, bool float_return> using FuncQuantilesBFloat16Weighted = AggregateFunctionQuantile<Value, QuantileBFloat16Histogram<Value>, NameQuantilesBFloat16Weighted, UInt64, std::conditional_t<float_return, Float64, void>, true, false>;

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

void registerAggregateFunctionsQuantileBFloat16Weighted(AggregateFunctionFactory & factory);
void registerAggregateFunctionsQuantileBFloat16Weighted(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Like [`quantileBFloat16`](/sql-reference/aggregate-functions/reference/quantilebfloat16) but takes into account the weight of each sequence member.

Computes an approximate [quantile](https://en.wikipedia.org/wiki/Quantile) of a sample consisting of [bfloat16](https://en.wikipedia.org/wiki/Bfloat16_floating-point_format) numbers.

`bfloat16` is a floating-point data type with 1 sign bit, 8 exponent bits and 7 fraction bits.
The function converts input values to 32-bit floats and takes the most significant 16 bits.
Then it calculates `bfloat16` quantile value and converts the result to a 64-bit float by appending zero bits.
The function is a fast quantile estimator with a maximum relative error of `0.78125%` (and an average relative error of approximately `0.27%`), corresponding to the 7-bit mantissa precision of `bfloat16`.
    )";
    FunctionDocumentation::Syntax syntax = R"(
quantileBFloat16Weighted(level)(expr, weight)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"expr", "Expression over the column values resulting in numeric data types, `Date` or `DateTime`.", {"(U)Int*", "Float*", "Date", "DateTime"}},
        {"weight", "Column with weights of sequence members. Weight is a number of value occurrences.", {"UInt*"}}
    };
    FunctionDocumentation::Parameters parameters = {
        {"level", "Optional. Level of quantile. Possible values are in the range from 0 to 1. Default value: 0.5.", {"Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Approximate quantile of the specified level. For `Date` and `DateTime` inputs the output format matches the input format.", {"Float64", "Date", "DateTime"}};
    FunctionDocumentation::Examples examples = {
    {
        "Computing weighted quantile with bfloat16",
        R"(
CREATE TABLE example_table (a UInt32, b Float32, w UInt32) ENGINE = Memory;
INSERT INTO example_table VALUES (1, 1.001, 1), (2, 1.002, 2), (3, 1.003, 3), (4, 1.004, 4);

SELECT quantileBFloat16Weighted(0.75)(a, w), quantileBFloat16Weighted(0.75)(b, w) FROM example_table;
        )",
        R"(
┌─quantileBFloat16Weighted(0.75)(a, w)─┬─quantileBFloat16Weighted(0.75)(b, w)─┐
│                                    3 │                                    1 │
└──────────────────────────────────────┴──────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction(NameQuantileBFloat16Weighted::name, {createAggregateFunctionQuantile<FuncQuantileBFloat16Weighted>, documentation});

    FunctionDocumentation::Description description_quantiles = R"(
Like [`quantilesBFloat16`](/sql-reference/aggregate-functions/reference/quantilesBFloat16) but takes into account the weight of each value.

Computes multiple approximate [quantiles](https://en.wikipedia.org/wiki/Quantile) of a sample consisting of [bfloat16](https://en.wikipedia.org/wiki/Bfloat16_floating-point_format) numbers at different levels simultaneously, in a single pass.
    )";
    FunctionDocumentation::Syntax syntax_quantiles = R"(
quantilesBFloat16Weighted(level1, level2, ...)(expr, weight)
    )";
    FunctionDocumentation::Arguments arguments_quantiles = {
        {"expr", "Expression over the column values resulting in numeric data types, `Date` or `DateTime`.", {"(U)Int*", "Float*", "Date", "DateTime"}},
        {"weight", "Column with weights of sequence members. Weight is a number of value occurrences.", {"UInt*"}}
    };
    FunctionDocumentation::Parameters parameters_quantiles = {
        {"level", "Levels of quantiles. One or more constant floating-point numbers from 0 to 1. We recommend using `level` values in the range of `[0.01, 0.99]`.", {"Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_quantiles = {"Array of approximate quantiles of the specified levels in the same order as the levels were specified. For `Date` and `DateTime` inputs the output format matches the input format.", {"Array(Float64)", "Array(Date)", "Array(DateTime)"}};
    FunctionDocumentation::Examples examples_quantiles = {
    {
        "Computing multiple weighted bfloat16 quantiles",
        R"(
SELECT quantilesBFloat16Weighted(0.25, 0.5, 0.75)(number, 1) FROM numbers(10)
        )",
        R"(
┌─quantilesBFloat16Weighted(0.25, 0.5, 0.75)(number, 1)─┐
│ [2,4,7]                                               │
└───────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_quantiles = {21, 10};
    FunctionDocumentation::Category category_quantiles = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_quantiles = {description_quantiles, syntax_quantiles, arguments_quantiles, parameters_quantiles, returned_value_quantiles, examples_quantiles, introduced_in_quantiles, category_quantiles};

    factory.registerFunction(NameQuantilesBFloat16Weighted::name, {createAggregateFunctionQuantile<FuncQuantilesBFloat16Weighted>, documentation_quantiles});

    /// 'median' is an alias for 'quantile'
    factory.registerAlias("medianBFloat16Weighted", NameQuantileBFloat16Weighted::name);
}

}
