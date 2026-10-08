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

void registerAggregateFunctionsQuantileBFloat16Weighted(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Like [`quantileBFloat16`](/sql-reference/aggregate-functions/reference/quantilebfloat16) but takes into account the weight of each sequence member.

Computes an approximate [quantile](https://en.wikipedia.org/wiki/Quantile) of a sample consisting of [bfloat16](https://en.wikipedia.org/wiki/Bfloat16_floating-point_format) numbers.

`bfloat16` is a floating-point data type with 1 sign bit, 8 exponent bits and 7 fraction bits.
The function converts input values to 32-bit floats and takes the most significant 16 bits.
Then it calculates `bfloat16` quantile value and converts the result to a 64-bit float by appending zero bits.
The function is a fast quantile estimator with a relative error no more than 0.390625%.
    )";
    FunctionDocumentation::Syntax syntax = R"(
quantileBFloat16Weighted(level)(expr, weight)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"expr", "Column with numeric data.", {"(U)Int*", "Float*"}},
        {"weight", "Column with weights of sequence members.", {"(U)Int*", "Float*"}}
    };
    FunctionDocumentation::Parameters parameters = {
        {"level", "Optional. Level of quantile. Possible values are in the range from 0 to 1. Default value: 0.5.", {"Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Approximate quantile of the specified level.", {"Float64"}};
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
    factory.registerFunction(NameQuantilesBFloat16Weighted::name, {createAggregateFunctionQuantile<FuncQuantilesBFloat16Weighted>, {}});

    /// 'median' is an alias for 'quantile'
    factory.registerAlias("medianBFloat16Weighted", NameQuantileBFloat16Weighted::name);
}

}
