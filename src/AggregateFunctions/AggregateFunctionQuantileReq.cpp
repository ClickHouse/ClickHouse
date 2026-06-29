#include "config.h"

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionQuantile.h>

#if USE_DATASKETCHES
#include <AggregateFunctions/QuantileReq.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Core/Field.h>
#endif


namespace DB
{
struct Settings;

#if USE_DATASKETCHES

namespace ErrorCodes
{
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <typename Value, bool float_return> using FuncQuantileReq = AggregateFunctionQuantile<Value, QuantileReq<Value>, NameQuantileReq, void, std::conditional_t<float_return, Float64, void>, false, true>;
template <typename Value, bool float_return> using FuncQuantilesReq = AggregateFunctionQuantile<Value, QuantileReq<Value>, NameQuantilesReq, void, std::conditional_t<float_return, Float64, void>, true, true>;


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

#endif

void registerAggregateFunctionsQuantileReq(AggregateFunctionFactory & factory);
void registerAggregateFunctionsQuantileReq(AggregateFunctionFactory & factory)
{
#if USE_DATASKETCHES
    /// For aggregate functions returning array we cannot return NULL on empty set.
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true };

    FunctionDocumentation::Description description = R"(
Computes an approximate [quantile](https://en.wikipedia.org/wiki/Quantile) of a sample using the
Relative Error Quantiles (REQ) sketch. Unlike `quantileTDigest` (no formal error bound) and
`quantileGK` (flat rank-error bound), REQ guarantees a *relative* rank error, so it stays accurate
at extreme quantiles such as p99.9. The sketch is fully mergeable, so it works under partial and
distributed aggregation.

Reference: "Relative Error Streaming Quantiles", Cormode, Karnin, Liberty, Thaler, Veselý
(JACM 2023, https://dl.acm.org/doi/10.1145/3617891).
    )";
    FunctionDocumentation::Syntax syntax = R"(
quantileReq(accuracy, [level])(expr)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"expr", "Column with numeric data.", {"(U)Int*", "Float*", "Date", "DateTime"}}
    };
    FunctionDocumentation::Parameters parameters = {
        {"accuracy", "Accuracy of the sketch (the `k` parameter). A constant positive even integer; larger values mean less error and more memory. Odd values are rounded up. The recommended value is 12.", {"UInt*"}},
        {"level", "Optional. Level of quantile. A constant floating-point number in the range from 0 to 1. Default value: 0.5.", {"Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Approximate quantile of the specified level.", {"Float64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Computing a tail quantile with the REQ sketch (result is approximate)",
        R"(
SELECT quantileReq(12, 0.999)(number) FROM numbers(1000000);
        )",
        R"(
┌─quantileReq(12, 0.999)(number)─┐
│                         998999 │
└────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction(NameQuantileReq::name, {createAggregateFunctionQuantile<FuncQuantileReq>, documentation});

    FunctionDocumentation::Description description_quantiles = R"(
Computes multiple approximate [quantiles](https://en.wikipedia.org/wiki/Quantile) of a sample at
different levels simultaneously using the Relative Error Quantiles (REQ) sketch.

This function works similarly to [`quantileReq`](/sql-reference/aggregate-functions/reference/quantileReq)
but allows computing multiple quantile levels in a single pass, which is more efficient than calling
individual quantile functions. Like `quantileReq`, it guarantees a *relative* rank error, so it stays
accurate at extreme quantiles such as p99.9, and is fully mergeable for distributed aggregation.

Reference: "Relative Error Streaming Quantiles", Cormode, Karnin, Liberty, Thaler, Veselý
(JACM 2023, https://dl.acm.org/doi/10.1145/3617891).
    )";
    FunctionDocumentation::Syntax syntax_quantiles = R"(
quantilesReq(accuracy, level1, level2, ...)(expr)
    )";
    FunctionDocumentation::Arguments arguments_quantiles = {
        {"expr", "Column with numeric data.", {"(U)Int*", "Float*", "Date", "DateTime"}}
    };
    FunctionDocumentation::Parameters parameters_quantiles = {
        {"accuracy", "Accuracy of the sketch (the `k` parameter). A constant positive even integer; larger values mean less error and more memory. Odd values are rounded up. The recommended value is 12.", {"UInt*"}},
        {"level", "Levels of quantiles. One or more constant floating-point numbers from 0 to 1.", {"Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_quantiles = {"Array of quantiles of the specified levels in the same order as the levels were specified.", {"Array(Float64)", "Array(Date)", "Array(DateTime)"}};
    FunctionDocumentation::Examples examples_quantiles = {
    {
        "Computing multiple tail quantiles with the REQ sketch (results are approximate)",
        R"(
SELECT quantilesReq(12, 0.5, 0.99, 0.999)(number) FROM numbers(1000000);
        )",
        R"(
┌─quantilesReq(12, 0.5, 0.99, 0.999)(number)─┐
│ [501294,989998,998999]                     │
└────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_quantiles = {25, 7};
    FunctionDocumentation::Category category_quantiles = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_quantiles = {description_quantiles, syntax_quantiles, arguments_quantiles, parameters_quantiles, returned_value_quantiles, examples_quantiles, introduced_in_quantiles, category_quantiles};

    factory.registerFunction(NameQuantilesReq::name, {createAggregateFunctionQuantile<FuncQuantilesReq>, documentation_quantiles, properties});

    /// 'median' is an alias for 'quantile'
    factory.registerAlias("medianReq", NameQuantileReq::name);
#else
    (void)factory;
#endif
}

}
