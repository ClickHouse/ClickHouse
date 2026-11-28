#include <AggregateFunctions/AggregateFunctionQuantile.h>
#include <AggregateFunctions/QuantileREQ.h>
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
    extern const int BAD_ARGUMENTS;
}

namespace
{

template <typename Value, bool float_return>
using FuncQuantileREQ = AggregateFunctionQuantile<
    Value,
    QuantileREQ<Value>,
    NameQuantileREQ,
    void,
    std::conditional_t<float_return, Float64, void>,
    false,
    true>;

template <typename Value, bool float_return>
using FuncQuantilesREQ = AggregateFunctionQuantile<
    Value,
    QuantileREQ<Value>,
    NameQuantilesREQ,
    void,
    std::conditional_t<float_return, Float64, void>,
    true,
    true>;

template <template <typename, bool> class Function>
AggregateFunctionPtr createAggregateFunctionQuantile(
    const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    if (argument_types.empty())
        throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Aggregate function {} requires at least one argument", name);

    if (params.empty())
        throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
            "Aggregate function {} requires at least one parameter: k (even integer in [4, 1024])", name);

    const auto & k_field = params[0];
    if (!isInt64OrUInt64FieldType(k_field.getType()))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "First parameter (k) of aggregate function {} must be an integer", name);

    /// The vendored REQ sketch documents `k` as supported in `[4, 1024]`. Reject values
    /// outside that range BEFORE narrowing to `uint16_t`, so e.g. `quantileREQ(70000)`
    /// doesn't silently truncate to a smaller value.
    Int64 k;
    if (k_field.getType() == Field::Types::Int64)
        k = k_field.safeGet<Int64>();
    else
    {
        UInt64 k_unsigned = k_field.safeGet<UInt64>();
        if (k_unsigned > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Parameter k of aggregate function {} must be in [4, 1024] (got {})", name, k_unsigned);
        k = static_cast<Int64>(k_unsigned);
    }

    if (k < 4 || k > 1024)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Parameter k of aggregate function {} must be in [4, 1024] (got {})", name, k);

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

void registerAggregateFunctionsQuantileREQ(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = true};

    FunctionDocumentation::Description description = R"(
Computes an approximate [quantile](https://en.wikipedia.org/wiki/Quantile) using the
[REQ (Relative Error Quantiles) sketch](https://datasketches.apache.org/docs/REQ/ReqSketch.html)
algorithm from the Apache DataSketches library.

Unlike `quantileKLL` which provides a rank-error guarantee (absolute), REQ provides a
**relative rank error** guarantee: the rank error scales with the rank itself, giving
higher accuracy at the tails. This makes REQ particularly suited to queries where tail
accuracy (p99, p999) matters more than accuracy near the median.

The sketch is configured with High Rank Accuracy (HRA) mode, meaning accuracy is
prioritised at high ranks (near 1.0). At `k=12`, the relative rank error is approximately
1% at 95% confidence for high ranks.

The serialized state uses the DataSketches REQ wire format, enabling cross-language
interoperability with Java and Python DataSketches clients.
    )";
    FunctionDocumentation::Syntax syntax = R"(
quantileREQ(k[, level])(expr)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"expr", "Column with numeric data.", {"(U)Int*", "Float*"}}
    };
    FunctionDocumentation::Parameters parameters = {
        {"k", "Sketch size parameter. Must be in `[4, 1024]`; odd values are rounded down to the nearest even number. "
              "Larger values give more accurate results. Default recommended value: 12 (~1% relative rank error "
              "at 95% confidence for high ranks).", {"UInt*"}},
        {"level", "Optional. Level of quantile. Constant floating-point number from 0 to 1. "
                  "Default value: 0.5. At `level=0.5` the function calculates the median.", {"Float64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Approximate quantile of the specified level.", {"Float64"}};
    FunctionDocumentation::Examples examples = {
        {"Basic usage",
         R"(
SELECT quantileREQ(12, 0.99)(number + 1) FROM numbers(10000);
         )",
         R"(
┌─quantileREQ(12, 0.99)(plus(number, 1))─┐
│                                   9901 │
└─────────────────────────────────────────┘
         )"},
        {"Multiple quantile levels",
         R"(
SELECT quantilesREQ(12, 0.5, 0.9, 0.99)(number + 1) FROM numbers(10000);
         )",
         R"(
┌─quantilesREQ(12, 0.5, 0.9, 0.99)(plus(number, 1))─┐
│ [5001,9001,9901]                                    │
└─────────────────────────────────────────────────────┘
         )"},
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction(NameQuantileREQ::name, {createAggregateFunctionQuantile<FuncQuantileREQ>, documentation});

    FunctionDocumentation::Description description_quantiles = R"(
Computes multiple approximate [quantiles](https://en.wikipedia.org/wiki/Quantile) of a numeric data
sequence at different levels simultaneously using the
[REQ (Relative Error Quantiles) sketch](https://datasketches.apache.org/docs/REQ/ReqSketch.html)
algorithm from the Apache DataSketches library.

This function works similarly with [`quantileREQ`](/sql-reference/aggregate-functions/reference/quantileREQ)
but allows computing multiple quantile levels in a single pass, which is more efficient than calling
individual quantile functions.

Like `quantileREQ`, the sketch is configured with High Rank Accuracy (HRA) mode: tail accuracy
(p99, p999) is prioritised over accuracy near the median.
    )";
    FunctionDocumentation::Syntax syntax_quantiles = R"(
quantilesREQ(k, level1[, level2, ...])(expr)
    )";
    FunctionDocumentation::Arguments arguments_quantiles = {
        {"expr", "Column with numeric data.", {"(U)Int*", "Float*"}}
    };
    FunctionDocumentation::Parameters parameters_quantiles = {
        {"k", "Sketch size parameter. Must be in `[4, 1024]`; odd values are rounded down to the nearest even number. "
              "Larger values give more accurate results. Recommended starting value: 12 (~1% relative rank error "
              "at 95% confidence for high ranks).", {"UInt*"}},
        {"level", "Levels of quantiles. One or more constant floating-point numbers from 0 to 1.", {"Float64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_quantiles = {"Array of approximate quantiles of the specified levels in the same order as the levels were specified.", {"Array(Float64)"}};
    FunctionDocumentation::Examples examples_quantiles = {
        {"Computing multiple quantiles with REQ algorithm",
         R"(
SELECT quantilesREQ(12, 0.5, 0.9, 0.99)(number + 1) FROM numbers(10000);
         )",
         R"(
┌─quantilesREQ(12, 0.5, 0.9, 0.99)(plus(number, 1))─┐
│ [5001,9001,9901]                                    │
└─────────────────────────────────────────────────────┘
         )"},
    };
    FunctionDocumentation::IntroducedIn introduced_in_quantiles = {26, 7};
    FunctionDocumentation::Category category_quantiles = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_quantiles = {description_quantiles, syntax_quantiles, arguments_quantiles, parameters_quantiles, returned_value_quantiles, examples_quantiles, introduced_in_quantiles, category_quantiles};

    factory.registerFunction(NameQuantilesREQ::name, {createAggregateFunctionQuantile<FuncQuantilesREQ>, documentation_quantiles, properties});

    factory.registerAlias("medianREQ", NameQuantileREQ::name);
}

}
