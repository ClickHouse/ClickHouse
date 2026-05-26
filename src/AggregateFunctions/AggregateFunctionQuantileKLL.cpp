#include <AggregateFunctions/AggregateFunctionQuantile.h>
#include <AggregateFunctions/QuantileKLL.h>
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
using FuncQuantileKLL = AggregateFunctionQuantile<
    Value,
    QuantileKLL<Value>,
    NameQuantileKLL,
    void,
    std::conditional_t<float_return, Float64, void>,
    false,
    true>;

template <typename Value, bool float_return>
using FuncQuantilesKLL = AggregateFunctionQuantile<
    Value,
    QuantileKLL<Value>,
    NameQuantilesKLL,
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
            "Aggregate function {} requires at least one parameter: k (integer >= 8)", name);

    const auto & k_field = params[0];
    if (!isInt64OrUInt64FieldType(k_field.getType()))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "First parameter (k) of aggregate function {} must be an integer", name);

    ssize_t k;
    if (k_field.getType() == Field::Types::Int64)
        k = static_cast<ssize_t>(k_field.safeGet<Int64>());
    else
        k = static_cast<ssize_t>(k_field.safeGet<UInt64>());

    if (k < 8)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Parameter k of aggregate function {} must be >= 8 (got {})", name, k);

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

void registerAggregateFunctionsQuantileKLL(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = true};

    FunctionDocumentation::Description description = R"(
Computes an approximate [quantile](https://en.wikipedia.org/wiki/Quantile) using the
[KLL sketch](https://datasketches.apache.org/docs/KLL/KLLSketch.html) algorithm from the
Apache DataSketches library.

KLL provides a rank-error guarantee: for any quantile estimate, the rank error is bounded by
±ε where ε ≈ 1.33/k for the default `k=200`. The sketch uses O(k·log(n/k)) memory, which is
provably optimal for rank-error sketches of this type.

The serialized state uses the DataSketches KLL wire format, enabling cross-language
interoperability with Java and Python DataSketches clients.
    )";
    FunctionDocumentation::Syntax syntax = R"(
quantileKLL(k[, level])(expr)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"expr", "Column with numeric data.", {"(U)Int*", "Float*"}}
    };
    FunctionDocumentation::Parameters parameters = {
        {"k", "Sketch size parameter controlling accuracy. Must be >= 8. Larger values give more accurate results. "
              "Default recommended value: 200 (≈1.33% rank error).", {"UInt*"}},
        {"level", "Optional. Level of quantile. Constant floating-point number from 0 to 1. "
                  "Default value: 0.5. At `level=0.5` the function calculates the median.", {"Float64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Approximate quantile of the specified level.", {"Float64"}};
    FunctionDocumentation::Examples examples = {
        {"Basic usage",
         R"(
SELECT quantileKLL(200, 0.5)(number + 1) FROM numbers(1000);
         )",
         R"(
┌─quantileKLL(200, 0.5)(plus(number, 1))─┐
│                                    500 │
└────────────────────────────────────────┘
         )"},
        {"Multiple quantile levels",
         R"(
SELECT quantilesKLL(200, 0.25, 0.5, 0.75)(number + 1) FROM numbers(1000);
         )",
         R"(
┌─quantilesKLL(200, 0.25, 0.5, 0.75)(plus(number, 1))─┐
│ [251,500,750]                                         │
└───────────────────────────────────────────────────────┘
         )"},
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction(NameQuantileKLL::name, {createAggregateFunctionQuantile<FuncQuantileKLL>, documentation});
    factory.registerFunction(NameQuantilesKLL::name, {createAggregateFunctionQuantile<FuncQuantilesKLL>, {}, properties});

    factory.registerAlias("medianKLL", NameQuantileKLL::name);
}

}
