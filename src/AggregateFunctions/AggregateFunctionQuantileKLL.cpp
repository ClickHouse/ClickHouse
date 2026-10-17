#include <AggregateFunctions/AggregateFunctionQuantile.h>
#include <AggregateFunctions/QuantileKLL.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Core/Field.h>

#include <kll_sketch.hpp>


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
            "Aggregate function {} requires at least one parameter: k (integer in [{}, {}])",
            name, datasketches::kll_constants::MIN_K, datasketches::kll_constants::MAX_K);

    const auto & k_field = params[0];
    if (!isInt64OrUInt64FieldType(k_field.getType()))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "First parameter (k) of aggregate function {} must be an integer", name);

    /// Range-check k as a full-width integer BEFORE narrowing to uint16_t — narrowing first
    /// would let values like 65536 wrap to 0 and reach DataSketches as `std::invalid_argument`.
    Int64 k_value;
    if (k_field.getType() == Field::Types::Int64)
    {
        k_value = k_field.safeGet<Int64>();
    }
    else
    {
        UInt64 k_unsigned = k_field.safeGet<UInt64>();
        if (k_unsigned > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Parameter k of aggregate function {} must be in [{}, {}] (got {})",
                name, datasketches::kll_constants::MIN_K, datasketches::kll_constants::MAX_K, k_unsigned);
        k_value = static_cast<Int64>(k_unsigned);
    }

    if (k_value < datasketches::kll_constants::MIN_K || k_value > datasketches::kll_constants::MAX_K)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Parameter k of aggregate function {} must be in [{}, {}] (got {})",
            name, datasketches::kll_constants::MIN_K, datasketches::kll_constants::MAX_K, k_value);
    /// `k_value` is now safely within `[MIN_K, MAX_K]` (which fits in uint16_t). The aggregate
    /// function reads `k` from `params` itself, so we do not need to pass it separately.

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

KLL provides a rank-error guarantee: for a single-sided estimate the rank error is approximately
`2.296 / pow(k, 0.9723)` — about 1.33% at `k=200` and about 0.20% at `k=1600`.
The sketch uses `O(k * log(n/k))` memory, which is provably optimal for rank-error sketches
of this type.

When estimates are produced from merged partial states (for example after `OPTIMIZE FINAL` or
across multiple shards), small values of `k` produce visibly unstable quantiles. Internal
benchmarks have only validated `k >= 1600` as merge-safe, so prefer `k >= 1600` whenever
the state is merged from many sources. `k=200` is convenient for single-pass estimation but
is not recommended for merge-heavy workloads.

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
        {"k", "Sketch size parameter controlling accuracy. Must be in `[8, 65535]`. Larger values give more accurate results. "
              "Rank error is approximately `2.296 / pow(k, 0.9723)` (≈1.33% at `k=200`, ≈0.20% at `k=1600`). "
              "Use `k >= 1600` for merge-heavy workloads.", {"UInt*"}},
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

    FunctionDocumentation::Description description_quantiles = R"(
Computes multiple approximate [quantiles](https://en.wikipedia.org/wiki/Quantile) at different
levels simultaneously using the [KLL sketch](https://datasketches.apache.org/docs/KLL/KLLSketch.html)
algorithm from the Apache DataSketches library.

This function is equivalent to [`quantileKLL`](#quantilekll) but computes multiple levels in a
single pass over a shared sketch state, which is more efficient than calling `quantileKLL`
multiple times.

KLL provides a rank-error guarantee: for a single-sided estimate the rank error is approximately
`2.296 / pow(k, 0.9723)` — about 1.33% at `k=200` and about 0.20% at `k=1600`.
The sketch uses `O(k * log(n/k))` memory, which is provably optimal for rank-error sketches
of this type.

When estimates are produced from merged partial states (for example after `OPTIMIZE FINAL` or
across multiple shards), small values of `k` produce visibly unstable quantiles. Internal
benchmarks have only validated `k >= 1600` as merge-safe, so prefer `k >= 1600` whenever
the state is merged from many sources.

The serialized state uses the DataSketches KLL wire format, enabling cross-language
interoperability with Java and Python DataSketches clients.
    )";
    FunctionDocumentation::Syntax syntax_quantiles = R"(
quantilesKLL(k, level1[, level2, ...])(expr)
    )";
    FunctionDocumentation::Arguments arguments_quantiles = {
        {"expr", "Column with numeric data.", {"(U)Int*", "Float*"}}
    };
    FunctionDocumentation::Parameters parameters_quantiles = {
        {"k", "Sketch size parameter controlling accuracy. Must be in `[8, 65535]`. Larger values give more accurate results. "
              "Rank error is approximately `2.296 / pow(k, 0.9723)` (≈1.33% at `k=200`, ≈0.20% at `k=1600`). "
              "Use `k >= 1600` for merge-heavy workloads.", {"UInt*"}},
        {"level1, level2, ...", "Levels of quantiles. One or more constant floating-point numbers from 0 to 1.", {"Float64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_quantiles = {"Array of approximate quantiles of the specified levels in the same order as the levels were specified.", {"Array(Float64)"}};
    FunctionDocumentation::Examples examples_quantiles = {
        {"Computing multiple quantiles with KLL",
         R"(
SELECT quantilesKLL(200, 0.25, 0.5, 0.75)(number + 1) FROM numbers(1000);
         )",
         R"(
┌─quantilesKLL(200, 0.25, 0.5, 0.75)(plus(number, 1))─┐
│ [251,500,750]                                         │
└───────────────────────────────────────────────────────┘
         )"},
    };
    FunctionDocumentation::IntroducedIn introduced_in_quantiles = {25, 10};
    FunctionDocumentation::Category category_quantiles = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_quantiles = {description_quantiles, syntax_quantiles, arguments_quantiles, parameters_quantiles, returned_value_quantiles, examples_quantiles, introduced_in_quantiles, category_quantiles};

    factory.registerFunction(NameQuantilesKLL::name, {createAggregateFunctionQuantile<FuncQuantilesKLL>, documentation_quantiles, properties});

    factory.registerAlias("medianKLL", NameQuantileKLL::name);
}

}
