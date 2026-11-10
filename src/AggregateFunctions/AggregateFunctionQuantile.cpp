#include <AggregateFunctions/AggregateFunctionQuantile.h>
#include <AggregateFunctions/ReservoirSampler.h>
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
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/** Quantile calculation with "reservoir sample" algorithm.
  * It collects pseudorandom subset of limited size from a stream of values,
  *  and approximate quantile from it.
  * The result is non-deterministic. Also look at QuantileReservoirSamplerDeterministic.
  *
  * This algorithm is quite inefficient in terms of precision for memory usage,
  *  but very efficient in CPU (though less efficient than QuantileTiming and than QuantileExact for small sets).
  */
template <typename Value>
struct QuantileReservoirSampler
{
    using Data = ReservoirSampler<Value, ReservoirSamplerOnEmpty::RETURN_NAN_OR_ZERO>;
    Data data;

    void add(const Value & x)
    {
        data.insert(x);
    }

    template <typename Weight>
    void add(const Value &, const Weight &)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method add with weight is not implemented for ReservoirSampler");
    }

    void merge(const QuantileReservoirSampler & rhs)
    {
        data.merge(rhs.data);
    }

    void serialize(WriteBuffer & buf) const
    {
        data.write(buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        data.read(buf);
    }

    /// Get the value of the `level` quantile. The level must be between 0 and 1.
    Value get(Float64 level)
    {
        if (data.empty())
            return {};

        if constexpr (is_decimal<Value>)
            return Value(static_cast<typename Value::NativeType>(data.quantileInterpolated(level)));
        else
            return static_cast<Value>(data.quantileInterpolated(level));
    }

    /// Get the `size` values of `levels` quantiles. Write `size` results starting with `result` address.
    /// indices - an array of index levels such that the corresponding elements will go in ascending order.
    void getMany(const Float64 * levels, const size_t * indices, size_t size, Value * result)
    {
        bool is_empty = data.empty();

        for (size_t i = 0; i < size; ++i)
        {
            if (is_empty)
            {
                result[i] = Value{};
            }
            else
            {
                if constexpr (is_decimal<Value>)
                    result[indices[i]] = Value(static_cast<typename Value::NativeType>(data.quantileInterpolated(levels[indices[i]])));
                else
                    result[indices[i]] = Value(data.quantileInterpolated(levels[indices[i]]));
            }
        }
    }

    /// The same, but in the case of an empty state, NaN is returned.
    Float64 getFloat(Float64 level)
    {
        return data.quantileInterpolated(level);
    }

    void getManyFloat(const Float64 * levels, const size_t * indices, size_t size, Float64 * result)
    {
        for (size_t i = 0; i < size; ++i)
            result[indices[i]] = data.quantileInterpolated(levels[indices[i]]);
    }
};


template <typename Value, bool float_return> using FuncQuantile = AggregateFunctionQuantile<Value, QuantileReservoirSampler<Value>, NameQuantile, void, std::conditional_t<float_return, Float64, void>, false, false>;
template <typename Value, bool float_return> using FuncQuantiles = AggregateFunctionQuantile<Value, QuantileReservoirSampler<Value>, NameQuantiles, void, std::conditional_t<float_return, Float64, void>, true, false>;

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

void registerAggregateFunctionsQuantile(AggregateFunctionFactory & factory)
{
    /// For aggregate functions returning array we cannot return NULL on empty set.
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true };

    FunctionDocumentation::Description description = R"(
Computes an approximate [`quantile`](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence.

This function applies [reservoir sampling](https://en.wikipedia.org/wiki/Reservoir_sampling) with a reservoir size up to 8192 and a random number generator for sampling.
The result is non-deterministic.
To get an exact quantile, use the [`quantileExact`](/sql-reference/aggregate-functions/reference/quantileexact#quantileExact) function.

When using multiple `quantile*` functions with different levels in a query, the internal states are not combined (that is, the query works less efficiently than it could).
In this case, use the [`quantiles`](/sql-reference/aggregate-functions/reference/quantiles#quantiles) function.

Note that for an empty numeric sequence, `quantile` will return NaN, but its `quantile*` variants will return either NaN or a default value for the sequence type, depending on the variant.
    )";
    FunctionDocumentation::Syntax syntax = R"(
quantile(level)(expr)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"expr", "Expression over the column values resulting in numeric data types, Date or DateTime.", {"(U)Int*", "Float*", "Decimal*", "Date", "DateTime"}}
    };
    FunctionDocumentation::Parameters parameters = {
        {"level", "Optional. Level of quantile. Constant floating-point number from 0 to 1. We recommend using a `level` value in the range of `[0.01, 0.99]`. Default value: 0.5. At `level=0.5` the function calculates median.", {"Float"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Approximate quantile of the specified level.", {"Float64", "Date", "DateTime"}};
    FunctionDocumentation::Examples examples = {
    {
        "Computing quantile",
        R"(
CREATE TABLE t (val UInt32) ENGINE = Memory;
INSERT INTO t VALUES (1), (1), (2), (3);

SELECT quantile(val) FROM t;
        )",
        R"(
┌─quantile(val)─┐
│           1.5 │
└───────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction(NameQuantile::name, {createAggregateFunctionQuantile<FuncQuantile>, documentation});

    FunctionDocumentation::Description description_quantiles = R"(
Computes multiple approximate [quantiles](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence at different levels simultaneously.

This function applies [reservoir sampling](https://en.wikipedia.org/wiki/Reservoir_sampling) with a reservoir size up to 8192 and a random number generator for sampling.
The result is non-deterministic.

Using `quantiles` is more efficient than calling multiple individual `quantile` functions when you need multiple quantile values, as all quantiles are calculated in a single pass through the data.
    )";
    FunctionDocumentation::Syntax syntax_quantiles = R"(
quantiles(level1, level2, ...)(expr)
    )";
    FunctionDocumentation::Arguments arguments_quantiles = {
        {"expr", "Expression over the column values resulting in numeric data types, Date or DateTime.", {"(U)Int*", "Float*", "Decimal*", "Date", "DateTime"}}
    };
    FunctionDocumentation::Parameters parameters_quantiles = {
        {"level", "Levels of quantiles. One or more constant floating-point numbers from 0 to 1. We recommend using `level` values in the range of `[0.01, 0.99]`.", {"Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_quantiles = {"Array of approximate quantiles of the specified levels in the same order as the levels were specified.", {"Array(Float64)", "Array(Date)", "Array(DateTime)"}};
    FunctionDocumentation::Examples examples_quantiles = {
    {
        "Computing multiple quantiles efficiently",
        R"(
CREATE TABLE t (val UInt32) ENGINE = Memory;
INSERT INTO t VALUES (1), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);

SELECT quantiles(0.25, 0.5, 0.75, 0.9)(val) FROM t;
        )",
        R"(
┌─quantiles(0.25, 0.5, 0.75, 0.9)(val)─┐
│ [3, 5.5, 8, 9.5]                     │
└──────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_quantiles = {1, 1};
    FunctionDocumentation::Category category_quantiles = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_quantiles = {description_quantiles, syntax_quantiles, arguments_quantiles, parameters_quantiles, returned_value_quantiles, examples_quantiles, introduced_in_quantiles, category_quantiles};

    factory.registerFunction(NameQuantiles::name, { createAggregateFunctionQuantile<FuncQuantiles>, documentation_quantiles, properties });

    /// 'median' is an alias for 'quantile'
    factory.registerAlias("median", NameQuantile::name);
}

}
