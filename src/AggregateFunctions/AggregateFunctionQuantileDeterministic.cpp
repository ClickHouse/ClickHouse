#include <AggregateFunctions/AggregateFunctionQuantile.h>
#include <AggregateFunctions/ReservoirSamplerDeterministic.h>
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
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/** Quantile calculation with "reservoir sample" algorithm.
  * It collects pseudorandom subset of limited size from a stream of values,
  *  and approximate quantile from it.
  * The function accept second argument, named "determinator"
  *  and a hash function from it is calculated and used as a source for randomness
  *  to apply random sampling.
  * The function is deterministic, but care should be taken with choose of "determinator" argument.
  */
template <typename Value>
struct QuantileReservoirSamplerDeterministic
{
    using Data = ReservoirSamplerDeterministic<Value, ReservoirSamplerDeterministicOnEmpty::RETURN_NAN_OR_ZERO>;
    Data data;

    void add(const Value &)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method add without determinator is not implemented for ReservoirSamplerDeterministic");
    }

    template <typename Determinator>
    void add(const Value & x, const Determinator & determinator)
    {
        data.insert(x, determinator);
    }

    void merge(const QuantileReservoirSamplerDeterministic & rhs)
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
            return static_cast<typename Value::NativeType>(data.quantileInterpolated(level));
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
                    result[indices[i]] = static_cast<typename Value::NativeType>(data.quantileInterpolated(levels[indices[i]]));
                else
                    result[indices[i]] = static_cast<Value>(data.quantileInterpolated(levels[indices[i]]));
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


template <typename Value, bool float_return> using FuncQuantileDeterministic = AggregateFunctionQuantile<Value, QuantileReservoirSamplerDeterministic<Value>, NameQuantileDeterministic, UInt64, std::conditional_t<float_return, Float64, void>, false, false>;
template <typename Value, bool float_return> using FuncQuantilesDeterministic = AggregateFunctionQuantile<Value, QuantileReservoirSamplerDeterministic<Value>, NameQuantilesDeterministic, UInt64, std::conditional_t<float_return, Float64, void>, true, false>;

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

    if (which.idx == TypeIndex::Int128) return std::make_shared<Function<Int128, true>>(argument_types, params);
    if (which.idx == TypeIndex::UInt128) return std::make_shared<Function<UInt128, true>>(argument_types, params);
    if (which.idx == TypeIndex::Int256) return std::make_shared<Function<Int256, true>>(argument_types, params);
    if (which.idx == TypeIndex::UInt256) return std::make_shared<Function<UInt256, true>>(argument_types, params);

    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}",
                    argument_type->getName(), name);
}

}

void registerAggregateFunctionsQuantileDeterministic(AggregateFunctionFactory & factory)
{
    /// For aggregate functions returning array we cannot return NULL on empty set.
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true };

    FunctionDocumentation::Description description = R"(
Computes an approximate [quantile](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence.

This function applies [reservoir sampling](https://en.wikipedia.org/wiki/Reservoir_sampling) with a reservoir size up to 8192 and deterministic algorithm of sampling.
The result is deterministic.
To get an exact quantile, use the [`quantileExact`](/sql-reference/aggregate-functions/reference/quantileexact#quantileExact) function.

When using multiple `quantile*` functions with different levels in a query, the internal states are not combined (that is, the query works less efficiently than it could).
In this case, use the [`quantiles`](/sql-reference/aggregate-functions/reference/quantiles#quantiles) function.
    )";
    FunctionDocumentation::Syntax syntax = R"(
quantileDeterministic(level)(expr, determinator)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"expr", "Expression over the column values resulting in numeric data types, Date or DateTime.", {"(U)Int*", "Float*", "Decimal*", "Date", "DateTime"}},
        {"determinator", "Number whose hash is used instead of a random number generator in the reservoir sampling algorithm to make the result of sampling deterministic. As a determinator you can use any deterministic positive number, for example, a user id or an event id. If the same determinator value occurs too often, the function works incorrectly.", {"(U)Int*"}}
    };
    FunctionDocumentation::Parameters parameters = {
        {"level", "Optional. Level of quantile. Constant floating-point number from 0 to 1. We recommend using a `level` value in the range of `[0.01, 0.99]`. Default value: 0.5. At `level=0.5` the function calculates median.", {"Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an approximate quantile of the specified level.", {"Float64", "Date", "DateTime"}};
    FunctionDocumentation::Examples examples = {
    {
        "Computing deterministic quantile",
        R"(
CREATE TABLE t (val UInt32) ENGINE = Memory;
INSERT INTO t VALUES (1), (1), (2), (3);

SELECT quantileDeterministic(val, 1) FROM t;
        )",
        R"(
┌─quantileDeterministic(val, 1)─┐
│                           1.5 │
└───────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction(NameQuantileDeterministic::name, {createAggregateFunctionQuantile<FuncQuantileDeterministic>, documentation});
    factory.registerFunction(NameQuantilesDeterministic::name, { createAggregateFunctionQuantile<FuncQuantilesDeterministic>, {}, properties });

    /// 'median' is an alias for 'quantile'
    factory.registerAlias("medianDeterministic", NameQuantileDeterministic::name);
}

}
