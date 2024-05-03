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


template <typename Value, bool float_return> using FuncQuantileDeterministic = AggregateFunctionQuantile<Value, QuantileReservoirSamplerDeterministic<Value>, NameQuantileDeterministic, true, std::conditional_t<float_return, Float64, void>, false, false>;
template <typename Value, bool float_return> using FuncQuantilesDeterministic = AggregateFunctionQuantile<Value, QuantileReservoirSamplerDeterministic<Value>, NameQuantilesDeterministic, true, std::conditional_t<float_return, Float64, void>, true, false>;

template <template <typename, bool> class Function>
AggregateFunctionPtr createAggregateFunctionQuantile(
    const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    /// Second argument type check doesn't depend on the type of the first one.
    Function<void, true>::assertSecondArg(argument_types);

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

    factory.registerFunction(NameQuantileDeterministic::name, createAggregateFunctionQuantile<FuncQuantileDeterministic>);
    factory.registerFunction(NameQuantilesDeterministic::name, { createAggregateFunctionQuantile<FuncQuantilesDeterministic>, properties });

    /// 'median' is an alias for 'quantile'
    factory.registerAlias("medianDeterministic", NameQuantileDeterministic::name);
}

}
