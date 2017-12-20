#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>

#include <AggregateFunctions/AggregateFunctionQuantile.h>

#include <AggregateFunctions/QuantileReservoirSampler.h>
#include <AggregateFunctions/QuantileReservoirSamplerDeterministic.h>
#include <AggregateFunctions/QuantileExact.h>
#include <AggregateFunctions/QuantileExactWeighted.h>
#include <AggregateFunctions/QuantileTiming.h>
#include <AggregateFunctions/QuantileTDigest.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

#define FOR_NUMERIC_TYPES(M) \
    M(UInt8) \
    M(UInt16) \
    M(UInt32) \
    M(UInt64) \
    M(Int8) \
    M(Int16) \
    M(Int32) \
    M(Int64) \
    M(Float32) \
    M(Float64)


template <template <typename> class Data, typename Name, bool returns_float, bool returns_many>
AggregateFunctionPtr createAggregateFunctionQuantile(const std::string & name, const DataTypes & argument_types, const Array & params)
{
    assertUnary(name, argument_types);
    const DataTypePtr & argument_type = argument_types[0];

#define CREATE(TYPE) \
    if (typeid_cast<const DataType ## TYPE *>(argument_type.get())) \
        return std::make_shared<AggregateFunctionQuantile<TYPE, void, Data<TYPE>, Name, returns_float, returns_many>>(argument_type, params);

    FOR_NUMERIC_TYPES(CREATE)
#undef CREATE

    if (typeid_cast<const DataTypeDate *>(argument_type.get()))
        return std::make_shared<AggregateFunctionQuantile<DataTypeDate::FieldType, void, Data<DataTypeDate::FieldType>, Name, false, returns_many>>(argument_type, params);
    if (typeid_cast<const DataTypeDateTime *>(argument_type.get()))
        return std::make_shared<AggregateFunctionQuantile<DataTypeDateTime::FieldType, void, Data<DataTypeDateTime::FieldType>, Name, false, returns_many>>(argument_type, params);

    throw Exception("Illegal type " + argument_type->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}


template <typename FirstArg, template <typename> class Data, typename Name, bool returns_float, bool returns_many>
AggregateFunctionPtr createAggregateFunctionQuantileTwoArgsForSecondArg(const std::string & name, const DataTypes & argument_types, const Array & params)
{
    const DataTypePtr & second_argument_type = argument_types[0];

#define CREATE(TYPE) \
    if (typeid_cast<const DataType ## TYPE *>(second_argument_type.get())) \
        return std::make_shared<AggregateFunctionQuantile<FirstArg, TYPE, Data<FirstArg>, Name, returns_float, returns_many>>(argument_types[0], params);

    FOR_NUMERIC_TYPES(CREATE)
#undef CREATE

    throw Exception("Illegal type " + second_argument_type->getName() + " of second argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

template <template <typename> class Data, typename Name, bool returns_float, bool returns_many>
AggregateFunctionPtr createAggregateFunctionQuantileTwoArgs(const std::string & name, const DataTypes & argument_types, const Array & params)
{
    assertBinary(name, argument_types);
    const DataTypePtr & argument_type = argument_types[0];

#define CREATE(TYPE) \
    if (typeid_cast<const DataType ## TYPE *>(argument_type.get())) \
        return createAggregateFunctionQuantileTwoArgsForSecondArg<TYPE, Data<TYPE>, Name, returns_float, returns_many>(name, argument_types, params);

    FOR_NUMERIC_TYPES(CREATE)
#undef CREATE

    if (typeid_cast<const DataTypeDate *>(argument_type.get()))
        return createAggregateFunctionQuantileTwoArgsForSecondArg<
            DataTypeDate::FieldType, Data<DataTypeDate::FieldType>, Name, false, returns_many>(name, argument_types, params);
    if (typeid_cast<const DataTypeDateTime *>(argument_type.get()))
        return createAggregateFunctionQuantileTwoArgsForSecondArg<
            DataTypeDateTime::FieldType, Data<DataTypeDateTime::FieldType>, Name, false, returns_many>(name, argument_types, params);

    throw Exception("Illegal type " + argument_type->getName() + " of first argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

#undef FOR_NUMERIC_TYPES


struct NameQuantile { static constexpr auto name = "quantile"; };
struct NameQuantiles { static constexpr auto name = "quantiles"; };
struct NameQuantileDeterministic { static constexpr auto name = "quantileDeterministic"; };
struct NameQuantilesDeterministic { static constexpr auto name = "quantilesDeterministic"; };

struct NameQuantileExact { static constexpr auto name = "quantileExact"; };
struct NameQuantileExactWeighted { static constexpr auto name = "quantileExactWeighted"; };
struct NameQuantilesExact { static constexpr auto name = "quantilesExact"; };
struct NameQuantilesExactWeighted { static constexpr auto name = "quantilesExactWeighted"; };

struct NameQuantileTiming { static constexpr auto name = "quantileTiming"; };
struct NameQuantileTimingWeighted { static constexpr auto name = "quantileTimingWeighted"; };
struct NameQuantilesTiming { static constexpr auto name = "quantilesTiming"; };
struct NameQuantilesTimingWeighted { static constexpr auto name = "quantilesTimingWeighted"; };

struct NameQuantileTDigest { static constexpr auto name = "quantileTDigest"; };
struct NameQuantileTDigestWeighted { static constexpr auto name = "quantileTDigestWeighted"; };
struct NameQuantilesTDigest { static constexpr auto name = "quantilesTDigest"; };
struct NameQuantilesTDigestWeighted { static constexpr auto name = "quantilesTDigestWeighted"; };

}

void registerAggregateFunctionsQuantile(AggregateFunctionFactory & factory)
{
    factory.registerFunction("quantile", createAggregateFunctionQuantile<QuantileReservoirSampler, NameQuantile, true, false>);
    factory.registerFunction("quantiles", createAggregateFunctionQuantile<QuantileReservoirSampler, NameQuantiles, true, true>);

    factory.registerFunction("quantileDeterministic",
        createAggregateFunctionQuantileTwoArgs<QuantileReservoirSamplerDeterministic, NameQuantileDeterministic, true, false>);
    factory.registerFunction("quantilesDeterministic",
        createAggregateFunctionQuantileTwoArgs<QuantileReservoirSamplerDeterministic, NameQuantilesDeterministic, true, true>);

    factory.registerFunction("quantileExact", createAggregateFunctionQuantile<QuantileExact, NameQuantileExact, false, false>);
    factory.registerFunction("quantilesExact", createAggregateFunctionQuantile<QuantileExact, NameQuantilesExact, false, true>);

    factory.registerFunction("quantileExactWeighted", createAggregateFunctionQuantileTwoArgs<QuantileExactWeighted, NameQuantileExactWeighted, false, false>);
    factory.registerFunction("quantilesExactWeighted", createAggregateFunctionQuantileTwoArgs<QuantileExactWeighted, NameQuantilesExactWeighted, false, true>);

    factory.registerFunction("quantileTiming", createAggregateFunctionQuantile<QuantileTiming, NameQuantileTiming, false, false>);
    factory.registerFunction("quantilesTiming", createAggregateFunctionQuantile<QuantileTiming, NameQuantilesTiming, false, true>);

    factory.registerFunction("quantileTimingWeighted", createAggregateFunctionQuantileTwoArgs<QuantileTiming, NameQuantileTimingWeighted, false, false>);
    factory.registerFunction("quantilesTimingWeighted", createAggregateFunctionQuantileTwoArgs<QuantileTiming, NameQuantilesTimingWeighted, false, true>);

    factory.registerFunction("quantileTDigest", createAggregateFunctionQuantile<QuantileTDigest, NameQuantileTDigest, true, false>);
    factory.registerFunction("quantilesTDigest", createAggregateFunctionQuantile<QuantileTDigest, NameQuantilesTDigest, true, true>);

    factory.registerFunction("quantileTDigestWeighted", createAggregateFunctionQuantileTwoArgs<QuantileTDigest, NameQuantileTDigestWeighted, true, false>);
    factory.registerFunction("quantilesTDigestWeighted", createAggregateFunctionQuantileTwoArgs<QuantileTDigest, NameQuantilesTDigestWeighted, true, true>);

    /// TODO Aliases
}

}
