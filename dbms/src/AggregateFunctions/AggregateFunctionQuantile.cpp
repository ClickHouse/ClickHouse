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


template <template <typename> class Data, typename Name, bool have_second_arg, bool returns_float, bool returns_many>
AggregateFunctionPtr createAggregateFunctionQuantile(const std::string & name, const DataTypes & argument_types, const Array & params)
{
    assertUnary(name, argument_types);
    const DataTypePtr & argument_type = argument_types[0];

#define CREATE(TYPE) \
    if (typeid_cast<const DataType ## TYPE *>(argument_type.get())) \
        return std::make_shared<AggregateFunctionQuantile<TYPE, Data<TYPE>, Name, have_second_arg, returns_float, returns_many>>(argument_type, params);

    FOR_NUMERIC_TYPES(CREATE)
#undef CREATE

    if (typeid_cast<const DataTypeDate *>(argument_type.get()))
        return std::make_shared<AggregateFunctionQuantile<
            DataTypeDate::FieldType, Data<DataTypeDate::FieldType>, Name, have_second_arg, false, returns_many>>(argument_type, params);
    if (typeid_cast<const DataTypeDateTime *>(argument_type.get()))
        return std::make_shared<AggregateFunctionQuantile<
            DataTypeDateTime::FieldType, Data<DataTypeDateTime::FieldType>, Name, have_second_arg, false, returns_many>>(argument_type, params);

    throw Exception("Illegal type " + argument_type->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
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
    factory.registerFunction(NameQuantile::name,
        createAggregateFunctionQuantile<QuantileReservoirSampler, NameQuantile, false, true, false>);
    factory.registerFunction(NameQuantiles::name,
        createAggregateFunctionQuantile<QuantileReservoirSampler, NameQuantiles, false, true, true>);

    factory.registerFunction(NameQuantileDeterministic::name,
        createAggregateFunctionQuantile<QuantileReservoirSamplerDeterministic, NameQuantileDeterministic, true, true, false>);
    factory.registerFunction(NameQuantilesDeterministic::name,
        createAggregateFunctionQuantile<QuantileReservoirSamplerDeterministic, NameQuantilesDeterministic, true, true, true>);

    factory.registerFunction(NameQuantileExact::name,
        createAggregateFunctionQuantile<QuantileExact, NameQuantileExact, false, false, false>);
    factory.registerFunction(NameQuantilesExact::name,
        createAggregateFunctionQuantile<QuantileExact, NameQuantilesExact, false, false, true>);

    factory.registerFunction(NameQuantileExactWeighted::name,
        createAggregateFunctionQuantile<QuantileExactWeighted, NameQuantileExactWeighted, true, false, false>);
    factory.registerFunction(NameQuantilesExactWeighted::name,
        createAggregateFunctionQuantile<QuantileExactWeighted, NameQuantilesExactWeighted, true, false, true>);

    factory.registerFunction(NameQuantileTiming::name,
        createAggregateFunctionQuantile<QuantileTiming, NameQuantileTiming, false, false, false>);
    factory.registerFunction(NameQuantilesTiming::name,
        createAggregateFunctionQuantile<QuantileTiming, NameQuantilesTiming, false, false, true>);

    factory.registerFunction(NameQuantileTimingWeighted::name,
        createAggregateFunctionQuantile<QuantileTiming, NameQuantileTimingWeighted, true, false, false>);
    factory.registerFunction(NameQuantilesTimingWeighted::name,
        createAggregateFunctionQuantile<QuantileTiming, NameQuantilesTimingWeighted, true, false, true>);

    factory.registerFunction(NameQuantileTDigest::name,
        createAggregateFunctionQuantile<QuantileTDigest, NameQuantileTDigest, false, true, false>);
    factory.registerFunction(NameQuantilesTDigest::name,
        createAggregateFunctionQuantile<QuantileTDigest, NameQuantilesTDigest, false, true, true>);

    factory.registerFunction(NameQuantileTDigestWeighted::name,
        createAggregateFunctionQuantile<QuantileTDigest, NameQuantileTDigestWeighted, true, true, false>);
    factory.registerFunction(NameQuantilesTDigestWeighted::name,
        createAggregateFunctionQuantile<QuantileTDigest, NameQuantilesTDigestWeighted, true, true, true>);

    /// 'median' is an alias for 'quantile'

    factory.registerFunction("median",
        createAggregateFunctionQuantile<QuantileReservoirSampler, NameQuantile, false, true, false>);

    factory.registerFunction("medianDeterministic",
        createAggregateFunctionQuantile<QuantileReservoirSamplerDeterministic, NameQuantileDeterministic, true, true, false>);

    factory.registerFunction("medianExact",
        createAggregateFunctionQuantile<QuantileExact, NameQuantileExact, false, false, false>);

    factory.registerFunction("medianExactWeighted",
        createAggregateFunctionQuantile<QuantileExactWeighted, NameQuantileExactWeighted, true, false, false>);

    factory.registerFunction("medianTiming",
        createAggregateFunctionQuantile<QuantileTiming, NameQuantileTiming, false, false, false>);

    factory.registerFunction("medianTimingWeighted",
        createAggregateFunctionQuantile<QuantileTiming, NameQuantileTimingWeighted, true, false, false>);

    factory.registerFunction("medianTDigest",
        createAggregateFunctionQuantile<QuantileTDigest, NameQuantileTDigest, false, true, false>);

    factory.registerFunction("medianTDigestWeighted",
        createAggregateFunctionQuantile<QuantileTDigest, NameQuantileTDigestWeighted, true, true, false>);
}

}
