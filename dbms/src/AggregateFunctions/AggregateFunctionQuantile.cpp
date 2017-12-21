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

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

template <template <typename> class Data, typename Name, bool have_second_arg, typename FloatReturnType, bool returns_many>
AggregateFunctionPtr createAggregateFunctionQuantile(const std::string & name, const DataTypes & argument_types, const Array & params)
{
    if (have_second_arg)
        assertBinary(name, argument_types);
    else
        assertUnary(name, argument_types);

    const DataTypePtr & argument_type = argument_types[0];

#define CREATE(TYPE) \
    if (typeid_cast<const DataType ## TYPE *>(argument_type.get())) \
        return std::make_shared<AggregateFunctionQuantile<TYPE, Data<TYPE>, Name, have_second_arg, FloatReturnType, returns_many>>(argument_type, params);

    FOR_NUMERIC_TYPES(CREATE)
#undef CREATE

    if (typeid_cast<const DataTypeDate *>(argument_type.get()))
        return std::make_shared<AggregateFunctionQuantile<
            DataTypeDate::FieldType, Data<DataTypeDate::FieldType>, Name, have_second_arg, void, returns_many>>(argument_type, params);
    if (typeid_cast<const DataTypeDateTime *>(argument_type.get()))
        return std::make_shared<AggregateFunctionQuantile<
            DataTypeDateTime::FieldType, Data<DataTypeDateTime::FieldType>, Name, have_second_arg, void, returns_many>>(argument_type, params);

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
        createAggregateFunctionQuantile<QuantileReservoirSampler, NameQuantile, false, Float64, false>);
    factory.registerFunction(NameQuantiles::name,
        createAggregateFunctionQuantile<QuantileReservoirSampler, NameQuantiles, false, Float64, true>);

    factory.registerFunction(NameQuantileDeterministic::name,
        createAggregateFunctionQuantile<QuantileReservoirSamplerDeterministic, NameQuantileDeterministic, true, Float64, false>);
    factory.registerFunction(NameQuantilesDeterministic::name,
        createAggregateFunctionQuantile<QuantileReservoirSamplerDeterministic, NameQuantilesDeterministic, true, Float64, true>);

    factory.registerFunction(NameQuantileExact::name,
        createAggregateFunctionQuantile<QuantileExact, NameQuantileExact, false, void, false>);
    factory.registerFunction(NameQuantilesExact::name,
        createAggregateFunctionQuantile<QuantileExact, NameQuantilesExact, false, void, true>);

    factory.registerFunction(NameQuantileExactWeighted::name,
        createAggregateFunctionQuantile<QuantileExactWeighted, NameQuantileExactWeighted, true, void, false>);
    factory.registerFunction(NameQuantilesExactWeighted::name,
        createAggregateFunctionQuantile<QuantileExactWeighted, NameQuantilesExactWeighted, true, void, true>);

    factory.registerFunction(NameQuantileTiming::name,
        createAggregateFunctionQuantile<QuantileTiming, NameQuantileTiming, false, Float32, false>);
    factory.registerFunction(NameQuantilesTiming::name,
        createAggregateFunctionQuantile<QuantileTiming, NameQuantilesTiming, false, Float32, true>);

    factory.registerFunction(NameQuantileTimingWeighted::name,
        createAggregateFunctionQuantile<QuantileTiming, NameQuantileTimingWeighted, true, Float32, false>);
    factory.registerFunction(NameQuantilesTimingWeighted::name,
        createAggregateFunctionQuantile<QuantileTiming, NameQuantilesTimingWeighted, true, Float32, true>);

    factory.registerFunction(NameQuantileTDigest::name,
        createAggregateFunctionQuantile<QuantileTDigest, NameQuantileTDigest, false, Float32, false>);
    factory.registerFunction(NameQuantilesTDigest::name,
        createAggregateFunctionQuantile<QuantileTDigest, NameQuantilesTDigest, false, Float32, true>);

    factory.registerFunction(NameQuantileTDigestWeighted::name,
        createAggregateFunctionQuantile<QuantileTDigest, NameQuantileTDigestWeighted, true, Float32, false>);
    factory.registerFunction(NameQuantilesTDigestWeighted::name,
        createAggregateFunctionQuantile<QuantileTDigest, NameQuantilesTDigestWeighted, true, Float32, true>);

    /// 'median' is an alias for 'quantile'

    factory.registerFunction("median",
        createAggregateFunctionQuantile<QuantileReservoirSampler, NameQuantile, false, Float64, false>);

    factory.registerFunction("medianDeterministic",
        createAggregateFunctionQuantile<QuantileReservoirSamplerDeterministic, NameQuantileDeterministic, true, Float64, false>);

    factory.registerFunction("medianExact",
        createAggregateFunctionQuantile<QuantileExact, NameQuantileExact, false, void, false>);

    factory.registerFunction("medianExactWeighted",
        createAggregateFunctionQuantile<QuantileExactWeighted, NameQuantileExactWeighted, true, void, false>);

    factory.registerFunction("medianTiming",
        createAggregateFunctionQuantile<QuantileTiming, NameQuantileTiming, false, Float32, false>);

    factory.registerFunction("medianTimingWeighted",
        createAggregateFunctionQuantile<QuantileTiming, NameQuantileTimingWeighted, true, Float32, false>);

    factory.registerFunction("medianTDigest",
        createAggregateFunctionQuantile<QuantileTDigest, NameQuantileTDigest, false, Float32, false>);

    factory.registerFunction("medianTDigestWeighted",
        createAggregateFunctionQuantile<QuantileTDigest, NameQuantileTDigestWeighted, true, Float32, false>);
}

}
