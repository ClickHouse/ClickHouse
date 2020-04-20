#include <AggregateFunctions/AggregateFunctionQuantile.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>

#include <Core/Field.h>
#include "registerAggregateFunctions.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <typename Value, bool float_return> using FuncQuantile = AggregateFunctionQuantile<QuantileReservoirSampler<Value, float_return>, NameQuantile, false>;
template <typename Value, bool float_return> using FuncQuantiles = AggregateFunctionQuantile<QuantileReservoirSampler<Value, float_return>, NameQuantiles, true>;

template <typename Value, bool float_return> using FuncQuantileDeterministic = AggregateFunctionQuantile<QuantileReservoirSamplerDeterministic<Value, float_return>, NameQuantileDeterministic, false>;
template <typename Value, bool float_return> using FuncQuantilesDeterministic = AggregateFunctionQuantile<QuantileReservoirSamplerDeterministic<Value, float_return>, NameQuantilesDeterministic, true>;

template <typename Value, bool _> using FuncQuantileExact = AggregateFunctionQuantile<QuantileExact<Value>, NameQuantileExact, false>;
template <typename Value, bool _> using FuncQuantilesExact = AggregateFunctionQuantile<QuantileExact<Value>, NameQuantilesExact, true>;

template <typename Value, bool _> using FuncQuantileExactExclusive = AggregateFunctionQuantile<QuantileExactExclusive<Value>, NameQuantileExactExclusive, false>;
template <typename Value, bool _> using FuncQuantilesExactExclusive = AggregateFunctionQuantile<QuantileExactExclusive<Value>, NameQuantilesExactExclusive, true>;

template <typename Value, bool _> using FuncQuantileExactInclusive = AggregateFunctionQuantile<QuantileExactInclusive<Value>, NameQuantileExactInclusive, false>;
template <typename Value, bool _> using FuncQuantilesExactInclusive = AggregateFunctionQuantile<QuantileExactInclusive<Value>, NameQuantilesExactInclusive, true>;

template <typename Value, bool _> using FuncQuantileExactWeighted = AggregateFunctionQuantile<QuantileExactWeighted<Value>, NameQuantileExactWeighted, false>;
template <typename Value, bool _> using FuncQuantilesExactWeighted = AggregateFunctionQuantile<QuantileExactWeighted<Value>, NameQuantilesExactWeighted, true>;

template <typename Value, bool _> using FuncQuantileTiming = AggregateFunctionQuantile<QuantileTiming<Value, false>, NameQuantileTiming, false>;
template <typename Value, bool _> using FuncQuantilesTiming = AggregateFunctionQuantile<QuantileTiming<Value, false>, NameQuantilesTiming, true>;

template <typename Value, bool _> using FuncQuantileTimingWeighted = AggregateFunctionQuantile<QuantileTiming<Value, true>, NameQuantileTimingWeighted, false>;
template <typename Value, bool _> using FuncQuantilesTimingWeighted = AggregateFunctionQuantile<QuantileTiming<Value, true>, NameQuantilesTimingWeighted, true>;

template <typename Value, bool float_return> using FuncQuantileTDigest = AggregateFunctionQuantile<QuantileTDigest<Value, float_return, false>, NameQuantileTDigest, false>;
template <typename Value, bool float_return> using FuncQuantilesTDigest = AggregateFunctionQuantile<QuantileTDigest<Value, float_return, false>, NameQuantilesTDigest, true>;

template <typename Value, bool float_return> using FuncQuantileTDigestWeighted = AggregateFunctionQuantile<QuantileTDigest<Value, float_return, true>, NameQuantileTDigestWeighted, false>;
template <typename Value, bool float_return> using FuncQuantilesTDigestWeighted = AggregateFunctionQuantile<QuantileTDigest<Value, float_return, true>, NameQuantilesTDigestWeighted, true>;


template <template <typename, bool> class Function>
static constexpr bool supportDecimal()
{
    return std::is_same_v<Function<Float32, false>, FuncQuantile<Float32, false>> ||
        std::is_same_v<Function<Float32, false>, FuncQuantiles<Float32, false>> ||
        std::is_same_v<Function<Float32, false>, FuncQuantileExact<Float32, false>> ||
        std::is_same_v<Function<Float32, false>, FuncQuantilesExact<Float32, false>> ||
        std::is_same_v<Function<Float32, false>, FuncQuantileExactWeighted<Float32, false>> ||
        std::is_same_v<Function<Float32, false>, FuncQuantilesExactWeighted<Float32, false>>;
}


template <template <typename, bool> class Function>
AggregateFunctionPtr createAggregateFunctionQuantile(const std::string & name, const DataTypes & argument_types, const Array & params)
{
    /// Second argument type check doesn't depend on the type of the first one.
    Function<Float32, true>::assertSecondArg(argument_types);

    const DataTypePtr & argument_type = argument_types[0];
    WhichDataType which(argument_type);

#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return std::make_shared<Function<TYPE, true>>(argument_type, params);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Date) return std::make_shared<Function<DataTypeDate::FieldType, false>>(argument_type, params);
    if (which.idx == TypeIndex::DateTime) return std::make_shared<Function<DataTypeDateTime::FieldType, false>>(argument_type, params);

    if constexpr (supportDecimal<Function>())
    {
        if (which.idx == TypeIndex::Decimal32) return std::make_shared<Function<Decimal32, false>>(argument_type, params);
        if (which.idx == TypeIndex::Decimal64) return std::make_shared<Function<Decimal64, false>>(argument_type, params);
        if (which.idx == TypeIndex::Decimal128) return std::make_shared<Function<Decimal128, false>>(argument_type, params);
    }

    throw Exception("Illegal type " + argument_type->getName() + " of argument for aggregate function " + name,
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

}

void registerAggregateFunctionsQuantile(AggregateFunctionFactory & factory)
{
    factory.registerFunction(NameQuantile::name, createAggregateFunctionQuantile<FuncQuantile>);
    factory.registerFunction(NameQuantiles::name, createAggregateFunctionQuantile<FuncQuantiles>);

    factory.registerFunction(NameQuantileDeterministic::name, createAggregateFunctionQuantile<FuncQuantileDeterministic>);
    factory.registerFunction(NameQuantilesDeterministic::name, createAggregateFunctionQuantile<FuncQuantilesDeterministic>);

    factory.registerFunction(NameQuantileExact::name, createAggregateFunctionQuantile<FuncQuantileExact>);
    factory.registerFunction(NameQuantilesExact::name, createAggregateFunctionQuantile<FuncQuantilesExact>);

    factory.registerFunction(NameQuantileExactExclusive::name, createAggregateFunctionQuantile<FuncQuantileExactExclusive>);
    factory.registerFunction(NameQuantilesExactExclusive::name, createAggregateFunctionQuantile<FuncQuantilesExactExclusive>);

    factory.registerFunction(NameQuantileExactInclusive::name, createAggregateFunctionQuantile<FuncQuantileExactInclusive>);
    factory.registerFunction(NameQuantilesExactInclusive::name, createAggregateFunctionQuantile<FuncQuantilesExactInclusive>);

    factory.registerFunction(NameQuantileExactWeighted::name, createAggregateFunctionQuantile<FuncQuantileExactWeighted>);
    factory.registerFunction(NameQuantilesExactWeighted::name, createAggregateFunctionQuantile<FuncQuantilesExactWeighted>);

    factory.registerFunction(NameQuantileTiming::name, createAggregateFunctionQuantile<FuncQuantileTiming>);
    factory.registerFunction(NameQuantilesTiming::name, createAggregateFunctionQuantile<FuncQuantilesTiming>);

    factory.registerFunction(NameQuantileTimingWeighted::name, createAggregateFunctionQuantile<FuncQuantileTimingWeighted>);
    factory.registerFunction(NameQuantilesTimingWeighted::name, createAggregateFunctionQuantile<FuncQuantilesTimingWeighted>);

    factory.registerFunction(NameQuantileTDigest::name, createAggregateFunctionQuantile<FuncQuantileTDigest>);
    factory.registerFunction(NameQuantilesTDigest::name, createAggregateFunctionQuantile<FuncQuantilesTDigest>);

    factory.registerFunction(NameQuantileTDigestWeighted::name, createAggregateFunctionQuantile<FuncQuantileTDigestWeighted>);
    factory.registerFunction(NameQuantilesTDigestWeighted::name, createAggregateFunctionQuantile<FuncQuantilesTDigestWeighted>);

    /// 'median' is an alias for 'quantile'
    factory.registerAlias("median", NameQuantile::name);
    factory.registerAlias("medianDeterministic", NameQuantileDeterministic::name);
    factory.registerAlias("medianExact", NameQuantileExact::name);
    factory.registerAlias("medianExactWeighted", NameQuantileExactWeighted::name);
    factory.registerAlias("medianTiming", NameQuantileTiming::name);
    factory.registerAlias("medianTimingWeighted", NameQuantileTimingWeighted::name);
    factory.registerAlias("medianTDigest", NameQuantileTDigest::name);
    factory.registerAlias("medianTDigestWeighted", NameQuantileTDigestWeighted::name);
}

}
