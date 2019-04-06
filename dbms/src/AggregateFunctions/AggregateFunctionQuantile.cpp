#include <AggregateFunctions/AggregateFunctionQuantile.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

template <typename Value, bool FloatReturn> using FuncQuantile = AggregateFunctionQuantile<Value, QuantileReservoirSampler<Value>, NameQuantile, false, std::conditional_t<FloatReturn, Float64, void>, false>;
template <typename Value, bool FloatReturn> using FuncQuantiles = AggregateFunctionQuantile<Value, QuantileReservoirSampler<Value>, NameQuantiles, false, std::conditional_t<FloatReturn, Float64, void>, true>;

template <typename Value, bool FloatReturn> using FuncQuantileDeterministic = AggregateFunctionQuantile<Value, QuantileReservoirSamplerDeterministic<Value>, NameQuantileDeterministic, true, std::conditional_t<FloatReturn, Float64, void>, false>;
template <typename Value, bool FloatReturn> using FuncQuantilesDeterministic = AggregateFunctionQuantile<Value, QuantileReservoirSamplerDeterministic<Value>, NameQuantilesDeterministic, true, std::conditional_t<FloatReturn, Float64, void>, true>;

template <typename Value, bool _> using FuncQuantileExact = AggregateFunctionQuantile<Value, QuantileExact<Value>, NameQuantileExact, false, void, false>;
template <typename Value, bool _> using FuncQuantilesExact = AggregateFunctionQuantile<Value, QuantileExact<Value>, NameQuantilesExact, false, void, true>;

template <typename Value, bool _> using FuncQuantileExactWeighted = AggregateFunctionQuantile<Value, QuantileExactWeighted<Value>, NameQuantileExactWeighted, true, void, false>;
template <typename Value, bool _> using FuncQuantilesExactWeighted = AggregateFunctionQuantile<Value, QuantileExactWeighted<Value>, NameQuantilesExactWeighted, true, void, true>;

template <typename Value, bool _> using FuncQuantileTiming = AggregateFunctionQuantile<Value, QuantileTiming<Value>, NameQuantileTiming, false, Float32, false>;
template <typename Value, bool _> using FuncQuantilesTiming = AggregateFunctionQuantile<Value, QuantileTiming<Value>, NameQuantilesTiming, false, Float32, true>;

template <typename Value, bool _> using FuncQuantileTimingWeighted = AggregateFunctionQuantile<Value, QuantileTiming<Value>, NameQuantileTimingWeighted, true, Float32, false>;
template <typename Value, bool _> using FuncQuantilesTimingWeighted = AggregateFunctionQuantile<Value, QuantileTiming<Value>, NameQuantilesTimingWeighted, true, Float32, true>;

template <typename Value, bool FloatReturn> using FuncQuantileTDigest = AggregateFunctionQuantile<Value, QuantileTDigest<Value>, NameQuantileTDigest, false, std::conditional_t<FloatReturn, Float32, void>, false>;
template <typename Value, bool FloatReturn> using FuncQuantilesTDigest = AggregateFunctionQuantile<Value, QuantileTDigest<Value>, NameQuantilesTDigest, false, std::conditional_t<FloatReturn, Float32, void>, true>;

template <typename Value, bool FloatReturn> using FuncQuantileTDigestWeighted = AggregateFunctionQuantile<Value, QuantileTDigest<Value>, NameQuantileTDigestWeighted, true, std::conditional_t<FloatReturn, Float32, void>, false>;
template <typename Value, bool FloatReturn> using FuncQuantilesTDigestWeighted = AggregateFunctionQuantile<Value, QuantileTDigest<Value>, NameQuantilesTDigestWeighted, true, std::conditional_t<FloatReturn, Float32, void>, true>;


template <template <typename, bool> class Function>
static constexpr bool supportDecimal()
{
    return std::is_same_v<Function<Float32, false>, FuncQuantileExact<Float32, false>> ||
        std::is_same_v<Function<Float32, false>, FuncQuantilesExact<Float32, false>>;
}


template <template <typename, bool> class Function>
AggregateFunctionPtr createAggregateFunctionQuantile(const std::string & name, const DataTypes & argument_types, const Array & params)
{
    /// Second argument type check doesn't depend on the type of the first one.
    Function<void, true>::assertSecondArg(argument_types);

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
        if (which.idx == TypeIndex::Decimal32) return std::make_shared<Function<Decimal32, true>>(argument_type, params);
        if (which.idx == TypeIndex::Decimal64) return std::make_shared<Function<Decimal64, true>>(argument_type, params);
        if (which.idx == TypeIndex::Decimal128) return std::make_shared<Function<Decimal128, true>>(argument_type, params);
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
