#include <AggregateFunctions/AggregateFunctionQuantile.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>

#include <Core/Field.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <typename Value, bool float_return> using FuncQuantile = AggregateFunctionQuantile<Value, QuantileReservoirSampler<Value>, NameQuantile, false, std::conditional_t<float_return, Float64, void>, false>;
template <typename Value, bool float_return> using FuncQuantiles = AggregateFunctionQuantile<Value, QuantileReservoirSampler<Value>, NameQuantiles, false, std::conditional_t<float_return, Float64, void>, true>;

template <typename Value, bool float_return> using FuncQuantileDeterministic = AggregateFunctionQuantile<Value, QuantileReservoirSamplerDeterministic<Value>, NameQuantileDeterministic, true, std::conditional_t<float_return, Float64, void>, false>;
template <typename Value, bool float_return> using FuncQuantilesDeterministic = AggregateFunctionQuantile<Value, QuantileReservoirSamplerDeterministic<Value>, NameQuantilesDeterministic, true, std::conditional_t<float_return, Float64, void>, true>;

template <typename Value, bool _> using FuncQuantileExact = AggregateFunctionQuantile<Value, QuantileExact<Value>, NameQuantileExact, false, void, false>;
template <typename Value, bool _> using FuncQuantilesExact = AggregateFunctionQuantile<Value, QuantileExact<Value>, NameQuantilesExact, false, void, true>;

template <typename Value, bool _> using FuncQuantileExactLow = AggregateFunctionQuantile<Value, QuantileExactLow<Value>, NameQuantileExactLow, false, void, false>;
template <typename Value, bool _> using FuncQuantilesExactLow = AggregateFunctionQuantile<Value, QuantileExactLow<Value>, NameQuantilesExactLow, false, void, true>;
template <typename Value, bool _> using FuncQuantileExactHigh = AggregateFunctionQuantile<Value, QuantileExactHigh<Value>, NameQuantileExactHigh, false, void, false>;
template <typename Value, bool _> using FuncQuantilesExactHigh = AggregateFunctionQuantile<Value, QuantileExactHigh<Value>, NameQuantilesExactHigh, false, void, true>;

template <typename Value, bool _> using FuncQuantileExactExclusive = AggregateFunctionQuantile<Value, QuantileExactExclusive<Value>, NameQuantileExactExclusive, false, Float64, false>;
template <typename Value, bool _> using FuncQuantilesExactExclusive = AggregateFunctionQuantile<Value, QuantileExactExclusive<Value>, NameQuantilesExactExclusive, false, Float64, true>;

template <typename Value, bool _> using FuncQuantileExactInclusive = AggregateFunctionQuantile<Value, QuantileExactInclusive<Value>, NameQuantileExactInclusive, false, Float64, false>;
template <typename Value, bool _> using FuncQuantilesExactInclusive = AggregateFunctionQuantile<Value, QuantileExactInclusive<Value>, NameQuantilesExactInclusive, false, Float64, true>;

template <typename Value, bool _> using FuncQuantileExactWeighted = AggregateFunctionQuantile<Value, QuantileExactWeighted<Value>, NameQuantileExactWeighted, true, void, false>;
template <typename Value, bool _> using FuncQuantilesExactWeighted = AggregateFunctionQuantile<Value, QuantileExactWeighted<Value>, NameQuantilesExactWeighted, true, void, true>;

template <typename Value, bool _> using FuncQuantileTiming = AggregateFunctionQuantile<Value, QuantileTiming<Value>, NameQuantileTiming, false, Float32, false>;
template <typename Value, bool _> using FuncQuantilesTiming = AggregateFunctionQuantile<Value, QuantileTiming<Value>, NameQuantilesTiming, false, Float32, true>;

template <typename Value, bool _> using FuncQuantileTimingWeighted = AggregateFunctionQuantile<Value, QuantileTiming<Value>, NameQuantileTimingWeighted, true, Float32, false>;
template <typename Value, bool _> using FuncQuantilesTimingWeighted = AggregateFunctionQuantile<Value, QuantileTiming<Value>, NameQuantilesTimingWeighted, true, Float32, true>;

template <typename Value, bool float_return> using FuncQuantileTDigest = AggregateFunctionQuantile<Value, QuantileTDigest<Value>, NameQuantileTDigest, false, std::conditional_t<float_return, Float32, void>, false>;
template <typename Value, bool float_return> using FuncQuantilesTDigest = AggregateFunctionQuantile<Value, QuantileTDigest<Value>, NameQuantilesTDigest, false, std::conditional_t<float_return, Float32, void>, true>;

template <typename Value, bool float_return> using FuncQuantileTDigestWeighted = AggregateFunctionQuantile<Value, QuantileTDigest<Value>, NameQuantileTDigestWeighted, true, std::conditional_t<float_return, Float32, void>, false>;
template <typename Value, bool float_return> using FuncQuantilesTDigestWeighted = AggregateFunctionQuantile<Value, QuantileTDigest<Value>, NameQuantilesTDigestWeighted, true, std::conditional_t<float_return, Float32, void>, true>;

template <typename Value, bool float_return> using FuncQuantileBFloat16 = AggregateFunctionQuantile<Value, QuantileBFloat16Histogram<Value>, NameQuantileBFloat16, false, std::conditional_t<float_return, Float64, void>, false>;
template <typename Value, bool float_return> using FuncQuantilesBFloat16 = AggregateFunctionQuantile<Value, QuantileBFloat16Histogram<Value>, NameQuantilesBFloat16, false, std::conditional_t<float_return, Float64, void>, true>;

template <typename Value, bool float_return> using FuncQuantileBFloat16Weighted = AggregateFunctionQuantile<Value, QuantileBFloat16Histogram<Value>, NameQuantileBFloat16Weighted, true, std::conditional_t<float_return, Float64, void>, false>;
template <typename Value, bool float_return> using FuncQuantilesBFloat16Weighted = AggregateFunctionQuantile<Value, QuantileBFloat16Histogram<Value>, NameQuantilesBFloat16Weighted, true, std::conditional_t<float_return, Float64, void>, true>;

template <template <typename, bool> class Function>
constexpr bool supportDecimal()
{
    return std::is_same_v<Function<Float32, false>, FuncQuantile<Float32, false>> ||
        std::is_same_v<Function<Float32, false>, FuncQuantiles<Float32, false>> ||
        std::is_same_v<Function<Float32, false>, FuncQuantileExact<Float32, false>> ||
        std::is_same_v<Function<Float32, false>, FuncQuantileExactLow<Float32, false>> ||
        std::is_same_v<Function<Float32, false>, FuncQuantileExactHigh<Float32, false>> ||
        std::is_same_v<Function<Float32, false>, FuncQuantilesExact<Float32, false>> ||
        std::is_same_v<Function<Float32, false>, FuncQuantilesExactLow<Float32, false>> ||
        std::is_same_v<Function<Float32, false>, FuncQuantilesExactHigh<Float32, false>> ||
        std::is_same_v<Function<Float32, false>, FuncQuantileExactWeighted<Float32, false>> ||
        std::is_same_v<Function<Float32, false>, FuncQuantilesExactWeighted<Float32, false>>;
}

template <template <typename, bool> class Function>
constexpr bool supportBigInt()
{
    return std::is_same_v<Function<Float32, false>, FuncQuantile<Float32, false>> ||
        std::is_same_v<Function<Float32, false>, FuncQuantiles<Float32, false>> ||
        std::is_same_v<Function<Float32, false>, FuncQuantileExact<Float32, false>> ||
        std::is_same_v<Function<Float32, false>, FuncQuantilesExact<Float32, false>> ||
        std::is_same_v<Function<Float32, false>, FuncQuantileExactWeighted<Float32, false>> ||
        std::is_same_v<Function<Float32, false>, FuncQuantilesExactWeighted<Float32, false>>;
}

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

    if constexpr (supportDecimal<Function>())
    {
        if (which.idx == TypeIndex::Decimal32) return std::make_shared<Function<Decimal32, false>>(argument_types, params);
        if (which.idx == TypeIndex::Decimal64) return std::make_shared<Function<Decimal64, false>>(argument_types, params);
        if (which.idx == TypeIndex::Decimal128) return std::make_shared<Function<Decimal128, false>>(argument_types, params);
        if (which.idx == TypeIndex::Decimal256) return std::make_shared<Function<Decimal256, false>>(argument_types, params);
        if (which.idx == TypeIndex::DateTime64) return std::make_shared<Function<DateTime64, false>>(argument_types, params);
    }

    if constexpr (supportBigInt<Function>())
    {
        if (which.idx == TypeIndex::Int128) return std::make_shared<Function<Int128, true>>(argument_types, params);
        if (which.idx == TypeIndex::UInt128) return std::make_shared<Function<Int128, true>>(argument_types, params);
        if (which.idx == TypeIndex::Int256) return std::make_shared<Function<Int256, true>>(argument_types, params);
        if (which.idx == TypeIndex::UInt256) return std::make_shared<Function<UInt256, true>>(argument_types, params);
    }

    throw Exception("Illegal type " + argument_type->getName() + " of argument for aggregate function " + name,
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

}

void registerAggregateFunctionsQuantile(AggregateFunctionFactory & factory)
{
    /// For aggregate functions returning array we cannot return NULL on empty set.
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true };

    factory.registerFunction(NameQuantile::name, createAggregateFunctionQuantile<FuncQuantile>);
    factory.registerFunction(NameQuantiles::name, { createAggregateFunctionQuantile<FuncQuantiles>, properties });

    factory.registerFunction(NameQuantileDeterministic::name, createAggregateFunctionQuantile<FuncQuantileDeterministic>);
    factory.registerFunction(NameQuantilesDeterministic::name, { createAggregateFunctionQuantile<FuncQuantilesDeterministic>, properties });

    factory.registerFunction(NameQuantileExact::name, createAggregateFunctionQuantile<FuncQuantileExact>);
    factory.registerFunction(NameQuantilesExact::name, { createAggregateFunctionQuantile<FuncQuantilesExact>, properties });

    factory.registerFunction(NameQuantileExactLow::name, createAggregateFunctionQuantile<FuncQuantileExactLow>);
    factory.registerFunction(NameQuantilesExactLow::name, { createAggregateFunctionQuantile<FuncQuantilesExactLow>, properties });

    factory.registerFunction(NameQuantileExactHigh::name, createAggregateFunctionQuantile<FuncQuantileExactHigh>);
    factory.registerFunction(NameQuantilesExactHigh::name, { createAggregateFunctionQuantile<FuncQuantilesExactHigh>, properties });

    factory.registerFunction(NameQuantileExactExclusive::name, createAggregateFunctionQuantile<FuncQuantileExactExclusive>);
    factory.registerFunction(NameQuantilesExactExclusive::name, { createAggregateFunctionQuantile<FuncQuantilesExactExclusive>, properties });

    factory.registerFunction(NameQuantileExactInclusive::name, createAggregateFunctionQuantile<FuncQuantileExactInclusive>);
    factory.registerFunction(NameQuantilesExactInclusive::name, { createAggregateFunctionQuantile<FuncQuantilesExactInclusive>, properties });

    factory.registerFunction(NameQuantileExactWeighted::name, createAggregateFunctionQuantile<FuncQuantileExactWeighted>);
    factory.registerFunction(NameQuantilesExactWeighted::name, { createAggregateFunctionQuantile<FuncQuantilesExactWeighted>, properties });

    factory.registerFunction(NameQuantileTiming::name, createAggregateFunctionQuantile<FuncQuantileTiming>);
    factory.registerFunction(NameQuantilesTiming::name, { createAggregateFunctionQuantile<FuncQuantilesTiming>, properties });

    factory.registerFunction(NameQuantileTimingWeighted::name, createAggregateFunctionQuantile<FuncQuantileTimingWeighted>);
    factory.registerFunction(NameQuantilesTimingWeighted::name, { createAggregateFunctionQuantile<FuncQuantilesTimingWeighted>, properties });

    factory.registerFunction(NameQuantileTDigest::name, createAggregateFunctionQuantile<FuncQuantileTDigest>);
    factory.registerFunction(NameQuantilesTDigest::name, { createAggregateFunctionQuantile<FuncQuantilesTDigest>, properties });

    factory.registerFunction(NameQuantileTDigestWeighted::name, createAggregateFunctionQuantile<FuncQuantileTDigestWeighted>);
    factory.registerFunction(NameQuantilesTDigestWeighted::name, { createAggregateFunctionQuantile<FuncQuantilesTDigestWeighted>, properties });

    factory.registerFunction(NameQuantileBFloat16::name, createAggregateFunctionQuantile<FuncQuantileBFloat16>);
    factory.registerFunction(NameQuantilesBFloat16::name, { createAggregateFunctionQuantile<FuncQuantilesBFloat16>, properties });

    factory.registerFunction(NameQuantileBFloat16Weighted::name, createAggregateFunctionQuantile<FuncQuantileBFloat16Weighted>);
    factory.registerFunction(NameQuantilesBFloat16Weighted::name, createAggregateFunctionQuantile<FuncQuantilesBFloat16Weighted>);

    /// 'median' is an alias for 'quantile'
    factory.registerAlias("median", NameQuantile::name);
    factory.registerAlias("medianDeterministic", NameQuantileDeterministic::name);
    factory.registerAlias("medianExact", NameQuantileExact::name);
    factory.registerAlias("medianExactLow", NameQuantileExactLow::name);
    factory.registerAlias("medianExactHigh", NameQuantileExactHigh::name);
    factory.registerAlias("medianExactWeighted", NameQuantileExactWeighted::name);
    factory.registerAlias("medianTiming", NameQuantileTiming::name);
    factory.registerAlias("medianTimingWeighted", NameQuantileTimingWeighted::name);
    factory.registerAlias("medianTDigest", NameQuantileTDigest::name);
    factory.registerAlias("medianTDigestWeighted", NameQuantileTDigestWeighted::name);
    factory.registerAlias("medianBFloat16", NameQuantileBFloat16::name);
    factory.registerAlias("medianBFloat16Weighted", NameQuantileBFloat16Weighted::name);
}

}
