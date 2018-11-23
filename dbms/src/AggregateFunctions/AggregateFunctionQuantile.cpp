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

template <typename T> using FuncQuantile = AggregateFunctionQuantile<T, QuantileReservoirSampler<T>, NameQuantile, false, Float64, false>;
template <typename T> using FuncQuantiles = AggregateFunctionQuantile<T, QuantileReservoirSampler<T>, NameQuantiles, false, Float64, true>;

template <typename T> using FuncQuantileDeterministic = AggregateFunctionQuantile<T, QuantileReservoirSamplerDeterministic<T>, NameQuantileDeterministic, true, Float64, false>;
template <typename T> using FuncQuantilesDeterministic = AggregateFunctionQuantile<T, QuantileReservoirSamplerDeterministic<T>, NameQuantilesDeterministic, true, Float64, true>;

template <typename T> using FuncQuantileExact = AggregateFunctionQuantile<T, QuantileExact<T>, NameQuantileExact, false, void, false>;
template <typename T> using FuncQuantilesExact = AggregateFunctionQuantile<T, QuantileExact<T>, NameQuantilesExact, false, void, true>;

template <typename T> using FuncQuantileExactWeighted = AggregateFunctionQuantile<T, QuantileExactWeighted<T>, NameQuantileExactWeighted, true, void, false>;
template <typename T> using FuncQuantilesExactWeighted = AggregateFunctionQuantile<T, QuantileExactWeighted<T>, NameQuantilesExactWeighted, true, void, true>;

template <typename T> using FuncQuantileTiming = AggregateFunctionQuantile<T, QuantileTiming<T>, NameQuantileTiming, false, Float32, false>;
template <typename T> using FuncQuantilesTiming = AggregateFunctionQuantile<T, QuantileTiming<T>, NameQuantilesTiming, false, Float32, true>;

template <typename T> using FuncQuantileTimingWeighted = AggregateFunctionQuantile<T, QuantileTiming<T>, NameQuantileTimingWeighted, true, Float32, false>;
template <typename T> using FuncQuantilesTimingWeighted = AggregateFunctionQuantile<T, QuantileTiming<T>, NameQuantilesTimingWeighted, true, Float32, true>;

template <typename T> using FuncQuantileTDigest = AggregateFunctionQuantile<T, QuantileTDigest<T>, NameQuantileTDigest, false, Float32, false>;
template <typename T> using FuncQuantilesTDigest = AggregateFunctionQuantile<T, QuantileTDigest<T>, NameQuantilesTDigest, false, Float32, true>;

template <typename T> using FuncQuantileTDigestWeighted = AggregateFunctionQuantile<T, QuantileTDigest<T>, NameQuantileTDigestWeighted, true, Float32, false>;
template <typename T> using FuncQuantilesTDigestWeighted = AggregateFunctionQuantile<T, QuantileTDigest<T>, NameQuantilesTDigestWeighted, true, Float32, true>;


template <template <typename> class Function>
static constexpr bool SupportDecimal()
{
    return std::is_same_v<Function<Float32>, FuncQuantileExact<Float32>> ||
        std::is_same_v<Function<Float32>, FuncQuantilesExact<Float32>>;
}


template <template <typename> class Function>
AggregateFunctionPtr createAggregateFunctionQuantile(const std::string & name, const DataTypes & argument_types, const Array & params)
{
    /// Second argument type check doesn't depend on the type of the first one.
    Function<void>::assertSecondArg(argument_types);

    const DataTypePtr & argument_type = argument_types[0];
    WhichDataType which(argument_type);

#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return std::make_shared<Function<TYPE>>(argument_type, params);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
#undef FOR_NUMERIC_TYPES
    if (which.idx == TypeIndex::Date) return std::make_shared<Function<DataTypeDate::FieldType>>(argument_type, params);
    if (which.idx == TypeIndex::DateTime) return std::make_shared<Function<DataTypeDateTime::FieldType>>(argument_type, params);

    if constexpr (SupportDecimal<Function>())
    {
        if (which.idx == TypeIndex::Decimal32) return std::make_shared<Function<Decimal32>>(argument_type, params);
        if (which.idx == TypeIndex::Decimal64) return std::make_shared<Function<Decimal64>>(argument_type, params);
        if (which.idx == TypeIndex::Decimal128) return std::make_shared<Function<Decimal128>>(argument_type, params);
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
