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

template <template <typename> Data, typename Name, bool returns_float, bool returns_many>
AggregateFunctionPtr createAggregateFunctionQuantile(const std::string & name, const DataTypes & argument_types, const Array & params)
{
    assertUnary(name, argument_types);
    const DataTypePtr & argument_type = argument_types[0];

    if (params.size() != 1)
        throw Exception("Aggregate function " + name + " requires exactly one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    Float64 level = applyVisitor(FieldVisitorConvertToNumber<Float64>(), params[0]);

         if (typeid_cast<const DataTypeUInt8 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantile<UInt8>>(argument_type, level);
    else if (typeid_cast<const DataTypeUInt16 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantile<UInt16>>(argument_type, level);
    else if (typeid_cast<const DataTypeUInt32 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantile<UInt32>>(argument_type, level);
    else if (typeid_cast<const DataTypeUInt64 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantile<UInt64>>(argument_type, level);
    else if (typeid_cast<const DataTypeInt8 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantile<Int8>>(argument_type, level);
    else if (typeid_cast<const DataTypeInt16 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantile<Int16>>(argument_type, level);
    else if (typeid_cast<const DataTypeInt32 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantile<Int32>>(argument_type, level);
    else if (typeid_cast<const DataTypeInt64 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantile<Int64>>(argument_type, level);
    else if (typeid_cast<const DataTypeFloat32 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantile<Float32>>(argument_type, level);
    else if (typeid_cast<const DataTypeFloat64 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantile<Float64>>(argument_type, level);
    else if (typeid_cast<const DataTypeDate *>(argument_type.get()))
        return std::make_shared<AggregateFunctionQuantile<DataTypeDate::FieldType, false>>();
    else if (typeid_cast<const DataTypeDateTime *>(argument_type.get()))
        return std::make_shared<AggregateFunctionQuantile<DataTypeDateTime::FieldType, false>>();
    else
        throw Exception("Illegal type " + argument_type->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

template <typename Data, typename Name, bool returns_float, bool returns_many>
AggregateFunctionPtr createAggregateFunctionQuantileTwoArgs(const std::string & name, const DataTypes & argument_types, const Array & params)
{
    assertUnary(name, argument_types);
    const DataTypePtr & argument_type = argument_types[0];

    if (params.size() != 1)
        throw Exception("Aggregate function " + name + " requires exactly one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    Float64 level = applyVisitor(FieldVisitorConvertToNumber<Float64>(), params[0]);

         if (typeid_cast<const DataTypeUInt8 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantile<UInt8>>(argument_type, level);
    else if (typeid_cast<const DataTypeUInt16 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantile<UInt16>>(argument_type, level);
    else if (typeid_cast<const DataTypeUInt32 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantile<UInt32>>(argument_type, level);
    else if (typeid_cast<const DataTypeUInt64 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantile<UInt64>>(argument_type, level);
    else if (typeid_cast<const DataTypeInt8 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantile<Int8>>(argument_type, level);
    else if (typeid_cast<const DataTypeInt16 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantile<Int16>>(argument_type, level);
    else if (typeid_cast<const DataTypeInt32 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantile<Int32>>(argument_type, level);
    else if (typeid_cast<const DataTypeInt64 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantile<Int64>>(argument_type, level);
    else if (typeid_cast<const DataTypeFloat32 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantile<Float32>>(argument_type, level);
    else if (typeid_cast<const DataTypeFloat64 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantile<Float64>>(argument_type, level);
    else if (typeid_cast<const DataTypeDate *>(argument_type.get()))
        return std::make_shared<AggregateFunctionQuantile<DataTypeDate::FieldType, false>>();
    else if (typeid_cast<const DataTypeDateTime *>(argument_type.get()))
        return std::make_shared<AggregateFunctionQuantile<DataTypeDateTime::FieldType, false>>();
    else
        throw Exception("Illegal type " + argument_type->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}


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

    /// Aliases
}

}
