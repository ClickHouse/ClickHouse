#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/AggregateFunctionQuantileTDigest.h>

namespace DB
{

namespace
{

template <template <typename, bool> class FunctionTemplate>
AggregateFunctionPtr createAggregateFunctionQuantileTDigest(const std::string & name, const DataTypes & argument_types)
{
    if (argument_types.size() != 1)
        throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const IDataType & argument_type = *argument_types[0];

         if (typeid_cast<const DataTypeUInt8     *>(&argument_type))    return std::make_shared<FunctionTemplate<UInt8, true>>();
    else if (typeid_cast<const DataTypeUInt16     *>(&argument_type))    return std::make_shared<FunctionTemplate<UInt16, true>>();
    else if (typeid_cast<const DataTypeUInt32     *>(&argument_type))    return std::make_shared<FunctionTemplate<UInt32, true>>();
    else if (typeid_cast<const DataTypeUInt64     *>(&argument_type))    return std::make_shared<FunctionTemplate<UInt64, true>>();
    else if (typeid_cast<const DataTypeInt8     *>(&argument_type))    return std::make_shared<FunctionTemplate<Int8, true>>();
    else if (typeid_cast<const DataTypeInt16     *>(&argument_type))    return std::make_shared<FunctionTemplate<Int16, true>>();
    else if (typeid_cast<const DataTypeInt32     *>(&argument_type))    return std::make_shared<FunctionTemplate<Int32, true>>();
    else if (typeid_cast<const DataTypeInt64     *>(&argument_type))    return std::make_shared<FunctionTemplate<Int64, true>>();
    else if (typeid_cast<const DataTypeFloat32 *>(&argument_type))    return std::make_shared<FunctionTemplate<Float32, true>>();
    else if (typeid_cast<const DataTypeFloat64 *>(&argument_type))    return std::make_shared<FunctionTemplate<Float64, true>>();
    else if (typeid_cast<const DataTypeDate     *>(&argument_type)) return std::make_shared<FunctionTemplate<DataTypeDate::FieldType, false>>();
    else if (typeid_cast<const DataTypeDateTime*>(&argument_type)) return std::make_shared<FunctionTemplate<DataTypeDateTime::FieldType, false>>();
    else
        throw Exception("Illegal type " + argument_type.getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

template <template <typename, typename, bool> class FunctionTemplate, typename T, bool returns_float>
AggregateFunctionPtr createAggregateFunctionQuantileTDigestWeightedImpl(const std::string & name, const DataTypes & argument_types)
{
    const IDataType & argument_type = *argument_types[1];

         if (typeid_cast<const DataTypeUInt8     *>(&argument_type))    return std::make_shared<FunctionTemplate<T, UInt8, returns_float>>();
    else if (typeid_cast<const DataTypeUInt16     *>(&argument_type))    return std::make_shared<FunctionTemplate<T, UInt16, returns_float>>();
    else if (typeid_cast<const DataTypeUInt32     *>(&argument_type))    return std::make_shared<FunctionTemplate<T, UInt32, returns_float>>();
    else if (typeid_cast<const DataTypeUInt64     *>(&argument_type))    return std::make_shared<FunctionTemplate<T, UInt64, returns_float>>();
    else if (typeid_cast<const DataTypeInt8     *>(&argument_type))    return std::make_shared<FunctionTemplate<T, Int8, returns_float>>();
    else if (typeid_cast<const DataTypeInt16     *>(&argument_type))    return std::make_shared<FunctionTemplate<T, Int16, returns_float>>();
    else if (typeid_cast<const DataTypeInt32     *>(&argument_type))    return std::make_shared<FunctionTemplate<T, Int32, returns_float>>();
    else if (typeid_cast<const DataTypeInt64     *>(&argument_type))    return std::make_shared<FunctionTemplate<T, Int64, returns_float>>();
    else if (typeid_cast<const DataTypeFloat32 *>(&argument_type))    return std::make_shared<FunctionTemplate<T, Float32, returns_float>>();
    else if (typeid_cast<const DataTypeFloat64 *>(&argument_type))    return std::make_shared<FunctionTemplate<T, Float64, returns_float>>();
    else
        throw Exception("Illegal type " + argument_type.getName() + " of second argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

template <template <typename, typename, bool> class FunctionTemplate>
AggregateFunctionPtr createAggregateFunctionQuantileTDigestWeighted(const std::string & name, const DataTypes & argument_types)
{
    if (argument_types.size() != 2)
        throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const IDataType & argument_type = *argument_types[0];

         if (typeid_cast<const DataTypeUInt8     *>(&argument_type))
        return createAggregateFunctionQuantileTDigestWeightedImpl<FunctionTemplate, UInt8, true>(name, argument_types);
    else if (typeid_cast<const DataTypeUInt16     *>(&argument_type))
        return createAggregateFunctionQuantileTDigestWeightedImpl<FunctionTemplate, UInt16, true>(name, argument_types);
    else if (typeid_cast<const DataTypeUInt32     *>(&argument_type))
        return createAggregateFunctionQuantileTDigestWeightedImpl<FunctionTemplate, UInt32, true>(name, argument_types);
    else if (typeid_cast<const DataTypeUInt64     *>(&argument_type))
        return createAggregateFunctionQuantileTDigestWeightedImpl<FunctionTemplate, UInt64, true>(name, argument_types);
    else if (typeid_cast<const DataTypeInt8     *>(&argument_type))
        return createAggregateFunctionQuantileTDigestWeightedImpl<FunctionTemplate, Int8, true>(name, argument_types);
    else if (typeid_cast<const DataTypeInt16     *>(&argument_type))
        return createAggregateFunctionQuantileTDigestWeightedImpl<FunctionTemplate, Int16, true>(name, argument_types);
    else if (typeid_cast<const DataTypeInt32     *>(&argument_type))
        return createAggregateFunctionQuantileTDigestWeightedImpl<FunctionTemplate, Int32, true>(name, argument_types);
    else if (typeid_cast<const DataTypeInt64     *>(&argument_type))
        return createAggregateFunctionQuantileTDigestWeightedImpl<FunctionTemplate, Int64, true>(name, argument_types);
    else if (typeid_cast<const DataTypeFloat32 *>(&argument_type))
        return createAggregateFunctionQuantileTDigestWeightedImpl<FunctionTemplate, Float32, true>(name, argument_types);
    else if (typeid_cast<const DataTypeFloat64 *>(&argument_type))
        return createAggregateFunctionQuantileTDigestWeightedImpl<FunctionTemplate, Float64, true>(name, argument_types);
    else if (typeid_cast<const DataTypeDate     *>(&argument_type))
        return createAggregateFunctionQuantileTDigestWeightedImpl<FunctionTemplate, DataTypeDate::FieldType, false>(name, argument_types);
    else if (typeid_cast<const DataTypeDateTime*>(&argument_type))
        return createAggregateFunctionQuantileTDigestWeightedImpl<FunctionTemplate, DataTypeDateTime::FieldType, false>(name, argument_types);
    else
        throw Exception("Illegal type " + argument_type.getName() + " of first argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

}

void registerAggregateFunctionsQuantileTDigest(AggregateFunctionFactory & factory)
{
    factory.registerFunction("quantileTDigest",
        createAggregateFunctionQuantileTDigest<AggregateFunctionQuantileTDigest>);
    factory.registerFunction("medianTDigest",
        createAggregateFunctionQuantileTDigest<AggregateFunctionQuantileTDigest>);
    factory.registerFunction("quantilesTDigest",
        createAggregateFunctionQuantileTDigest<AggregateFunctionQuantilesTDigest>);
    factory.registerFunction("quantileTDigestWeighted",
        createAggregateFunctionQuantileTDigestWeighted<AggregateFunctionQuantileTDigestWeighted>);
    factory.registerFunction("medianTDigestWeighted",
        createAggregateFunctionQuantileTDigestWeighted<AggregateFunctionQuantileTDigestWeighted>);
    factory.registerFunction("quantilesTDigestWeighted",
        createAggregateFunctionQuantileTDigestWeighted<AggregateFunctionQuantilesTDigestWeighted>);
}

}
