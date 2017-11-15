#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/AggregateFunctionQuantileExact.h>

namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionQuantileExact(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    if (argument_types.size() != 1)
        throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const IDataType & argument_type = *argument_types[0];

         if (typeid_cast<const DataTypeUInt8     *>(&argument_type))    return std::make_shared<AggregateFunctionQuantileExact<UInt8>>();
    else if (typeid_cast<const DataTypeUInt16     *>(&argument_type))    return std::make_shared<AggregateFunctionQuantileExact<UInt16>>();
    else if (typeid_cast<const DataTypeUInt32     *>(&argument_type))    return std::make_shared<AggregateFunctionQuantileExact<UInt32>>();
    else if (typeid_cast<const DataTypeUInt64     *>(&argument_type))    return std::make_shared<AggregateFunctionQuantileExact<UInt64>>();
    else if (typeid_cast<const DataTypeInt8     *>(&argument_type))    return std::make_shared<AggregateFunctionQuantileExact<Int8>>();
    else if (typeid_cast<const DataTypeInt16     *>(&argument_type))    return std::make_shared<AggregateFunctionQuantileExact<Int16>>();
    else if (typeid_cast<const DataTypeInt32     *>(&argument_type))    return std::make_shared<AggregateFunctionQuantileExact<Int32>>();
    else if (typeid_cast<const DataTypeInt64     *>(&argument_type))    return std::make_shared<AggregateFunctionQuantileExact<Int64>>();
    else if (typeid_cast<const DataTypeFloat32 *>(&argument_type))    return std::make_shared<AggregateFunctionQuantileExact<Float32>>();
    else if (typeid_cast<const DataTypeFloat64 *>(&argument_type))    return std::make_shared<AggregateFunctionQuantileExact<Float64>>();
    else if (typeid_cast<const DataTypeDate     *>(&argument_type))
        return std::make_shared<AggregateFunctionQuantileExact<DataTypeDate::FieldType>>();
    else if (typeid_cast<const DataTypeDateTime*>(&argument_type))
        return std::make_shared<AggregateFunctionQuantileExact<DataTypeDateTime::FieldType>>();
    else
        throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}


AggregateFunctionPtr createAggregateFunctionQuantilesExact(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    if (argument_types.size() != 1)
        throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const IDataType & argument_type = *argument_types[0];

         if (typeid_cast<const DataTypeUInt8     *>(&argument_type))    return std::make_shared<AggregateFunctionQuantilesExact<UInt8>>();
    else if (typeid_cast<const DataTypeUInt16     *>(&argument_type))    return std::make_shared<AggregateFunctionQuantilesExact<UInt16>>();
    else if (typeid_cast<const DataTypeUInt32     *>(&argument_type))    return std::make_shared<AggregateFunctionQuantilesExact<UInt32>>();
    else if (typeid_cast<const DataTypeUInt64     *>(&argument_type))    return std::make_shared<AggregateFunctionQuantilesExact<UInt64>>();
    else if (typeid_cast<const DataTypeInt8     *>(&argument_type))    return std::make_shared<AggregateFunctionQuantilesExact<Int8>>();
    else if (typeid_cast<const DataTypeInt16     *>(&argument_type))    return std::make_shared<AggregateFunctionQuantilesExact<Int16>>();
    else if (typeid_cast<const DataTypeInt32     *>(&argument_type))    return std::make_shared<AggregateFunctionQuantilesExact<Int32>>();
    else if (typeid_cast<const DataTypeInt64     *>(&argument_type))    return std::make_shared<AggregateFunctionQuantilesExact<Int64>>();
    else if (typeid_cast<const DataTypeFloat32 *>(&argument_type))    return std::make_shared<AggregateFunctionQuantilesExact<Float32>>();
    else if (typeid_cast<const DataTypeFloat64 *>(&argument_type))    return std::make_shared<AggregateFunctionQuantilesExact<Float64>>();
    else if (typeid_cast<const DataTypeDate     *>(&argument_type))
        return std::make_shared<AggregateFunctionQuantilesExact<DataTypeDate::FieldType>>();
    else if (typeid_cast<const DataTypeDateTime*>(&argument_type))
        return std::make_shared<AggregateFunctionQuantilesExact<DataTypeDateTime::FieldType>>();
    else
        throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

}

void registerAggregateFunctionsQuantileExact(AggregateFunctionFactory & factory)
{
    factory.registerFunction("quantileExact", createAggregateFunctionQuantileExact);
    factory.registerFunction("medianExact", createAggregateFunctionQuantileExact);
    factory.registerFunction("quantilesExact", createAggregateFunctionQuantilesExact);
}

}
