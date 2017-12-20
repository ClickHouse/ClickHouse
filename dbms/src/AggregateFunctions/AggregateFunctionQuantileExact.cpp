#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/AggregateFunctionQuantileExact.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionQuantileExact(const std::string & name, const DataTypes & argument_types, const Array & params)
{
    assertUnary(name, argument_types);
    const DataTypePtr & argument_type = argument_types[0];

    if (params.size() != 1)
        throw Exception("Aggregate function " + name + " requires exactly one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    Float64 level = applyVisitor(FieldVisitorConvertToNumber<Float64>(), params[0]);

         if (typeid_cast<const DataTypeUInt8 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantileExact<UInt8>>(argument_type, level);
    else if (typeid_cast<const DataTypeUInt16 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantileExact<UInt16>>(argument_type, level);
    else if (typeid_cast<const DataTypeUInt32 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantileExact<UInt32>>(argument_type, level);
    else if (typeid_cast<const DataTypeUInt64 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantileExact<UInt64>>(argument_type, level);
    else if (typeid_cast<const DataTypeInt8 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantileExact<Int8>>(argument_type, level);
    else if (typeid_cast<const DataTypeInt16 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantileExact<Int16>>(argument_type, level);
    else if (typeid_cast<const DataTypeInt32 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantileExact<Int32>>(argument_type, level);
    else if (typeid_cast<const DataTypeInt64 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantileExact<Int64>>(argument_type, level);
    else if (typeid_cast<const DataTypeFloat32 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantileExact<Float32>>(argument_type, level);
    else if (typeid_cast<const DataTypeFloat64 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantileExact<Float64>>(argument_type, level);
    else if (typeid_cast<const DataTypeDate *>(argument_type.get()))
        return std::make_shared<AggregateFunctionQuantileExact<DataTypeDate::FieldType, false>>();
    else if (typeid_cast<const DataTypeDateTime *>(argument_type.get()))
        return std::make_shared<AggregateFunctionQuantileExact<DataTypeDateTime::FieldType, false>>();
    else
        throw Exception("Illegal type " + argument_type->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}


AggregateFunctionPtr createAggregateFunctionQuantilesExact(const std::string & name, const DataTypes & argument_types, const Array & params)
{
    assertUnary(name, argument_types);
    const DataTypePtr & argument_type = argument_types[0];

         if (typeid_cast<const DataTypeUInt8 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantilesExact<UInt8>>(argument_type, params);
    else if (typeid_cast<const DataTypeUInt16 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantilesExact<UInt16>>(argument_type, params);
    else if (typeid_cast<const DataTypeUInt32 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantilesExact<UInt32>>(argument_type, params);
    else if (typeid_cast<const DataTypeUInt64 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantilesExact<UInt64>>(argument_type, params);
    else if (typeid_cast<const DataTypeInt8 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantilesExact<Int8>>(argument_type, params);
    else if (typeid_cast<const DataTypeInt16 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantilesExact<Int16>>(argument_type, params);
    else if (typeid_cast<const DataTypeInt32 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantilesExact<Int32>>(argument_type, params);
    else if (typeid_cast<const DataTypeInt64 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantilesExact<Int64>>(argument_type, params);
    else if (typeid_cast<const DataTypeFloat32 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantilesExact<Float32>>(argument_type, params);
    else if (typeid_cast<const DataTypeFloat64 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantilesExact<Float64>>(argument_type, params);
    else if (typeid_cast<const DataTypeDate *>(argument_type.get()))
        return std::make_shared<AggregateFunctionQuantilesExact<DataTypeDate::FieldType, false>>(argument_type, params);
    else if (typeid_cast<const DataTypeDateTime *>(argument_type.get()))
        return std::make_shared<AggregateFunctionQuantilesExact<DataTypeDateTime::FieldType, false>>(argument_type, params);
    else
        throw Exception("Illegal type " + argument_type->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

}

void registerAggregateFunctionsQuantileExact(AggregateFunctionFactory & factory)
{
    factory.registerFunction("quantileExact", createAggregateFunctionQuantileExact);
    factory.registerFunction("medianExact", createAggregateFunctionQuantileExact);
    factory.registerFunction("quantilesExact", createAggregateFunctionQuantilesExact);
}

}
