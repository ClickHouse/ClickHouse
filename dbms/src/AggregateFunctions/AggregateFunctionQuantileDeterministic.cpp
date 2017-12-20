#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/AggregateFunctionQuantileDeterministic.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

AggregateFunctionPtr createAggregateFunctionQuantileDeterministic(const std::string & name, const DataTypes & argument_types, const Array & params)
{
    assertBinary(name, argument_types);

    const auto determinator_type = argument_types[1].get();
    if (!typeid_cast<const DataTypeInt32 *>(determinator_type) &&
        !typeid_cast<const DataTypeUInt32 *>(determinator_type) &&
        !typeid_cast<const DataTypeInt64 *>(determinator_type) &&
        !typeid_cast<const DataTypeUInt64 *>(determinator_type))
    {
        throw Exception{
            "Illegal type " + determinator_type->getName() + " of second argument for aggregate function " + name +
            ", Int32, UInt32, Int64 or UInt64 required",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
    }

    const DataTypePtr & argument_type = argument_types[0];

    if (params.size() != 1)
        throw Exception("Aggregate function " + name + " requires exactly one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    Float64 level = applyVisitor(FieldVisitorConvertToNumber<Float64>(), params[0]);

         if (typeid_cast<const DataTypeUInt8 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantileDeterministic<UInt8>>(argument_type, level);
    else if (typeid_cast<const DataTypeUInt16 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantileDeterministic<UInt16>>(argument_type, level);
    else if (typeid_cast<const DataTypeUInt32 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantileDeterministic<UInt32>>(argument_type, level);
    else if (typeid_cast<const DataTypeUInt64 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantileDeterministic<UInt64>>(argument_type, level);
    else if (typeid_cast<const DataTypeInt8 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantileDeterministic<Int8>>(argument_type, level);
    else if (typeid_cast<const DataTypeInt16 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantileDeterministic<Int16>>(argument_type, level);
    else if (typeid_cast<const DataTypeInt32 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantileDeterministic<Int32>>(argument_type, level);
    else if (typeid_cast<const DataTypeInt64 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantileDeterministic<Int64>>(argument_type, level);
    else if (typeid_cast<const DataTypeFloat32 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantileDeterministic<Float32>>(argument_type, level);
    else if (typeid_cast<const DataTypeFloat64 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantileDeterministic<Float64>>(argument_type, level);
    else if (typeid_cast<const DataTypeDate *>(argument_type.get()))
        return std::make_shared<AggregateFunctionQuantileDeterministic<DataTypeDate::FieldType, false>>();
    else if (typeid_cast<const DataTypeDateTime *>(argument_type.get()))
        return std::make_shared<AggregateFunctionQuantileDeterministic<DataTypeDateTime::FieldType, false>>();
    else
        throw Exception("Illegal type " + argument_type->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}


AggregateFunctionPtr createAggregateFunctionQuantilesDeterministic(const std::string & name, const DataTypes & argument_types, const Array & params)
{
    assertBinary(name, argument_types);

    const auto determinator_type = argument_types[1].get();
    if (!typeid_cast<const DataTypeInt32 *>(determinator_type) &&
        !typeid_cast<const DataTypeUInt32 *>(determinator_type) &&
        !typeid_cast<const DataTypeInt64 *>(determinator_type) &&
        !typeid_cast<const DataTypeUInt64 *>(determinator_type))
    {
        throw Exception{
            "Illegal type " + determinator_type->getName() + " of second argument for aggregate function " + name +
            ", Int32, UInt32, Int64 or UInt64 required",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
    }

    const DataTypePtr & argument_type = argument_types[0];

    if (params.empty())
        throw Exception("Aggregate function " + name + " requires at least one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    size_t size = params.size();
    std::vector<Float64> levels(size);

    for (size_t i = 0; i < size; ++i)
        levels[i] = applyVisitor(FieldVisitorConvertToNumber<Float64>(), params[i]);

         if (typeid_cast<const DataTypeUInt8 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantilesDeterministic<UInt8>>(argument_type, levels);
    else if (typeid_cast<const DataTypeUInt16 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantilesDeterministic<UInt16>>(argument_type, levels);
    else if (typeid_cast<const DataTypeUInt32 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantilesDeterministic<UInt32>>(argument_type, levels);
    else if (typeid_cast<const DataTypeUInt64 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantilesDeterministic<UInt64>>(argument_type, levels);
    else if (typeid_cast<const DataTypeInt8 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantilesDeterministic<Int8>>(argument_type, levels);
    else if (typeid_cast<const DataTypeInt16 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantilesDeterministic<Int16>>(argument_type, levels);
    else if (typeid_cast<const DataTypeInt32 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantilesDeterministic<Int32>>(argument_type, levels);
    else if (typeid_cast<const DataTypeInt64 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantilesDeterministic<Int64>>(argument_type, levels);
    else if (typeid_cast<const DataTypeFloat32 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantilesDeterministic<Float32>>(argument_type, levels);
    else if (typeid_cast<const DataTypeFloat64 *>(argument_type.get())) return std::make_shared<AggregateFunctionQuantilesDeterministic<Float64>>(argument_type, levels);
    else if (typeid_cast<const DataTypeDate *>(argument_type.get()))
        return std::make_shared<AggregateFunctionQuantilesDeterministic<DataTypeDate::FieldType, false>>(argument_type, levels);
    else if (typeid_cast<const DataTypeDateTime *>(argument_type.get()))
        return std::make_shared<AggregateFunctionQuantilesDeterministic<DataTypeDateTime::FieldType, false>>(argument_type, levels);
    else
        throw Exception("Illegal type " + argument_type->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

}

void registerAggregateFunctionsQuantileDeterministic(AggregateFunctionFactory & factory)
{
    factory.registerFunction("quantileDeterministic", createAggregateFunctionQuantileDeterministic);
    factory.registerFunction("medianDeterministic", createAggregateFunctionQuantileDeterministic);
    factory.registerFunction("quantilesDeterministic", createAggregateFunctionQuantilesDeterministic);
}

}
