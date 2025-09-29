#include <memory>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionAvg.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeTime64.h>

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
bool allowType(const DataTypePtr& type) noexcept
{
    const WhichDataType t(type);
    return t.isInt()
        || t.isUInt()
        || t.isFloat()
        || t.isDecimal()
        || t.isDate()
        || t.isDate32()
        || t.isDateTime()
        || t.isTime()
        || t.isDateTime64()
        || t.isTime64();
}

AggregateFunctionPtr createAggregateFunctionAvg(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    const DataTypePtr& data_type = argument_types[0];

    if (!allowType(data_type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}",
            data_type->getName(), name);

    AggregateFunctionPtr res;

    const WhichDataType which(data_type);

    if (which.isDateTime64())
    {
        res = std::make_shared<AggregateFunctionAvg<DateTime64>>(argument_types, data_type, getDecimalScale(*data_type));
    }
    else if (which.isTime64())
    {
        res = std::make_shared<AggregateFunctionAvg<Time64>>(argument_types, data_type, getDecimalScale(*data_type));
    }
    else if (isDecimal(data_type))
    {
        res.reset(createWithDecimalType<AggregateFunctionAvg>(*data_type, argument_types, getDecimalScale(*data_type)));
    }
    else if (which.isDate())
    {
        // Preserve Date result type
        res = std::make_shared<AggregateFunctionAvg<UInt16>>(argument_types, data_type);
    }
    else if (which.isDate32())
    {
        // Preserve Date32 result type
        res = std::make_shared<AggregateFunctionAvg<Int32>>(argument_types, data_type);
    }
    else if (which.isDateTime())
    {
        // Preserve DateTime result type
        res = std::make_shared<AggregateFunctionAvg<UInt32>>(argument_types, data_type);
    }
    else if (which.isTime())
    {
        // Preserve Time result type
        res = std::make_shared<AggregateFunctionAvg<Int32>>(argument_types, data_type);
    }
    else
    {
        res.reset(createWithNumericType<AggregateFunctionAvg>(*data_type, argument_types));
    }

    return res;
}
}

void registerAggregateFunctionAvg(AggregateFunctionFactory & factory)
{
    factory.registerFunction("avg", createAggregateFunctionAvg, AggregateFunctionFactory::Case::Insensitive);
}
}
