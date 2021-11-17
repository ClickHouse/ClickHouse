#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionSumCount.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>


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
    return t.isInt() || t.isUInt() || t.isFloat() || t.isDecimal();
}

AggregateFunctionPtr
createAggregateFunctionSumCount(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    AggregateFunctionPtr res;
    DataTypePtr data_type = argument_types[0];
    if (!allowType(data_type))
        throw Exception("Illegal type " + data_type->getName() + " of argument for aggregate function " + name,
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    if (isDecimal(data_type))
        res.reset(createWithDecimalType<AggregateFunctionSumCount>(
            *data_type, argument_types, getDecimalScale(*data_type)));
    else
        res.reset(createWithNumericType<AggregateFunctionSumCount>(*data_type, argument_types));

    return res;
}

}

void registerAggregateFunctionSumCount(AggregateFunctionFactory & factory)
{
    factory.registerFunction("sumCount", createAggregateFunctionSumCount);
}

}
