#include <memory>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionAvg.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include "registerAggregateFunctions.h"

namespace DB
{
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

AggregateFunctionPtr createAggregateFunctionAvg(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    if (!allowType(argument_types[0]))
        throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return std::make_shared<AggregateFunctionAvg>(argument_types);
}
}

void registerAggregateFunctionAvg(AggregateFunctionFactory & factory)
{
    factory.registerFunction("avg", createAggregateFunctionAvg, AggregateFunctionFactory::CaseInsensitive);
}
}
