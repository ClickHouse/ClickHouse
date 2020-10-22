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
AggregateFunctionPtr createAggregateFunctionAvg(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    AggregateFunctionPtr res;
    DataTypePtr data_type = argument_types[0];

    if (isDecimal(data_type))
        res.reset(createWithDecimalType<AggregateFunctionAvg>(*data_type, *data_type, argument_types));
    else
        res.reset(createWithNumericType<AggregateFunctionAvg>(*data_type, argument_types));

    if (!res)
        throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    return res;
}
}

void registerAggregateFunctionAvg(AggregateFunctionFactory & factory)
{
    factory.registerFunction("avg", createAggregateFunctionAvg, AggregateFunctionFactory::CaseInsensitive);
}
}
