#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/HelpersMinMaxAny.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include "registerAggregateFunctions.h"


namespace DB
{
struct Settings;

namespace
{

AggregateFunctionPtr createAggregateFunctionSingleValueOrNull(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionSingleValueOrNullData>(name, argument_types, parameters, settings));
}

}

void registerAggregateFunctionSingleValueOrNull(AggregateFunctionFactory & factory)
{
    factory.registerFunction("singleValueOrNull", createAggregateFunctionSingleValueOrNull);
}


}
