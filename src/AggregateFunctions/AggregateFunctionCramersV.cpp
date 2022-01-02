#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionCramersV.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include "registerAggregateFunctions.h"
#include <memory>

namespace DB
{
namespace
{

AggregateFunctionPtr createAggregateFunctionCramersV(const std::string & name, const DataTypes & argument_types,
                                                         const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    return std::make_shared<AggregateFunctionCramersV<AggregateFunctionCramersVData>>(argument_types);
}

}

void registerAggregateFunctionCramersV(AggregateFunctionFactory & factory)
{
    factory.registerFunction("CramersV", createAggregateFunctionCramersV);
}

}
