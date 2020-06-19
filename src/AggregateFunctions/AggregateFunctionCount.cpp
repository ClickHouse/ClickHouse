#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionCount.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include "registerAggregateFunctions.h"


namespace DB
{

AggregateFunctionPtr AggregateFunctionCount::getOwnNullAdapter(
    const AggregateFunctionPtr &, const DataTypes & types, const Array & params) const
{
    return std::make_shared<AggregateFunctionCountNotNullUnary>(types[0], params);
}

namespace
{

AggregateFunctionPtr createAggregateFunctionCount(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertArityAtMost<1>(name, argument_types);

    return std::make_shared<AggregateFunctionCount>(argument_types);
}

}

void registerAggregateFunctionCount(AggregateFunctionFactory & factory)
{
    factory.registerFunction("count", {createAggregateFunctionCount, {true}}, AggregateFunctionFactory::CaseInsensitive);
}

}
