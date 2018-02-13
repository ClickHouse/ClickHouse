#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionCount.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionCount(const std::string & name, const DataTypes & /*argument_types*/, const Array & parameters)
{
    assertNoParameters(name, parameters);

    /// 'count' accept any number of arguments and (in this case of non-Nullable types) simply ignore them.
    return std::make_shared<AggregateFunctionCount>();
}

}

void registerAggregateFunctionCount(AggregateFunctionFactory & factory)
{
    factory.registerFunction("count", createAggregateFunctionCount, AggregateFunctionFactory::CaseInsensitive);
}

}
