#include <AggregateFunctions/AggregateFunctionMerge.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{

AggregateFunctionPtr createAggregateFunctionMerge(AggregateFunctionPtr & nested, const DataTypes & argument_types)
{
    assertUnary(argument_types);
    return std::make_shared<AggregateFunctionMerge>(nested, *argument_types[0]);
}

}
