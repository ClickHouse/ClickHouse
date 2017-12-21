#include <AggregateFunctions/AggregateFunctionArray.h>

namespace DB
{

AggregateFunctionPtr createAggregateFunctionArray(AggregateFunctionPtr & nested, const DataTypes & types)
{
    return std::make_shared<AggregateFunctionArray>(nested, types);
}


}
