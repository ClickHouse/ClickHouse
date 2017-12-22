#include <AggregateFunctions/AggregateFunctionIf.h>

namespace DB
{

AggregateFunctionPtr createAggregateFunctionIf(AggregateFunctionPtr & nested, const DataTypes & types)
{
    return std::make_shared<AggregateFunctionIf>(nested, types);
}

}
