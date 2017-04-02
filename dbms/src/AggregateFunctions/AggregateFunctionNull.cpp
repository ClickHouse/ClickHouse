#include <AggregateFunctions/AggregateFunctionNull.h>

namespace DB
{

AggregateFunctionPtr createAggregateFunctionNullUnary(AggregateFunctionPtr & nested)
{
    return std::make_shared<AggregateFunctionNullUnary>(nested);
}

AggregateFunctionPtr createAggregateFunctionNullVariadic(AggregateFunctionPtr & nested)
{
    return std::make_shared<AggregateFunctionNullVariadic>(nested);
}

}
