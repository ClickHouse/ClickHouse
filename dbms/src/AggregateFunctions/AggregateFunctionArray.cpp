#include <AggregateFunctions/AggregateFunctionArray.h>

namespace DB
{

AggregateFunctionPtr createAggregateFunctionArray(AggregateFunctionPtr & nested)
{
    return std::make_shared<AggregateFunctionArray>(nested);
}


}
