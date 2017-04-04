#include <AggregateFunctions/AggregateFunctionIf.h>

namespace DB
{

AggregateFunctionPtr createAggregateFunctionIf(AggregateFunctionPtr & nested)
{
    return std::make_shared<AggregateFunctionIf>(nested);
}

}
