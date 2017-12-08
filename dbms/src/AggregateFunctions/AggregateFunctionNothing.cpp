#include <AggregateFunctions/AggregateFunctionNothing.h>

namespace DB
{

AggregateFunctionPtr createAggregateFunctionNothing()
{
    return std::make_shared<AggregateFunctionNothing>();
}

}
