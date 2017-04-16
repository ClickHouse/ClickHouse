#include "AggregateFunctionForEach.h"

namespace DB
{

AggregateFunctionPtr createAggregateFunctionForEach(AggregateFunctionPtr & nested)
{
    return std::make_shared<AggregateFunctionForEach>(nested);
}

}
