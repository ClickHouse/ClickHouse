#include "AggregateFunctionForEach.h"

namespace DB
{

AggregateFunctionPtr createAggregateFunctionForEach(AggregateFunctionPtr & nested, const DataTypes & arguments)
{
    return std::make_shared<AggregateFunctionForEach>(nested, arguments);
}

}
