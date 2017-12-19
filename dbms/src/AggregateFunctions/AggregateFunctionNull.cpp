#include <AggregateFunctions/AggregateFunctionNull.h>

namespace DB
{

AggregateFunctionPtr createAggregateFunctionNullUnary(AggregateFunctionPtr & nested)
{
    const DataTypePtr & nested_return_type = nested->getReturnType();
    if (nested_return_type && !nested_return_type->canBeInsideNullable())
        return std::make_shared<AggregateFunctionNullUnary<false>>(nested);
    else
        return std::make_shared<AggregateFunctionNullUnary<true>>(nested);
}

AggregateFunctionPtr createAggregateFunctionNullVariadic(AggregateFunctionPtr & nested)
{
    const DataTypePtr & nested_return_type = nested->getReturnType();
    if (nested_return_type && !nested_return_type->canBeInsideNullable())
        return std::make_shared<AggregateFunctionNullVariadic<false>>(nested);
    else
        return std::make_shared<AggregateFunctionNullVariadic<true>>(nested);
}

}
