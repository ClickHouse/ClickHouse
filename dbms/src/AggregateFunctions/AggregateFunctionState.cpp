#include <AggregateFunctions/AggregateFunctionState.h>
#include <AggregateFunctions/AggregateFunctionMerge.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

DataTypePtr AggregateFunctionState::getReturnType() const
{
    auto ptr = std::make_shared<DataTypeAggregateFunction>(nested_func_owner, arguments, params);

    /// Special case: it is -MergeState combinator
    if (typeid_cast<const AggregateFunctionMerge *>(ptr->getFunction().get()))
    {
        if (arguments.size() != 1)
            throw Exception("Combinator -MergeState expects only one argument", ErrorCodes::BAD_ARGUMENTS);

        if (!typeid_cast<const DataTypeAggregateFunction *>(arguments[0].get()))
            throw Exception("Combinator -MergeState expects argument with AggregateFunction type", ErrorCodes::BAD_ARGUMENTS);

        return arguments[0];
    }

    return ptr;
}


AggregateFunctionPtr createAggregateFunctionState(AggregateFunctionPtr & nested)
{
    return std::make_shared<AggregateFunctionState>(nested);
}

}
