#include <AggregateFunctions/AggregateFunctionForEach.h>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <Common/typeid_cast.h>
#include "registerAggregateFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

class AggregateFunctionCombinatorForEach final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "ForEach"; }

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        DataTypes nested_arguments;
        for (const auto & type : arguments)
        {
            if (const DataTypeArray * array = typeid_cast<const DataTypeArray *>(type.get()))
                nested_arguments.push_back(array->getNestedType());
            else
                throw Exception("Illegal type " + type->getName() + " of argument"
                    " for aggregate function with " + getName() + " suffix. Must be array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return nested_arguments;
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties &,
        const DataTypes & arguments,
        const Array &) const override
    {
        return std::make_shared<AggregateFunctionForEach>(nested_function, arguments);
    }
};

}

void registerAggregateFunctionCombinatorForEach(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorForEach>());
}

}
