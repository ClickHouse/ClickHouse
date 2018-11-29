#include <AggregateFunctions/AggregateFunctionMerge.h>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <DataTypes/DataTypeAggregateFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

class AggregateFunctionCombinatorMerge final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "Merge"; }

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception("Incorrect number of arguments for aggregate function with " + getName() + " suffix", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const DataTypePtr & argument = arguments[0];

        const DataTypeAggregateFunction * function = typeid_cast<const DataTypeAggregateFunction *>(argument.get());
        if (!function)
            throw Exception("Illegal type " + argument->getName() + " of argument for aggregate function with " + getName() + " suffix"
                + " must be AggregateFunction(...)", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return function->getArgumentsDataTypes();
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function, const DataTypes & arguments, const Array &) const override
    {
        const DataTypePtr & argument = arguments[0];

        const DataTypeAggregateFunction * function = typeid_cast<const DataTypeAggregateFunction *>(argument.get());
        if (!function)
            throw Exception("Illegal type " + argument->getName() + " of argument for aggregate function with " + getName() + " suffix"
                + " must be AggregateFunction(...)", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (nested_function->getName() != function->getFunctionName())
            throw Exception("Illegal type " + argument->getName() + " of argument for aggregate function with " + getName() + " suffix"
                + ", because it corresponds to different aggregate function: " + function->getFunctionName() + " instead of " + nested_function->getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<AggregateFunctionMerge>(nested_function, *argument);
    }
};

void registerAggregateFunctionCombinatorMerge(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorMerge>());
}

}
