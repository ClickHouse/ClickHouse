#include <AggregateFunctions/AggregateFunctionMerge.h>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <DataTypes/DataTypeAggregateFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

class AggregateFunctionCombinatorMerge final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "Merge"; }

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Incorrect number of arguments for aggregate function with {} suffix",
                getName());

        const DataTypePtr & argument = arguments[0];

        const DataTypeAggregateFunction * function = typeid_cast<const DataTypeAggregateFunction *>(argument.get());
        if (!function)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument for aggregate function with {} suffix. It must be AggregateFunction(...)",
                argument->getName(),
                getName());

        return function->getArgumentsDataTypes();
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties &,
        const DataTypes & arguments,
        const Array & params) const override
    {
        const DataTypePtr & argument = arguments[0];

        const DataTypeAggregateFunction * function = typeid_cast<const DataTypeAggregateFunction *>(argument.get());
        if (!function)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument for aggregate function with {} suffix. It must be AggregateFunction(...)",
                argument->getName(),
                getName());

        if (!nested_function->haveSameStateRepresentation(*function->getFunction()))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument for aggregate function with {} suffix. because it corresponds to different aggregate "
                "function: {} instead of {}",
                argument->getName(),
                getName(),
                function->getFunctionName(),
                nested_function->getName());

        return std::make_shared<AggregateFunctionMerge>(nested_function, argument, params);
    }
};

}

void registerAggregateFunctionCombinatorMerge(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorMerge>());
}

}
