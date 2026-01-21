#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionMerge.h>

#include <Columns/ColumnAggregateFunction.h>
#include <DataTypes/DataTypeAggregateFunction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


AggregateFunctionMerge::AggregateFunctionMerge(const AggregateFunctionPtr & nested_, const DataTypePtr & argument, const Array & params_)
    : IAggregateFunctionHelper<AggregateFunctionMerge>({argument}, params_, createResultType(nested_))
    , nested_func(nested_)
{
    const DataTypeAggregateFunction * data_type = typeid_cast<const DataTypeAggregateFunction *>(argument.get());

    if (!data_type)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of argument for aggregate function {}, expected {} or equivalent type",
            argument->getName(),
            getName(),
            getStateType()->getName());

    argument_func = data_type->getFunction();

    if (nested_func->haveSameStateRepresentation(*argument_func))
        return;

    if (data_type->getFunctionName() != nested_func->getName())
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of argument for aggregate function with Merge suffix because it corresponds to different aggregate function: "
            "{} instead of {}",
            argument->getName(),
            data_type->getFunctionName(),
            nested_func->getName());

    const auto & nested_base = nested_func->getBaseAggregateFunctionWithSameStateRepresentation();
    const auto & argument_base = argument_func->getBaseAggregateFunctionWithSameStateRepresentation();

    if (nested_base.getName() == argument_base.getName() && nested_base.getStateVariant() != argument_base.getStateVariant()
        && nested_base.canMergeStateFromDifferentVariant(argument_base))
    {
        nested_base_for_variant_merge = &nested_base;
        argument_base_for_variant_merge = &argument_base;
        return;
    }

    throw Exception(
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
        "Illegal type {} of argument for aggregate function with Merge suffix because it corresponds to a different implementation "
        "of aggregate function '{}'. The state was produced by a different implementation (for example, aggregation vs window variant). "
        "Merging between window and aggregation variants is not supported for this aggregate function. "
        "State variants: '{}' vs '{}'. State types: '{}' vs '{}'",
        argument->getName(),
        data_type->getFunctionName(),
        toString(nested_base.getStateVariant()),
        toString(argument_base.getStateVariant()),
        nested_func->getStateType()->getName(),
        argument_func->getStateType()->getName());
}

void AggregateFunctionMerge::add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const
{
    const auto & column = assert_cast<const ColumnAggregateFunction &>(*columns[0]);
    auto * const rhs = column.getData()[row_num];

    if (nested_base_for_variant_merge)
        nested_base_for_variant_merge->mergeStateFromDifferentVariant(place, *argument_base_for_variant_merge, rhs, arena);
    else
        nested_func->merge(place, rhs, arena);
}

namespace
{

class AggregateFunctionCombinatorMerge final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "Merge"; }

    bool transformsArgumentTypes() const override { return true; }

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

        return std::make_shared<AggregateFunctionMerge>(nested_function, argument, params);
    }
    };

}

void registerAggregateFunctionCombinatorMerge(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorMerge>());
}

}
