
#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionTuple.h>

#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

class AggregateFunctionCombinatorTuple final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "Tuple"; }

    bool transformsArgumentTypes() const override { return true; }

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Aggregate function with {} suffix requires exactly one Tuple argument", getName());

        const auto * tuple_type = typeid_cast<const DataTypeTuple *>(arguments[0].get());
        if (!tuple_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument for aggregate function with {} suffix. Must be Tuple.",
                arguments[0]->getName(), getName());

        const auto & elem_types = tuple_type->getElements();
        if (elem_types.empty())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Tuple must not be empty for aggregate function with {} suffix", getName());

        /// Return first element type as a placeholder so that the factory can resolve the nested function name.
        /// The actual per-element functions will be created inside AggregateFunctionTuple.
        return DataTypes({elem_types[0]});
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties &,
        const DataTypes & arguments,
        const Array & params) const override
    {
        return std::make_shared<AggregateFunctionTuple>(nested_function, arguments, params);
    }
};

}

void registerAggregateFunctionCombinatorTuple(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorTuple>());
}

}
