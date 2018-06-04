#include <AggregateFunctions/AggregateFunctionIf.h>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class AggregateFunctionCombinatorIf final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "If"; }

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception("Incorrect number of arguments for aggregate function with " + getName() + " suffix",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!typeid_cast<const DataTypeUInt8 *>(arguments.back().get()))
            throw Exception("Illegal type " + arguments.back()->getName() + " of last argument for aggregate function with " + getName() + " suffix",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return DataTypes(arguments.begin(), std::prev(arguments.end()));
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function, const DataTypes & arguments, const Array &) const override
    {
        return std::make_shared<AggregateFunctionIf>(nested_function, arguments);
    }
};

void registerAggregateFunctionCombinatorIf(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorIf>());
}

}
