#include <AggregateFunctions/Combinators/AggregateFunctionArray.h>
#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>

#include <Common/typeid_cast.h>

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

class AggregateFunctionCombinatorArray final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "Array"; }

    bool supportsNesting() const override { return true; }

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Array aggregate functions require at least one argument");

        DataTypes nested_arguments;
        for (const auto & type : arguments)
        {
            if (const DataTypeArray * array = typeid_cast<const DataTypeArray *>(type.get()))
                nested_arguments.push_back(array->getNestedType());
            else
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument"
                                " for aggregate function with {} suffix. Must be array.", type->getName(), getName());
        }

        return nested_arguments;
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties &,
        const DataTypes & arguments,
        const Array & params) const override
    {
        return std::make_shared<AggregateFunctionArray>(nested_function, arguments, params);
    }
};

}

void registerAggregateFunctionCombinatorArray(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorArray>());
}

}
