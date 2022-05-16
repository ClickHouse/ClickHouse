#include <AggregateFunctions/AggregateFunctionBy.h>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <Common/typeid_cast.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
    class AggregateFunctionCombinatorBy final : public IAggregateFunctionCombinator
    {
    public:
        String getName() const override { return "By"; }

        bool supportsNesting() const override { return true; }

        DataTypes transformArguments(const DataTypes & arguments) const override
        {
            if (arguments.empty())
                throw Exception("-By aggregate functions require at least one argument", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            DataTypes newArguments(arguments.size() - 1);
            for (size_t i = 0; i < arguments.size() - 1; i++) {
                newArguments[i] = arguments[i];
            }
            return newArguments;
//            DataTypes nested_arguments;
//            for (const auto & type : arguments)
//            {
//                if (const DataTypeArray * array = typeid_cast<const DataTypeArray *>(type.get()))
//                nested_arguments.push_back(array->getNestedType());
//                else
//                    throw Exception("Illegal type " + type->getName() + " of argument"
//                                                                        " for aggregate function with " + getName() + " suffix. Must be array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
//            }
//            return arguments;
//            return nested_arguments;
        }

        AggregateFunctionPtr transformAggregateFunction(
            const AggregateFunctionPtr & nested_function,
            const AggregateFunctionProperties &,
            const DataTypes & arguments,
            const Array & params) const override
        {
            return std::make_shared<AggregateFunctionBy>(nested_function, arguments, params);
        }
    };

}

void registerAggregateFunctionCombinatorBy(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorBy>());
}

}
