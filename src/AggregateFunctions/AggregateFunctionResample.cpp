#include <AggregateFunctions/AggregateFunctionResample.h>

#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include "registerAggregateFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class AggregateFunctionCombinatorResample final : public IAggregateFunctionCombinator
{
public:
    String getName() const override
    {
        return "Resample";
    }

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception("Incorrect number of arguments for aggregate function with "
                    + getName() + " suffix",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return DataTypes(arguments.begin(), arguments.end() - 1);
    }

    Array transformParameters(const Array & params) const override
    {
        if (params.size() < 3)
            throw Exception("Incorrect number of parameters for aggregate function with "
                    + getName() + " suffix",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return Array(params.begin(), params.end() - 3);
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties &,
        const DataTypes & arguments,
        const Array & params) const override
    {
        WhichDataType which{arguments.back()};

        if (which.isNativeUInt() || which.isDateOrDateTime())
        {
            UInt64 begin = params[params.size() - 3].safeGet<UInt64>();
            UInt64 end = params[params.size() - 2].safeGet<UInt64>();

            UInt64 step = params[params.size() - 1].safeGet<UInt64>();

            return std::make_shared<AggregateFunctionResample<UInt64>>(
                nested_function,
                begin,
                end,
                step,
                arguments,
                params);
        }

        if (which.isNativeInt() || which.isEnum() || which.isInterval())
        {
            Int64 begin, end;

            // notice: UInt64 -> Int64 may lead to overflow
            if (!params[params.size() - 3].tryGet<Int64>(begin))
                begin = params[params.size() - 3].safeGet<UInt64>();
            if (!params[params.size() - 2].tryGet<Int64>(end))
                end = params[params.size() - 2].safeGet<UInt64>();

            UInt64 step = params[params.size() - 1].safeGet<UInt64>();

            return std::make_shared<AggregateFunctionResample<Int64>>(
                nested_function,
                begin,
                end,
                step,
                arguments,
                params);
        }

        throw Exception(
            "Illegal types of argument for aggregate function " + getName()
                + ", the type of the last argument should be native integer or integer-like",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
};

void registerAggregateFunctionCombinatorResample(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorResample>());
}

}
