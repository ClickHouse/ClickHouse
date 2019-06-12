#include <AggregateFunctions/AggregateFunctionResample.h>

#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class AggregateFunctionCombinatorResample final : public
    IAggregateFunctionCombinator
{
public:
    String getName() const override {
        return "Resample";
    }

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception {
                "Incorrect number of arguments for aggregate function with "
                    + getName() + " suffix",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH
            };

        return DataTypes(arguments.begin(), arguments.end() - 1);
    }

    Array transformParameters(const Array & params) const override
    {
        if (params.size() < 3)
            throw Exception {
                "Incorrect number of parameters for aggregate function with "
                    + getName() + " suffix",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH
            };

        return Array(params.begin(), params.end() - 3);
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const DataTypes & arguments,
        const Array & params
    ) const override
    {
        for (const Field & param : params)
        {
            if (
                param.getType() != Field::Types::UInt64
                && param.getType() != Field::Types::Int64
            )
                return nullptr;
        }

        WhichDataType which {
            arguments.back()
        };

        if (
            which.isNativeUInt()
            || which.isDateOrDateTime()
        )
            return std::make_shared<AggregateFunctionResample<UInt64>>(
                nested_function,
                params.front().get<UInt64>(),
                params[1].get<UInt64>(),
                params.back().get<UInt64>(),
                arguments,
                params
            );

        if (
            which.isNativeInt()
            || which.isEnum()
            || which.isInterval()
        )
            return std::make_shared<AggregateFunctionResample<Int64>>(
                nested_function,
                params.front().get<Int64>(),
                params[1].get<Int64>(),
                params.back().get<Int64>(),
                arguments,
                params
            );

        // TODO
        return nullptr;
    }
};

void registerAggregateFunctionCombinatorResample(
    AggregateFunctionCombinatorFactory & factory
)
{
    factory.registerCombinator(
        std::make_shared<AggregateFunctionCombinatorResample>()
    );
}

}
