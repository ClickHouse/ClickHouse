#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionResample.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

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
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Incorrect number of arguments for aggregate function with {} suffix", getName());

        return DataTypes(arguments.begin(), arguments.end() - 1);
    }

    Array transformParameters(const Array & params) const override
    {
        if (params.size() < 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Incorrect number of parameters for aggregate function with {} suffix", getName());

        return Array(params.begin(), params.end() - 3);
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties &,
        const DataTypes & arguments,
        const Array & params) const override
    {
        WhichDataType which{arguments.back()};

        if (params.size() < 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Incorrect number of parameters for aggregate function with {} suffix", getName());

        if (which.isNativeUInt() || which.isDate() || which.isDateTime() || which.isDateTime64())
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
            Int64 begin = 0;
            Int64 end = 0;

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

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal types of argument for aggregate function {}, the type "
                        "of the last argument should be native integer or integer-like", getName());
    }
};

}

void registerAggregateFunctionCombinatorResample(AggregateFunctionCombinatorFactory & factory);
void registerAggregateFunctionCombinatorResample(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorResample>(), Documentation{
        .description = "Applied as a suffix to an aggregate function name (e.g. `sumResample`), it partitions the data into intervals `[start, end)` of width `step` according to a resampling key column and aggregates each interval separately, returning an array of results. The interval bounds and step are passed in the first parameter list; the nested aggregate function arguments and the resampling key are passed in the second.",
        .syntax = "<aggregate_function>Resample(start, end, step)(<aggregate_function_arguments>, resampling_key)",
        .related = {}});
}

}
