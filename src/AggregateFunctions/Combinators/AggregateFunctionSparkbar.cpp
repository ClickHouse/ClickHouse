#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionSparkbar.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

class AggregateFunctionCombinatorSparkbar final : public IAggregateFunctionCombinator
{
public:
    String getName() const override
    {
        return "Sparkbar";
    }

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Incorrect number of arguments for aggregate function with {} suffix. "
                "At least one argument (bucket key) is required",
                getName());

        /// Remove the first argument (bucket key); pass the rest to the nested function.
        return DataTypes(arguments.begin() + 1, arguments.end());
    }

    Array transformParameters(const Array & params) const override
    {
        if (params.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Aggregate function with {} suffix requires exactly 3 parameters: "
                "(width, begin_x, end_x), got {}",
                getName(), params.size());

        /// All parameters are consumed by the combinator; the nested function receives none.
        return Array{};
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties &,
        const DataTypes & arguments,
        const Array & params) const override
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Incorrect number of arguments for aggregate function with {} suffix. "
                "At least one argument (bucket key) is required",
                getName());

        if (params.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Aggregate function with {} suffix requires exactly 3 parameters: "
                "(width, begin_x, end_x), got {}",
                getName(), params.size());

        const size_t width = params[0].safeGet<UInt64>();

        /// Validate that the nested function returns a numeric type that can be
        /// converted to Float64 for sparkbar rendering. Non-numeric return types
        /// (e.g. String, Array) cannot be visualised and must be rejected early.
        {
            WhichDataType result_which{nested_function->getResultType()};
            if (!result_which.isNumber())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Aggregate function with {} suffix requires a nested function that returns "
                    "a numeric type, but '{}' returns {}",
                    getName(), nested_function->getName(), nested_function->getResultType()->getName());
        }

        WhichDataType which{arguments[0]};

        if (which.isNativeUInt() || which.isDate() || which.isDateTime() || which.isDateTime64())
        {
            const UInt64 begin_x = params[1].safeGet<UInt64>();
            const UInt64 end_x   = params[2].safeGet<UInt64>();

            return std::make_shared<AggregateFunctionSparkbar<UInt64>>(
                nested_function, width, begin_x, end_x, arguments, params);
        }

        if (which.isDate32())
        {
            Int64 tmp;
            const Int32 begin_x = params[1].tryGet<Int64>(tmp) ? static_cast<Int32>(tmp) : static_cast<Int32>(params[1].safeGet<UInt64>());
            const Int32 end_x   = params[2].tryGet<Int64>(tmp) ? static_cast<Int32>(tmp) : static_cast<Int32>(params[2].safeGet<UInt64>());

            return std::make_shared<AggregateFunctionSparkbar<Int32>>(
                nested_function, width, begin_x, end_x, arguments, params);
        }

        if (which.isNativeInt() || which.isEnum() || which.isInterval())
        {
            Int64 tmp;
            const Int64 begin_x = params[1].tryGet<Int64>(tmp) ? tmp : static_cast<Int64>(params[1].safeGet<UInt64>());
            const Int64 end_x   = params[2].tryGet<Int64>(tmp) ? tmp : static_cast<Int64>(params[2].safeGet<UInt64>());

            return std::make_shared<AggregateFunctionSparkbar<Int64>>(
                nested_function, width, begin_x, end_x, arguments, params);
        }

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of the first argument for aggregate function with {} suffix. "
            "The type must be a native integer, Date, Date32, DateTime, DateTime64, Enum, or Interval",
            arguments[0]->getName(), getName());
    }
};

}

void registerAggregateFunctionCombinatorSparkbar(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorSparkbar>());
}

}
