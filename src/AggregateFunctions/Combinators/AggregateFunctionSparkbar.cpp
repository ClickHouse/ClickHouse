#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionSparkbar.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

namespace
{

template <typename T>
T getParam(const Field & field)
{
    if constexpr (std::is_signed_v<T>)
    {
        Int64 value;
        if (field.tryGet<Int64>(value))
            return static_cast<T>(value);
        return static_cast<T>(field.safeGet<UInt64>());
    }
    else
    {
        return static_cast<T>(field.safeGet<UInt64>());
    }
}

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
                "At least one argument (bucket key) is required", getName());

        /// Remove the first argument (bucket key), pass the rest to nested function
        return DataTypes(arguments.begin() + 1, arguments.end());
    }

    Array transformParameters(const Array & params) const override
    {
        /// Sparkbar combinator requires exactly 3 parameters: width, min_x, max_x
        /// These are consumed by the combinator, not passed to nested function
        if (params.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Aggregate function with {} suffix requires exactly 3 parameters (width, min_x, max_x), got {}",
                getName(), params.size());

        /// Return empty array - all parameters are consumed by the combinator
        return Array();
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
                "At least one argument (bucket key) is required", getName());

        if (params.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Aggregate function with {} suffix requires exactly 3 parameters (width, min_x, max_x), got {}", getName(), params.size());

        UInt64 width = params[0].safeGet<UInt64>();

        WhichDataType which{arguments[0]};

        if (which.isNativeUInt() || which.isDate() || which.isDateTime())
            return std::make_shared<AggregateFunctionSparkbar<UInt64>>(
                nested_function, width, getParam<UInt64>(params[1]), getParam<UInt64>(params[2]), arguments, params);

        if (which.isNativeInt())
            return std::make_shared<AggregateFunctionSparkbar<Int64>>(
                nested_function, width, getParam<Int64>(params[1]), getParam<Int64>(params[2]), arguments, params);

        if (which.isDate32())
            return std::make_shared<AggregateFunctionSparkbar<Int32>>(
                nested_function, width, getParam<Int32>(params[1]), getParam<Int32>(params[2]), arguments, params);

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of the first argument for aggregate function with {} suffix. "
            "The type should be native integer, Date, Date32 or DateTime",
            arguments[0]->getName(), getName());
    }
};

}

void registerAggregateFunctionCombinatorSparkbar(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorSparkbar>());
}

}
