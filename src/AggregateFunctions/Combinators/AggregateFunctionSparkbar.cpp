#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionSparkbar.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <base/arithmeticOverflow.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int DECIMAL_OVERFLOW;
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
        if (params.size() < 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Aggregate function with {} suffix requires at least 3 parameters: "
                "(..., width, begin_x, end_x), got {}",
                getName(), params.size());

        /// The last 3 parameters (width, begin_x, end_x) are consumed by the combinator;
        /// any leading parameters are forwarded to the nested function (e.g. the quantile
        /// level in `quantileSparkbar(0.9, width, begin_x, end_x)`).
        return Array(params.begin(), params.end() - 3);
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

        if (params.size() < 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Aggregate function with {} suffix requires at least 3 parameters: "
                "(..., width, begin_x, end_x), got {}",
                getName(), params.size());

        /// The last 3 parameters belong to the combinator; the rest are the nested function's
        /// own parameters and are kept in `params` for the nested function's state.
        const size_t n = params.size();
        const size_t width = params[n - 3].safeGet<UInt64>();

        /// Validate that the nested function returns a numeric type that can be
        /// converted to Float64 for sparkbar rendering. Non-numeric return types
        /// (e.g. String, Array) cannot be visualised and must be rejected early.
        /// Nullable(<numeric>) is accepted: the Nullable wrapper is stripped before
        /// the numeric check, so compositions like avgOrNullSparkbar work correctly.
        {
            WhichDataType result_which{removeNullable(nested_function->getResultType())};
            if (!result_which.isNumber())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Aggregate function with {} suffix requires a nested function that returns "
                    "a numeric type, but '{}' returns {}",
                    getName(), nested_function->getName(), nested_function->getResultType()->getName());
        }

        WhichDataType which{arguments[0]};

        if (which.isNativeUInt() || which.isDate() || which.isDateTime())
        {
            const UInt64 begin_x = params[n - 2].safeGet<UInt64>();
            const UInt64 end_x   = params[n - 1].safeGet<UInt64>();

            return std::make_shared<AggregateFunctionSparkbar<UInt64>>(
                nested_function, width, begin_x, end_x, arguments, params);
        }

        if (which.isDateTime64())
        {
            /// Rescale parameter ticks to the column scale so that begin_x/end_x and
            /// the keys read in add() are in the same unit.
            const UInt32 col_scale = typeid_cast<const DataTypeDateTime64 &>(*arguments[0]).getScale();

            /// `round_up == true` is used for `begin_x` and `round_up == false` for `end_x`.
            /// When the parameter has a finer scale than the column (`param_scale > col_scale`),
            /// the rescaled value may fall between two column ticks. To keep the inclusive
            /// `[begin_x, end_x]` contract we round `begin_x` up (toward +inf) and `end_x` down
            /// (toward -inf), so the rescaled range never admits keys outside the requested one.
            const auto extract = [col_scale](const Field & f, bool round_up) -> Int64
            {
                const auto & dec = f.safeGet<DecimalField<DateTime64>>();
                const Int64 ticks = static_cast<Int64>(dec.getValue());
                const UInt32 param_scale = dec.getScale();
                if (param_scale == col_scale)
                    return ticks;
                if (col_scale > param_scale)
                {
                    const Int64 multiplier = static_cast<Int64>(DecimalUtils::scaleMultiplier<Int64>(col_scale - param_scale));
                    Int64 result = 0;
                    if (common::mulOverflow(ticks, multiplier, result))
                        throw Exception(ErrorCodes::DECIMAL_OVERFLOW,
                            "DateTime64 value overflows Int64 when rescaling from scale {} to {} "
                            "for aggregate function with Sparkbar suffix",
                            param_scale, col_scale);
                    return result;
                }

                /// `param_scale > col_scale`: integer division truncates toward zero, which
                /// would silently shift inclusive bounds. Round directionally instead.
                const Int64 divisor = static_cast<Int64>(DecimalUtils::scaleMultiplier<Int64>(param_scale - col_scale));
                const Int64 quotient = ticks / divisor;
                const Int64 remainder = ticks % divisor;
                if (remainder == 0)
                    return quotient;
                if (round_up)
                    return remainder > 0 ? quotient + 1 : quotient;
                return remainder < 0 ? quotient - 1 : quotient;
            };
            return std::make_shared<AggregateFunctionSparkbar<Int64>>(
                nested_function, width, extract(params[n - 2], /*round_up=*/true), extract(params[n - 1], /*round_up=*/false), arguments, params);
        }

        if (which.isDate32())
        {
            Int64 tmp;
            const Int32 begin_x = params[n - 2].tryGet<Int64>(tmp) ? static_cast<Int32>(tmp) : static_cast<Int32>(params[n - 2].safeGet<UInt64>());
            const Int32 end_x   = params[n - 1].tryGet<Int64>(tmp) ? static_cast<Int32>(tmp) : static_cast<Int32>(params[n - 1].safeGet<UInt64>());

            return std::make_shared<AggregateFunctionSparkbar<Int32>>(
                nested_function, width, begin_x, end_x, arguments, params);
        }

        if (which.isNativeInt() || which.isEnum() || which.isInterval())
        {
            Int64 tmp;
            const Int64 begin_x = params[n - 2].tryGet<Int64>(tmp) ? tmp : static_cast<Int64>(params[n - 2].safeGet<UInt64>());
            const Int64 end_x   = params[n - 1].tryGet<Int64>(tmp) ? tmp : static_cast<Int64>(params[n - 1].safeGet<UInt64>());

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
