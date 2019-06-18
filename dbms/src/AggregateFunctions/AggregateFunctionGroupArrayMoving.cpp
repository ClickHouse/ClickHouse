#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupArrayMoving.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

namespace
{

template <typename T, typename Tlimit_num_elems>
struct MovingSum
{
    using DataType = MovingSumData<T>;
    using Function = MovingImpl<T, Tlimit_num_elems, DataType>;
};

template <typename T, typename Tlimit_num_elems>
struct MovingAvg
{
    using DataType = MovingAvgData<T>;
    using Function = MovingImpl<T, Tlimit_num_elems, DataType>;
};

template <typename T, typename Tlimit_num_elems> using MovingSumTemplate = typename MovingSum<T, Tlimit_num_elems>::Function;
template <typename T, typename Tlimit_num_elems> using MovingAvgTemplate = typename MovingAvg<T, Tlimit_num_elems>::Function;

template <template <typename, typename> class Function, typename has_limit, typename ... TArgs>
inline AggregateFunctionPtr createAggregateFunctionMovingImpl(const std::string & name, const DataTypePtr & argument_type, TArgs ... args)
{
    AggregateFunctionPtr res;

    if (isDecimal(argument_type))
        res.reset(createWithDecimalType<Function, has_limit>(*argument_type, argument_type, std::forward<TArgs>(args)...));
    else
        res.reset(createWithNumericType<Function, has_limit>(*argument_type, argument_type, std::forward<TArgs>(args)...));

    if (!res)
        throw Exception("Illegal type " + argument_type->getName() + " of argument for aggregate function " + name,
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

template <template <typename, typename> class Function>
AggregateFunctionPtr createAggregateFunctionMoving(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertUnary(name, argument_types);

    bool limit_size = false;

    UInt64 max_elems = std::numeric_limits<UInt64>::max();

    if (parameters.empty())
    {
        // cumulative sum without parameter
    }
    else if (parameters.size() == 1)
    {
        auto type = parameters[0].getType();
        if (type != Field::Types::Int64 && type != Field::Types::UInt64)
               throw Exception("Parameter for aggregate function " + name + " should be positive number", ErrorCodes::BAD_ARGUMENTS);

        if ((type == Field::Types::Int64 && parameters[0].get<Int64>() < 0) ||
            (type == Field::Types::UInt64 && parameters[0].get<UInt64>() == 0))
            throw Exception("Parameter for aggregate function " + name + " should be positive number", ErrorCodes::BAD_ARGUMENTS);

        limit_size = true;
        max_elems = parameters[0].get<UInt64>();
    }
    else
        throw Exception("Incorrect number of parameters for aggregate function " + name + ", should be 0 or 1",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (!limit_size)
        return createAggregateFunctionMovingImpl<Function, std::false_type>(name, argument_types[0]);
    else
        return createAggregateFunctionMovingImpl<Function, std::true_type>(name, argument_types[0], max_elems);
}

}


void registerAggregateFunctionMoving(AggregateFunctionFactory & factory)
{
    factory.registerFunction("groupArrayMovingSum", createAggregateFunctionMoving<MovingSumTemplate>);
    factory.registerFunction("groupArrayMovingAvg", createAggregateFunctionMoving<MovingAvgTemplate>);
}

}
