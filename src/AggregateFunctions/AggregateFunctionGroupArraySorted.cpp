#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupArraySorted.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Common/Exception.h>

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

namespace
{

template <template <typename> class AggregateFunctionTemplate, typename ... TArgs>
AggregateFunctionPtr createWithNumericOrTimeType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::Date) return std::make_shared<AggregateFunctionTemplate<UInt16>>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::DateTime) return std::make_shared<AggregateFunctionTemplate<UInt32>>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::IPv4) return std::make_shared<AggregateFunctionTemplate<IPv4>>(std::forward<TArgs>(args)...);
    return AggregateFunctionPtr(createWithNumericType<AggregateFunctionTemplate, TArgs...>(argument_type, std::forward<TArgs>(args)...));
}

template <typename ... TArgs>
inline AggregateFunctionPtr createAggregateFunctionGroupArraySortedImpl(const DataTypePtr & argument_type, const Array & parameters, TArgs ... args)
{
    if (auto res = createWithNumericOrTimeType<GroupArraySortedNumericImpl>(*argument_type, argument_type, parameters, std::forward<TArgs>(args)...))
        return AggregateFunctionPtr(res);

    WhichDataType which(argument_type);
    return std::make_shared<GroupArraySortedGeneralImpl<GroupArraySortedNodeGeneral>>(argument_type, parameters, std::forward<TArgs>(args)...);
}

AggregateFunctionPtr createAggregateFunctionGroupArraySorted(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertUnary(name, argument_types);

    UInt64 max_elems = std::numeric_limits<UInt64>::max();

    if (parameters.empty())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should have limit argument", name);
    }
    else if (parameters.size() == 1)
    {
        auto type = parameters[0].getType();
        if (type != Field::Types::Int64 && type != Field::Types::UInt64)
               throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be positive number", name);

        if ((type == Field::Types::Int64 && parameters[0].get<Int64>() < 0) ||
            (type == Field::Types::UInt64 && parameters[0].get<UInt64>() == 0))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be positive number", name);

        max_elems = parameters[0].get<UInt64>();
    }
    else
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Function {} does not support this number of arguments", name);

    return createAggregateFunctionGroupArraySortedImpl(argument_types[0], parameters, max_elems);
}

}


void registerAggregateFunctionGroupArraySorted(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = false, .is_order_dependent = false };

    factory.registerFunction("groupArraySorted", { createAggregateFunctionGroupArraySorted, properties });
}

}
