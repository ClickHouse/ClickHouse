#include <type_traits>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionAvgWeighted.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include "registerAggregateFunctions.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
constexpr bool allowTypes(const DataTypePtr& left, const DataTypePtr& right)
{
    const WhichDataType l_dt(left), r_dt(right);

    constexpr auto allow = [](WhichDataType t)
    {
        return t.isInt() || t.isUInt() || t.isFloat() || t.isDecimal();
    };

    return allow(l_dt) && allow(r_dt);
}

// TODO signed to unsigned
template <class U, class V, class = void> struct LargestType { using Type = V; };

template <class U, class V>
struct LargestType<U, V, std::enable_if_t<(sizeof(U) > sizeof(V))>> { using Type = typename LargestType<V, U>::Type; };

template <class U, class V> using LargestTypeT = typename LargestType<U, V>::Type;
template <class U, class V> using Function = AggregateFunctionAvgWeighted<LargestTypeT<U, V>, U, V>;

template <typename... TArgs>
static IAggregateFunction * create(const IDataType & first_type, const IDataType & second_type, TArgs && ... args)
{

}

AggregateFunctionPtr createAggregateFunctionAvgWeighted(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertBinary(name, argument_types);

    const auto data_type = static_cast<const DataTypePtr>(argument_types[0]);
    const auto data_type_weight = static_cast<const DataTypePtr>(argument_types[1]);

    if (!allowTypes(data_type, data_type_weight))
        throw Exception(
            "Types " + data_type->getName() +
            " and " + data_type_weight->getName() +
            " are non-conforming as arguments for aggregate function " + name,
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    AggregateFunctionPtr res;
    res.reset(create(*data_type, *data_type_weight, argument_types));

    if (!res)
        throw Exception("Illegal type " + data_type->getName() + " of argument for aggregate function " + name,
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}
}

void registerAggregateFunctionAvgWeighted(AggregateFunctionFactory & factory)
{
    factory.registerFunction("avgWeighted", createAggregateFunctionAvgWeighted, AggregateFunctionFactory::CaseSensitive);
}
}
