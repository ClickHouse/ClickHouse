#include <memory>
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
bool allowTypes(const DataTypePtr& left, const DataTypePtr& right) noexcept
{
    const WhichDataType l_dt(left), r_dt(right);

    constexpr auto allow = [](WhichDataType t)
    {
        return t.isInt() || t.isUInt() || t.isFloat() || t.isDecimal();
    };

    return allow(l_dt) && allow(r_dt);
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

    return std::make_shared<AggregateFunctionAvgWeighted>(argument_types);
}
}

void registerAggregateFunctionAvgWeighted(AggregateFunctionFactory & factory)
{
    factory.registerFunction("avgWeighted", createAggregateFunctionAvgWeighted, AggregateFunctionFactory::CaseSensitive);
}
}
