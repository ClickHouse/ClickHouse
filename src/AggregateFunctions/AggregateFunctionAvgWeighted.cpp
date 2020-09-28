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

template <typename T>
struct AvgWeighted
{
    using FieldType = std::conditional_t<
        IsDecimalNumber<T>,
        std::conditional_t<std::is_same_v<T, Decimal256>,
            Decimal256,
            Decimal128>,
        NearestFieldType<T>>;

    using Function = AggregateFunctionAvgWeighted<T, AggregateFunctionAvgData<FieldType, FieldType>>;
};

template <typename T>
using AggregateFuncAvgWeighted = typename AvgWeighted<T>::Function;

bool allowTypes(const DataTypePtr& left, const DataTypePtr& right)
{
    return (isInteger(left) || isFloat(left)) && (isInteger(right) || isFloat(right));
}

AggregateFunctionPtr createAggregateFunctionAvgWeighted(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertBinary(name, argument_types);

    AggregateFunctionPtr res;

    const auto data_type = static_cast<const DataTypePtr>(argument_types[0]);
    const auto data_type_weight = static_cast<const DataTypePtr>(argument_types[1]);

    if (!allowTypes(data_type, data_type_weight))
        throw Exception(
            "Types " + data_type->getName() +
            " and " + data_type_weight->getName() +
            " are non-conforming as arguments for aggregate function " + name,
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    if (isDecimal(data_type))
        res.reset(createWithDecimalType<AggregateFuncAvgWeighted>(*data_type, *data_type, argument_types));
    else
        res.reset(createWithNumericType<AggregateFuncAvgWeighted>(*data_type, argument_types));

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
