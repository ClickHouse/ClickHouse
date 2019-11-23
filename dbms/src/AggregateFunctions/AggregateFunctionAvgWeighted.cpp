#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionAvgWeighted.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>

namespace DB
{

namespace
{

template <typename T>
struct AvgWeighted
{
    using FieldType = std::conditional_t<IsDecimalNumber<T>, Decimal128, NearestFieldType<T>>;
    using Function = AggregateFunctionAvgWeighted<T, AggregateFunctionAvgWeightedData<FieldType>>;
};

template <typename T>
using AggregateFuncAvgWeighted = typename AvgWeighted<T>::Function;

AggregateFunctionPtr createAggregateFunctionAvgWeighted(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertBinary(name, argument_types);

    AggregateFunctionPtr res;
    DataTypePtr data_type = argument_types[0];
    if (isDecimal(data_type))
        res.reset(createWithDecimalType<AggregateFuncAvgWeighted>(*data_type, *data_type, argument_types));
    else
        res.reset(createWithNumericType<AggregateFuncAvgWeighted>(*data_type, argument_types));

    if (!res)
        throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    return res;
}

}

void registerAggregateFunctionAvgWeighted(AggregateFunctionFactory & factory)
{
    factory.registerFunction("AvgWeighted", createAggregateFunctionAvgWeighted, AggregateFunctionFactory::CaseInsensitive);
}

}
