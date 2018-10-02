#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionAvg.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>

namespace DB
{

namespace
{

template <typename T>
struct Avg
{
    using FieldType = std::conditional_t<IsDecimalNumber<T>, Decimal128, typename NearestFieldType<T>::Type>;
    using Function = AggregateFunctionAvg<T, AggregateFunctionAvgData<FieldType>>;
};

template <typename T>
using AggregateFuncAvg = typename Avg<T>::Function;

AggregateFunctionPtr createAggregateFunctionAvg(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    AggregateFunctionPtr res;
    DataTypePtr data_type = argument_types[0];
    if (isDecimal(data_type))
        res.reset(createWithDecimalType<AggregateFuncAvg>(*data_type));
    else
        res.reset(createWithNumericType<AggregateFuncAvg>(*data_type));

    if (!res)
        throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    return res;
}

}

void registerAggregateFunctionAvg(AggregateFunctionFactory & factory)
{
    factory.registerFunction("avg", createAggregateFunctionAvg, AggregateFunctionFactory::CaseInsensitive);
}

}
