#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionSumCount.h>
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
struct SumSimple
{
    /// @note It uses slow Decimal128 (cause we need such a variant). sumWithOverflow is faster for Decimal32/64
    using ResultType = std::conditional_t<IsDecimalNumber<T>,
                                        std::conditional_t<std::is_same_v<T, Decimal256>, Decimal256, Decimal128>,
                                        NearestFieldType<T>>;
    using AggregateDataType = AggregateFunctionSumCountData<ResultType>;
    using Function = AggregateFunctionSumCount<T, ResultType, AggregateDataType>;
};



template <typename T> using AggregateFunctionSumCountSimple = typename SumSimple<T>::Function;

template <template <typename> class Function>
AggregateFunctionPtr createAggregateFunctionSumCount(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    AggregateFunctionPtr res;
    DataTypePtr data_type = argument_types[0];
    if (isDecimal(data_type))
        res.reset(createWithDecimalType<Function>(*data_type, *data_type, argument_types));
    else
        res.reset(createWithNumericType<Function>(*data_type, argument_types));

    if (!res)
        throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    return res;
}

}

void registerAggregateFunctionSumCount(AggregateFunctionFactory & factory)
{
    factory.registerFunction("sumCount", createAggregateFunctionSumCount<AggregateFunctionSumCountSimple>);
}

}
