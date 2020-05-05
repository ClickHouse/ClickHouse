#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionWelchTTest.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{

namespace
{

template <typename X, typename Y, typename Ret>
AggregateFunctionPtr createAggregateFunctionWelchTTest(const DataTypes & argument_types, const Array & parameters)
{
    return std::make_shared<AggregateFunctionWelchTTest<X, Y, Ret>>(argument_types, parameters);

}

}

void registerAggregateFunctionWelchTTest(AggregateFunctionFactory & factory)
{

    factory.registerFunction("WelchTTest", createAggregateFunctionWelchTTest, AggregateFunctionFactory::CaseInsensitive);
}

}
