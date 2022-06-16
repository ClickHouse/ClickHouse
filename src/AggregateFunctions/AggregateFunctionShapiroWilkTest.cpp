#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionShapiroWilkTest.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB {

    namespace
    {

        AggregateFunctionPtr createAggregateFunctionShapiroWilkTest(const std::string & name, const DataTypes & arguments, const Array & parameters, const Settings *)
        {
            assertNoParameters(name, parameters);
            assertArityAtMost<1>(name, arguments);

            return std::make_shared<AggregateFunctionShapiroWilkTest>(arguments, parameters);
        }

    }

    void registerAggregateFunctionShapiroWilkTest(AggregateFunctionFactory & factory)
    {
        AggregateFunctionProperties properties = { .is_order_dependent = false };
        factory.registerFunction("shapiro", {createAggregateFunctionShapiroWilkTest, properties}, AggregateFunctionFactory::CaseInsensitive);
    }

}
