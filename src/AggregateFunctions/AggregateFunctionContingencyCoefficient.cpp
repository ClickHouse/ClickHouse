#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/CrossTab.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <memory>
#include <cmath>


namespace DB
{

namespace
{

struct ContingencyData : CrossTabData
{
    static const char * getName()
    {
        return "contingency";
    }

    Float64 getResult() const
    {
        if (count < 2)
            return std::numeric_limits<Float64>::quiet_NaN();

        Float64 phi = getPhiSquared();
        return sqrt(phi / (phi + count));
    }
};

}

void registerAggregateFunctionContingency(AggregateFunctionFactory & factory)
{
    factory.registerFunction(ContingencyData::getName(),
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            assertBinary(name, argument_types);
            assertNoParameters(name, parameters);
            return std::make_shared<AggregateFunctionCrossTab<ContingencyData>>(argument_types);
        });
}

}
