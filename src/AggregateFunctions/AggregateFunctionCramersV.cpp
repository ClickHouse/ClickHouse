#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/CrossTab.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <memory>
#include <cmath>


namespace DB
{

namespace
{

struct CramersVData : CrossTabData
{
    static const char * getName()
    {
        return "cramersV";
    }

    Float64 getResult() const
    {
        if (count < 2)
            return std::numeric_limits<Float64>::quiet_NaN();

        UInt64 q = std::min(count_a.size(), count_b.size());
        return sqrt(getPhiSquared() / (q - 1));
    }
};

}

void registerAggregateFunctionCramersV(AggregateFunctionFactory & factory)
{
    factory.registerFunction(CramersVData::getName(),
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            assertBinary(name, argument_types);
            assertNoParameters(name, parameters);
            return std::make_shared<AggregateFunctionCrossTab<CramersVData>>(argument_types);
        });
}

}
