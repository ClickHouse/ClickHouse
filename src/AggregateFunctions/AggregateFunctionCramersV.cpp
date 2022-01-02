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

        Float64 phi = 0.0;
        for (const auto & [key, value_ab] : count_ab)
        {
            Float64 value_a = count_a.at(key.items[0]);
            Float64 value_b = count_b.at(key.items[1]);

            phi += value_ab * value_ab / (value_a * value_b) * count - 2 * value_ab + (value_a * value_b) / count;
        }

        phi /= count;
        UInt64 q = std::min(count_a.size(), count_b.size());
        phi /= q - 1;

        return sqrt(phi);
    }
};

}

void registerAggregateFunctionCramersV(AggregateFunctionFactory & factory)
{
    factory.registerFunction(CramersVData::getName(),
 [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            assertNoParameters(name, parameters);
            return std::make_shared<AggregateFunctionCrossTab<CramersVData>>(argument_types);
        });
}

}
