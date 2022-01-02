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
        static const char * getName() { return "contingency"; }

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

            return sqrt(phi / (phi + count));
        }
    };
}

void registerAggregateFunctionContingency(AggregateFunctionFactory & factory)
{
    factory.registerFunction(ContingencyData::getName(),
    [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            assertNoParameters(name, parameters);
            return std::make_shared<AggregateFunctionCrossTab<ContingencyData>>(argument_types);
        });
}

}
