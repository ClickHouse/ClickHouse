#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/CrossTab.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <memory>
#include <cmath>


namespace DB
{

namespace
{

struct CramersVBiasCorrectedData : CrossTabData
{
    static const char * getName()
    {
        return "cramersVBiasCorrected";
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

        Float64 res = std::max(0.0, phi - (static_cast<Float64>(count_a.size()) - 1) * (static_cast<Float64>(count_b.size()) - 1) / (count - 1));
        Float64 correction_a = count_a.size() - (static_cast<Float64>(count_a.size()) - 1) * (static_cast<Float64>(count_a.size()) - 1) / (count - 1);
        Float64 correction_b = count_b.size() - (static_cast<Float64>(count_b.size()) - 1) * (static_cast<Float64>(count_b.size()) - 1) / (count - 1);
        res /= std::min(correction_a, correction_b) - 1;

        return sqrt(res);
    }
};

}

void registerAggregateFunctionCramersVBiasCorrected(AggregateFunctionFactory & factory)
{
    factory.registerFunction(CramersVBiasCorrectedData::getName(),
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            assertBinary(name, argument_types);
            assertNoParameters(name, parameters);
            return std::make_shared<AggregateFunctionCrossTab<CramersVBiasCorrectedData>>(argument_types);
        });
}

}
