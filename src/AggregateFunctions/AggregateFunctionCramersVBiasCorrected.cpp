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

        Float64 phi = getPhiSquared();

        Float64 a_size_adjusted = count_a.size() - 1;
        Float64 b_size_adjusted = count_b.size() - 1;
        Float64 count_adjusted = count - 1;

        Float64 res = std::max(0.0, phi - a_size_adjusted * b_size_adjusted / count_adjusted);
        Float64 correction_a = count_a.size() - a_size_adjusted * a_size_adjusted / count_adjusted;
        Float64 correction_b = count_b.size() - b_size_adjusted * b_size_adjusted / count_adjusted;

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
