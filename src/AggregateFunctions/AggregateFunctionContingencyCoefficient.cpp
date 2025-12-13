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

    /**
    * Based on https://en.wikipedia.org/wiki/Contingency_table#Cram%C3%A9r's_V_and_the_contingency_coefficient_C
    *
    * Pearson's contingency coefficient is defined as:
    * C = sqrt(χ² / (χ² + n)), where χ² is the chi-squared statistic and n is the total number of observations.
    *
    * We have,
    * phi_squared, φ² = χ² / n
    * => χ² = n · φ².
    *
    * Substituting χ² into the formula for C,
    * C = sqrt((n · φ²) / (n · φ² + n))
    *   = sqrt(φ² / (φ² + 1)).
    **/

    Float64 getResult() const
    {
        if (count < 2)
            return std::numeric_limits<Float64>::quiet_NaN();

        Float64 phi_sq = getPhiSquared();
        return std::sqrt(phi_sq / (phi_sq + 1.0));
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
