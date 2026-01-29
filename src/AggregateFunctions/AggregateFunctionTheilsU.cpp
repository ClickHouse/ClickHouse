#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/CrossTab.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <memory>
#include <cmath>


namespace DB
{

namespace
{

struct TheilsUData : CrossTabData
{
    static const char * getName()
    {
        return "theilsU";
    }

    /// Based on https://en.wikipedia.org/wiki/Uncertainty_coefficient.
    Float64 getResult() const
    {
        if (count < 2)
            return std::numeric_limits<Float64>::quiet_NaN();

        Float64 h_a = 0.0;
        for (const auto & [key, value] : count_a)
        {
            Float64 value_float = value;
            Float64 prob_a = value_float / count;
            h_a += prob_a * log(prob_a);
        }

        if (h_a == 0.0)
            return 0.0;

        Float64 dep = 0.0;
        for (const auto & [key, value] : count_ab)
        {
            Float64 value_ab = value;
            Float64 value_b = count_b.at(key.items[UInt128::_impl::little(1)]);
            Float64 prob_ab = value_ab / count;
            Float64 prob_a_given_b = value_ab / value_b;
            dep += prob_ab * log(prob_a_given_b);
        }

        Float64 coef = (h_a - dep) / h_a;
        return coef;
    }
};

}

void registerAggregateFunctionTheilsU(AggregateFunctionFactory & factory)
{
    factory.registerFunction(TheilsUData::getName(),
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            assertBinary(name, argument_types);
            assertNoParameters(name, parameters);
            return std::make_shared<AggregateFunctionCrossTab<TheilsUData>>(argument_types);
        });
}

}
