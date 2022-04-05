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

    Float64 getResult() const
    {
        if (count < 2)
            return std::numeric_limits<Float64>::quiet_NaN();

        Float64 h_a = 0.0;
        for (const auto & [key, value] : count_a)
        {
            Float64 value_float = value;
            h_a += (value_float / count) * log(value_float / count);
        }

        Float64 dep = 0.0;
        for (const auto & [key, value] : count_ab)
        {
            Float64 value_ab = value;
            Float64 value_b = count_b.at(key.items[1]);

            dep += (value_ab / count) * log(value_ab / value_b);
        }

        dep -= h_a;
        dep /= h_a;
        return dep;
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
