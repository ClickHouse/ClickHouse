#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionMeanZTest.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Moments.h>


namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


namespace DB
{
struct Settings;

namespace
{

struct MeanZTestData : public ZTestMoments<Float64>
{
    static constexpr auto name = "meanZTest";

    std::pair<Float64, Float64> getResult(Float64 pop_var_x, Float64 pop_var_y) const
    {
        Float64 mean_x = getMeanX();
        Float64 mean_y = getMeanY();

        /// z = \frac{\bar{X_{1}} - \bar{X_{2}}}{\sqrt{\frac{\sigma_{1}^{2}}{n_{1}} + \frac{\sigma_{2}^{2}}{n_{2}}}}
        Float64 zstat = (mean_x - mean_y) / getStandardError(pop_var_x, pop_var_y);
        if (!std::isfinite(zstat))
        {
            return {std::numeric_limits<Float64>::quiet_NaN(), std::numeric_limits<Float64>::quiet_NaN()};
        }

        Float64 pvalue = 2.0 * boost::math::cdf(boost::math::normal(0.0, 1.0), -1.0 * std::abs(zstat));

        return {zstat, pvalue};
    }
};

AggregateFunctionPtr createAggregateFunctionMeanZTest(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertBinary(name, argument_types);

    if (parameters.size() != 3)
        throw Exception("Aggregate function " + name + " requires three parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (!isNumber(argument_types[0]) || !isNumber(argument_types[1]))
        throw Exception("Aggregate function " + name + " only supports numerical types", ErrorCodes::BAD_ARGUMENTS);

    return std::make_shared<AggregateFunctionMeanZTest<MeanZTestData>>(argument_types, parameters);
}

}

void registerAggregateFunctionMeanZTest(AggregateFunctionFactory & factory)
{
    factory.registerFunction("meanZTest", createAggregateFunctionMeanZTest);
}

}
