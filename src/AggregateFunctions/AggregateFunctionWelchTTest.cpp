#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionTTest.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Moments.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
}

namespace
{

struct WelchTTestData : public TTestMoments<Float64>
{
    static constexpr auto name = "welchTTest";

    bool hasEnoughObservations() const
    {
        return nx > 1 && ny > 1;
    }

    Float64 getDegreesOfFreedom() const
    {
        Float64 mean_x = getMeanX();
        Float64 mean_y = getMeanY();

        Float64 sx2 = (x2 + nx * mean_x * mean_x - 2 * mean_x * x1) / (nx - 1);
        Float64 sy2 = (y2 + ny * mean_y * mean_y - 2 * mean_y * y1) / (ny - 1);

        Float64 numerator_sqrt = sx2 / nx + sy2 / ny;
        Float64 numerator = numerator_sqrt * numerator_sqrt;

        Float64 denominator_x = sx2 * sx2 / (nx * nx * (nx - 1));
        Float64 denominator_y = sy2 * sy2 / (ny * ny * (ny - 1));

        auto result = numerator / (denominator_x + denominator_y);

        if (result <= 0 || std::isinf(result) || isNaN(result))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Cannot calculate p_value, because the t-distribution \
                has inappropriate value of degrees of freedom (={}). It should be > 0", result);

        return result;
    }

    std::tuple<Float64, Float64> getResult() const
    {
        Float64 mean_x = getMeanX();
        Float64 mean_y = getMeanY();

        /// t-statistic
        Float64 se = getStandardError();
        Float64 t_stat = (mean_x - mean_y) / se;

        if (unlikely(!std::isfinite(t_stat)))
            return {std::numeric_limits<Float64>::quiet_NaN(), std::numeric_limits<Float64>::quiet_NaN()};

        auto students_t_distribution = boost::math::students_t_distribution<Float64>(getDegreesOfFreedom());
        Float64 pvalue = 0;
        if (t_stat > 0)
            pvalue = 2 * boost::math::cdf<Float64>(students_t_distribution, -t_stat);
        else
            pvalue = 2 * boost::math::cdf<Float64>(students_t_distribution, t_stat);

        return {t_stat, pvalue};
    }
};

AggregateFunctionPtr createAggregateFunctionWelchTTest(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertBinary(name, argument_types);

    if (parameters.size() > 1)
        throw Exception(ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION, "Aggregate function {} requires zero or one parameter.", name);

    if (!isNumber(argument_types[0]) || !isNumber(argument_types[1]))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Aggregate function {} only supports numerical types", name);

    return std::make_shared<AggregateFunctionTTest<WelchTTestData>>(argument_types, parameters);
}

}

void registerAggregateFunctionWelchTTest(AggregateFunctionFactory & factory)
{
    factory.registerFunction("welchTTest", createAggregateFunctionWelchTTest);
}

}
