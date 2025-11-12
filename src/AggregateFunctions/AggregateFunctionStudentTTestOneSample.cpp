#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionOneSampleTTest.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Moments.h>
#include <boost/math/distributions/students_t.hpp>


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

/** One-sample Student T-test applies to a single sample of independent random variables
  * that has a normal distribution. It allows to answer the question whether the mean
  * of the sample differs from a known population mean.
  */
struct StudentTTestOneSampleData : public OneSampleTTestMoments<Float64>
{
    static constexpr auto name = "studentTTestOneSample";

    std::tuple<Float64, Float64> getResult() const
    {
        if (!hasEnoughObservations())
            return {std::numeric_limits<Float64>::quiet_NaN(), std::numeric_limits<Float64>::quiet_NaN()};

        Float64 t_statistic = getTStatistic();

        if (unlikely(!std::isfinite(t_statistic)))
            return {std::numeric_limits<Float64>::quiet_NaN(), std::numeric_limits<Float64>::quiet_NaN()};

        Float64 degrees_of_freedom = getDegreesOfFreedom();
        auto student = boost::math::students_t_distribution<Float64>(degrees_of_freedom);
        Float64 p_value = 0;

        if (t_statistic > 0)
            p_value = 2 * boost::math::cdf<Float64>(student, -t_statistic);
        else
            p_value = 2 * boost::math::cdf<Float64>(student, t_statistic);

        return {t_statistic, p_value};
    }
};

AggregateFunctionPtr createAggregateFunctionStudentTTestOneSample(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    if (argument_types.size() != 1 && argument_types.size() != 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires one or two arguments.", name);

    if (!isNumber(argument_types[0]) || (argument_types.size() == 2 && !isNumber(argument_types[1])))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Aggregate function {} only supports numerical types", name);

    if (parameters.size() > 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} allows at most one parameter (confidence level).", name);

    return std::make_shared<AggregateFunctionOneSampleTTest<StudentTTestOneSampleData>>(argument_types, parameters);
}

}

void registerAggregateFunctionStudentTTestOneSample(AggregateFunctionFactory & factory)
{
    factory.registerFunction("studentTTestOneSample", createAggregateFunctionStudentTTestOneSample);
}

}
