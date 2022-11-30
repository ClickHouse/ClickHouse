#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionTTest.h>
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

/** Student T-test applies to two samples of independent random variables
  * that have normal distributions with equal (but unknown) variances.
  * It allows to answer the question whether means of the distributions differ.
  *
  * If variances are not considered equal, Welch T-test should be used instead.
  */
struct StudentTTestData : public TTestMoments<Float64>
{
    static constexpr auto name = "studentTTest";

    bool hasEnoughObservations() const
    {
        return nx > 0 && ny > 0 && nx + ny > 2;
    }

    Float64 getDegreesOfFreedom() const
    {
        return nx + ny - 2;
    }

    std::tuple<Float64, Float64> getResult() const
    {
        Float64 mean_x = getMeanX();
        Float64 mean_y = getMeanY();

        /// To estimate the variance we first estimate two means.
        /// That's why the number of degrees of freedom is the total number of values of both samples minus 2.
        Float64 degrees_of_freedom = getDegreesOfFreedom();

        /// Calculate s^2
        /// The original formulae looks like
        /// \frac{\sum_{i = 1}^{n_x}{(x_i - \bar{x}) ^ 2} + \sum_{i = 1}^{n_y}{(y_i - \bar{y}) ^ 2}}{n_x + n_y - 2}
        /// But we made some mathematical transformations not to store original sequences.
        /// Also we dropped sqrt, because later it will be squared later.

        Float64 all_x = x2 + nx * mean_x * mean_x - 2 * mean_x * x1;
        Float64 all_y = y2 + ny * mean_y * mean_y - 2 * mean_y * y1;

        Float64 s2 = (all_x + all_y) / degrees_of_freedom;
        Float64 std_err2 = s2 * (1. / nx + 1. / ny);

        /// t-statistic
        Float64 t_stat = (mean_x - mean_y) / sqrt(std_err2);

        return {t_stat, getPValue(degrees_of_freedom, t_stat * t_stat)};
    }
};

AggregateFunctionPtr createAggregateFunctionStudentTTest(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertBinary(name, argument_types);

    if (parameters.size() > 1)
        throw Exception("Aggregate function " + name + " requires zero or one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (!isNumber(argument_types[0]) || !isNumber(argument_types[1]))
        throw Exception("Aggregate function " + name + " only supports numerical types", ErrorCodes::BAD_ARGUMENTS);

    return std::make_shared<AggregateFunctionTTest<StudentTTestData>>(argument_types, parameters);
}

}

void registerAggregateFunctionStudentTTest(AggregateFunctionFactory & factory)
{
    factory.registerFunction("studentTTest", createAggregateFunctionStudentTTest);
}

}
