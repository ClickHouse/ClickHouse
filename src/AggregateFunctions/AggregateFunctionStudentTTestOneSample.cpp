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
    FunctionDocumentation::Description description_studentTTestOneSample = R"(
Applies the one-sample Student's t-test to determine whether the mean of a sample differs from a known population mean.

Normality is assumed. The null hypothesis is that the sample mean equals the population mean.

The optional `confidence_level` enables confidence interval calculation.

**Notes:**
- At least 2 observations are required; otherwise the result is `(nan, nan)` (and intervals if requested are `nan`).
- Constant or near-constant input will also return `nan` due to zero (or effectively zero) standard error.

**See Also**

- [Student's t-test](https://en.wikipedia.org/wiki/Student%27s_t-test)
- [studentTTest function](/sql-reference/aggregate-functions/reference/studentttest)
    )";
    FunctionDocumentation::Syntax syntax_studentTTestOneSample = R"(
studentTTestOneSample([confidence_level])(sample_data, population_mean)
    )";
    FunctionDocumentation::Parameters parameters_studentTTestOneSample = {
        {"confidence_level", "Optional. Confidence level for confidence intervals. Float in (0, 1).", {"Float*"}}
    };
    FunctionDocumentation::Arguments arguments_studentTTestOneSample = {
        {"sample_data", "Sample data.", {"Integer", "Float", "Decimal"}},
        {"population_mean", "Known population mean to test against (usually a constant).", {"(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_studentTTestOneSample = {"Returns a tuple with two or four elements (if `confidence_level` is specified): calculated t-statistic, calculated p-value (two-tailed), [calculated confidence-interval-low], [calculated confidence-interval-high]. Confidence intervals are for the sample mean at the given confidence level.", {"Tuple(Float64, Float64)", "Tuple(Float64, Float64, Float64, Float64)"}};
    FunctionDocumentation::Examples examples_studentTTestOneSample = {
    {
        "Without confidence interval",
        R"(
SELECT studentTTestOneSample()(value, 20.0) FROM t;
        )",
        R"(
        )"
    },
    {
        "With confidence interval (95%)",
        R"(
SELECT studentTTestOneSample(0.95)(value, 20.0) FROM t;
        )",
        R"(
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_studentTTestOneSample = {25, 10};
    FunctionDocumentation::Category category_studentTTestOneSample = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_studentTTestOneSample = {description_studentTTestOneSample, syntax_studentTTestOneSample, arguments_studentTTestOneSample, parameters_studentTTestOneSample, returned_value_studentTTestOneSample, examples_studentTTestOneSample, introduced_in_studentTTestOneSample, category_studentTTestOneSample};

    factory.registerFunction("studentTTestOneSample", {createAggregateFunctionStudentTTestOneSample, documentation_studentTTestOneSample});
}

}
