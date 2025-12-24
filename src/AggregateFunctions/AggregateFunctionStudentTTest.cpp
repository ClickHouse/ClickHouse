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

        if (unlikely(!std::isfinite(t_stat)))
            return {std::numeric_limits<Float64>::quiet_NaN(), std::numeric_limits<Float64>::quiet_NaN()};

        auto student = boost::math::students_t_distribution<Float64>(getDegreesOfFreedom());
        Float64 pvalue = 0;
        if (t_stat > 0)
            pvalue = 2 * boost::math::cdf<Float64>(student, -t_stat);
        else
            pvalue = 2 * boost::math::cdf<Float64>(student, t_stat);

        return {t_stat, pvalue};
    }
};

AggregateFunctionPtr createAggregateFunctionStudentTTest(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertBinary(name, argument_types);

    if (parameters.size() > 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires zero or one parameter.", name);

    if (!isNumber(argument_types[0]) || !isNumber(argument_types[1]))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Aggregate function {} only supports numerical types", name);

    return std::make_shared<AggregateFunctionTTest<StudentTTestData>>(argument_types, parameters);
}

}

void registerAggregateFunctionStudentTTest(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Applies Student's t-test to samples from two populations.

Values of both samples are in the `sample_data` column.
If `sample_index` equals 0, then the value in that row belongs to the sample from the first population.
Otherwise, it belongs to the sample from the second population.
The null hypothesis is that the means of the populations are equal.
Normal distribution with equal variances is assumed.

    )";
    FunctionDocumentation::Syntax syntax = R"(
studentTTest([confidence_level])(sample_data, sample_index)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"sample_data", "Sample data.", {"Integer", "Float", "Decimal"}},
        {"sample_index", "Sample index.", {"Integer"}}
    };
    FunctionDocumentation::Parameters parameters = {
        {"confidence_level", "Optional. Confidence level in order to calculate confidence intervals.", {"Float"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a tuple with two or four elements (if the optional `confidence_level` is specified): calculated t-statistic, calculated p-value, and optionally calculated confidence-interval-low and confidence-interval-high.", {"Tuple(Float64, Float64)", "Tuple(Float64, Float64, Float64, Float64)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Student's t-test on sample data",
        R"(
CREATE TABLE student_ttest (sample_data Float64, sample_index UInt8) ENGINE = Memory;
INSERT INTO student_ttest VALUES (20.3, 0), (21.1, 0), (21.9, 1), (21.7, 0), (19.9, 1), (21.8, 1);

SELECT studentTTest(sample_data, sample_index) FROM student_ttest;
        )",
        R"(
┌─studentTTest(sample_data, sample_index)───┐
│ (-0.21739130434783777,0.8385421208415731) │
└───────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction("studentTTest", {createAggregateFunctionStudentTTest, {}, documentation});
}

}
