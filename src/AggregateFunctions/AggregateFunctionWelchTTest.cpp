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
    /// welchTTest documentation
    FunctionDocumentation::Description description_welchTTest = R"(
Applies [Welch's t-test](https://en.wikipedia.org/wiki/Welch%27s_t-test) to samples from two populations.

Values of both samples are in the `sample_data` column.
If `sample_index` equals to 0 then the value in that row belongs to the sample from the first population.
Otherwise it belongs to the sample from the second population.
The null hypothesis is that means of populations are equal.
Normal distribution is assumed.
Populations may have unequal variance.
    )";
    FunctionDocumentation::Syntax syntax_welchTTest = R"(
welchTTest([confidence_level])(sample_data, sample_index)
    )";
    FunctionDocumentation::Parameters parameters_welchTTest = {
        {"confidence_level", "Optional. Confidence level in order to calculate confidence intervals.", {"Float"}}
    };
    FunctionDocumentation::Arguments arguments_welchTTest = {
        {"sample_data", "Sample data.", {"Int*", "UInt*", "Float*", "Decimal*"}},
        {"sample_index", "Sample index.", {"Int*", "UInt*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_welchTTest = {"Returns a Tuple with two or four elements (if the optional `confidence_level` is specified): calculated t-statistic, calculated p-value, and optionally calculated confidence-interval-low and confidence-interval-high.", {"Tuple(Float64, Float64)", "Tuple(Float64, Float64, Float64, Float64)"}};
    FunctionDocumentation::Examples examples_welchTTest = {
    {
        "Basic Welch's t-test",
        R"(
CREATE TABLE welch_ttest (sample_data Float64, sample_index UInt8) ENGINE = Memory;
INSERT INTO welch_ttest VALUES (20.3, 0), (22.1, 0), (21.9, 0), (18.9, 1), (20.3, 1), (19, 1);

SELECT welchTTest(sample_data, sample_index) FROM welch_ttest;
        )",
        R"(
┌─welchTTest(sample_data, sample_index)──────┐
│ (2.7988719532211235, 0.051807360348581945) │
└────────────────────────────────────────────┘
        )"
    },
    {
        "With confidence level",
        R"(
SELECT welchTTest(0.95)(sample_data, sample_index) FROM welch_ttest;
        )",
        R"(
┌─welchTTest(0.95)(sample_data, sample_index)─────────────────────────────────────────┐
│ (2.7988719532211235, 0.05180736034858519, -0.026294346671631885, 4.092961013338302) │
└─────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_welchTTest = {21, 1};
    FunctionDocumentation::Category category_welchTTest = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_welchTTest = {description_welchTTest, syntax_welchTTest, arguments_welchTTest, parameters_welchTTest, returned_value_welchTTest, examples_welchTTest, introduced_in_welchTTest, category_welchTTest};

    factory.registerFunction("welchTTest", {createAggregateFunctionWelchTTest, documentation_welchTTest});
}

}
