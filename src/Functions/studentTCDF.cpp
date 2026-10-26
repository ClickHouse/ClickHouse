#include <Functions/FunctionFactory.h>
#include <Functions/FunctionMathBinaryFloat64.h>

#include <boost/math/distributions/students_t.hpp>

namespace DB
{
namespace
{

/// The function is evaluated per row, so a single bad value must not abort the query. Under this
/// policy Boost returns NaN instead of throwing, which is what the t-test aggregate functions also
/// return for a degenerate sample: zero, negative and NaN degrees of freedom all give NaN.
using NoThrowPolicy = boost::math::policies::policy<
    boost::math::policies::domain_error<boost::math::policies::ignore_error>,
    boost::math::policies::pole_error<boost::math::policies::ignore_error>,
    boost::math::policies::overflow_error<boost::math::policies::ignore_error>,
    boost::math::policies::evaluation_error<boost::math::policies::ignore_error>>;

Float64 studentTCDF(Float64 x, Float64 degrees_of_freedom)
{
    boost::math::students_t_distribution<Float64, NoThrowPolicy> distribution(degrees_of_freedom);
    return boost::math::cdf(distribution, x);
}

struct StudentTCDFName { static constexpr auto name = "studentTCDF"; };
using FunctionStudentTCDF = FunctionMathBinaryFloat64<BinaryFunctionVectorized<StudentTCDFName, studentTCDF>>;

}

REGISTER_FUNCTION(StudentTCDF)
{
    FunctionDocumentation::Description description = R"(
Returns the cumulative distribution function of Student's t-distribution with `df` degrees of freedom evaluated at `x`, that is, the probability that a t-distributed random variable takes a value not greater than `x`.

The t-test aggregate functions derive the statistic and the degrees of freedom from the sample themselves. This function instead evaluates the distribution directly, which makes it possible to obtain a p-value for a statistic that was computed elsewhere — for example by a weighted test, whose degrees of freedom are not whole numbers.

`df` may be any positive real number, not only an integer. As `df` grows the distribution approaches the standard normal one, so `studentTCDF(x, df)` approaches `(1 + erf(x / sqrt(2))) / 2`.

**See Also**

- [Student's t-distribution](https://en.wikipedia.org/wiki/Student%27s_t-distribution)
- [studentTTestOneSample function](/reference/functions/aggregate-functions/studentTTestOneSample)
)";
    FunctionDocumentation::Syntax syntax = "studentTCDF(x, df)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The value at which to evaluate the distribution.", {"(U)Int*", "Float*", "BFloat16", "Decimal*"}},
        {"df", "Degrees of freedom. Any positive real number; `NaN` is returned otherwise.", {"(U)Int*", "Float*", "BFloat16", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the probability that a Student t-distributed random variable with `df` degrees of freedom is not greater than `x`, or `NaN` if `df` is not positive", {"Float64"}};
    FunctionDocumentation::Examples examples = {
        {
            "Two-tailed p-value for a t-statistic with fractional degrees of freedom",
            "SELECT 2 * studentTCDF(-abs(2.1), 12.75)",
            "0.056219619193416864"
        },
        {
            "Approaches the standard normal distribution as the degrees of freedom grow",
            "SELECT studentTCDF(1.96, 4), studentTCDF(1.96, 1000000)",
            "0.9392226800749082\t0.9750019662073651"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionStudentTCDF>(documentation);
}

}
