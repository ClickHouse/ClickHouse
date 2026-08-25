#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

#if defined(OS_DARWIN)
extern "C"
{
    double lgamma_r(double x, int * signgamp);
}
#endif

namespace DB
{
namespace
{

/// Use wrapper and use lgamma_r version because std::lgamma is not threadsafe.
Float64 lgamma_wrapper(Float64 arg)
{
    if (arg < 0 && arg == std::floor(arg))
        return std::numeric_limits<Float64>::quiet_NaN();

    int signp;
    return lgamma_r(arg, &signp);
}

struct LGammaName { static constexpr auto name = "lgamma"; };
using FunctionLGamma = FunctionMathUnary<UnaryFunctionVectorized<LGammaName, lgamma_wrapper>>;

}

REGISTER_FUNCTION(LGamma)
{
    FunctionDocumentation::Description description = R"(
Returns the logarithm of the gamma function.
)";
    FunctionDocumentation::Syntax syntax = "lgamma(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The number for which to compute the logarithm of the gamma function.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the logarithm of the gamma function of `x`.", {"Float*"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT lgamma(5);", "3.1780538303479458"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionLGamma>(documentation);
}

}
