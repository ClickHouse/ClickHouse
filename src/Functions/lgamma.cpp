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
    factory.registerFunction<FunctionLGamma>();
}

}
