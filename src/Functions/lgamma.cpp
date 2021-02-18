#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

#if defined(OS_DARWIN)
extern "C"
{
    /// Is defined in libglibc-compatibility.a
    double lgamma_r(double x, int * signgamp);
}
#endif

namespace DB
{

/// Use wrapper and use lgamma_r version because std::lgamma is not threadsafe.
static Float64 lgamma_wrapper(Float64 arg)
{
    int signp;
    return lgamma_r(arg, &signp);
}

struct LGammaName { static constexpr auto name = "lgamma"; };
using FunctionLGamma = FunctionMathUnary<UnaryFunctionPlain<LGammaName, lgamma_wrapper>>;

void registerFunctionLGamma(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLGamma>();
}

}
