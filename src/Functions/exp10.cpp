#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>
#if !USE_VECTORCLASS
#   include <common/preciseExp10.h>
#endif

namespace DB
{

struct Exp10Name { static constexpr auto name = "exp10"; };

using FunctionExp10 = FunctionMathUnary<UnaryFunctionVectorized<Exp10Name,
#if USE_VECTORCLASS
    exp10
#else
    preciseExp10
#endif
>>;

void registerFunctionExp10(FunctionFactory & factory)
{
    factory.registerFunction<FunctionExp10>();
}

}
