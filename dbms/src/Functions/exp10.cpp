#include <Functions/FunctionMathUnaryFloat64.h>
#include <Functions/FunctionFactory.h>
#include <common/preciseExp10.h>

namespace DB
{

struct Exp10Name { static constexpr auto name = "exp10"; };

using FunctionExp10 = FunctionMathUnaryFloat64<UnaryFunctionVectorized<Exp10Name,
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
