#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>
#include <common/preciseExp10.h>

namespace DB
{

struct Exp10Name { static constexpr auto name = "exp10"; };

using FunctionExp10 = FunctionMathUnary<UnaryFunctionVectorized<Exp10Name, preciseExp10>>;

void registerFunctionExp10(FunctionFactory & factory)
{
    factory.registerFunction<FunctionExp10>();
}

}
