#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

struct Exp2Name { static constexpr auto name = "exp2"; };
using FunctionExp2 = FunctionMathUnary<UnaryFunctionVectorized<Exp2Name, exp2>>;

void registerFunctionExp2(FunctionFactory & factory)
{
    factory.registerFunction<FunctionExp2>();
}

}
