#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>
#include <base/preciseExp10.h>

namespace DB
{
namespace
{

struct Exp10Name { static constexpr auto name = "exp10"; };
using FunctionExp10 = FunctionMathUnary<UnaryFunctionVectorized<Exp10Name, preciseExp10>>;

}

REGISTER_FUNCTION(Exp10)
{
    factory.registerFunction<FunctionExp10>();
}

}
