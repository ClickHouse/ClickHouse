#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{

struct SinName { static constexpr auto name = "sin"; };
using FunctionSin = FunctionMathUnary<UnaryFunctionVectorized<SinName, sin>>;

}

REGISTER_FUNCTION(Sin)
{
    factory.registerFunction<FunctionSin>(FunctionFactory::CaseInsensitive);
}

}
