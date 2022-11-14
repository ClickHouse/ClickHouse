#include <Functions/FunctionMathBinaryFloat64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{

struct PowName { static constexpr auto name = "pow"; };
using FunctionPow = FunctionMathBinaryFloat64<BinaryFunctionVectorized<PowName, pow>>;

}

REGISTER_FUNCTION(Pow)
{
    factory.registerFunction<FunctionPow>(FunctionFactory::CaseInsensitive);
    factory.registerAlias("power", "pow", FunctionFactory::CaseInsensitive);
}

}
