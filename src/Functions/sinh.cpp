#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{

struct SinhName { static constexpr auto name = "sinh"; };
using FunctionSinh = FunctionMathUnary<UnaryFunctionVectorized<SinhName, sinh>>;

}

void registerFunctionSinh(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSinh>();
}

}
