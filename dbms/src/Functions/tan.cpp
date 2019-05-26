#include <Functions/FunctionMathUnaryFloat64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

struct TanName { static constexpr auto name = "tan"; };
using FunctionTan = FunctionMathUnaryFloat64<UnaryFunctionVectorized<TanName, tan>>;

void registerFunctionTan(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTan>(FunctionFactory::CaseInsensitive);
}

}
