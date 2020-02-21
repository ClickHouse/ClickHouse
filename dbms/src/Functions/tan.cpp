#include <Functions/FunctionMathUnary.h>


namespace DB
{

struct TanName { static constexpr auto name = "tan"; };
using FunctionTan = FunctionMathUnary<UnaryFunctionVectorized<TanName, tan>>;

void registerFunctionTan(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTan>(FunctionFactory::CaseInsensitive);
}

}
