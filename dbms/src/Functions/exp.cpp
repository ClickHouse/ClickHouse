#include <Functions/FunctionMathUnaryFloat64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

struct ExpName { static constexpr auto name = "exp"; };
using FunctionExp = FunctionMathUnaryFloat64<UnaryFunctionVectorized<ExpName, exp>>;

void registerFunctionExp(FunctionFactory & factory)
{
    factory.registerFunction<FunctionExp>(FunctionFactory::CaseInsensitive);
}

}
