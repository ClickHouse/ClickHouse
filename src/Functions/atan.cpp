#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

struct AtanName { static constexpr auto name = "atan"; };
using FunctionAtan = FunctionMathUnary<UnaryFunctionVectorized<AtanName, atan>>;

void registerFunctionAtan(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAtan>(FunctionFactory::CaseInsensitive);
}

}
