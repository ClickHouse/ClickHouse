#include <Functions/FunctionMathUnaryFloat64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

struct AtanName { static constexpr auto name = "atan"; };
using FunctionAtan = FunctionMathUnaryFloat64<UnaryFunctionVectorized<AtanName, atan>>;

void registerFunctionAtan(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAtan>(FunctionFactory::CaseInsensitive);
}

}
