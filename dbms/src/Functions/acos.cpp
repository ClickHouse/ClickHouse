#include <Functions/FunctionMathUnaryFloat64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

struct AcosName { static constexpr auto name = "acos"; };
using FunctionAcos = FunctionMathUnaryFloat64<UnaryFunctionVectorized<AcosName, acos>>;

void registerFunctionAcos(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAcos>(FunctionFactory::CaseInsensitive);
}

}
