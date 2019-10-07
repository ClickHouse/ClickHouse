#include <Functions/FunctionMathUnaryFloat64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

struct Log10Name { static constexpr auto name = "log10"; };
using FunctionLog10 = FunctionMathUnaryFloat64<UnaryFunctionVectorized<Log10Name, log10>>;

void registerFunctionLog10(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLog10>(FunctionFactory::CaseInsensitive);
}

}
