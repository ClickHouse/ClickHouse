#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{

struct Log10Name { static constexpr auto name = "log10"; };
using FunctionLog10 = FunctionMathUnary<UnaryFunctionVectorized<Log10Name, log10>>;

}

void registerFunctionLog10(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLog10>(FunctionFactory::CaseInsensitive);
}

}
