#include <Functions/FunctionMathUnaryFloat64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

struct LogName { static constexpr auto name = "log"; };
using FunctionLog = FunctionMathUnaryFloat64<UnaryFunctionVectorized<LogName, log>>;

void registerFunctionLog(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLog>(FunctionFactory::CaseInsensitive);
    factory.registerAlias("ln", "log", FunctionFactory::CaseInsensitive);
}

}
