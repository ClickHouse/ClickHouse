#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsMath.h>

namespace DB
{

const double EImpl::value = 2.7182818284590452353602874713526624977572470;
const double PiImpl::value = 3.1415926535897932384626433832795028841971693;


void registerFunctionsMath(FunctionFactory & factory)
{
    factory.registerFunction<FunctionE>();
    factory.registerFunction<FunctionPi>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionExp>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionLog>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionExp2>();
    factory.registerFunction<FunctionLog2>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionExp10>();
    factory.registerFunction<FunctionLog10>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionSqrt>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionCbrt>();
    factory.registerFunction<FunctionErf>();
    factory.registerFunction<FunctionErfc>();
    factory.registerFunction<FunctionLGamma>();
    factory.registerFunction<FunctionTGamma>();
    factory.registerFunction<FunctionSin>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionCos>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionTan>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionAsin>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionAcos>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionAtan>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionPow>(FunctionFactory::CaseInsensitive);

    factory.registerAlias("power", "pow", FunctionFactory::CaseInsensitive);
    factory.registerAlias("ln", "log", FunctionFactory::CaseInsensitive);
}

}
