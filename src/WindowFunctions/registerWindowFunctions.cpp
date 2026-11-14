#include <WindowFunctions/registerWindowFunctions.h>

#include <WindowFunctions/WindowFunctionFactory.h>

namespace DB
{

void registerWindowFunctionRank(WindowFunctionFactory & factory);
void registerWindowFunctionDenseRank(WindowFunctionFactory & factory);
void registerWindowFunctionPercentRank(WindowFunctionFactory & factory);
void registerWindowFunctionCumeDist(WindowFunctionFactory & factory);
void registerWindowFunctionRowNumber(WindowFunctionFactory & factory);
void registerWindowFunctionNtile(WindowFunctionFactory & factory);
void registerWindowFunctionNthValue(WindowFunctionFactory & factory);
void registerWindowFunctionsLagLead(WindowFunctionFactory & factory);
void registerWindowFunctionsExponentialTimeDecayed(WindowFunctionFactory & factory);
void registerWindowFunctionNonNegativeDerivative(WindowFunctionFactory & factory);

void registerWindowFunctions()
{
    auto & factory = WindowFunctionFactory::instance();

    registerWindowFunctionRank(factory);
    registerWindowFunctionDenseRank(factory);
    registerWindowFunctionPercentRank(factory);
    registerWindowFunctionCumeDist(factory);
    registerWindowFunctionRowNumber(factory);
    registerWindowFunctionNtile(factory);
    registerWindowFunctionNthValue(factory);
    registerWindowFunctionsLagLead(factory);
    registerWindowFunctionsExponentialTimeDecayed(factory);
    registerWindowFunctionNonNegativeDerivative(factory);
}

}
