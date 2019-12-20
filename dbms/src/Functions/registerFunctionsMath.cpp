#include "registerFunctions.h"
namespace DB
{
void registerFunctionsMath(FunctionFactory & factory)
{
    registerFunctionE(factory);
    registerFunctionPi(factory);
    registerFunctionExp(factory);
    registerFunctionLog(factory);
    registerFunctionExp2(factory);
    registerFunctionLog2(factory);
    registerFunctionExp10(factory);
    registerFunctionLog10(factory);
    registerFunctionSqrt(factory);
    registerFunctionCbrt(factory);
    registerFunctionErf(factory);
    registerFunctionErfc(factory);
    registerFunctionLGamma(factory);
    registerFunctionTGamma(factory);
    registerFunctionSin(factory);
    registerFunctionCos(factory);
    registerFunctionTan(factory);
    registerFunctionAsin(factory);
    registerFunctionAcos(factory);
    registerFunctionAtan(factory);
    registerFunctionSigmoid(factory);
    registerFunctionTanh(factory);
    registerFunctionPow(factory);
}

}
