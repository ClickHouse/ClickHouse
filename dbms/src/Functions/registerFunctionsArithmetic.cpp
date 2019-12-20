#include "registerFunctions.h"
namespace DB
{
void registerFunctionsArithmetic(FunctionFactory & factory)
{
    registerFunctionPlus(factory);
    registerFunctionMinus(factory);
    registerFunctionMultiply(factory);
    registerFunctionDivide(factory);
    registerFunctionIntDiv(factory);
    registerFunctionIntDivOrZero(factory);
    registerFunctionModulo(factory);
    registerFunctionNegate(factory);
    registerFunctionAbs(factory);
    registerFunctionBitAnd(factory);
    registerFunctionBitOr(factory);
    registerFunctionBitXor(factory);
    registerFunctionBitNot(factory);
    registerFunctionBitShiftLeft(factory);
    registerFunctionBitShiftRight(factory);
    registerFunctionBitRotateLeft(factory);
    registerFunctionBitRotateRight(factory);
    registerFunctionLeast(factory);
    registerFunctionGreatest(factory);
    registerFunctionBitTest(factory);
    registerFunctionBitTestAny(factory);
    registerFunctionBitTestAll(factory);
    registerFunctionGCD(factory);
    registerFunctionLCM(factory);
    registerFunctionIntExp2(factory);
    registerFunctionIntExp10(factory);
    registerFunctionRoundToExp2(factory);
    registerFunctionRoundDuration(factory);
    registerFunctionRoundAge(factory);

    /// Not for external use.
    registerFunctionBitBoolMaskOr(factory);
    registerFunctionBitBoolMaskAnd(factory);
    registerFunctionBitWrapperFunc(factory);
    registerFunctionBitSwapLastTwo(factory);
}

}
