namespace DB
{

class FunctionFactory;

void registerFunctionPlus(FunctionFactory & factory);
void registerFunctionMinus(FunctionFactory & factory);
void registerFunctionMultiply(FunctionFactory & factory);
void registerFunctionDivide(FunctionFactory & factory);
void registerFunctionIntDiv(FunctionFactory & factory);
void registerFunctionIntDivOrZero(FunctionFactory & factory);
void registerFunctionModulo(FunctionFactory & factory);
void registerFunctionModuloOrZero(FunctionFactory & factory);
void registerFunctionNegate(FunctionFactory & factory);
void registerFunctionAbs(FunctionFactory & factory);
void registerFunctionBitAnd(FunctionFactory & factory);
void registerFunctionBitOr(FunctionFactory & factory);
void registerFunctionBitXor(FunctionFactory & factory);
void registerFunctionBitNot(FunctionFactory & factory);
void registerFunctionBitShiftLeft(FunctionFactory & factory);
void registerFunctionBitShiftRight(FunctionFactory & factory);
void registerFunctionBitRotateLeft(FunctionFactory & factory);
void registerFunctionBitRotateRight(FunctionFactory & factory);
void registerFunctionBitCount(FunctionFactory & factory);
void registerFunctionLeast(FunctionFactory & factory);
void registerFunctionGreatest(FunctionFactory & factory);
void registerFunctionBitTest(FunctionFactory & factory);
void registerFunctionBitTestAny(FunctionFactory & factory);
void registerFunctionBitTestAll(FunctionFactory & factory);
void registerFunctionGCD(FunctionFactory & factory);
void registerFunctionLCM(FunctionFactory & factory);
void registerFunctionIntExp2(FunctionFactory & factory);
void registerFunctionIntExp10(FunctionFactory & factory);
void registerFunctionRoundToExp2(FunctionFactory & factory);
void registerFunctionRoundDuration(FunctionFactory & factory);
void registerFunctionRoundAge(FunctionFactory & factory);

void registerFunctionBitBoolMaskOr(FunctionFactory & factory);
void registerFunctionBitBoolMaskAnd(FunctionFactory & factory);
void registerFunctionBitWrapperFunc(FunctionFactory & factory);
void registerFunctionBitSwapLastTwo(FunctionFactory & factory);


void registerFunctionsArithmetic(FunctionFactory & factory)
{
    registerFunctionPlus(factory);
    registerFunctionMinus(factory);
    registerFunctionMultiply(factory);
    registerFunctionDivide(factory);
    registerFunctionIntDiv(factory);
    registerFunctionIntDivOrZero(factory);
    registerFunctionModulo(factory);
    registerFunctionModuloOrZero(factory);
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
    registerFunctionBitCount(factory);
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
