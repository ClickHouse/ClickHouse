namespace DB
{

class FunctionFactory;

void registerFunctionArrayMap(FunctionFactory &);
void registerFunctionArrayFilter(FunctionFactory &);
void registerFunctionArrayCount(FunctionFactory &);
void registerFunctionArrayExists(FunctionFactory &);
void registerFunctionArrayAll(FunctionFactory &);
void registerFunctionArrayCompact(FunctionFactory &);
void registerFunctionArraySum(FunctionFactory &);
void registerFunctionArrayFirst(FunctionFactory &);
void registerFunctionArrayFirstIndex(FunctionFactory &);
void registerFunctionsArrayFill(FunctionFactory &);
void registerFunctionsArraySplit(FunctionFactory &);
void registerFunctionsArraySort(FunctionFactory &);
void registerFunctionArrayCumSum(FunctionFactory &);
void registerFunctionArrayCumSumNonNegative(FunctionFactory &);
void registerFunctionArrayDifference(FunctionFactory &);

void registerFunctionsHigherOrder(FunctionFactory & factory)
{
    registerFunctionArrayMap(factory);
    registerFunctionArrayFilter(factory);
    registerFunctionArrayCount(factory);
    registerFunctionArrayExists(factory);
    registerFunctionArrayAll(factory);
    registerFunctionArrayCompact(factory);
    registerFunctionArraySum(factory);
    registerFunctionArrayFirst(factory);
    registerFunctionArrayFirstIndex(factory);
    registerFunctionsArrayFill(factory);
    registerFunctionsArraySplit(factory);
    registerFunctionsArraySort(factory);
    registerFunctionArrayCumSum(factory);
    registerFunctionArrayCumSumNonNegative(factory);
    registerFunctionArrayDifference(factory);
}

}
