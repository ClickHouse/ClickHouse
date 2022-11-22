namespace DB
{

class FunctionFactory;

void registerFunctionArrayMap(FunctionFactory & factory);
void registerFunctionArrayFilter(FunctionFactory & factory);
void registerFunctionArrayCount(FunctionFactory & factory);
void registerFunctionArrayExists(FunctionFactory & factory);
void registerFunctionArrayAll(FunctionFactory & factory);
void registerFunctionArrayCompact(FunctionFactory & factory);
void registerFunctionArrayAggregation(FunctionFactory & factory);
void registerFunctionArrayFirst(FunctionFactory & factory);
void registerFunctionArrayFirstIndex(FunctionFactory & factory);
void registerFunctionsArrayFill(FunctionFactory & factory);
void registerFunctionsArraySplit(FunctionFactory & factory);
void registerFunctionsArraySort(FunctionFactory & factory);
void registerFunctionArrayCumSum(FunctionFactory & factory);
void registerFunctionArrayCumSumNonNegative(FunctionFactory & factory);
void registerFunctionArrayDifference(FunctionFactory & factory);
void registerFunctionMapApply(FunctionFactory & factory);

void registerFunctionsHigherOrder(FunctionFactory & factory)
{
    registerFunctionArrayMap(factory);
    registerFunctionArrayFilter(factory);
    registerFunctionArrayCount(factory);
    registerFunctionArrayExists(factory);
    registerFunctionArrayAll(factory);
    registerFunctionArrayCompact(factory);
    registerFunctionArrayAggregation(factory);
    registerFunctionArrayFirst(factory);
    registerFunctionArrayFirstIndex(factory);
    registerFunctionsArrayFill(factory);
    registerFunctionsArraySplit(factory);
    registerFunctionsArraySort(factory);
    registerFunctionArrayCumSum(factory);
    registerFunctionArrayCumSumNonNegative(factory);
    registerFunctionArrayDifference(factory);
    registerFunctionMapApply(factory);
}

}
