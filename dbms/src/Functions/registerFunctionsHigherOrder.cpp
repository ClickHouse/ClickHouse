#include <Functions/FunctionFactory.h>

namespace DB
{

void registerFunctionArrayMap(FunctionFactory &);
void registerFunctionArrayFilter(FunctionFactory &);
void registerFunctionArrayCount(FunctionFactory &);
void registerFunctionArrayExists(FunctionFactory &);
void registerFunctionArrayAll(FunctionFactory &);
void registerFunctionArraySum(FunctionFactory &);
void registerFunctionArrayFirst(FunctionFactory &);
void registerFunctionArrayFirstIndex(FunctionFactory &);
void registerFunctionsArraySort(FunctionFactory &);
void registerFunctionArrayReverseSort(FunctionFactory &);
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
    registerFunctionArraySum(factory);
    registerFunctionArrayFirst(factory);
    registerFunctionArrayFirstIndex(factory);
    registerFunctionsArraySort(factory);
    registerFunctionArrayCumSum(factory);
    registerFunctionArrayCumSumNonNegative(factory);
    registerFunctionArrayDifference(factory);
}

}
