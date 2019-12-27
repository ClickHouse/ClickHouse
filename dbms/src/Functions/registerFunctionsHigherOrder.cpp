#include "registerFunctions.h"
#include "array/registerFunctionsArray.h"

namespace DB
{
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
