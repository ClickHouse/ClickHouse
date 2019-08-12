#include <Functions/array/registerFunctionsArray.h>
#include <Functions/registerFunctions.h>

namespace DB
{
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
