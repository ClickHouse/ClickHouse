#include "registerFunctionsArray.h"

namespace DB
{

void registerFunctionsArray(FunctionFactory & factory)
{
    registerFunctionArray(factory);
    registerFunctionArrayElement(factory);
    registerFunctionArrayResize(factory);
    registerFunctionHas(factory);
    registerFunctionHasAll(factory);
    registerFunctionHasAny(factory);
    registerFunctionIndexOf(factory);
    registerFunctionCountEqual(factory);
    registerFunctionArrayIntersect(factory);
    registerFunctionArrayPushFront(factory);
    registerFunctionArrayPushBack(factory);
    registerFunctionArrayPopFront(factory);
    registerFunctionArrayPopBack(factory);
    registerFunctionArrayConcat(factory);
    registerFunctionArraySlice(factory);
    registerFunctionArrayReverse(factory);
    registerFunctionArrayReduce(factory);
    registerFunctionRange(factory);
    registerFunctionsEmptyArray(factory);
    registerFunctionEmptyArrayToSingle(factory);
    registerFunctionArrayEnumerate(factory);
    registerFunctionArrayEnumerateUniq(factory);
    registerFunctionArrayEnumerateDense(factory);
    registerFunctionArrayEnumerateUniqRanked(factory);
    registerFunctionArrayEnumerateDenseRanked(factory);
    registerFunctionArrayUniq(factory);
    registerFunctionArrayDistinct(factory);
    registerFunctionArrayFlatten(factory);
    registerFunctionArrayWithConstant(factory);
    registerFunctionArrayZip(factory);
}

}

