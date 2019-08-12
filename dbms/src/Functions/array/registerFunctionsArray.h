#pragma once

namespace DB
{
class FunctionFactory;
void registerFunctionsArray(FunctionFactory & factory);

class FunctionFactory;

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
void registerFunctionArrayJoin(FunctionFactory &);
void registerFunctionArray(FunctionFactory &);
void registerFunctionArrayElement(FunctionFactory &);
void registerFunctionArrayResize(FunctionFactory &);
void registerFunctionHas(FunctionFactory &);
void registerFunctionHasAll(FunctionFactory &);
void registerFunctionHasAny(FunctionFactory &);
void registerFunctionIndexOf(FunctionFactory &);
void registerFunctionCountEqual(FunctionFactory &);
void registerFunctionArrayIntersect(FunctionFactory &);
void registerFunctionArrayPushFront(FunctionFactory &);
void registerFunctionArrayPushBack(FunctionFactory &);
void registerFunctionArrayPopFront(FunctionFactory &);
void registerFunctionArrayPopBack(FunctionFactory &);
void registerFunctionArrayConcat(FunctionFactory &);
void registerFunctionArraySlice(FunctionFactory &);
void registerFunctionArrayReverse(FunctionFactory &);
void registerFunctionArrayReduce(FunctionFactory &);
void registerFunctionRange(FunctionFactory &);
void registerFunctionsEmptyArray(FunctionFactory &);
void registerFunctionEmptyArrayToSingle(FunctionFactory &);
void registerFunctionArrayEnumerate(FunctionFactory &);
void registerFunctionArrayEnumerateUniq(FunctionFactory &);
void registerFunctionArrayEnumerateDense(FunctionFactory &);
void registerFunctionArrayEnumerateUniqRanked(FunctionFactory &);
void registerFunctionArrayEnumerateDenseRanked(FunctionFactory &);
void registerFunctionArrayUniq(FunctionFactory &);
void registerFunctionArrayDistinct(FunctionFactory &);
void registerFunctionArrayFlatten(FunctionFactory &);
void registerFunctionArrayWithConstant(FunctionFactory &);
}
