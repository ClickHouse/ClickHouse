namespace DB
{
class FunctionFactory;

void registerFunctionArray(FunctionFactory & factory);
void registerFunctionArrayElement(FunctionFactory & factory);
void registerFunctionArrayResize(FunctionFactory & factory);
void registerFunctionHas(FunctionFactory & factory);
void registerFunctionHasAll(FunctionFactory & factory);
void registerFunctionHasAny(FunctionFactory & factory);
void registerFunctionIndexOf(FunctionFactory & factory);
void registerFunctionCountEqual(FunctionFactory & factory);
void registerFunctionArrayIntersect(FunctionFactory & factory);
void registerFunctionArrayPushFront(FunctionFactory & factory);
void registerFunctionArrayPushBack(FunctionFactory & factory);
void registerFunctionArrayPopFront(FunctionFactory & factory);
void registerFunctionArrayPopBack(FunctionFactory & factory);
void registerFunctionArrayConcat(FunctionFactory & factory);
void registerFunctionArraySlice(FunctionFactory & factory);
void registerFunctionArrayReverse(FunctionFactory & factory);
void registerFunctionArrayReduce(FunctionFactory & factory);
void registerFunctionRange(FunctionFactory & factory);
void registerFunctionsEmptyArray(FunctionFactory & factory);
void registerFunctionEmptyArrayToSingle(FunctionFactory & factory);
void registerFunctionArrayEnumerate(FunctionFactory & factory);
void registerFunctionArrayEnumerateUniq(FunctionFactory & factory);
void registerFunctionArrayEnumerateDense(FunctionFactory & factory);
void registerFunctionArrayEnumerateUniqRanked(FunctionFactory & factory);
void registerFunctionArrayEnumerateDenseRanked(FunctionFactory & factory);
void registerFunctionArrayUniq(FunctionFactory & factory);
void registerFunctionArrayDistinct(FunctionFactory & factory);
void registerFunctionArrayFlatten(FunctionFactory & factory);
void registerFunctionArrayWithConstant(FunctionFactory & factory);
void registerFunctionArrayZip(FunctionFactory & factory);
void registerFunctionArrayAUC(FunctionFactory &);

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
    registerFunctionArrayAUC(factory);
}

}
