#include <Functions/FunctionFactory.h>

// TODO include this last because of a broken roaring header. See the comment inside.
#include <Functions/FunctionsBitmap.h>


namespace DB
{

void registerFunctionsBitmap(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitmapBuild>();
    factory.registerFunction<FunctionBitmapToArray>();
    factory.registerFunction<FunctionBitmapSubsetInRange>();
    factory.registerFunction<FunctionBitmapSubsetLimit>();
    factory.registerFunction<FunctionBitmapSubsetOffsetLimit>();
    factory.registerFunction<FunctionBitmapTransform>();

    factory.registerFunction<FunctionBitmapSelfCardinality>();
    factory.registerFunction<FunctionBitmapMin>();
    factory.registerFunction<FunctionBitmapMax>();
    factory.registerFunction<FunctionBitmapAndCardinality>();
    factory.registerFunction<FunctionBitmapOrCardinality>();
    factory.registerFunction<FunctionBitmapXorCardinality>();
    factory.registerFunction<FunctionBitmapAndnotCardinality>();

    factory.registerFunction<FunctionBitmapAnd>();
    factory.registerFunction<FunctionBitmapOr>();
    factory.registerFunction<FunctionBitmapXor>();
    factory.registerFunction<FunctionBitmapAndnot>();

    factory.registerFunction<FunctionBitmapHasAll>();
    factory.registerFunction<FunctionBitmapHasAny>();
    factory.registerFunction<FunctionBitmapContains>();
}
}
