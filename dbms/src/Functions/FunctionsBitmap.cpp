#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsBitmap.h>


namespace DB
{

void registerFunctionsBitmap(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitmapBuild>();
    factory.registerFunction<FunctionBitmapToArray>();

    factory.registerFunction<FunctionBitmapSelfCardinality>();
    factory.registerFunction<FunctionBitmapAndCardinality>();
    factory.registerFunction<FunctionBitmapOrCardinality>();
    factory.registerFunction<FunctionBitmapXorCardinality>();
    factory.registerFunction<FunctionBitmapAndnotCardinality>();

    factory.registerFunction<FunctionBitmapAnd>();
    factory.registerFunction<FunctionBitmapOr>();
    factory.registerFunction<FunctionBitmapXor>();
    factory.registerFunction<FunctionBitmapAndnot>();

}
}
