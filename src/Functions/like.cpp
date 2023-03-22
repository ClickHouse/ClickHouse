#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "like.h"


namespace DB
{

void registerFunctionLike(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLike>();
}

}
