#include "FunctionHashID.h"

#if USE_HASHIDSXX

#include <Functions/FunctionFactory.h>


namespace DB
{

void registerFunctionHashID(FunctionFactory & factory)
{
    factory.registerFunction<FunctionHashID>();
}
}

#endif
