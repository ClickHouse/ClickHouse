#include "FunctionHashID.h"


#include <Functions/FunctionFactory.h>

#if USE_HASHIDSXX

namespace DB
{

void registerFunctionHashID(FunctionFactory & factory)
{
    factory.registerFunction<FunctionHashID>();
}

}

#endif
