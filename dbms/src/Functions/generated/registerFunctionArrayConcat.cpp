#include "registerFunctionArrayConcat.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionArrayConcat(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayConcat>();
}

}
