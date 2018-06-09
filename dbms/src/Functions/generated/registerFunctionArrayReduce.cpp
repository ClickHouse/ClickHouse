#include "registerFunctionArrayReduce.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionArrayReduce(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayReduce>();
}

}
