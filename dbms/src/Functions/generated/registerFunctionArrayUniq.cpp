#include "registerFunctionArrayUniq.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionArrayUniq(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayUniq>();
}

}
