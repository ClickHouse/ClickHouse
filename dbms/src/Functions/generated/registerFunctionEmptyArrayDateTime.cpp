#include "registerFunctionEmptyArrayDateTime.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionEmptyArrayDateTime(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayDateTime>();
}

}
