#include "registerFunctionEmptyArrayString.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionEmptyArrayString(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayString>();
}

}
