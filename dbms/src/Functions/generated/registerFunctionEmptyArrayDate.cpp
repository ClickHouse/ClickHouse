#include "registerFunctionEmptyArrayDate.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionEmptyArrayDate(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayDate>();
}

}
