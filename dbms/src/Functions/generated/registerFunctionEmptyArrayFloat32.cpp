#include "registerFunctionEmptyArrayFloat32.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionEmptyArrayFloat32(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayFloat32>();
}

}
