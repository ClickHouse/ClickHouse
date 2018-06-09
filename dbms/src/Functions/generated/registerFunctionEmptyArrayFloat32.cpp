#include <Functions/registerFunctionEmptyArrayFloat32.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionEmptyArrayFloat32(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayFloat32>();
}

}
