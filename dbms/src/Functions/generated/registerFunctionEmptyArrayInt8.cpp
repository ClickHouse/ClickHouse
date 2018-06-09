#include <Functions/registerFunctionEmptyArrayInt8.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionEmptyArrayInt8(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayInt8>();
}

}
