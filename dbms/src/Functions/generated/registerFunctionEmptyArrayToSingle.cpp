#include <Functions/registerFunctionEmptyArrayToSingle.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionEmptyArrayToSingle(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayToSingle>();
}

}
