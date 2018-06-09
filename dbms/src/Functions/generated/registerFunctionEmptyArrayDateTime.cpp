#include <Functions/registerFunctionEmptyArrayDateTime.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionEmptyArrayDateTime(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayDateTime>();
}

}
