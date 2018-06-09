#include <Functions/registerFunctionIndexOf.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionIndexOf(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIndexOf>();
}

}
