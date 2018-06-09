#include <Functions/registerFunctionArrayUniq.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionArrayUniq(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayUniq>();
}

}
