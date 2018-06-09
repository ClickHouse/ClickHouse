#include <Functions/registerFunctionEmptyArrayString.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionEmptyArrayString(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayString>();
}

}
