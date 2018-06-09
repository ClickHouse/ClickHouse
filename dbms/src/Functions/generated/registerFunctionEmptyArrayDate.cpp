#include <Functions/registerFunctionEmptyArrayDate.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionEmptyArrayDate(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayDate>();
}

}
