#include <Functions/FunctionFactory.h>
#include <Functions/toFixedString.h>


namespace DB
{

void registerFunctionFixedString(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToFixedString>();
}

}
