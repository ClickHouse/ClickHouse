#include <Functions/sleep.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

void registerFunctionSleep(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSleep<FunctionSleepVariant::PerBlock>>();
}

}
