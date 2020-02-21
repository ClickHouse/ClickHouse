#include <Functions/sleep.h>



namespace DB
{

void registerFunctionSleep(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSleep<FunctionSleepVariant::PerBlock>>();
}

}
