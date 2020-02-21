#include <Functions/sleep.h>



namespace DB
{

void registerFunctionSleepEachRow(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSleep<FunctionSleepVariant::PerRow>>();
}

}
