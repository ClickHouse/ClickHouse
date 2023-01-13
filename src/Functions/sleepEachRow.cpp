#include <Functions/sleep.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

void registerFunctionSleepEachRow(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSleep<FunctionSleepVariant::PerRow>>();
}

}
