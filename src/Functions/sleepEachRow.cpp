#include <Functions/sleep.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

REGISTER_FUNCTION(SleepEachRow)
{
    factory.registerFunction<FunctionSleep<FunctionSleepVariant::PerRow>>();
}

}
