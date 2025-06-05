#include <Functions/sleep.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

REGISTER_FUNCTION(Sleep)
{
    factory.registerFunction<FunctionSleep<FunctionSleepVariant::PerBlock>>();
}

}
