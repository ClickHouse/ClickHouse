#include <Functions/runningDifference.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

REGISTER_FUNCTION(RunningDifference)
{
    factory.registerFunction<FunctionRunningDifferenceImpl<true>>();
}

}
