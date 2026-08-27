#include <Functions/runningDifference.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

REGISTER_FUNCTION(RunningDifferenceStartingWithFirstValue)
{
    factory.registerFunction<FunctionRunningDifferenceImpl<false>>();
}

}
