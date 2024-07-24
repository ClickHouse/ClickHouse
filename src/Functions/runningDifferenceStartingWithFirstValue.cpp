#include <Functions/runningDifference.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

REGISTER_FUNCTION(RunningDifferenceStartingWithFirstValue)
{
    factory.registerFunction<FunctionRunningDifferenceImpl<false>>({}, {.is_deterministic = false, .is_deterministic_in_scope_of_query = false, .is_stateful = true});
}

}
