#include <Functions/runningDifference.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

REGISTER_FUNCTION(RunningDifference)
{
    factory.registerFunction<FunctionRunningDifferenceImpl<true>>({}, {.is_deterministic = false, .is_deterministic_in_scope_of_query = false, .is_stateful = true});
}

}
