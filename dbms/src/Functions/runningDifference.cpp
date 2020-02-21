#include <Functions/runningDifference.h>



namespace DB
{

void registerFunctionRunningDifference(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRunningDifferenceImpl<true>>();
}

}
