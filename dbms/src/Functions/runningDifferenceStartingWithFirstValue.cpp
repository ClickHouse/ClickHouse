#include <Functions/runningDifference.h>



namespace DB
{

void registerFunctionRunningDifferenceStartingWithFirstValue(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRunningDifferenceImpl<false>>();
}

}
