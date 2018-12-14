#include <Functions/runningDifference.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

void registerFunctionRunningDifferenceStartingWithFirstValue(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRunningDifferenceImpl<false>>();
}

}
