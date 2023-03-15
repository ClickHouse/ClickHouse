#include <Functions/runningDifference.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

void registerFunctionRunningDifference(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRunningDifferenceImpl<true>>();
}

}
