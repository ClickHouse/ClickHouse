#include <Functions/FunctionsWindow.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
void registerFunctionsWindow(FunctionFactory& factory)
{
    factory.registerFunction<FunctionTumble>();
    factory.registerFunction<FunctionHop>();
    factory.registerFunction<FunctionTumbleStart>();
    factory.registerFunction<FunctionTumbleEnd>();
    factory.registerFunction<FunctionHopStart>();
    factory.registerFunction<FunctionHopEnd>();
}
}