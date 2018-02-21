#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsHigherOrder.h>

namespace DB
{

void registerFunctionsHigherOrder(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayMap>();
    factory.registerFunction<FunctionArrayFilter>();
    factory.registerFunction<FunctionArrayCount>();
    factory.registerFunction<FunctionArrayExists>();
    factory.registerFunction<FunctionArrayAll>();
    factory.registerFunction<FunctionArraySum>();
    factory.registerFunction<FunctionArrayFirst>();
    factory.registerFunction<FunctionArrayFirstIndex>();
    factory.registerFunction<FunctionArraySort>();
    factory.registerFunction<FunctionArrayReverseSort>();
    factory.registerFunction<FunctionArrayCumSum>();
}

}
