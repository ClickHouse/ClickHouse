#include <Functions/FunctionFactory.h>
#include <Functions/materialize.h>


namespace DB
{

void registerFunctionMaterialize(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMaterialize>();
}

}
