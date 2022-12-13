#include <Functions/FunctionFactory.h>
#include <Functions/materialize.h>


namespace DB
{

REGISTER_FUNCTION(Materialize)
{
    factory.registerFunction<FunctionMaterialize>();
}

}
