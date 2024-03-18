#include <Functions/identity.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(Identity)
{
    factory.registerFunction<FunctionIdentity>();
}

REGISTER_FUNCTION(ScalarSubqueryResult)
{
    factory.registerFunction<FunctionScalarSubqueryResult>();
}

}
