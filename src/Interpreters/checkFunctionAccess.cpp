#include <Interpreters/checkFunctionAccess.h>

#include <Access/AccessControl.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

void checkFunctionAccess(const ContextPtr & context, const String & function_name)
{
    if (!AccessControl::hasFunctionsRequiringGrant() || !context)
        return;

    AccessControl::checkFunctionGrant(context, FunctionFactory::instance().getCanonicalNameIfAny(function_name));
}

}
